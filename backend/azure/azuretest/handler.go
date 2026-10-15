package azuretest

import (
	"bytes"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"hash/crc64"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	serviceVersion           = "2026-12-06"
	maximumListResults       = 5000
	maximumBlockIDBytes      = 64
	maximumCommittedBlocks   = 50_000
	maximumUncommittedBlocks = 100_000
	metadataHeaderPrefix     = "x-ms-meta-"
	defaultContentType       = "application/octet-stream"
)

// crc64Table is the polynomial Azure uses for x-ms-content-crc64.
var crc64Table = crc64.MakeTable(0x9A6C9329AC4BC9B5)

type block struct {
	id   string // base64 block ID as sent
	data []byte
}

type copyState struct {
	id        string
	source    string
	completed time.Time
	size      int64
}

// blobRecord is one committed blob version; records are replaced, never
// mutated, so readers may use them after releasing the lock.
type blobRecord struct {
	data        []byte
	etag        string
	modified    time.Time
	created     time.Time
	contentType string
	contentMD5  []byte
	metadata    map[string]string
	blocks      []block
	copy        *copyState
}

type containerRecord struct {
	blobs       map[string]*blobRecord
	uncommitted map[string][]block
	created     time.Time
	etag        string
}

// service is the fake Blob service state and HTTP handler.
type service struct {
	mu         sync.Mutex
	account    string
	key        []byte
	clock      func() time.Time
	containers map[string]*containerRecord
	sequence   uint64
}

func newService(account, key string, clock func() time.Time) (*service, error) {
	decoded, err := base64.StdEncoding.DecodeString(key)
	if err != nil {
		return nil, fmt.Errorf("azuretest: decode account key: %w", err)
	}
	if clock == nil {
		clock = time.Now
	}
	return &service{account: account, key: decoded, clock: clock, containers: map[string]*containerRecord{}}, nil
}

func (svc *service) now() time.Time { return svc.clock().UTC() }

// nextETag returns a fresh quoted entity tag; callers hold mu.
func (svc *service) nextETag() string {
	svc.sequence++
	return fmt.Sprintf("\"0x8D%014X\"", svc.sequence)
}

// apiError is an Azure error response.
type apiError struct {
	status  int
	code    string
	message string
	header  http.Header
}

func failure(status int, code, message string) *apiError {
	return &apiError{status: status, code: code, message: message}
}

var (
	errContainerNotFound = failure(http.StatusNotFound, "ContainerNotFound", "The specified container does not exist.")
	errBlobNotFound      = failure(http.StatusNotFound, "BlobNotFound", "The specified blob does not exist.")
	errConditionNotMet   = failure(http.StatusPreconditionFailed, "ConditionNotMet", "The condition specified using HTTP conditional header(s) is not met.")
	errBlobExists        = failure(http.StatusConflict, "BlobAlreadyExists", "The specified blob already exists.")
	errNotModified       = failure(http.StatusNotModified, "ConditionNotMet", "The condition specified using HTTP conditional header(s) is not met.")
)

func writeError(writer http.ResponseWriter, request *http.Request, err *apiError) {
	for name, values := range err.header {
		writer.Header()[name] = values
	}
	writer.Header().Set("x-ms-error-code", err.code)
	if request.Method == http.MethodHead || err.status == http.StatusNotModified {
		writer.WriteHeader(err.status)
		return
	}
	var body bytes.Buffer
	body.WriteString(xml.Header)
	_ = xml.NewEncoder(&body).Encode(struct {
		XMLName xml.Name `xml:"Error"`
		Code    string   `xml:"Code"`
		Message string   `xml:"Message"`
	}{Code: err.code, Message: err.message + "\nRequestId:" + writer.Header().Get("x-ms-request-id") + "\nTime:" + time.Now().UTC().Format(time.RFC3339Nano)})
	writer.Header().Set("Content-Type", "application/xml")
	writer.Header().Set("Content-Length", strconv.Itoa(body.Len()))
	writer.WriteHeader(err.status)
	_, _ = writer.Write(body.Bytes())
}

// ServeHTTP routes path-style requests /<account>/<container>[/<blob>].
func (svc *service) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("x-ms-request-id", newID())
	writer.Header().Set("x-ms-version", serviceVersion)
	writer.Header().Set("Date", svc.now().Format(http.TimeFormat))
	rawPath, _, _ := strings.Cut(request.RequestURI, "?")
	if strings.HasPrefix(rawPath, "http://") || strings.HasPrefix(rawPath, "https://") {
		rawPath = request.URL.EscapedPath()
	}
	segments := strings.SplitN(strings.TrimPrefix(rawPath, "/"), "/", 3)
	if len(segments) < 2 || segments[0] != svc.account || segments[1] == "" {
		writeError(writer, request, failure(http.StatusBadRequest, "InvalidUri", "The requested URI does not represent any resource on the server."))
		return
	}
	container, err := url.PathUnescape(segments[1])
	if err != nil {
		writeError(writer, request, failure(http.StatusBadRequest, "InvalidUri", "The container name is not a valid URI segment."))
		return
	}
	blob := ""
	if len(segments) == 3 {
		if blob, err = url.PathUnescape(segments[2]); err != nil {
			writeError(writer, request, failure(http.StatusBadRequest, "InvalidUri", "The blob name is not a valid URI segment."))
			return
		}
	}
	query := request.URL.Query()
	operation, apiErr := svc.route(request.Method, query, container, blob)
	if apiErr != nil {
		writeError(writer, request, apiErr)
		return
	}
	creates := false
	if operation.permission == permissionWrite && blob != "" {
		svc.mu.Lock()
		record := svc.containers[container]
		creates = record == nil || record.blobs[blob] == nil
		svc.mu.Unlock()
	}
	if failed := svc.authenticate(request, rawPath, container, blob, operation.permission, creates); failed != nil {
		writeError(writer, request, failure(failed.status, failed.code, failed.message))
		return
	}
	if apiErr := operation.handle(writer, request, container, blob); apiErr != nil {
		writeError(writer, request, apiErr)
	}
}

type operation struct {
	permission permission
	handle     func(http.ResponseWriter, *http.Request, string, string) *apiError
}

func (svc *service) route(method string, query url.Values, container, blob string) (operation, *apiError) {
	comp, restype := query.Get("comp"), query.Get("restype")
	if blob == "" {
		switch {
		case method == http.MethodPut && restype == "container" && comp == "":
			return operation{permissionWrite, svc.handleCreateContainer}, nil
		case method == http.MethodGet && restype == "container" && comp == "list":
			return operation{permissionList, svc.handleListBlobs}, nil
		}
		return operation{}, failure(http.StatusBadRequest, "UnsupportedHttpVerb", "The fake does not support this container operation.")
	}
	switch {
	case method == http.MethodPut && comp == "":
		return operation{permissionWrite, svc.handlePutOrCopy}, nil
	case method == http.MethodPut && comp == "block":
		return operation{permissionWrite, svc.handlePutBlock}, nil
	case method == http.MethodPut && comp == "blocklist":
		return operation{permissionWrite, svc.handlePutBlockList}, nil
	case method == http.MethodPut && comp == "properties":
		return operation{permissionWrite, svc.handleSetProperties}, nil
	case method == http.MethodPut && comp == "metadata":
		return operation{permissionWrite, svc.handleSetMetadata}, nil
	case method == http.MethodGet && comp == "":
		return operation{permissionRead, svc.handleGetBlob}, nil
	case method == http.MethodGet && comp == "blocklist":
		return operation{permissionRead, svc.handleGetBlockList}, nil
	case method == http.MethodHead && comp == "":
		return operation{permissionRead, svc.handleGetProperties}, nil
	case method == http.MethodDelete && comp == "":
		return operation{permissionDelete, svc.handleDelete}, nil
	}
	return operation{}, failure(http.StatusBadRequest, "UnsupportedHttpVerb", "The fake does not support this blob operation.")
}

// createContainer creates a container; it returns the HTTP status and code.
func (svc *service) createContainer(name string) (int, string) {
	if !validContainerName(name) {
		return http.StatusBadRequest, "InvalidResourceName"
	}
	svc.mu.Lock()
	defer svc.mu.Unlock()
	if svc.containers[name] != nil {
		return http.StatusConflict, "ContainerAlreadyExists"
	}
	svc.containers[name] = &containerRecord{
		blobs: map[string]*blobRecord{}, uncommitted: map[string][]block{}, created: svc.now().Truncate(time.Second), etag: svc.nextETag(),
	}
	return http.StatusCreated, ""
}

func (svc *service) handleCreateContainer(writer http.ResponseWriter, _ *http.Request, container, _ string) *apiError {
	status, code := svc.createContainer(container)
	if status != http.StatusCreated {
		message := "The specified container already exists."
		if code == "InvalidResourceName" {
			message = "The specified resource name contains invalid characters."
		}
		return failure(status, code, message)
	}
	svc.mu.Lock()
	record := svc.containers[container]
	svc.mu.Unlock()
	writer.Header().Set("ETag", record.etag)
	writer.Header().Set("Last-Modified", record.created.Format(http.TimeFormat))
	writer.WriteHeader(http.StatusCreated)
	return nil
}

func validContainerName(name string) bool {
	switch name {
	case "$root", "$web", "$logs":
		return true
	}
	if len(name) < 3 || len(name) > 63 || name[0] == '-' || name[len(name)-1] == '-' || strings.Contains(name, "--") {
		return false
	}
	for _, character := range name {
		if (character < 'a' || character > 'z') && (character < '0' || character > '9') && character != '-' {
			return false
		}
	}
	return true
}

// readBody reads the request body and verifies Content-MD5 and
// x-ms-content-crc64 when present.
func readBody(request *http.Request) ([]byte, *apiError) {
	data, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, failure(http.StatusBadRequest, "InvalidInput", "The request body could not be read: "+err.Error())
	}
	if value := request.Header.Get("Content-Md5"); value != "" {
		sum := md5.Sum(data)
		if value != base64.StdEncoding.EncodeToString(sum[:]) {
			return nil, failure(http.StatusBadRequest, "Md5Mismatch", "The MD5 value specified in the request did not match with the MD5 value calculated by the server.")
		}
	}
	if value := request.Header.Get("X-Ms-Content-Crc64"); value != "" {
		var encoded [8]byte
		binary.LittleEndian.PutUint64(encoded[:], crc64.Checksum(data, crc64Table))
		if value != base64.StdEncoding.EncodeToString(encoded[:]) {
			return nil, failure(http.StatusBadRequest, "Crc64Mismatch", "The CRC64 value specified in the request did not match with the CRC64 value calculated by the server.")
		}
	}
	return data, nil
}

// requestMetadata extracts x-ms-meta-* headers; names are lower-cased
// because HTTP header names are case-insensitive.
func requestMetadata(header http.Header) (map[string]string, bool, *apiError) {
	metadata := map[string]string{}
	present := false
	for name, values := range header {
		lower := strings.ToLower(name)
		if !strings.HasPrefix(lower, metadataHeaderPrefix) {
			continue
		}
		present = true
		key := strings.TrimPrefix(lower, metadataHeaderPrefix)
		if !validMetadataName(key) || len(values) != 1 {
			return nil, false, failure(http.StatusBadRequest, "InvalidMetadata", "The metadata specified is invalid. It has characters that are not permitted.")
		}
		metadata[key] = values[0]
	}
	return metadata, present, nil
}

func validMetadataName(name string) bool {
	if name == "" {
		return false
	}
	for index, character := range name {
		letter := (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') || character == '_'
		if !letter && (index == 0 || character < '0' || character > '9') {
			return false
		}
	}
	return true
}

// checkWrite evaluates destination conditions for a write; existing may be
// nil.
func checkWrite(header http.Header, existing *blobRecord) *apiError {
	if match := header.Get("If-Match"); match != "" {
		if existing == nil || (match != "*" && match != existing.etag) {
			return errConditionNotMet
		}
	}
	if none := header.Get("If-None-Match"); none != "" && existing != nil {
		if none == "*" {
			return errBlobExists
		}
		if none == existing.etag {
			return errConditionNotMet
		}
	}
	if existing != nil {
		if since, err := http.ParseTime(header.Get("If-Modified-Since")); err == nil && !existing.modified.After(since) {
			return errConditionNotMet
		}
		if since, err := http.ParseTime(header.Get("If-Unmodified-Since")); err == nil && existing.modified.After(since) {
			return errConditionNotMet
		}
	}
	return nil
}

// checkRead evaluates conditions for a read of an existing blob.
func checkRead(header http.Header, existing *blobRecord) *apiError {
	if match := header.Get("If-Match"); match != "" && match != "*" && match != existing.etag {
		return errConditionNotMet
	}
	if since, err := http.ParseTime(header.Get("If-Unmodified-Since")); err == nil && existing.modified.After(since) {
		return errConditionNotMet
	}
	if none := header.Get("If-None-Match"); none != "" && (none == "*" || none == existing.etag) {
		return errNotModified
	}
	if since, err := http.ParseTime(header.Get("If-Modified-Since")); err == nil && !existing.modified.After(since) {
		return errNotModified
	}
	return nil
}

// containerLocked returns the container; callers hold mu.
func (svc *service) containerLocked(name string) (*containerRecord, *apiError) {
	record := svc.containers[name]
	if record == nil {
		return nil, errContainerNotFound
	}
	return record, nil
}

func blobContentType(header http.Header) string {
	if value := header.Get("X-Ms-Blob-Content-Type"); value != "" {
		return value
	}
	return defaultContentType
}

func (svc *service) handlePutOrCopy(writer http.ResponseWriter, request *http.Request, container, blob string) *apiError {
	if request.Header.Get("X-Ms-Copy-Source") != "" {
		return svc.handleCopy(writer, request, container, blob)
	}
	if blobType := request.Header.Get("X-Ms-Blob-Type"); blobType != "BlockBlob" {
		return failure(http.StatusBadRequest, "InvalidHeaderValue", "The value for one of the HTTP headers is not in the correct format: x-ms-blob-type.")
	}
	data, apiErr := readBody(request)
	if apiErr != nil {
		return apiErr
	}
	metadata, _, apiErr := requestMetadata(request.Header)
	if apiErr != nil {
		return apiErr
	}
	contentType := request.Header.Get("X-Ms-Blob-Content-Type")
	if contentType == "" {
		contentType = request.Header.Get("Content-Type")
	}
	if contentType == "" {
		contentType = defaultContentType
	}
	sum := md5.Sum(data)
	svc.mu.Lock()
	defer svc.mu.Unlock()
	record, apiErr := svc.containerLocked(container)
	if apiErr != nil {
		return apiErr
	}
	existing := record.blobs[blob]
	if apiErr := checkWrite(request.Header, existing); apiErr != nil {
		return apiErr
	}
	stored := svc.newRecordLocked(existing, data, contentType, metadata, nil)
	stored.contentMD5 = sum[:]
	record.blobs[blob] = stored
	delete(record.uncommitted, blob)
	writer.Header().Set("Content-MD5", base64.StdEncoding.EncodeToString(sum[:]))
	writeWriteResponse(writer, stored, http.StatusCreated)
	return nil
}

// newRecordLocked builds a blob record with a fresh entity tag; callers
// hold mu.
func (svc *service) newRecordLocked(existing *blobRecord, data []byte, contentType string, metadata map[string]string, blocks []block) *blobRecord {
	now := svc.now().Truncate(time.Second)
	created := now
	if existing != nil {
		created = existing.created
	}
	return &blobRecord{
		data: data, etag: svc.nextETag(), modified: now, created: created,
		contentType: contentType, metadata: metadata, blocks: blocks,
	}
}

func writeWriteResponse(writer http.ResponseWriter, stored *blobRecord, status int) {
	writer.Header().Set("ETag", stored.etag)
	writer.Header().Set("Last-Modified", stored.modified.Format(http.TimeFormat))
	writer.Header().Set("x-ms-request-server-encrypted", "true")
	writer.WriteHeader(status)
}

func decodeBlockID(id string) (int, *apiError) {
	decoded, err := base64.StdEncoding.DecodeString(id)
	if id == "" || err != nil || len(decoded) > maximumBlockIDBytes {
		return 0, failure(http.StatusBadRequest, "InvalidQueryParameterValue", "Value for one of the query parameters specified in the request URI is invalid: blockid.")
	}
	return len(decoded), nil
}

func (svc *service) handlePutBlock(writer http.ResponseWriter, request *http.Request, container, blob string) *apiError {
	id := request.URL.Query().Get("blockid")
	length, apiErr := decodeBlockID(id)
	if apiErr != nil {
		return apiErr
	}
	data, apiErr := readBody(request)
	if apiErr != nil {
		return apiErr
	}
	svc.mu.Lock()
	defer svc.mu.Unlock()
	record, apiErr := svc.containerLocked(container)
	if apiErr != nil {
		return apiErr
	}
	blocks := record.uncommitted[blob]
	replaced := false
	for index, existing := range blocks {
		existingLength, _ := decodeBlockID(existing.id)
		if existingLength != length {
			return failure(http.StatusBadRequest, "InvalidBlobOrBlock", "The specified blob or block content is invalid: all block IDs of a blob must have the same length.")
		}
		if existing.id == id {
			blocks[index] = block{id: id, data: data}
			replaced = true
		}
	}
	if !replaced {
		if len(blocks) >= maximumUncommittedBlocks {
			return failure(http.StatusConflict, "BlockCountExceedsLimit", "The uncommitted block count cannot exceed the maximum limit of 100,000 blocks.")
		}
		record.uncommitted[blob] = append(blocks, block{id: id, data: data})
	}
	sum := md5.Sum(data)
	writer.Header().Set("Content-MD5", base64.StdEncoding.EncodeToString(sum[:]))
	if value := request.Header.Get("X-Ms-Content-Crc64"); value != "" {
		writer.Header().Set("x-ms-content-crc64", value)
	}
	writer.Header().Set("x-ms-request-server-encrypted", "true")
	writer.WriteHeader(http.StatusCreated)
	return nil
}

type blockReference struct {
	kind string
	id   string
}

// parseBlockList decodes a Put Block List body, keeping element order.
func parseBlockList(data []byte) ([]blockReference, error) {
	decoder := xml.NewDecoder(bytes.NewReader(data))
	var references []blockReference
	depth := 0
	for {
		token, err := decoder.Token()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		switch element := token.(type) {
		case xml.StartElement:
			depth++
			if depth == 1 {
				if element.Name.Local != "BlockList" {
					return nil, errors.New("root element is not BlockList")
				}
				continue
			}
			if depth != 2 {
				return nil, errors.New("unexpected nested element")
			}
			switch element.Name.Local {
			case "Committed", "Uncommitted", "Latest":
			default:
				return nil, fmt.Errorf("unexpected element %s", element.Name.Local)
			}
			var id string
			if err := decoder.DecodeElement(&id, &element); err != nil {
				return nil, err
			}
			depth--
			references = append(references, blockReference{kind: element.Name.Local, id: strings.TrimSpace(id)})
		case xml.EndElement:
			depth--
		}
	}
	return references, nil
}

func (svc *service) handlePutBlockList(writer http.ResponseWriter, request *http.Request, container, blob string) *apiError {
	data, apiErr := readBody(request)
	if apiErr != nil {
		return apiErr
	}
	references, err := parseBlockList(data)
	if err != nil {
		return failure(http.StatusBadRequest, "InvalidXmlDocument", "XML specified is not syntactically valid: "+err.Error())
	}
	if len(references) > maximumCommittedBlocks {
		return failure(http.StatusBadRequest, "BlockListTooLong", "The block list may not contain more than 50,000 blocks.")
	}
	metadata, _, apiErr := requestMetadata(request.Header)
	if apiErr != nil {
		return apiErr
	}
	svc.mu.Lock()
	defer svc.mu.Unlock()
	record, apiErr := svc.containerLocked(container)
	if apiErr != nil {
		return apiErr
	}
	existing := record.blobs[blob]
	if apiErr := checkWrite(request.Header, existing); apiErr != nil {
		return apiErr
	}
	find := func(blocks []block, id string) (block, bool) {
		for _, candidate := range blocks {
			if candidate.id == id {
				return candidate, true
			}
		}
		return block{}, false
	}
	var committed []block
	if existing != nil {
		committed = existing.blocks
	}
	uncommitted := record.uncommitted[blob]
	blocks := make([]block, 0, len(references))
	var content bytes.Buffer
	for _, reference := range references {
		var found block
		ok := false
		switch reference.kind {
		case "Committed":
			found, ok = find(committed, reference.id)
		case "Uncommitted":
			found, ok = find(uncommitted, reference.id)
		default:
			if found, ok = find(uncommitted, reference.id); !ok {
				found, ok = find(committed, reference.id)
			}
		}
		if !ok {
			return failure(http.StatusBadRequest, "InvalidBlockList", "The specified block list is invalid.")
		}
		blocks = append(blocks, found)
		content.Write(found.data)
	}
	stored := svc.newRecordLocked(existing, content.Bytes(), blobContentType(request.Header), metadata, blocks)
	record.blobs[blob] = stored
	delete(record.uncommitted, blob)
	writeWriteResponse(writer, stored, http.StatusCreated)
	return nil
}

type blockListXML struct {
	XMLName     xml.Name     `xml:"BlockList"`
	Committed   []blockEntry `xml:"CommittedBlocks>Block"`
	Uncommitted []blockEntry `xml:"UncommittedBlocks>Block"`
}

type blockEntry struct {
	Name string `xml:"Name"`
	Size int64  `xml:"Size"`
}

func (svc *service) handleGetBlockList(writer http.ResponseWriter, request *http.Request, container, blob string) *apiError {
	listType := strings.ToLower(request.URL.Query().Get("blocklisttype"))
	if listType == "" {
		listType = "committed"
	}
	if listType != "committed" && listType != "uncommitted" && listType != "all" {
		return failure(http.StatusBadRequest, "InvalidQueryParameterValue", "Value for one of the query parameters specified in the request URI is invalid: blocklisttype.")
	}
	svc.mu.Lock()
	record, apiErr := svc.containerLocked(container)
	if apiErr != nil {
		svc.mu.Unlock()
		return apiErr
	}
	existing := record.blobs[blob]
	uncommitted := record.uncommitted[blob]
	svc.mu.Unlock()
	if existing == nil && len(uncommitted) == 0 {
		return errBlobNotFound
	}
	if existing != nil {
		if apiErr := checkRead(request.Header, existing); apiErr != nil {
			return apiErr
		}
	}
	list := blockListXML{Committed: []blockEntry{}, Uncommitted: []blockEntry{}}
	if existing != nil && listType != "uncommitted" {
		for _, entry := range existing.blocks {
			list.Committed = append(list.Committed, blockEntry{Name: entry.id, Size: int64(len(entry.data))})
		}
	}
	if listType != "committed" {
		for _, entry := range uncommitted {
			list.Uncommitted = append(list.Uncommitted, blockEntry{Name: entry.id, Size: int64(len(entry.data))})
		}
	}
	if existing != nil {
		writer.Header().Set("ETag", existing.etag)
		writer.Header().Set("Last-Modified", existing.modified.Format(http.TimeFormat))
		writer.Header().Set("x-ms-blob-content-length", strconv.Itoa(len(existing.data)))
	}
	writeXML(writer, http.StatusOK, list)
	return nil
}

func writeXML(writer http.ResponseWriter, status int, value any) {
	var body bytes.Buffer
	body.WriteString(xml.Header)
	_ = xml.NewEncoder(&body).Encode(value)
	writer.Header().Set("Content-Type", "application/xml")
	writer.Header().Set("Content-Length", strconv.Itoa(body.Len()))
	writer.WriteHeader(status)
	_, _ = writer.Write(body.Bytes())
}

// lookup returns an existing blob or the matching not-found error.
func (svc *service) lookup(container, blob string) (*blobRecord, *apiError) {
	svc.mu.Lock()
	defer svc.mu.Unlock()
	record, apiErr := svc.containerLocked(container)
	if apiErr != nil {
		return nil, apiErr
	}
	existing := record.blobs[blob]
	if existing == nil {
		return nil, errBlobNotFound
	}
	return existing, nil
}

func writeProperties(header http.Header, stored *blobRecord) {
	header.Set("ETag", stored.etag)
	header.Set("Last-Modified", stored.modified.Format(http.TimeFormat))
	header.Set("x-ms-creation-time", stored.created.Format(http.TimeFormat))
	header.Set("Content-Type", stored.contentType)
	header.Set("x-ms-blob-type", "BlockBlob")
	header.Set("Accept-Ranges", "bytes")
	header.Set("x-ms-server-encrypted", "true")
	header.Set("x-ms-lease-state", "available")
	header.Set("x-ms-lease-status", "unlocked")
	if stored.copy != nil {
		header.Set("x-ms-copy-id", stored.copy.id)
		header.Set("x-ms-copy-source", stored.copy.source)
		header.Set("x-ms-copy-status", "success")
		header.Set("x-ms-copy-progress", fmt.Sprintf("%d/%d", stored.copy.size, stored.copy.size))
		header.Set("x-ms-copy-completion-time", stored.copy.completed.Format(http.TimeFormat))
	}
	for name, value := range stored.metadata {
		header[metadataHeaderPrefix+name] = []string{value}
	}
}

func (svc *service) handleGetProperties(writer http.ResponseWriter, request *http.Request, container, blob string) *apiError {
	stored, apiErr := svc.lookup(container, blob)
	if apiErr != nil {
		return apiErr
	}
	if apiErr := checkRead(request.Header, stored); apiErr != nil {
		return apiErr
	}
	writeProperties(writer.Header(), stored)
	if stored.contentMD5 != nil {
		writer.Header().Set("Content-MD5", base64.StdEncoding.EncodeToString(stored.contentMD5))
	}
	writer.Header().Set("Content-Length", strconv.Itoa(len(stored.data)))
	writer.WriteHeader(http.StatusOK)
	return nil
}

// parseRange parses "bytes=start-[end]"; ok is false for absent headers.
func parseRange(value string) (start, end int64, present bool, valid bool) {
	if value == "" {
		return 0, 0, false, true
	}
	spec, found := strings.CutPrefix(value, "bytes=")
	first, last, dash := strings.Cut(spec, "-")
	if !found || !dash || strings.Contains(spec, ",") {
		return 0, 0, true, false
	}
	start, err := strconv.ParseInt(first, 10, 64)
	if err != nil || start < 0 {
		return 0, 0, true, false
	}
	end = -1
	if last != "" {
		if end, err = strconv.ParseInt(last, 10, 64); err != nil || end < start {
			return 0, 0, true, false
		}
	}
	return start, end, true, true
}

func (svc *service) handleGetBlob(writer http.ResponseWriter, request *http.Request, container, blob string) *apiError {
	stored, apiErr := svc.lookup(container, blob)
	if apiErr != nil {
		return apiErr
	}
	if apiErr := checkRead(request.Header, stored); apiErr != nil {
		return apiErr
	}
	value := request.Header.Get("X-Ms-Range")
	if value == "" {
		value = request.Header.Get("Range")
	}
	start, end, ranged, valid := parseRange(value)
	if !valid {
		return failure(http.StatusBadRequest, "InvalidHeaderValue", "The value for one of the HTTP headers is not in the correct format: range.")
	}
	size := int64(len(stored.data))
	if ranged && start >= size {
		apiErr := failure(http.StatusRequestedRangeNotSatisfiable, "InvalidRange", "The range specified is invalid for the current size of the resource.")
		apiErr.header = http.Header{"Content-Range": {fmt.Sprintf("bytes */%d", size)}}
		return apiErr
	}
	writeProperties(writer.Header(), stored)
	if !ranged {
		if stored.contentMD5 != nil {
			writer.Header().Set("Content-MD5", base64.StdEncoding.EncodeToString(stored.contentMD5))
		}
		writer.Header().Set("Content-Length", strconv.FormatInt(size, 10))
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write(stored.data)
		return nil
	}
	if end < 0 || end >= size {
		end = size - 1
	}
	writer.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, size))
	writer.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
	writer.WriteHeader(http.StatusPartialContent)
	_, _ = writer.Write(stored.data[start : end+1])
	return nil
}

func (svc *service) handleSetProperties(writer http.ResponseWriter, request *http.Request, container, blob string) *apiError {
	return svc.update(writer, request, container, blob, func(updated *blobRecord) *apiError {
		updated.contentType = blobContentType(request.Header)
		if value := request.Header.Get("X-Ms-Blob-Content-Md5"); value != "" {
			decoded, err := base64.StdEncoding.DecodeString(value)
			if err != nil {
				return failure(http.StatusBadRequest, "InvalidHeaderValue", "The value for one of the HTTP headers is not in the correct format: x-ms-blob-content-md5.")
			}
			updated.contentMD5 = decoded
		}
		return nil
	})
}

func (svc *service) handleSetMetadata(writer http.ResponseWriter, request *http.Request, container, blob string) *apiError {
	metadata, _, apiErr := requestMetadata(request.Header)
	if apiErr != nil {
		return apiErr
	}
	return svc.update(writer, request, container, blob, func(updated *blobRecord) *apiError {
		updated.metadata = metadata
		return nil
	})
}

// update applies a property change to an existing blob under its write
// conditions and assigns a fresh entity tag.
func (svc *service) update(writer http.ResponseWriter, request *http.Request, container, blob string, change func(*blobRecord) *apiError) *apiError {
	svc.mu.Lock()
	defer svc.mu.Unlock()
	record, apiErr := svc.containerLocked(container)
	if apiErr != nil {
		return apiErr
	}
	existing := record.blobs[blob]
	if existing == nil {
		return errBlobNotFound
	}
	if apiErr := checkWrite(request.Header, existing); apiErr != nil {
		return apiErr
	}
	updated := *existing
	if apiErr := change(&updated); apiErr != nil {
		return apiErr
	}
	updated.etag = svc.nextETag()
	updated.modified = svc.now().Truncate(time.Second)
	record.blobs[blob] = &updated
	writeWriteResponse(writer, &updated, http.StatusOK)
	return nil
}

func (svc *service) handleDelete(writer http.ResponseWriter, request *http.Request, container, blob string) *apiError {
	svc.mu.Lock()
	defer svc.mu.Unlock()
	record, apiErr := svc.containerLocked(container)
	if apiErr != nil {
		return apiErr
	}
	existing := record.blobs[blob]
	if existing == nil {
		return errBlobNotFound
	}
	if apiErr := checkWrite(request.Header, existing); apiErr != nil {
		return apiErr
	}
	delete(record.blobs, blob)
	delete(record.uncommitted, blob)
	writer.Header().Set("x-ms-delete-type-permanent", "true")
	writer.WriteHeader(http.StatusAccepted)
	return nil
}

// copySourcePath resolves an x-ms-copy-source URL in this account.
func (svc *service) copySourcePath(source string) (string, string, *apiError) {
	parsed, err := url.Parse(source)
	invalid := failure(http.StatusBadRequest, "InvalidHeaderValue", "The value for one of the HTTP headers is not in the correct format: x-ms-copy-source.")
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return "", "", invalid
	}
	segments := strings.SplitN(strings.TrimPrefix(parsed.EscapedPath(), "/"), "/", 3)
	if len(segments) != 3 || segments[0] != svc.account {
		return "", "", failure(http.StatusBadRequest, "CannotVerifyCopySource", "The fake only copies blobs within its own account.")
	}
	container, containerErr := url.PathUnescape(segments[1])
	blob, blobErr := url.PathUnescape(segments[2])
	if containerErr != nil || blobErr != nil || blob == "" {
		return "", "", invalid
	}
	return container, blob, nil
}

func (svc *service) handleCopy(writer http.ResponseWriter, request *http.Request, container, blob string) *apiError {
	source := request.Header.Get("X-Ms-Copy-Source")
	sourceContainer, sourceBlob, apiErr := svc.copySourcePath(source)
	if apiErr != nil {
		return apiErr
	}
	metadata, replaceMetadata, apiErr := requestMetadata(request.Header)
	if apiErr != nil {
		return apiErr
	}
	svc.mu.Lock()
	defer svc.mu.Unlock()
	sourceRecord := svc.containers[sourceContainer]
	if sourceRecord == nil || sourceRecord.blobs[sourceBlob] == nil {
		return failure(http.StatusNotFound, "CannotVerifyCopySource", "The specified blob does not exist.")
	}
	original := sourceRecord.blobs[sourceBlob]
	sourceFailed := failure(http.StatusPreconditionFailed, "SourceConditionNotMet", "The source condition specified using HTTP conditional header(s) is not met.")
	if match := request.Header.Get("X-Ms-Source-If-Match"); match != "" && match != "*" && match != original.etag {
		return sourceFailed
	}
	if none := request.Header.Get("X-Ms-Source-If-None-Match"); none != "" && (none == "*" || none == original.etag) {
		return sourceFailed
	}
	record, apiErr := svc.containerLocked(container)
	if apiErr != nil {
		return apiErr
	}
	existing := record.blobs[blob]
	if apiErr := checkWrite(request.Header, existing); apiErr != nil {
		if apiErr == errConditionNotMet {
			return failure(http.StatusPreconditionFailed, "TargetConditionNotMet", "The target condition specified using HTTP conditional header(s) is not met.")
		}
		return apiErr
	}
	if !replaceMetadata {
		metadata = make(map[string]string, len(original.metadata))
		for name, value := range original.metadata {
			metadata[name] = value
		}
	}
	stored := svc.newRecordLocked(existing, original.data, original.contentType, metadata, original.blocks)
	stored.contentMD5 = original.contentMD5
	stored.copy = &copyState{id: newID(), source: source, completed: stored.modified, size: int64(len(original.data))}
	record.blobs[blob] = stored
	delete(record.uncommitted, blob)
	writer.Header().Set("x-ms-copy-id", stored.copy.id)
	writer.Header().Set("x-ms-copy-status", "success")
	writeWriteResponse(writer, stored, http.StatusAccepted)
	return nil
}

type listResultsXML struct {
	XMLName         xml.Name      `xml:"EnumerationResults"`
	ServiceEndpoint string        `xml:"ServiceEndpoint,attr"`
	ContainerName   string        `xml:"ContainerName,attr"`
	Prefix          string        `xml:"Prefix,omitempty"`
	Marker          string        `xml:"Marker,omitempty"`
	MaxResults      int           `xml:"MaxResults,omitempty"`
	Blobs           []listBlobXML `xml:"Blobs>Blob"`
	NextMarker      string        `xml:"NextMarker"`
}

type listNameXML struct {
	Encoded bool   `xml:"Encoded,attr,omitempty"`
	Value   string `xml:",chardata"`
}

type listBlobXML struct {
	Name       listNameXML       `xml:"Name"`
	Properties listPropertiesXML `xml:"Properties"`
	Metadata   *metadataXML      `xml:"Metadata,omitempty"`
}

type listPropertiesXML struct {
	CreationTime  string `xml:"Creation-Time"`
	LastModified  string `xml:"Last-Modified"`
	ETag          string `xml:"Etag"`
	ContentLength int64  `xml:"Content-Length"`
	ContentType   string `xml:"Content-Type"`
	ContentMD5    string `xml:"Content-MD5,omitempty"`
	BlobType      string `xml:"BlobType"`
	LeaseStatus   string `xml:"LeaseStatus"`
	LeaseState    string `xml:"LeaseState"`
	Encrypted     bool   `xml:"ServerEncrypted"`
}

type metadataXML map[string]string

// MarshalXML writes metadata as one element per name, sorted by name.
func (metadata metadataXML) MarshalXML(encoder *xml.Encoder, start xml.StartElement) error {
	if err := encoder.EncodeToken(start); err != nil {
		return err
	}
	names := make([]string, 0, len(metadata))
	for name := range metadata {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := encoder.EncodeElement(metadata[name], xml.StartElement{Name: xml.Name{Local: name}}); err != nil {
			return err
		}
	}
	return encoder.EncodeToken(start.End())
}

// encodeListName percent-encodes names that XML 1.0 cannot carry, as Azure
// does with Encoded="true".
func encodeListName(name string) listNameXML {
	for _, character := range name {
		if character < 0x20 || character == 0xFFFE || character == 0xFFFF {
			return listNameXML{Encoded: true, Value: url.QueryEscape(name)}
		}
	}
	return listNameXML{Value: name}
}

func (svc *service) handleListBlobs(writer http.ResponseWriter, request *http.Request, container, _ string) *apiError {
	query := request.URL.Query()
	if query.Get("delimiter") != "" {
		return failure(http.StatusBadRequest, "UnsupportedQueryParameter", "The fake supports flat listings only.")
	}
	maximum := maximumListResults
	if value := query.Get("maxresults"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed <= 0 {
			return failure(http.StatusBadRequest, "OutOfRangeQueryParameterValue", "One of the query parameters specified in the request URI is outside the permissible range: maxresults.")
		}
		maximum = min(parsed, maximumListResults)
	}
	start := query.Get("startFrom")
	if marker := query.Get("marker"); marker != "" {
		decoded, err := base64.RawURLEncoding.DecodeString(marker)
		if err != nil {
			return failure(http.StatusBadRequest, "OutOfRangeInput", "One of the request inputs is out of range: marker.")
		}
		start = max(start, string(decoded))
	}
	includeMetadata := false
	for _, value := range strings.Split(query.Get("include"), ",") {
		if strings.TrimSpace(value) == "metadata" {
			includeMetadata = true
		}
	}
	prefix := query.Get("prefix")
	svc.mu.Lock()
	record, apiErr := svc.containerLocked(container)
	if apiErr != nil {
		svc.mu.Unlock()
		return apiErr
	}
	names := make([]string, 0, len(record.blobs))
	for name := range record.blobs {
		if strings.HasPrefix(name, prefix) && name >= start {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	result := listResultsXML{
		ServiceEndpoint: "http://" + request.Host + "/" + svc.account + "/", ContainerName: container,
		Prefix: prefix, Marker: query.Get("marker"), Blobs: []listBlobXML{},
	}
	if query.Get("maxresults") != "" {
		result.MaxResults = maximum
	}
	if len(names) > maximum {
		result.NextMarker = base64.RawURLEncoding.EncodeToString([]byte(names[maximum]))
		names = names[:maximum]
	}
	for _, name := range names {
		stored := record.blobs[name]
		entry := listBlobXML{Name: encodeListName(name), Properties: listPropertiesXML{
			CreationTime: stored.created.Format(http.TimeFormat), LastModified: stored.modified.Format(http.TimeFormat),
			ETag: strings.Trim(stored.etag, "\""), ContentLength: int64(len(stored.data)), ContentType: stored.contentType,
			BlobType: "BlockBlob", LeaseStatus: "unlocked", LeaseState: "available", Encrypted: true,
		}}
		if stored.contentMD5 != nil {
			entry.Properties.ContentMD5 = base64.StdEncoding.EncodeToString(stored.contentMD5)
		}
		if includeMetadata {
			metadata := metadataXML(stored.metadata)
			if metadata == nil {
				metadata = metadataXML{}
			}
			entry.Metadata = &metadata
		}
		result.Blobs = append(result.Blobs, entry)
	}
	svc.mu.Unlock()
	writeXML(writer, http.StatusOK, result)
	return nil
}

func newID() string {
	var raw [16]byte
	_, _ = rand.Read(raw[:])
	encoded := hex.EncodeToString(raw[:])
	return encoded[:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:]
}
