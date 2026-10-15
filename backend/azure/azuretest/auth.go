package azuretest

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

// maximumClockSkew is the Shared Key request date window Azure enforces.
const maximumClockSkew = 15 * time.Minute

// sasTimeFormats are the ISO 8601 layouts Azure accepts for st and se.
var sasTimeFormats = []string{"2006-01-02T15:04:05.0000000Z", "2006-01-02T15:04:05Z", "2006-01-02T15:04Z", "2006-01-02"}

// permission names the SAS permission an operation requires.
type permission byte

const (
	permissionRead   permission = 'r'
	permissionWrite  permission = 'w'
	permissionDelete permission = 'd'
	permissionList   permission = 'l'
)

// authFailure describes a rejected request.
type authFailure struct {
	status  int
	code    string
	message string
}

// authenticate verifies a Shared Key Authorization header or a service SAS.
// The caller passes the permission the operation needs; for writes that
// create a blob, a SAS "c" permission is accepted in place of "w".
func (svc *service) authenticate(request *http.Request, rawPath, container, blob string, needed permission, creates bool) *authFailure {
	query := request.URL.Query()
	if query.Get("sig") != "" {
		return svc.authenticateSAS(query, container, blob, needed, creates)
	}
	header := request.Header.Get("Authorization")
	if header == "" {
		return &authFailure{http.StatusUnauthorized, "NoAuthenticationInformation", "Server failed to authenticate the request. Please refer to the information in the www-authenticate header."}
	}
	scheme, credential, found := strings.Cut(header, " ")
	account, signature, colon := strings.Cut(credential, ":")
	if !found || scheme != "SharedKey" || !colon || account != svc.account {
		return &authFailure{http.StatusForbidden, "AuthenticationFailed", "Server failed to authenticate the request. Unsupported authorization scheme or account."}
	}
	date := request.Header.Get("X-Ms-Date")
	if date == "" {
		date = request.Header.Get("Date")
	}
	sent, err := http.ParseTime(date)
	if err != nil {
		return &authFailure{http.StatusForbidden, "AuthenticationFailed", "Server failed to authenticate the request. The x-ms-date header is missing or invalid."}
	}
	if now := svc.now(); sent.Before(now.Add(-maximumClockSkew)) || sent.After(now.Add(maximumClockSkew)) {
		return &authFailure{http.StatusForbidden, "AuthenticationFailed", "Server failed to authenticate the request. Request date header too old or too new."}
	}
	expected := svc.sign(sharedKeyStringToSign(request, svc.account, rawPath))
	if !hmac.Equal([]byte(expected), []byte(signature)) {
		return &authFailure{http.StatusForbidden, "AuthenticationFailed", "Server failed to authenticate the request. Make sure the value of Authorization header is formed correctly including the signature."}
	}
	return nil
}

func (svc *service) sign(message string) string {
	mac := hmac.New(sha256.New, svc.key)
	_, _ = mac.Write([]byte(message))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

// sharedKeyStringToSign recomputes the Blob service Shared Key
// string-to-sign the way the azblob SDK builds it.
func sharedKeyStringToSign(request *http.Request, account, rawPath string) string {
	header := request.Header
	contentLength := header.Get("Content-Length")
	if contentLength == "0" {
		contentLength = ""
	}
	date := ""
	if header.Get("X-Ms-Date") == "" {
		date = header.Get("Date")
	}
	return strings.Join([]string{
		request.Method,
		header.Get("Content-Encoding"),
		header.Get("Content-Language"),
		contentLength,
		header.Get("Content-Md5"),
		header.Get("Content-Type"),
		date,
		header.Get("If-Modified-Since"),
		header.Get("If-Match"),
		header.Get("If-None-Match"),
		header.Get("If-Unmodified-Since"),
		header.Get("Range"),
		canonicalizedHeaders(header),
		canonicalizedResource(account, rawPath, request.URL.RawQuery),
	}, "\n")
}

func canonicalizedHeaders(header http.Header) string {
	values := map[string][]string{}
	for name, value := range header {
		lower := strings.ToLower(strings.TrimSpace(name))
		if strings.HasPrefix(lower, "x-ms-") {
			values[lower] = append(values[lower], value...)
		}
	}
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Slice(names, func(left, right int) bool { return compareHeaderNames(names[left], names[right]) < 0 })
	lines := make([]string, len(names))
	for index, name := range names {
		lines[index] = name + ":" + strings.Join(values[name], ",")
	}
	return strings.Join(lines, "\n")
}

func canonicalizedResource(account, rawPath, rawQuery string) string {
	var builder strings.Builder
	builder.WriteString("/" + account)
	if rawPath == "" {
		rawPath = "/"
	}
	builder.WriteString(rawPath)
	params, err := url.ParseQuery(rawQuery)
	if err != nil {
		return builder.String()
	}
	names := make([]string, 0, len(params))
	for name := range params {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		values := append([]string(nil), params[name]...)
		sort.Strings(values)
		builder.WriteString("\n" + strings.ToLower(name) + ":" + strings.Join(values, ","))
	}
	return builder.String()
}

// authenticateSAS verifies a Shared Key signed service SAS (sr=b or sr=c).
func (svc *service) authenticateSAS(query url.Values, container, blob string, needed permission, creates bool) *authFailure {
	failed := func(message string) *authFailure {
		return &authFailure{http.StatusForbidden, "AuthenticationFailed", "Server failed to authenticate the request. " + message}
	}
	if query.Get("ss") != "" || query.Get("srt") != "" || query.Get("skoid") != "" {
		return failed("Only service SAS signed with the account key is supported.")
	}
	resource := query.Get("sr")
	canonical := "/blob/" + svc.account + "/" + container
	switch resource {
	case "b":
		if blob == "" {
			return failed("Blob SAS used for a container request.")
		}
		canonical += "/" + blob
	case "c":
	default:
		return failed("Unsupported signed resource.")
	}
	stringToSign := strings.Join([]string{
		query.Get("sp"), query.Get("st"), query.Get("se"), canonical, query.Get("si"), query.Get("sip"),
		query.Get("spr"), query.Get("sv"), resource, "", query.Get("ses"),
		query.Get("rscc"), query.Get("rscd"), query.Get("rsce"), query.Get("rscl"), query.Get("rsct"),
	}, "\n")
	if !hmac.Equal([]byte(svc.sign(stringToSign)), []byte(query.Get("sig"))) {
		return failed("Signature did not match.")
	}
	now := svc.now()
	expiry, ok := parseSASTime(query.Get("se"))
	if !ok {
		return failed("Signed expiry time is missing or invalid.")
	}
	if !now.Before(expiry) {
		return failed("Signed expiry time [" + query.Get("se") + "] must be after signed start time and current time.")
	}
	if start := query.Get("st"); start != "" {
		begins, ok := parseSASTime(start)
		if !ok || now.Before(begins) {
			return failed("Signed start time is invalid or in the future.")
		}
	}
	if protocol := query.Get("spr"); protocol == "https" {
		return &authFailure{http.StatusForbidden, "AuthorizationProtocolMismatch", "This request is not authorized to perform this operation using this protocol."}
	}
	permissions := query.Get("sp")
	granted := strings.IndexByte(permissions, byte(needed)) >= 0 ||
		(needed == permissionWrite && creates && strings.IndexByte(permissions, 'c') >= 0)
	if !granted || (needed == permissionList && resource != "c") {
		return &authFailure{http.StatusForbidden, "AuthorizationPermissionMismatch", "This request is not authorized to perform this operation using this permission."}
	}
	return nil
}

func parseSASTime(value string) (time.Time, bool) {
	for _, layout := range sasTimeFormats {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed, true
		}
	}
	return time.Time{}, false
}

// compareHeaderNames orders canonicalized header names with the same
// two-level collation the azblob SDK uses when signing: hyphens are ignored
// at the first level and only break ties at the second.
func compareHeaderNames(left, right string) int {
	level, i, j := 0, 0, 0
	for level < len(headerWeights) {
		if level == len(headerWeights)-1 && i != j {
			if i > j {
				return -1
			}
			return 1
		}
		leftWeight, rightWeight := 1, 1
		if i < len(left) {
			leftWeight = headerWeight(level, left[i])
		}
		if j < len(right) {
			rightWeight = headerWeight(level, right[j])
		}
		switch {
		case leftWeight == 1 && rightWeight == 1:
			i, j = 0, 0
			level++
		case leftWeight == rightWeight:
			i++
			j++
		case leftWeight == 0:
			i++
		case rightWeight == 0:
			j++
		case leftWeight < rightWeight:
			return -1
		default:
			return 1
		}
	}
	return 0
}

func headerWeight(level int, character byte) int {
	if int(character) >= len(headerWeights[level]) {
		return 0
	}
	return headerWeights[level][character]
}

// headerWeights are the collation weights of the SDK's header comparer for
// ASCII bytes; bytes outside the tables weigh zero (ignored).
var headerWeights = [2][]int{
	{
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x71c, 0x0, 0x71f, 0x721, 0x723, 0x725,
		0x0, 0x0, 0x0, 0x72d, 0x803, 0x0, 0x0, 0x733, 0x0, 0xd03, 0xd1a, 0xd1c, 0xd1e,
		0xd20, 0xd22, 0xd24, 0xd26, 0xd28, 0xd2a, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
		0xe02, 0xe09, 0xe0a, 0xe1a, 0xe21, 0xe23, 0xe25, 0xe2c, 0xe32, 0xe35, 0xe36, 0xe48, 0xe51,
		0xe70, 0xe7c, 0xe7e, 0xe89, 0xe8a, 0xe91, 0xe99, 0xe9f, 0xea2, 0xea4, 0xea6, 0xea7, 0xea9,
		0x0, 0x0, 0x0, 0x743, 0x744, 0x748, 0xe02, 0xe09, 0xe0a, 0xe1a, 0xe21, 0xe23, 0xe25,
		0xe2c, 0xe32, 0xe35, 0xe36, 0xe48, 0xe51, 0xe70, 0xe7c, 0xe7e, 0xe89, 0xe8a, 0xe91, 0xe99,
		0xe9f, 0xea2, 0xea4, 0xea6, 0xea7, 0xea9, 0x0, 0x74c, 0x0, 0x750, 0x0,
	},
	{
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x8012, 0x0, 0x0, 0x0, 0x0, 0x0, 0x8212, 0x0, 0x0,
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
	},
}
