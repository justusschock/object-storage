//go:build integration

package s3

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	objectstore "github.com/justusschock/go-object-storage"
	"github.com/justusschock/go-object-storage/internal/storetest"
)

func TestIntegrationConformance(t *testing.T) {
	endpoint, region, bucket := os.Getenv("S3_ENDPOINT"), os.Getenv("S3_REGION"), os.Getenv("S3_BUCKET")
	if endpoint == "" || region == "" || bucket == "" {
		t.Skip("S3_ENDPOINT, S3_REGION, and S3_BUCKET are required")
	}
	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		t.Skipf("AWS configuration unavailable: %v", err)
	}
	if _, err := cfg.Credentials.Retrieve(ctx); err != nil {
		t.Skip("caller-owned AWS credential provider is unavailable")
	}
	client := awss3.NewFromConfig(cfg, func(options *awss3.Options) {
		options.BaseEndpoint = &endpoint
		options.UsePathStyle = true
	})
	store, err := NewWithOptions(client, Options{PartSize: 5 << 20})
	if err != nil {
		t.Fatal(err)
	}
	var sequence atomic.Uint64
	runID := time.Now().UTC().UnixNano()
	runPrefix := fmt.Sprintf("go-object-storage-conformance/%d/", runID)
	cleanup := &integrationCleanup{}
	t.Cleanup(func() { cleanup.run(t, client, bucket, runPrefix) })
	storetest.Run(t, func(t *testing.T) objectstore.MultipartStore {
		prefix := fmt.Sprintf("%s%d/", runPrefix, sequence.Add(1))
		return &boundStore{store: store, bucket: objectstore.Bucket(bucket), prefix: prefix, cleanup: cleanup}
	})
}

// boundStore maps every logical bucket to a run-scoped prefix of one real
// bucket: objects in logical bucket b live under prefix + b + "/".
type boundStore struct {
	store   *Store
	bucket  objectstore.Bucket
	prefix  string
	cleanup *integrationCleanup
}

func (store *boundStore) Put(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, body io.Reader, options objectstore.PutOptions) (objectstore.Object, error) {
	object, err := store.store.Put(ctx, store.bucket, store.key(bucket, key), body, options)
	return store.object(bucket, object), err
}

func (store *boundStore) Get(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, options objectstore.GetOptions) (io.ReadCloser, objectstore.Object, error) {
	body, object, err := store.store.Get(ctx, store.bucket, store.key(bucket, key), options)
	return body, store.object(bucket, object), err
}

func (store *boundStore) Stat(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key) (objectstore.Object, error) {
	object, err := store.store.Stat(ctx, store.bucket, store.key(bucket, key))
	return store.object(bucket, object), err
}

func (store *boundStore) Delete(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, version string) error {
	return store.store.Delete(ctx, store.bucket, store.key(bucket, key), version)
}

func (store *boundStore) List(ctx context.Context, bucket objectstore.Bucket, options objectstore.ListOptions) (objectstore.Page, error) {
	base := store.prefix + string(bucket) + "/"
	options.Prefix = base + options.Prefix
	if options.After != "" {
		options.After = store.key(bucket, options.After)
	}
	page, err := store.store.List(ctx, store.bucket, options)
	for index := range page.Objects {
		page.Objects[index] = store.object(bucket, page.Objects[index])
	}
	if page.NextAfter != "" {
		page.NextAfter = objectstore.Key(strings.TrimPrefix(string(page.NextAfter), base))
	}
	return page, err
}

func (store *boundStore) BeginMultipart(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, options objectstore.PutOptions) (objectstore.Upload, error) {
	upload, err := store.store.BeginMultipart(ctx, store.bucket, store.key(bucket, key), options)
	if err == nil {
		store.cleanup.record(upload)
	}
	upload.Bucket, upload.Key = bucket, key
	return upload, err
}

func (store *boundStore) PutPart(ctx context.Context, upload objectstore.Upload, number int, body io.Reader, size int64, digest [32]byte) (objectstore.CompletedPart, error) {
	return store.store.PutPart(ctx, store.upload(upload), number, body, size, digest)
}

func (store *boundStore) CompleteMultipart(ctx context.Context, upload objectstore.Upload, parts []objectstore.CompletedPart) (objectstore.Object, error) {
	object, err := store.store.CompleteMultipart(ctx, store.upload(upload), parts)
	return store.object(upload.Bucket, object), err
}

func (store *boundStore) AbortMultipart(ctx context.Context, upload objectstore.Upload) error {
	return store.store.AbortMultipart(ctx, store.upload(upload))
}

func (store *boundStore) StatMany(ctx context.Context, bucket objectstore.Bucket, keys []objectstore.Key) ([]objectstore.StatResult, error) {
	mapped := make([]objectstore.Key, len(keys))
	for index, key := range keys {
		mapped[index] = store.key(bucket, key)
	}
	results, err := store.store.StatMany(ctx, store.bucket, mapped)
	for index := range results {
		results[index].Object = store.object(bucket, results[index].Object)
	}
	return results, err
}

func (store *boundStore) DeleteMany(ctx context.Context, bucket objectstore.Bucket, keys []objectstore.Key) ([]error, error) {
	mapped := make([]objectstore.Key, len(keys))
	for index, key := range keys {
		mapped[index] = store.key(bucket, key)
	}
	return store.store.DeleteMany(ctx, store.bucket, mapped)
}

func (store *boundStore) Copy(ctx context.Context, srcBucket objectstore.Bucket, srcKey objectstore.Key, dstBucket objectstore.Bucket, dstKey objectstore.Key, options objectstore.CopyOptions) (objectstore.Object, error) {
	object, err := store.store.Copy(ctx, store.bucket, store.key(srcBucket, srcKey), store.bucket, store.key(dstBucket, dstKey), options)
	return store.object(dstBucket, object), err
}

func (store *boundStore) PresignGet(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, expires time.Duration, byteRange *objectstore.ByteRange) (objectstore.PresignedRequest, error) {
	return store.store.PresignGet(ctx, store.bucket, store.key(bucket, key), expires, byteRange)
}

func (store *boundStore) PresignPut(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, expires time.Duration, options objectstore.PutOptions) (objectstore.PresignedRequest, error) {
	return store.store.PresignPut(ctx, store.bucket, store.key(bucket, key), expires, options)
}

func (store *boundStore) PresignUploadPart(ctx context.Context, upload objectstore.Upload, number int, size int64, digest [32]byte, expires time.Duration) (objectstore.PresignedRequest, error) {
	return store.store.PresignUploadPart(ctx, store.upload(upload), number, size, digest, expires)
}

func (store *boundStore) key(bucket objectstore.Bucket, key objectstore.Key) objectstore.Key {
	return objectstore.Key(store.prefix + string(bucket) + "/" + string(key))
}

func (store *boundStore) upload(upload objectstore.Upload) objectstore.Upload {
	upload.Bucket, upload.Key = store.bucket, store.key(upload.Bucket, upload.Key)
	return upload
}

func (store *boundStore) object(bucket objectstore.Bucket, object objectstore.Object) objectstore.Object {
	if object.Key == "" {
		return object
	}
	object.Bucket = bucket
	object.Key = objectstore.Key(strings.TrimPrefix(string(object.Key), store.prefix+string(bucket)+"/"))
	return object
}

type integrationCleanup struct {
	mu      sync.Mutex
	uploads []objectstore.Upload
}

func (cleanup *integrationCleanup) record(upload objectstore.Upload) {
	cleanup.mu.Lock()
	defer cleanup.mu.Unlock()
	cleanup.uploads = append(cleanup.uploads, upload)
}

func (cleanup *integrationCleanup) run(t *testing.T, client *awss3.Client, bucket string, prefix string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cleanup.mu.Lock()
	uploads := append([]objectstore.Upload(nil), cleanup.uploads...)
	cleanup.mu.Unlock()
	for _, upload := range uploads {
		custody, err := decodeMultipartCustody(ctx, upload)
		if err != nil {
			t.Errorf("decode multipart cleanup token: %v", err)
			continue
		}
		key := string(custody.Key)
		_, _ = client.AbortMultipartUpload(ctx, &awss3.AbortMultipartUploadInput{Bucket: &bucket, Key: &key, UploadId: &custody.UploadID})
	}
	var token *string
	for {
		output, err := client.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{Bucket: &bucket, Prefix: &prefix, ContinuationToken: token})
		if err != nil {
			t.Errorf("list integration cleanup prefix: %v", err)
			return
		}
		for _, object := range output.Contents {
			if object.Key == nil {
				continue
			}
			if _, err := client.DeleteObject(ctx, &awss3.DeleteObjectInput{Bucket: &bucket, Key: object.Key}); err != nil {
				t.Errorf("delete integration object: %v", err)
			}
		}
		if output.IsTruncated == nil || !*output.IsTruncated {
			return
		}
		if output.NextContinuationToken == nil || *output.NextContinuationToken == "" {
			t.Error("integration cleanup listing was truncated without a continuation token")
			return
		}
		token = output.NextContinuationToken
	}
}
