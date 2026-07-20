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
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
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
	uploader := manager.NewUploader(client)
	store, err := New(client, uploader)
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

type boundStore struct {
	store   objectstore.MultipartStore
	bucket  objectstore.Bucket
	prefix  string
	cleanup *integrationCleanup
}

func (store *boundStore) Put(ctx context.Context, _ objectstore.Bucket, key objectstore.Key, body io.Reader, options objectstore.PutOptions) (objectstore.Object, error) {
	object, err := store.store.Put(ctx, store.bucket, store.key(key), body, options)
	return store.object(object), err
}

func (store *boundStore) Get(ctx context.Context, _ objectstore.Bucket, key objectstore.Key, options objectstore.GetOptions) (io.ReadCloser, objectstore.Object, error) {
	body, object, err := store.store.Get(ctx, store.bucket, store.key(key), options)
	return body, store.object(object), err
}

func (store *boundStore) Stat(ctx context.Context, _ objectstore.Bucket, key objectstore.Key) (objectstore.Object, error) {
	object, err := store.store.Stat(ctx, store.bucket, store.key(key))
	return store.object(object), err
}

func (store *boundStore) Delete(ctx context.Context, _ objectstore.Bucket, key objectstore.Key, version string) error {
	return store.store.Delete(ctx, store.bucket, store.key(key), version)
}

func (store *boundStore) List(ctx context.Context, _ objectstore.Bucket, options objectstore.ListOptions) (objectstore.Page, error) {
	options.Prefix = store.prefix + options.Prefix
	if options.After != "" {
		options.After = store.key(options.After)
	}
	page, err := store.store.List(ctx, store.bucket, options)
	for index := range page.Objects {
		page.Objects[index] = store.object(page.Objects[index])
	}
	if page.NextAfter != "" {
		page.NextAfter = objectstore.Key(strings.TrimPrefix(string(page.NextAfter), store.prefix))
	}
	return page, err
}

func (store *boundStore) BeginMultipart(ctx context.Context, _ objectstore.Bucket, key objectstore.Key, options objectstore.PutOptions) (objectstore.Upload, error) {
	upload, err := store.store.BeginMultipart(ctx, store.bucket, store.key(key), options)
	if err == nil {
		store.cleanup.record(upload)
	}
	upload.Bucket, upload.Key = "objects", key
	return upload, err
}

func (store *boundStore) PutPart(ctx context.Context, upload objectstore.Upload, number int, body io.Reader, size int64, digest [32]byte) (objectstore.CompletedPart, error) {
	return store.store.PutPart(ctx, store.upload(upload), number, body, size, digest)
}

func (store *boundStore) CompleteMultipart(ctx context.Context, upload objectstore.Upload, parts []objectstore.CompletedPart) (objectstore.Object, error) {
	object, err := store.store.CompleteMultipart(ctx, store.upload(upload), parts)
	return store.object(object), err
}

func (store *boundStore) AbortMultipart(ctx context.Context, upload objectstore.Upload) error {
	return store.store.AbortMultipart(ctx, store.upload(upload))
}

func (store *boundStore) key(key objectstore.Key) objectstore.Key {
	return objectstore.Key(store.prefix + string(key))
}

func (store *boundStore) upload(upload objectstore.Upload) objectstore.Upload {
	upload.Bucket, upload.Key = store.bucket, store.key(upload.Key)
	return upload
}

func (store *boundStore) object(object objectstore.Object) objectstore.Object {
	if object.Key == "" {
		return object
	}
	object.Bucket = "objects"
	object.Key = objectstore.Key(strings.TrimPrefix(string(object.Key), store.prefix))
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
		_, _ = client.AbortMultipartUpload(ctx, &awss3.AbortMultipartUploadInput{
			Bucket: &bucket, Key: &custody.StagingKey, UploadId: &custody.UploadID,
		})
		_, _ = client.DeleteObject(ctx, &awss3.DeleteObjectInput{Bucket: &bucket, Key: &custody.StagingKey})
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
