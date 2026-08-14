package eos

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func newTestS3(t *testing.T, transport http.RoundTripper) *S3 {
	return newTestS3WithMaxRetries(t, transport, aws.Int(0))
}

func newTestS3WithMaxRetries(t *testing.T, transport http.RoundTripper, maxRetries *int) *S3 {
	t.Helper()
	httpClient := &http.Client{Transport: transport}
	awsClient := s3.New(session.Must(session.NewSession(&aws.Config{
		Credentials:      credentials.NewStaticCredentials("ak", "sk", ""),
		Endpoint:         aws.String("http://storage.test"),
		Region:           aws.String("us-east-1"),
		S3ForcePathStyle: aws.Bool(true),
		HTTPClient:       httpClient,
		MaxRetries:       maxRetries,
	})))
	return &S3{BucketName: "bucket", client: awsClient, cfg: &BucketConfig{S3CreateOnlySupported: true}}
}

func TestS3PutIfAbsentSendsSignedConditionalHeader(t *testing.T) {
	var calls int32
	store := newTestS3(t, roundTripFunc(func(req *http.Request) (*http.Response, error) {
		atomic.AddInt32(&calls, 1)
		if got := req.Header.Get("If-None-Match"); got != "*" {
			t.Fatalf("If-None-Match = %q, want *", got)
		}
		if auth := req.Header.Get("Authorization"); auth != "" && !strings.Contains(strings.ToLower(auth), "if-none-match") {
			t.Fatalf("Authorization signed headers do not include if-none-match: %s", auth)
		}
		return response(req, http.StatusOK, ""), nil
	}))

	err := store.Put(context.Background(), "doc", strings.NewReader("winner"), nil, PutIfAbsent())
	if err != nil {
		t.Fatalf("PutIfAbsent failed: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("request calls = %d, want 1", got)
	}
}

func TestS3PutWithoutIfAbsentKeepsOverwriteAndRetryBehavior(t *testing.T) {
	var calls int32
	store := newTestS3(t, roundTripFunc(func(req *http.Request) (*http.Response, error) {
		call := atomic.AddInt32(&calls, 1)
		if got := req.Header.Get("If-None-Match"); got != "" {
			t.Fatalf("ordinary Put sent If-None-Match = %q", got)
		}
		if call < 3 {
			return response(req, http.StatusInternalServerError, "temporary failure"), nil
		}
		return response(req, http.StatusOK, ""), nil
	}))

	if err := store.Put(context.Background(), "doc", strings.NewReader("overwrite"), nil); err != nil {
		t.Fatalf("ordinary Put failed: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 3 {
		t.Fatalf("request calls = %d, want 3", got)
	}
}

func TestS3PutIfAbsentMapsPreconditionFailureWithoutRetry(t *testing.T) {
	var calls int32
	store := newTestS3(t, roundTripFunc(func(req *http.Request) (*http.Response, error) {
		atomic.AddInt32(&calls, 1)
		return response(req, http.StatusPreconditionFailed,
			`<Error><Code>PreconditionFailed</Code><Message>already exists</Message></Error>`), nil
	}))

	err := store.Put(context.Background(), "doc", strings.NewReader("loser"), nil, PutIfAbsent())
	if !errors.Is(err, ErrObjectAlreadyExists) {
		t.Fatalf("PutIfAbsent error = %v, want ErrObjectAlreadyExists", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("request calls = %d, want 1", got)
	}
}

func TestS3PutIfAbsentDisablesSDKRetriesOnServerError(t *testing.T) {
	var calls int32
	store := newTestS3WithMaxRetries(t, roundTripFunc(func(req *http.Request) (*http.Response, error) {
		atomic.AddInt32(&calls, 1)
		return response(req, http.StatusInternalServerError,
			`<Error><Code>InternalError</Code><Message>response may be lost</Message></Error>`), nil
	}), nil)

	err := store.Put(context.Background(), "doc", strings.NewReader("winner"), nil, PutIfAbsent())
	if err == nil {
		t.Fatal("PutIfAbsent error = nil, want server error")
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("request calls = %d, want 1", got)
	}
}

func TestS3PutIfAbsentFailsClosedWithoutVerifiedBackendCapability(t *testing.T) {
	var calls int32
	store := newTestS3(t, roundTripFunc(func(req *http.Request) (*http.Response, error) {
		atomic.AddInt32(&calls, 1)
		return response(req, http.StatusOK, ""), nil
	}))
	store.cfg = &BucketConfig{}

	err := store.Put(context.Background(), "doc", strings.NewReader("winner"), nil, PutIfAbsent())
	if !errors.Is(err, ErrCreateOnlyUnsupported) {
		t.Fatalf("PutIfAbsent error = %v, want ErrCreateOnlyUnsupported", err)
	}
	if got := atomic.LoadInt32(&calls); got != 0 {
		t.Fatalf("request calls = %d, want 0", got)
	}
}

func TestPutIfAbsentRejectsUnsupportedProviders(t *testing.T) {
	tests := []struct {
		name  string
		store Client
	}{
		{name: "local file", store: &LocalFile{}},
		{name: "oss", store: &OSS{cfg: &BucketConfig{}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.store.Put(context.Background(), "doc", strings.NewReader("content"), nil, PutIfAbsent())
			if !errors.Is(err, ErrCreateOnlyUnsupported) {
				t.Fatalf("PutIfAbsent error = %v, want ErrCreateOnlyUnsupported", err)
			}
		})
	}
}

func TestLocalFilePutIfAbsentRejectsAllWriteEntryPointsWithoutMutation(t *testing.T) {
	writeEntries := []struct {
		name string
		put  func(*LocalFile, string, string, ...PutOptions) error
	}{
		{
			name: "Put",
			put: func(store *LocalFile, key, body string, options ...PutOptions) error {
				return store.Put(context.Background(), key, strings.NewReader(body), nil, options...)
			},
		},
		{
			name: "PutAndCompress",
			put: func(store *LocalFile, key, body string, options ...PutOptions) error {
				return store.PutAndCompress(context.Background(), key, strings.NewReader(body), nil, options...)
			},
		},
	}

	for _, entry := range writeEntries {
		t.Run(entry.name, func(t *testing.T) {
			store, err := NewLocalFile(t.TempDir())
			if err != nil {
				t.Fatalf("NewLocalFile failed: %v", err)
			}

			err = entry.put(store, "missing", "new-content", PutIfAbsent())
			if !errors.Is(err, ErrCreateOnlyUnsupported) {
				t.Fatalf("create-only write error = %v, want ErrCreateOnlyUnsupported", err)
			}
			exists, err := store.Exists(context.Background(), "missing")
			if err != nil {
				t.Fatalf("Exists failed: %v", err)
			}
			if exists {
				t.Fatal("create-only write created the missing object")
			}

			if err := store.Put(context.Background(), "existing", strings.NewReader("winner"), nil); err != nil {
				t.Fatalf("seed Put failed: %v", err)
			}
			err = entry.put(store, "existing", "loser", PutIfAbsent())
			if !errors.Is(err, ErrCreateOnlyUnsupported) {
				t.Fatalf("create-only overwrite error = %v, want ErrCreateOnlyUnsupported", err)
			}
			got, err := store.Get(context.Background(), "existing")
			if err != nil {
				t.Fatalf("Get failed: %v", err)
			}
			if got != "winner" {
				t.Fatalf("existing object = %q, want winner", got)
			}
		})
	}
}

func TestOSSPutIfAbsentRejectsAllWriteEntryPoints(t *testing.T) {
	store := &OSS{cfg: &BucketConfig{}}
	writeEntries := []struct {
		name string
		put  func(...PutOptions) error
	}{
		{
			name: "Put",
			put: func(options ...PutOptions) error {
				return store.Put(context.Background(), "doc", strings.NewReader("content"), nil, options...)
			},
		},
		{
			name: "PutAndCompress",
			put: func(options ...PutOptions) error {
				return store.PutAndCompress(context.Background(), "doc", strings.NewReader("content"), nil, options...)
			},
		},
	}

	for _, entry := range writeEntries {
		t.Run(entry.name, func(t *testing.T) {
			if err := entry.put(PutIfAbsent()); !errors.Is(err, ErrCreateOnlyUnsupported) {
				t.Fatalf("PutIfAbsent error = %v, want ErrCreateOnlyUnsupported", err)
			}
		})
	}
}

func response(req *http.Request, status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": []string{"application/xml"}},
		Body:       io.NopCloser(bytes.NewBufferString(body)),
		Request:    req,
	}
}
