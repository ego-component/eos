package eos

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func TestS3CreateOnlyIntegration(t *testing.T) {
	endpoint := os.Getenv("EOS_S3_INTEGRATION_ENDPOINT")
	accessKey := os.Getenv("EOS_S3_INTEGRATION_ACCESS_KEY")
	secretKey := os.Getenv("EOS_S3_INTEGRATION_SECRET_KEY")
	bucket := os.Getenv("EOS_S3_INTEGRATION_BUCKET")
	if endpoint == "" || accessKey == "" || secretKey == "" || bucket == "" {
		t.Skip("set EOS_S3_INTEGRATION_ENDPOINT, ACCESS_KEY, SECRET_KEY, and BUCKET")
	}

	region := os.Getenv("EOS_S3_INTEGRATION_REGION")
	if region == "" {
		region = "us-east-1"
	}
	forcePathStyle := true
	if raw := os.Getenv("EOS_S3_INTEGRATION_FORCE_PATH_STYLE"); raw != "" {
		parsed, err := strconv.ParseBool(raw)
		if err != nil {
			t.Fatalf("parse EOS_S3_INTEGRATION_FORCE_PATH_STYLE: %v", err)
		}
		forcePathStyle = parsed
	}

	awsClient := s3.New(session.Must(session.NewSession(&aws.Config{
		Credentials:      credentials.NewStaticCredentials(accessKey, secretKey, ""),
		Endpoint:         aws.String(endpoint),
		Region:           aws.String(region),
		S3ForcePathStyle: aws.Bool(forcePathStyle),
	})))
	store := &S3{
		BucketName: bucket,
		client:     awsClient,
		cfg:        &BucketConfig{S3CreateOnlySupported: true},
	}

	prefix := os.Getenv("EOS_S3_INTEGRATION_PREFIX")
	if prefix == "" {
		prefix = fmt.Sprintf("eos-create-only-probe/%d/", time.Now().UnixNano())
	}
	ctx := context.Background()
	keys := []string{prefix + "duplicate", prefix + "concurrent", prefix + "ordinary"}
	t.Cleanup(func() {
		for _, key := range keys {
			if err := store.Del(ctx, key); err != nil {
				t.Errorf("cleanup %q: %v", key, err)
				continue
			}
			exists, err := store.Exists(ctx, key)
			if err != nil {
				t.Errorf("verify cleanup %q: %v", key, err)
			} else if exists {
				t.Errorf("verify cleanup %q: object still exists", key)
			}
		}
	})

	if err := store.Put(ctx, keys[0], strings.NewReader("first-write"), nil, PutIfAbsent()); err != nil {
		t.Fatalf("first create-only Put: %v", err)
	}
	duplicateErr := store.Put(ctx, keys[0], strings.NewReader("second-write"), nil, PutIfAbsent())
	duplicateBody, readErr := store.Get(ctx, keys[0])
	if !errors.Is(duplicateErr, ErrObjectAlreadyExists) || readErr != nil || duplicateBody != "first-write" {
		t.Fatalf("duplicate result: Put error=%v final=%q read error=%v; want conflict and preserved first-write", duplicateErr, duplicateBody, readErr)
	}

	type putResult struct {
		body string
		err  error
	}
	start := make(chan struct{})
	results := make(chan putResult, 2)
	for _, body := range []string{"candidate-a", "candidate-b"} {
		body := body
		go func() {
			<-start
			results <- putResult{body: body, err: store.Put(ctx, keys[1], strings.NewReader(body), nil, PutIfAbsent())}
		}()
	}
	close(start)

	successes, conflicts := 0, 0
	winner := ""
	for i := 0; i < 2; i++ {
		result := <-results
		switch {
		case result.err == nil:
			successes++
			winner = result.body
		case errors.Is(result.err, ErrObjectAlreadyExists):
			conflicts++
		default:
			t.Fatalf("concurrent create-only Put body=%q: %v", result.body, result.err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("concurrent summary success=%d conflict=%d, want 1/1", successes, conflicts)
	}
	if got, err := store.Get(ctx, keys[1]); err != nil || got != winner {
		t.Fatalf("concurrent final object = %q, err = %v, want winner %q", got, err, winner)
	}

	if err := store.Put(ctx, keys[2], strings.NewReader("ordinary-first"), nil); err != nil {
		t.Fatalf("ordinary first Put: %v", err)
	}
	if err := store.Put(ctx, keys[2], strings.NewReader("ordinary-overwrite"), nil); err != nil {
		t.Fatalf("ordinary overwrite Put: %v", err)
	}
	if got, err := store.Get(ctx, keys[2]); err != nil || got != "ordinary-overwrite" {
		t.Fatalf("ordinary final object = %q, err = %v, want ordinary-overwrite", got, err)
	}

	t.Logf("verified bucket=%q prefix=%q: duplicate and concurrent create-only writes preserved the winner; ordinary Put overwrote", bucket, prefix)
}
