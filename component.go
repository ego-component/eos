package eos

import (
	"context"
	"io"

	"github.com/gotomicro/ego/core/elog"
)

type Component struct {
	config        *config
	logger        *elog.Component
	clients       map[string]Client
	defaultClient Client
}

const defaultClientKey = "__default__"

// DefaultClient return default storage client
func (c *Component) DefaultClient() Client {
	return c.defaultClient
}

// Client return specific storage client instance
func (c *Component) Client(bucket string) Client {
	s, ok := c.clients[bucket]
	if !ok {
		c.logger.Panic("Get Client fail, bucket not init or not in config", elog.String("bucket", bucket))
	}
	return s
}

func (c *Component) Copy(ctx context.Context, srcKey, dstKey string, options ...CopyOption) error {
	return c.defaultClient.Copy(ctx, srcKey, dstKey, options...)
}

func (c *Component) GetBucketName(ctx context.Context, key string) (string, error) {
	return c.defaultClient.GetBucketName(ctx, key)
}

func (c *Component) Get(ctx context.Context, key string, options ...GetOptions) (string, error) {
	return c.defaultClient.Get(ctx, key, options...)
}

// GetAsReader don't forget to call the close() method of the io.ReadCloser
func (c *Component) GetAsReader(ctx context.Context, key string, options ...GetOptions) (io.ReadCloser, error) {
	return c.defaultClient.GetAsReader(ctx, key, options...)
}

// GetWithMeta don't forget to call the close() method of the io.ReadCloser
func (c *Component) GetWithMeta(ctx context.Context, key string, attributes []string, options ...GetOptions) (io.ReadCloser, map[string]string, error) {
	return c.defaultClient.GetWithMeta(ctx, key, attributes, options...)
}

func (c *Component) GetBytes(ctx context.Context, key string, options ...GetOptions) ([]byte, error) {
	return c.defaultClient.GetBytes(ctx, key, options...)
}

func (c *Component) Range(ctx context.Context, key string, offset int64, length int64) (io.ReadCloser, error) {
	return c.defaultClient.Range(ctx, key, offset, length)
}

func (c *Component) Put(ctx context.Context, key string, reader io.Reader, meta map[string]string, options ...PutOptions) error {
	return c.defaultClient.Put(ctx, key, reader, meta, options...)
}

func (c *Component) Del(ctx context.Context, key string) error {
	return c.defaultClient.Del(ctx, key)
}

func (c *Component) DelMulti(ctx context.Context, keys []string) error {
	return c.defaultClient.DelMulti(ctx, keys)
}

func (c *Component) Head(ctx context.Context, key string, attributes []string) (map[string]string, error) {
	return c.defaultClient.Head(ctx, key, attributes)
}

func (c *Component) ListObjects(ctx context.Context, continuationToken *string, options ...ListObjectsOption) (*ListObjectsResult, error) {
	return c.defaultClient.ListObjects(ctx, continuationToken, options...)
}

func (c *Component) SignURL(ctx context.Context, key string, expired int64, options ...SignOptions) (string, error) {
	return c.defaultClient.SignURL(ctx, key, expired, options...)
}

func (c *Component) Exists(ctx context.Context, key string) (bool, error) {
	return c.defaultClient.Exists(ctx, key)
}
