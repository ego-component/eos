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

// Copy copies an object
func (c *Component) Copy(ctx context.Context, srcKey, dstKey string, options ...CopyOption) error {
	return c.defaultClient.Copy(ctx, srcKey, dstKey, options...)
}

// GetBucketName returns the sharded BucketName based on the Key
func (c *Component) GetBucketName(ctx context.Context, key string) (string, error) {
	return c.defaultClient.GetBucketName(ctx, key)
}

// Get retrieves the Object content corresponding to the specified Key
func (c *Component) Get(ctx context.Context, key string, options ...GetOptions) (string, error) {
	return c.defaultClient.Get(ctx, key, options...)
}

// GetAsReader retrieves the Object content corresponding to the specified Key, but returns an io.ReadCloser.
// Don't forget to call the close() method on the io.ReadCloser
func (c *Component) GetAsReader(ctx context.Context, key string, options ...GetOptions) (io.ReadCloser, error) {
	return c.defaultClient.GetAsReader(ctx, key, options...)
}

// GetWithMeta retrieves the Object content and metadata corresponding to the specified Key
// Don't forget to call the close() method on the io.ReadCloser
func (c *Component) GetWithMeta(ctx context.Context, key string, attributes []string, options ...GetOptions) (io.ReadCloser, map[string]string, error) {
	return c.defaultClient.GetWithMeta(ctx, key, attributes, options...)
}

// GetBytes retrieves the Object content corresponding to the specified Key and returns it as []byte
func (c *Component) GetBytes(ctx context.Context, key string, options ...GetOptions) ([]byte, error) {
	return c.defaultClient.GetBytes(ctx, key, options...)
}

// Range retrieves data from the Object content at the specified offset position for the given Key
func (c *Component) Range(ctx context.Context, key string, offset int64, length int64) (io.ReadCloser, error) {
	return c.defaultClient.Range(ctx, key, offset, length)
}

// Put writes an Object to the specified Key
func (c *Component) Put(ctx context.Context, key string, reader io.Reader, meta map[string]string, options ...PutOptions) error {
	return c.defaultClient.Put(ctx, key, reader, meta, options...)
}

// Del deletes an Object
func (c *Component) Del(ctx context.Context, key string) error {
	return c.defaultClient.Del(ctx, key)
}

// DelMulti deletes multiple Objects
func (c *Component) DelMulti(ctx context.Context, keys []string) error {
	return c.defaultClient.DelMulti(ctx, keys)
}

// Head retrieves the metadata of the Object
func (c *Component) Head(ctx context.Context, key string, attributes []string) (map[string]string, error) {
	return c.defaultClient.Head(ctx, key, attributes)
}

// ListObjects lists objects. Note: if using a cursor for the next page, make sure to specify continuationToken.
// For specific examples, refer to the test cases `TestS3_ListObjects(t)`
func (c *Component) ListObjects(ctx context.Context, continuationToken *string, options ...ListObjectsOption) (*ListObjectsResult, error) {
	return c.defaultClient.ListObjects(ctx, continuationToken, options...)
}

// SignURL generates an authorization link to access the object
func (c *Component) SignURL(ctx context.Context, key string, expired int64, options ...SignOptions) (string, error) {
	return c.defaultClient.SignURL(ctx, key, expired, options...)
}

// Exists checks whether the object exists
func (c *Component) Exists(ctx context.Context, key string) (bool, error) {
	return c.defaultClient.Exists(ctx, key)
}
