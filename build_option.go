package eos

import (
	"time"
)

type BuildOption func(c *Container)

func WithDebug(debug bool) BuildOption {
	return func(c *Container) {
		c.config.Debug = debug
	}
}

func WithStorageType(storageType string) BuildOption {
	return func(c *Container) {
		c.config.StorageType = storageType
	}
}

func WithAccessKeyID(ak string) BuildOption {
	return func(c *Container) {
		c.config.AccessKeyID = ak
	}
}

func WithAccessKeySecret(sk string) BuildOption {
	return func(c *Container) {
		c.config.AccessKeySecret = sk
	}
}

func WithEndpoint(endpoint string) BuildOption {
	return func(c *Container) {
		c.config.Endpoint = endpoint
	}
}

func WithBucket(bucket string) BuildOption {
	return func(c *Container) {
		c.config.Bucket = bucket
	}
}

func WithShards(shards []string) BuildOption {
	return func(c *Container) {
		c.config.Shards = shards
	}
}

func WithRegion(region string) BuildOption {
	return func(c *Container) {
		c.config.Region = region
	}
}

func WithS3ForcePathStyle(s3ForcePathStyle bool) BuildOption {
	return func(c *Container) {
		c.config.S3ForcePathStyle = s3ForcePathStyle
	}
}

// WithS3CreateOnlySupported enables PutIfAbsent for a backend whose
// If-None-Match: * behavior has been verified as atomic.
func WithS3CreateOnlySupported(supported bool) BuildOption {
	return func(c *Container) {
		c.config.S3CreateOnlySupported = supported
	}
}

func WithSSL(ssl bool) BuildOption {
	return func(c *Container) {
		c.config.SSL = ssl
	}
}

func WithS3HttpTimeoutSecs(s3HttpTimeoutSecs int64) BuildOption {
	return func(c *Container) {
		c.config.S3HttpTimeoutSecs = s3HttpTimeoutSecs
	}
}

func WithMaxIdleConnsPerHost(maxIdleConnsPerHost int) BuildOption {
	return func(c *Container) {
		c.config.MaxIdleConnsPerHost = maxIdleConnsPerHost
	}
}

func WithMaxIdleConns(maxIdleConns int) BuildOption {
	return func(c *Container) {
		c.config.MaxIdleConns = maxIdleConns
	}
}

func WithKeepAlives(enableKeepAlives bool) BuildOption {
	return func(c *Container) {
		c.config.EnableKeepAlives = enableKeepAlives
	}
}

func WithIdleConnTimeout(idleConnTimeout time.Duration) BuildOption {
	return func(c *Container) {
		c.config.IdleConnTimeout = idleConnTimeout
	}
}
