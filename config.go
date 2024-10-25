package eos

import (
	"runtime"
	"time"
)

type config struct {
	// BucketConfig 全局默认配置
	BucketConfig `mapstructure:",squash"`
	// 单个buckets自定义配置
	Buckets map[string]BucketConfig
}

type BucketConfig struct {
	// Debug
	Debug bool
	// Required, value is one of oss/s3, case insensetive
	StorageType string
	// Required
	AccessKeyID string
	// Required
	AccessKeySecret string
	// Required
	Endpoint string
	// Required Bucket name
	Bucket string
	// Prefix sets a default global prefix, if not empty, this prefix will automatically be added to all keys by default
	Prefix string
	// Optional, choose which bucket to use based on the last character of the key,
	// if bucket is 'content', shards is ['abc', 'edf'],
	// then the last character of the key with a/b/c will automatically use the content-abc bucket, and vice versa
	Shards []string
	// Only for s3-like
	Region string
	// Only for s3-like, whether to force path style URLs for S3 objects.
	S3ForcePathStyle bool
	// Only for s3-like
	SSL bool
	// Only for s3-like, set http client timeout.
	// oss has default timeout, but s3 default timeout is 0 means no timeout.
	S3HttpTimeoutSecs int64
	// EnableTraceInterceptor enable otel trace (only for s3)
	EnableTraceInterceptor bool
	// EnableMetricInterceptor enable prom metrics
	EnableMetricInterceptor bool
	// EnableClientTrace
	EnableClientTrace bool
	// MaxIdleConns 设置最大空闲连接数
	MaxIdleConns int
	// MaxIdleConnsPerHost 设置长连接个数
	MaxIdleConnsPerHost int
	// EnableKeepAlives 是否开启长连接，默认打开
	EnableKeepAlives bool
	// IdleConnTimeout 设置空闲连接时间，默认90 * time.Second
	IdleConnTimeout time.Duration
}

// DefaultConfig 返回默认配置
func DefaultConfig() *config {
	return &config{BucketConfig: BucketConfig{
		StorageType:             "s3",
		S3HttpTimeoutSecs:       60,
		EnableTraceInterceptor:  true,
		EnableMetricInterceptor: true,
		EnableKeepAlives:        true,
		IdleConnTimeout:         90 * time.Second,
		MaxIdleConnsPerHost:     runtime.GOMAXPROCS(0) + 1,
		MaxIdleConns:            100,
	}}
}
