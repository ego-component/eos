package eos

import "time"

type putOptions struct {
	contentType        string
	contentEncoding    *string
	contentDisposition *string
	cacheControl       *string
	expires            *time.Time
}

type PutOptions func(options *putOptions)

func PutWithContentType(contentType string) PutOptions {
	return func(options *putOptions) {
		options.contentType = contentType
	}
}

func PutWithContentEncoding(contentEncoding string) PutOptions {
	return func(options *putOptions) {
		options.contentEncoding = &contentEncoding
	}
}

func PutWithContentDisposition(contentDisposition string) PutOptions {
	return func(options *putOptions) {
		options.contentDisposition = &contentDisposition
	}
}

func PutWithCacheControl(cacheControl string) PutOptions {
	return func(options *putOptions) {
		options.cacheControl = &cacheControl
	}
}

func PutWithExpireTime(expires time.Time) PutOptions {
	return func(options *putOptions) {
		options.expires = &expires
	}
}

func DefaultPutOptions() *putOptions {
	return &putOptions{
		contentType: "text/plain",
	}
}

type getOptions struct {
	contentType         *string
	contentEncoding     *string
	enableCRCValidation bool
}

func DefaultGetOptions() *getOptions {
	return &getOptions{}
}

type GetOptions func(options *getOptions)

func GetWithContentType(contentType string) GetOptions {
	return func(options *getOptions) {
		options.contentType = &contentType
	}
}

func GetWithContentEncoding(contentEncoding string) GetOptions {
	return func(options *getOptions) {
		options.contentEncoding = &contentEncoding
	}
}

func EnableCRCValidation() GetOptions {
	return func(options *getOptions) {
		options.enableCRCValidation = true
	}
}

type copyOptions struct {
	metaKeysToCopy []string
	rawSrcKey      bool
	meta           map[string]string
}

func DefaultCopyOptions() *copyOptions {
	return &copyOptions{
		metaKeysToCopy: nil,
		rawSrcKey:      false,
		meta:           nil,
	}
}

type CopyOption func(options *copyOptions)

// CopyWithAttributes specify metadata keys to copy
func CopyWithAttributes(meta []string) CopyOption {
	return func(options *copyOptions) {
		options.metaKeysToCopy = meta
	}
}

// CopyWithNewAttributes append new attributes(meta) to new object
//
// NOTE: if this option was specified, the metadata(s) of source object would be dropped expect specifying keys to copy
// using CopyWithAttributes() option.
func CopyWithNewAttributes(meta map[string]string) CopyOption {
	return func(options *copyOptions) {
		options.meta = meta
	}
}

func CopyWithRawSrcKey() CopyOption {
	return func(options *copyOptions) {
		options.rawSrcKey = true
	}
}

type SignOptions func(options *signOptions)

func SignWithProcess(process string) SignOptions {
	return func(options *signOptions) {
		options.process = &process
	}
}

type signOptions struct {
	process *string
}

func DefaultSignOptions() *signOptions {
	return &signOptions{}
}

type listObjectsV2Options struct {
	shardingKey       string
	continuationToken string
	maxKeys           int
}

type ListObjectsV2Option func(opt *listObjectsV2Options)

func ListWithShardingKey(key string) ListObjectsV2Option {
	return func(opt *listObjectsV2Options) {
		opt.shardingKey = key
	}
}

func ListWithContinuationToken(continuationToken string) ListObjectsV2Option {
	return func(opt *listObjectsV2Options) {
		opt.continuationToken = continuationToken
	}
}

func ListWithMaxKeys(maxKeys int) ListObjectsV2Option {
	return func(opt *listObjectsV2Options) {
		opt.maxKeys = maxKeys
	}
}
