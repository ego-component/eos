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

type listObjectV2Options struct {
	continuationToken *string
	delimiter         *string
	maxKeys           *int64
	startAfter        *string
}

func buildListObjectV2Options(options ...ListObjectV2Option) listObjectV2Options {
	var opts listObjectV2Options
	for _, opt := range options {
		opt(&opts)
	}
	return opts
}

type ListObjectV2Option func(l *listObjectV2Options)

func ListObjectWithContinuationToken(continuationToken *string) ListObjectV2Option {
	return func(l *listObjectV2Options) {
		l.continuationToken = continuationToken
	}
}

func ListObjectWithDelimiter(delimiter *string) ListObjectV2Option {
	return func(l *listObjectV2Options) {
		l.delimiter = delimiter
	}
}

func ListObjectWithMaxKeys(maxKeys *int64) ListObjectV2Option {
	return func(l *listObjectV2Options) {
		l.maxKeys = maxKeys
	}
}

// ListObjectWithStartAfter 从 startAfter 之后（按字典序，不含 startAfter）开始列举
func ListObjectWithStartAfter(startAfter *string) ListObjectV2Option {
	return func(l *listObjectV2Options) {
		l.startAfter = startAfter
	}
}
