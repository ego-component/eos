package eos

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/avast/retry-go"
	"github.com/samber/lo"
)

var _ Client = (*OSS)(nil)

type OSS struct {
	Bucket *oss.Bucket
	Shards map[string]*oss.Bucket
	cfg    *BucketConfig
}

// 返回带prefix的key
func (o *OSS) keyWithPrefix(key string) string {
	return o.cfg.Prefix + key
}

func (o *OSS) Copy(ctx context.Context, srcKey, dstKey string, options ...CopyOption) error {
	cfg := DefaultCopyOptions()
	for _, opt := range options {
		opt(cfg)
	}
	bucket, dstKey, err := o.getBucket(ctx, dstKey)
	if err != nil {
		return err
	}
	srcKeyWithBucket := srcKey
	if !cfg.rawSrcKey {
		srcBucket, srcKey, err := o.getBucket(ctx, srcKey)
		if err != nil {
			return err
		}
		srcKeyWithBucket = fmt.Sprintf("/%s/%s", srcBucket.BucketName, srcKey)
	}
	var ossOptions []oss.Option
	if cfg.metaKeysToCopy != nil || cfg.meta != nil {
		ossOptions = append(ossOptions, oss.MetadataDirective(oss.MetaReplace))
	}
	ossOptions = append(ossOptions, oss.WithContext(ctx))
	if len(cfg.metaKeysToCopy) > 0 {
		// 如果传了 attributes 数组的情况下只做部分 meta 的拷贝
		meta, err := o.Head(ctx, srcKey, cfg.metaKeysToCopy)
		if err != nil {
			return err
		}
		for k, v := range meta {
			ossOptions = append(ossOptions, oss.Meta(k, v))
		}
	}
	for k, v := range cfg.meta {
		ossOptions = append(ossOptions, oss.Meta(k, v))
	}
	if _, err = bucket.CopyObject(dstKey, srcKeyWithBucket, ossOptions...); err != nil {
		return err
	}
	return nil
}

func (o *OSS) GetBucketName(ctx context.Context, key string) (string, error) {
	b, _, err := o.getBucket(ctx, key)
	if err != nil {
		return "", err
	}
	return b.BucketName, nil
}

func (o *OSS) Get(ctx context.Context, key string, options ...GetOptions) (string, error) {
	data, err := o.GetBytes(ctx, key, options...)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// GetAsReader don't forget to call the close() method of the io.ReadCloser
func (o *OSS) GetAsReader(ctx context.Context, key string, options ...GetOptions) (io.ReadCloser, error) {
	bucket, key, err := o.getBucket(ctx, key)
	if err != nil {
		return nil, err
	}

	getOpts := DefaultGetOptions()
	for _, opt := range options {
		opt(getOpts)
	}
	readCloser, err := bucket.GetObject(key, getOSSOptions(ctx, getOpts)...)
	if err != nil {
		if oerr, ok := err.(oss.ServiceError); ok {
			if oerr.StatusCode == 404 {
				return nil, nil
			}
		}
		return nil, err
	}

	return readCloser, nil
}

// GetWithMeta don't forget to call the close() method of the io.ReadCloser
func (o *OSS) GetWithMeta(ctx context.Context, key string, attributes []string, options ...GetOptions) (io.ReadCloser, map[string]string, error) {
	getOpts := DefaultGetOptions()
	for _, opt := range options {
		opt(getOpts)
	}
	result, err := o.get(ctx, key, getOpts)
	if err != nil {
		return nil, nil, err
	}
	if result == nil {
		return nil, nil, nil
	}

	return result.Response.Body, getOSSMeta(ctx, attributes, result.Response.Headers), nil
}

func (o *OSS) GetBytes(ctx context.Context, key string, options ...GetOptions) ([]byte, error) {
	getOpts := DefaultGetOptions()
	for _, opt := range options {
		opt(getOpts)
	}
	result, err := o.get(ctx, key, getOpts)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, nil
	}

	body := result.Response
	defer func() {
		if body != nil {
			body.Close()
		}
	}()

	data, err := io.ReadAll(body)
	if err != nil {
		return nil, err
	}

	if getOpts.enableCRCValidation && result.ServerCRC > 0 && result.ClientCRC.Sum64() != result.ServerCRC {
		return nil, fmt.Errorf("crc64 check failed, reqId:%s, serverCRC:%d, clientCRC:%d", extractOSSRequestID(result.Response),
			result.ServerCRC, result.ClientCRC.Sum64())
	}
	return data, err
}

func (o *OSS) Range(ctx context.Context, key string, offset int64, length int64) (io.ReadCloser, error) {
	bucket, key, err := o.getBucket(ctx, key)
	if err != nil {
		return nil, err
	}
	var opts = []oss.Option{
		oss.WithContext(ctx),
		oss.Range(offset, offset+length-1),
	}
	return bucket.GetObject(key, opts...)
}

func (o *OSS) Put(ctx context.Context, key string, reader io.Reader, meta map[string]string, options ...PutOptions) error {
	bucket, key, err := o.getBucket(ctx, key)
	if err != nil {
		return err
	}

	opts := DefaultPutOptions()
	for _, opt := range options {
		opt(opts)
	}

	ossOpts := make([]oss.Option, 0)
	for k, v := range meta {
		ossOpts = append(ossOpts, oss.Meta(k, v))
	}
	ossOpts = append(ossOpts, oss.ContentType(opts.contentType))
	if opts.contentEncoding != nil {
		ossOpts = append(ossOpts, oss.ContentEncoding(*opts.contentEncoding))
	}
	if opts.contentDisposition != nil {
		ossOpts = append(ossOpts, oss.ContentDisposition(*opts.contentDisposition))
	}
	if opts.cacheControl != nil {
		ossOpts = append(ossOpts, oss.CacheControl(*opts.cacheControl))
	}
	if opts.expires != nil {
		ossOpts = append(ossOpts, oss.Expires(*opts.expires))
	}
	ossOpts = append(ossOpts, oss.WithContext(ctx))

	r, ok := reader.(io.ReadSeeker)
	if ok {
		return retry.Do(func() error {
			err := bucket.PutObject(key, r, ossOpts...)
			if err != nil && reader != nil {
				// Reset the body reader after the request since at this point it's already read
				// Note that it's safe to ignore the error here since the 0,0 position is always valid
				_, _ = r.Seek(0, 0)
			}
			return err
		}, retry.Attempts(3), retry.Delay(1*time.Second))
	} else {
		return bucket.PutObject(key, reader, ossOpts...)
	}
}

func (o *OSS) Del(ctx context.Context, key string) error {
	bucket, key, err := o.getBucket(ctx, key)
	if err != nil {
		return err
	}

	return bucket.DeleteObject(key, oss.WithContext(ctx))
}

func (o *OSS) DelMulti(ctx context.Context, keys []string) error {
	bucketsKeys := make(map[*oss.Bucket][]string)
	for _, k := range keys {
		bucket, key, err := o.getBucket(ctx, k)
		if err != nil {
			return err
		}
		bucketsKeys[bucket] = append(bucketsKeys[bucket], key)
	}

	for bucket, bKeys := range bucketsKeys {
		_, err := bucket.DeleteObjects(bKeys, oss.WithContext(ctx))
		if err != nil {
			return err
		}
	}

	return nil
}

func (o *OSS) Head(ctx context.Context, key string, attributes []string) (map[string]string, error) {
	bucket, key, err := o.getBucket(ctx, key)
	if err != nil {
		return nil, err
	}

	headers, err := bucket.GetObjectDetailedMeta(key, oss.WithContext(ctx))
	if err != nil {
		if oerr, ok := err.(oss.ServiceError); ok {
			if oerr.StatusCode == 404 {
				return nil, nil
			}
		}
		return nil, err
	}

	return getOSSMeta(ctx, attributes, headers), nil
}

func (o *OSS) ListObjects(ctx context.Context, continuationToken *string, options ...ListObjectsOption) (*ListObjectsResult, error) {
	var opt listObjectsOptions
	for _, f := range options {
		f(&opt)
	}

	bucket, _, err := o.getBucket(ctx, opt.shardingKey)
	if err != nil {
		return nil, err
	}

	var ossOpts = []oss.Option{
		oss.Prefix(o.cfg.Prefix + opt.prefix),
	}
	if continuationToken != nil {
		ossOpts = append(ossOpts, oss.ContinuationToken(*continuationToken))
	}
	if opt.maxKeys > 0 {
		ossOpts = append(ossOpts, oss.MaxKeys(opt.maxKeys))
	}
	if opt.startAfter != "" {
		ossOpts = append(ossOpts, oss.StartAfter(o.cfg.Prefix+opt.startAfter))
	}

	res, err := bucket.ListObjectsV2(ossOpts...)
	if err != nil {
		return nil, fmt.Errorf("oss.Bucket.ListObjectsV2: %w", err)
	}

	var result = &ListObjectsResult{
		Objects: lo.Map(res.Objects, func(v oss.ObjectProperties, _ int) *Object { return newObjectFromOss(&v) }),
	}
	if res.NextContinuationToken != "" {
		result.NextContinuationToken = &res.NextContinuationToken
	}
	result.IsTruncated = &res.IsTruncated
	return result, nil
}

func (o *OSS) SignURL(ctx context.Context, key string, expired int64, options ...SignOptions) (string, error) {
	bucket, key, err := o.getBucket(ctx, key)
	if err != nil {
		return "", err
	}
	opts := DefaultSignOptions()
	for _, opt := range options {
		opt(opts)
	}
	if opts.process != nil {
		return bucket.SignURL(key, oss.HTTPGet, expired, oss.Process(*opts.process), oss.WithContext(ctx))
	}
	return bucket.SignURL(key, oss.HTTPGet, expired, oss.WithContext(ctx))
}

func (o *OSS) Exists(ctx context.Context, key string) (bool, error) {
	bucket, key, err := o.getBucket(ctx, key)
	if err != nil {
		return false, err
	}
	return bucket.IsObjectExist(key, oss.WithContext(ctx))
}

func (o *OSS) get(ctx context.Context, key string, options *getOptions) (*oss.GetObjectResult, error) {
	bucket, key, err := o.getBucket(ctx, key)
	if err != nil {
		return nil, err
	}

	result, err := bucket.DoGetObject(&oss.GetObjectRequest{ObjectKey: key}, getOSSOptions(ctx, options))
	if err != nil {
		if oerr, ok := err.(oss.ServiceError); ok {
			if oerr.StatusCode == 404 {
				return nil, nil
			}
		}
		return nil, err
	}

	return result, nil
}

func extractOSSRequestID(resp *oss.Response) string {
	if resp == nil {
		return ""
	}
	return resp.Headers.Get(oss.HTTPHeaderOssRequestID)
}

func getOSSMeta(ctx context.Context, attributes []string, headers http.Header) map[string]string {
	meta := make(map[string]string)
	for _, v := range attributes {
		meta[v] = headers.Get(v)
		if headers.Get(v) == "" {
			meta[v] = headers.Get(oss.HTTPHeaderOssMetaPrefix + v)
		}
	}
	return meta
}

func getOSSOptions(ctx context.Context, getOpts *getOptions) []oss.Option {
	ossOpts := make([]oss.Option, 0)
	if getOpts.contentEncoding != nil {
		ossOpts = append(ossOpts, oss.ContentEncoding(*getOpts.contentEncoding))
	}
	if getOpts.contentType != nil {
		ossOpts = append(ossOpts, oss.ContentEncoding(*getOpts.contentType))
	}
	ossOpts = append(ossOpts, oss.WithContext(ctx))

	return ossOpts
}

func (o *OSS) getBucket(ctx context.Context, key string) (*oss.Bucket, string, error) {
	key = o.keyWithPrefix(key)
	if len(o.Shards) > 0 {
		keyLength := len(key)
		bucket := o.Shards[strings.ToLower(key[keyLength-1:keyLength])]
		if bucket == nil {
			return nil, "", errors.New("shards can't find bucket")
		}

		return bucket, key, nil
	}

	return o.Bucket, key, nil
}
