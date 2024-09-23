package eos

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/avast/retry-go"
	"github.com/golang/snappy"
	"github.com/samber/lo"
)

var _ Client = (*OSS)(nil)

type OSS struct {
	Bucket     *oss.Bucket
	Shards     map[string]*oss.Bucket
	cfg        *BucketConfig
	compressor Compressor
}

// 返回带prefix的key
func (ossClient *OSS) keyWithPrefix(key string) string {
	return ossClient.cfg.Prefix + key
}

func (ossClient *OSS) Copy(ctx context.Context, srcKey, dstKey string, options ...CopyOption) error {
	cfg := DefaultCopyOptions()
	for _, opt := range options {
		opt(cfg)
	}
	bucket, dstKey, err := ossClient.getBucket(ctx, dstKey)
	if err != nil {
		return err
	}
	srcKeyWithBucket := srcKey
	if !cfg.rawSrcKey {
		srcBucket, srcKey, err := ossClient.getBucket(ctx, srcKey)
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
		meta, err := ossClient.Head(ctx, srcKey, cfg.metaKeysToCopy)
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
	_, err = bucket.CopyObject(dstKey, srcKeyWithBucket, ossOptions...)
	if err != nil {
		return err
	}
	return nil
}

func (ossClient *OSS) GetBucketName(ctx context.Context, key string) (string, error) {
	b, _, err := ossClient.getBucket(ctx, key)
	if err != nil {
		return "", err
	}
	return b.BucketName, nil
}

func (ossClient *OSS) Get(ctx context.Context, key string, options ...GetOptions) (string, error) {
	data, err := ossClient.GetBytes(ctx, key, options...)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// GetAsReader don't forget to call the close() method of the io.ReadCloser
func (ossClient *OSS) GetAsReader(ctx context.Context, key string, options ...GetOptions) (io.ReadCloser, error) {
	bucket, key, err := ossClient.getBucket(ctx, key)
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
func (ossClient *OSS) GetWithMeta(ctx context.Context, key string, attributes []string, options ...GetOptions) (io.ReadCloser, map[string]string, error) {
	getOpts := DefaultGetOptions()
	for _, opt := range options {
		opt(getOpts)
	}
	result, err := ossClient.get(ctx, key, getOpts)
	if err != nil {
		return nil, nil, err
	}
	if result == nil {
		return nil, nil, nil
	}

	return result.Response.Body, getOSSMeta(ctx, attributes, result.Response.Headers), nil
}

func (ossClient *OSS) GetBytes(ctx context.Context, key string, options ...GetOptions) ([]byte, error) {
	getOpts := DefaultGetOptions()
	for _, opt := range options {
		opt(getOpts)
	}
	result, err := ossClient.get(ctx, key, getOpts)
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

	data, err := ioutil.ReadAll(body)
	if err != nil {
		return nil, err
	}

	if getOpts.enableCRCValidation && result.ServerCRC > 0 && result.ClientCRC.Sum64() != result.ServerCRC {
		return nil, fmt.Errorf("crc64 check failed, reqId:%s, serverCRC:%d, clientCRC:%d", extractOSSRequestID(result.Response),
			result.ServerCRC, result.ClientCRC.Sum64())
	}
	return data, err
}

func (ossClient *OSS) GetAndDecompress(ctx context.Context, key string) (string, error) {
	result, err := ossClient.get(ctx, key, DefaultGetOptions())
	if err != nil {
		return "", err
	}
	if result == nil {
		return "", nil
	}

	body := result.Response
	defer func() {
		if body != nil {
			body.Close()
		}
	}()

	compressor := body.Headers.Get("X-Oss-Meta-Compressor")
	if compressor != "" {
		if compressor != "snappy" {
			return "", errors.New("GetAndDecompress only supports snappy for now, got " + compressor)
		}

		rawBytes, err := ioutil.ReadAll(body)
		if err != nil {
			return "", err
		}

		decodedBytes, err := snappy.Decode(nil, rawBytes)
		if err != nil {
			if errors.Is(err, snappy.ErrCorrupt) {
				reader := snappy.NewReader(bytes.NewReader(rawBytes))
				data, err := ioutil.ReadAll(reader)
				if err != nil {
					return "", err
				}

				return string(data), nil
			}
			return "", err
		}

		return string(decodedBytes), err
	}

	data, err := ioutil.ReadAll(body)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

func (ossClient *OSS) GetAndDecompressAsReader(ctx context.Context, key string) (io.ReadCloser, error) {
	ret, err := ossClient.GetAndDecompress(ctx, key)
	if err != nil {
		return nil, err
	}
	return ioutil.NopCloser(strings.NewReader(ret)), nil
}

func (ossClient *OSS) Range(ctx context.Context, key string, offset int64, length int64) (io.ReadCloser, error) {
	bucket, key, err := ossClient.getBucket(ctx, key)
	if err != nil {
		return nil, err
	}
	var opts = []oss.Option{
		oss.WithContext(ctx),
		oss.Range(offset, offset+length-1),
	}
	return bucket.GetObject(key, opts...)
}

func (ossClient *OSS) Put(ctx context.Context, key string, reader io.ReadSeeker, meta map[string]string, options ...PutOptions) error {
	bucket, key, err := ossClient.getBucket(ctx, key)
	if err != nil {
		return err
	}

	putOptions := DefaultPutOptions()
	for _, opt := range options {
		opt(putOptions)
	}

	ossOptions := make([]oss.Option, 0)
	if meta != nil {
		for k, v := range meta {
			ossOptions = append(ossOptions, oss.Meta(k, v))
		}
	}
	ossOptions = append(ossOptions, oss.ContentType(putOptions.contentType))
	if putOptions.contentEncoding != nil {
		ossOptions = append(ossOptions, oss.ContentEncoding(*putOptions.contentEncoding))
	}
	if putOptions.contentDisposition != nil {
		ossOptions = append(ossOptions, oss.ContentDisposition(*putOptions.contentDisposition))
	}
	if putOptions.cacheControl != nil {
		ossOptions = append(ossOptions, oss.CacheControl(*putOptions.cacheControl))
	}
	if putOptions.expires != nil {
		ossOptions = append(ossOptions, oss.Expires(*putOptions.expires))
	}

	if ossClient.compressor != nil {
		l, err := GetReaderLength(reader)
		if err != nil {
			return err
		}
		if l > ossClient.cfg.CompressLimit {
			reader, _, err = ossClient.compressor.Compress(reader)
			if err != nil {
				return err
			}
			ossOptions = append(ossOptions, oss.ContentEncoding(ossClient.compressor.ContentEncoding()))
		}
	}
	ossOptions = append(ossOptions, oss.WithContext(ctx))

	return retry.Do(func() error {
		err := bucket.PutObject(key, reader, ossOptions...)
		if err != nil && reader != nil {
			// Reset the body reader after the request since at this point it's already read
			// Note that it's safe to ignore the error here since the 0,0 position is always valid
			_, _ = reader.Seek(0, 0)
		}
		return err
	}, retry.Attempts(3), retry.Delay(1*time.Second))
}

func (ossClient *OSS) PutAndCompress(ctx context.Context, key string, reader io.ReadSeeker, meta map[string]string, options ...PutOptions) error {
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return err
	}
	if meta == nil {
		meta = make(map[string]string)
	}

	encodedBytes := snappy.Encode(nil, data)
	meta["Compressor"] = "snappy"

	return ossClient.Put(ctx, key, bytes.NewReader(encodedBytes), meta, options...)
}

func (ossClient *OSS) Del(ctx context.Context, key string) error {
	bucket, key, err := ossClient.getBucket(ctx, key)
	if err != nil {
		return err
	}

	return bucket.DeleteObject(key, oss.WithContext(ctx))
}

func (ossClient *OSS) DelMulti(ctx context.Context, keys []string) error {
	bucketsKeys := make(map[*oss.Bucket][]string)
	for _, k := range keys {
		bucket, key, err := ossClient.getBucket(ctx, k)
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

func (ossClient *OSS) Head(ctx context.Context, key string, attributes []string) (map[string]string, error) {
	bucket, key, err := ossClient.getBucket(ctx, key)
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

func (ossClient *OSS) ListObject(ctx context.Context, key string, prefix string, marker string, maxKeys int, delimiter string) ([]string, error) {
	bucket, _, err := ossClient.getBucket(ctx, key)
	if err != nil {
		return nil, err
	}

	prefix = ossClient.cfg.Prefix + prefix
	res, err := bucket.ListObjects(oss.Prefix(prefix), oss.Marker(marker), oss.MaxKeys(maxKeys), oss.Delimiter(delimiter), oss.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("oss.Bucket.ListObjects: %w", err)
	}
	keys := make([]string, 0)
	for _, v := range res.Objects {
		keys = append(keys, v.Key)
	}

	return keys, nil
}

func (ossClient *OSS) ListObjectsV2(ctx context.Context, prefix string, options ...ListObjectsV2Option) (*ListObjectsV2Result, error) {
	var opt listObjectsV2Options
	for _, f := range options {
		f(&opt)
	}

	bucket, _, err := ossClient.getBucket(ctx, opt.shardingKey)
	if err != nil {
		return nil, err
	}

	var ossOpts = []oss.Option{
		oss.Prefix(ossClient.cfg.Prefix + prefix),
	}
	if opt.continuationToken != "" {
		ossOpts = append(ossOpts, oss.ContinuationToken(opt.continuationToken))
	}
	if opt.maxKeys > 0 {
		ossOpts = append(ossOpts, oss.MaxKeys(opt.maxKeys))
	}

	res, err := bucket.ListObjectsV2(ossOpts...)
	if err != nil {
		return nil, fmt.Errorf("oss.Bucket.ListObjectsV2: %w", err)
	}

	var result = &ListObjectsV2Result{
		Contents:              lo.Map(res.Objects, func(v oss.ObjectProperties, _ int) *Object { return newObjectFromOss(&v) }),
		NextContinuationToken: nil,
	}
	if res.NextContinuationToken != "" {
		result.NextContinuationToken = &res.NextContinuationToken
	}
	return result, nil
}

func (ossClient *OSS) SignURL(ctx context.Context, key string, expired int64, options ...SignOptions) (string, error) {
	bucket, key, err := ossClient.getBucket(ctx, key)
	if err != nil {
		return "", err
	}
	signOptions := DefaultSignOptions()
	for _, opt := range options {
		opt(signOptions)
	}
	if signOptions.process != nil {
		return bucket.SignURL(key, oss.HTTPGet, expired, oss.Process(*signOptions.process), oss.WithContext(ctx))
	}
	return bucket.SignURL(key, oss.HTTPGet, expired, oss.WithContext(ctx))
}

func (ossClient *OSS) Exists(ctx context.Context, key string) (bool, error) {
	bucket, key, err := ossClient.getBucket(ctx, key)
	if err != nil {
		return false, err
	}
	return bucket.IsObjectExist(key, oss.WithContext(ctx))
}

func (ossClient *OSS) get(ctx context.Context, key string, options *getOptions) (*oss.GetObjectResult, error) {
	bucket, key, err := ossClient.getBucket(ctx, key)
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

func (ossClient *OSS) getBucket(ctx context.Context, key string) (*oss.Bucket, string, error) {
	key = ossClient.keyWithPrefix(key)
	if ossClient.Shards != nil && len(ossClient.Shards) > 0 {
		keyLength := len(key)
		bucket := ossClient.Shards[strings.ToLower(key[keyLength-1:keyLength])]
		if bucket == nil {
			return nil, "", errors.New("shards can't find bucket")
		}

		return bucket, key, nil
	}

	return ossClient.Bucket, key, nil
}
