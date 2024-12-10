package eos

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/avast/retry-go"
	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/samber/lo"
)

var _ Client = (*S3)(nil)

type S3 struct {
	ShardsBucket  map[string]string
	BucketName    string
	client        *s3.Client
	cfg           *BucketConfig
	presignClient *s3.PresignClient
}

// 返回带prefix的key
func (s *S3) keyWithPrefix(key string) string {
	return s.cfg.Prefix + key
}

func (s *S3) Copy(ctx context.Context, srcKey, dstKey string, options ...CopyOption) error {
	cfg := DefaultCopyOptions()
	for _, opt := range options {
		opt(cfg)
	}
	var copySource = srcKey
	if !cfg.rawSrcKey {
		srcBucket, srcKey, err := s.getBucketAndKey(ctx, srcKey)
		if err != nil {
			return err
		}
		copySource = fmt.Sprintf("/%s/%s", srcBucket, srcKey)
	}
	bucketName, dstKey, err := s.getBucketAndKey(ctx, dstKey)
	if err != nil {
		return err
	}
	input := &s3.CopyObjectInput{
		Bucket:     aws.String(bucketName),
		CopySource: aws.String(copySource),
		Key:        aws.String(dstKey),
	}
	if cfg.metaKeysToCopy != nil || cfg.meta != nil {
		// todo 这里要看下
		cfg.metaKeysToCopy = append(cfg.metaKeysToCopy, "Content-Encoding") // always copy content-encoding
		metadata, err := s.Head(ctx, srcKey, cfg.metaKeysToCopy)
		if err != nil {
			return err
		}
		input.Metadata = make(map[string]string, len(metadata)+len(cfg.meta))
		if len(metadata) > 0 {
			for k, v := range metadata {
				if k == "Content-Encoding" {
					input.ContentEncoding = aws.String(v)
					continue
				}
				input.Metadata[k] = v
			}
		}
		// specify new metadata
		for k, v := range cfg.meta {
			input.Metadata[k] = v
		}
	}
	if _, err = s.client.CopyObject(ctx, input); err != nil {
		return err
	}
	return nil
}

func (s *S3) GetBucketName(ctx context.Context, key string) (string, error) {
	bucketName, _, err := s.getBucketAndKey(ctx, key)
	return bucketName, err
}

func (s *S3) getBucketAndKey(ctx context.Context, key string) (string, string, error) {
	key = s.keyWithPrefix(key)
	if len(s.ShardsBucket) > 0 {
		keyLength := len(key)
		bucketName := s.ShardsBucket[strings.ToLower(key[keyLength-1:keyLength])]
		if bucketName == "" {
			return "", key, errors.New("shards can't find bucket")
		}

		return bucketName, key, nil
	}

	return s.BucketName, key, nil
}

// GetAsReader don't forget to call the close() method of the io.ReadCloser
func (s *S3) GetAsReader(ctx context.Context, key string, options ...GetOptions) (io.ReadCloser, error) {
	bucketName, key, err := s.getBucketAndKey(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("GetAsReader getBucketAndKey fail, err: %w", err)
	}

	input := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}
	setS3Options(ctx, options, input)

	result, err := s.client.GetObject(ctx, input)
	if err != nil {
		if isNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("GetAsReader GetObject fail, err: %w", err)
	}

	return result.Body, nil
}

// GetWithMeta don't forget to call the close() method of the io.ReadCloser
func (s *S3) GetWithMeta(ctx context.Context, key string, attributes []string, options ...GetOptions) (io.ReadCloser, map[string]string, error) {
	bucketName, key, err := s.getBucketAndKey(ctx, key)
	if err != nil {
		return nil, nil, err
	}

	input := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}
	setS3Options(ctx, options, input)

	result, err := s.client.GetObject(ctx, input)
	if err != nil {
		if isNotFound(err) {
			return nil, nil, nil
		}
		return nil, nil, fmt.Errorf("GetWithMeta GetObject fail, err: %w", err)
	}
	return result.Body, getS3Meta(ctx, attributes, mergeHttpStandardHeaders(&HeadGetObjectOutputWrapper{
		getObjectOutput: result,
	})), err
}

func (s *S3) Get(ctx context.Context, key string, options ...GetOptions) (string, error) {
	data, err := s.GetBytes(ctx, key, options...)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (s *S3) GetBytes(ctx context.Context, key string, options ...GetOptions) ([]byte, error) {
	result, err := s.get(ctx, key, options...)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, nil
	}

	body := result.Body
	defer func() {
		if body != nil {
			body.Close()
		}
	}()
	var buf bytes.Buffer
	_, err = io.Copy(&buf, body)
	return buf.Bytes(), err
}

func (s *S3) Range(ctx context.Context, key string, offset int64, length int64) (io.ReadCloser, error) {
	readRange := fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
	bucketName, key, err := s.getBucketAndKey(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("range getBucketAndKey fail, %w", err)
	}
	input := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Range:  &readRange,
	}
	r, err := s.client.GetObject(ctx, input)
	if err != nil {
		return nil, err
	}
	return r.Body, nil
}

func (s *S3) Put(ctx context.Context, key string, reader io.Reader, meta map[string]string, options ...PutOptions) error {
	bucketName, key, err := s.getBucketAndKey(ctx, key)
	if err != nil {
		return err
	}
	putOptions := DefaultPutOptions()
	for _, opt := range options {
		opt(putOptions)
	}
	input := &s3.PutObjectInput{
		Body:        reader,
		Bucket:      aws.String(bucketName),
		Key:         aws.String(key),
		Metadata:    meta,
		ContentType: aws.String(putOptions.contentType),
	}
	if putOptions.contentEncoding != nil {
		input.ContentEncoding = putOptions.contentEncoding
	}
	if putOptions.contentDisposition != nil {
		input.ContentDisposition = putOptions.contentDisposition
	}
	if putOptions.cacheControl != nil {
		input.CacheControl = putOptions.cacheControl
	}
	if putOptions.expires != nil {
		input.Expires = putOptions.expires
	}

	r, ok := reader.(io.ReadSeeker)
	if ok {
		// 只有实现了io.ReadSeeker接口才考虑做重试
		err = retry.Do(func() error {
			_, err := s.client.PutObject(ctx, input)
			if err != nil && reader != nil {
				// Reset the body reader after the request since at this point it's already read
				// Note that it's safe to ignore the error here since the 0,0 position is always valid
				_, _ = r.Seek(0, 0)
			}
			return err
		}, retry.Attempts(3), retry.Delay(1*time.Second))
	} else {
		// 没有实现io.ReadSeeker接口无法做重试
		_, err = s.client.PutObject(ctx, input)
	}

	return err
}

func (s *S3) Del(ctx context.Context, key string) error {
	bucketName, key, err := s.getBucketAndKey(ctx, key)
	if err != nil {
		return err
	}

	input := &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}

	_, err = s.client.DeleteObject(ctx, input)
	return err
}

func (s *S3) DelMulti(ctx context.Context, keys []string) error {
	bucketsNameKeys := make(map[string][]string)
	for _, k := range keys {
		bucketName, key, err := s.getBucketAndKey(ctx, k)
		if err != nil {
			return err
		}
		bucketsNameKeys[bucketName] = append(bucketsNameKeys[bucketName], key)
	}

	for bucketName, BKeys := range bucketsNameKeys {
		delObjects := make([]types.ObjectIdentifier, len(BKeys))

		for idx, key := range BKeys {
			delObjects[idx] = types.ObjectIdentifier{
				Key: aws.String(key),
			}
		}

		input := &s3.DeleteObjectsInput{
			Bucket: aws.String(bucketName),
			Delete: &types.Delete{
				Objects: delObjects,
				Quiet:   aws.Bool(false),
			},
		}

		_, err := s.client.DeleteObjects(ctx, input)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *S3) Head(ctx context.Context, key string, attributes []string) (map[string]string, error) {
	bucketName, key, err := s.getBucketAndKey(ctx, key)
	if err != nil {
		return nil, err
	}

	input := &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}

	result, err := s.client.HeadObject(ctx, input)
	if err != nil {
		if isNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return getS3Meta(ctx, attributes, mergeHttpStandardHeaders(&HeadGetObjectOutputWrapper{
		headObjectOutput: result,
	})), nil
}

func (s *S3) ListObjects(ctx context.Context, continuationToken *string, options ...ListObjectsOption) (*ListObjectsResult, error) {
	var opt listObjectsOptions
	for _, f := range options {
		f(&opt)
	}

	bucketName, _, err := s.getBucketAndKey(ctx, opt.shardingKey)
	if err != nil {
		return nil, err
	}

	input := &s3.ListObjectsV2Input{
		Bucket:            aws.String(bucketName),
		ContinuationToken: continuationToken,
	}
	input.Prefix = aws.String(s.cfg.Prefix)
	if opt.prefix != "" {
		input.Prefix = aws.String(s.cfg.Prefix + opt.prefix)
	}
	if opt.maxKeys > 0 {
		input.MaxKeys = aws.Int32(int32(opt.maxKeys))
	}
	if opt.startAfter != "" {
		input.StartAfter = aws.String(s.cfg.Prefix + opt.startAfter)
	}

	output, err := s.client.ListObjectsV2(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("S3.ListObjectsV2WithContext: %w", err)
	}

	result := &ListObjectsResult{
		Objects: lo.Map(output.Contents, func(v types.Object, _ int) *Object {
			*v.Key = strings.TrimPrefix(*v.Key, s.cfg.Prefix)
			return newObjectFromS3(&v)
		}),
		NextContinuationToken: output.NextContinuationToken,
		IsTruncated:           output.IsTruncated,
	}
	return result, nil
}

func (s *S3) SignURL(ctx context.Context, key string, expired int64, options ...SignOptions) (string, error) {
	bucketName, key, err := s.getBucketAndKey(ctx, key)
	if err != nil {
		return "", err
	}
	input := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}
	so := DefaultSignOptions()
	for _, opt := range options {
		opt(so)
	}
	if so.process != nil {
		panic("process option is not supported for s3")
	}
	req, err := s.presignClient.PresignGetObject(ctx, input, func(options *s3.PresignOptions) {
		options.Expires = time.Duration(expired) * time.Second
	})
	if err != nil {
		return "", err
	}

	return req.URL, nil
}

func (s *S3) Exists(ctx context.Context, key string) (bool, error) {
	bucketName, key, err := s.getBucketAndKey(ctx, key)
	if err != nil {
		return false, err
	}

	input := &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}
	if _, err = s.client.HeadObject(ctx, input); err == nil {
		return true, nil
	}
	if isNotFound(err) {
		return false, nil
	}
	return false, err
}

func (s *S3) CreateMultipartUpload(ctx context.Context, key string) (*CreateMultipartUploadResult, error) {
	bucketName, key, err := s.getBucketAndKey(ctx, key)
	if err != nil {
		return nil, err
	}

	input := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}

	resp, err := s.client.CreateMultipartUpload(ctx, input)
	if err != nil {
		return nil, err
	}

	return newCreateMultiUploadResFromS3(resp), nil
}

func (s *S3) CompleteMultipartUpload(ctx context.Context, key, uploadId string, parts []MultiUploadCompletedPart) error {
	bucketName, key, err := s.getBucketAndKey(ctx, key)
	if err != nil {
		return err
	}

	input := &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: newMultiUploadCompletedPartsToS3(parts),
		},
		UploadId: aws.String(uploadId),
	}

	_, err = s.client.CompleteMultipartUpload(ctx, input)
	if err != nil {
		return err
	}

	return nil
}

func (s *S3) SignUploadPartURL(ctx context.Context, key, uploadId string, partNumber int32, expired int64, options ...SignOptions) (*v4.PresignedHTTPRequest, error) {
	bucketName, key, err := s.getBucketAndKey(ctx, key)
	if err != nil {
		return nil, err
	}
	input := &s3.UploadPartInput{
		Bucket:     aws.String(bucketName),
		Key:        aws.String(key),
		PartNumber: &partNumber,
		UploadId:   aws.String(uploadId),
	}
	so := DefaultSignOptions()
	for _, opt := range options {
		opt(so)
	}
	if so.process != nil {
		panic("process option is not supported for s3")
	}
	req, err := s.presignClient.PresignUploadPart(ctx, input, func(options *s3.PresignOptions) {
		options.Expires = time.Duration(expired) * time.Second
	})
	if err != nil {
		return nil, err
	}

	return req, nil
}

func (s *S3) get(ctx context.Context, key string, options ...GetOptions) (*s3.GetObjectOutput, error) {
	bucketName, key, err := s.getBucketAndKey(ctx, key)
	if err != nil {
		return nil, err
	}

	input := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}
	setS3Options(ctx, options, input)
	result, err := s.client.GetObject(ctx, input)
	if err != nil {
		if isNotFound(err) {
			return nil, nil
		}
		return nil, err
	}

	return result, nil
}

func getS3Meta(ctx context.Context, attributes []string, metaData map[string]*string) map[string]string {
	// https://github.com/aws/aws-sdk-go-v2/issues/445
	// aws 会将 meta 的首字母大写，在这里需要转换下
	res := make(map[string]string)
	// 创建临时map，用来存key全部转成首字母大写后的header
	tmpMetaData := make(map[string]*string, len(metaData))
	for k, v := range metaData {
		//nolint:staticcheck
		tmpMetaData[strings.Title(k)] = v
	}
	for _, v := range attributes {
		//nolint:staticcheck
		// 把需要查询的key转成首字母大写后去tmpMetaData里面查
		tmpV := strings.Title(v)
		if val := tmpMetaData[tmpV]; val != nil {
			res[v] = *val
		}
	}
	return res
}

func setS3Options(ctx context.Context, options []GetOptions, getObjectInput *s3.GetObjectInput) {
	getOpts := DefaultGetOptions()
	for _, opt := range options {
		opt(getOpts)
	}
	if getOpts.contentEncoding != nil {
		getObjectInput.ResponseContentEncoding = getOpts.contentEncoding
	}
	if getOpts.contentType != nil {
		getObjectInput.ResponseContentType = getOpts.contentType
	}
}

// isNotFound 检查err是否为404类的错误
func isNotFound(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.(type) {
		case *types.NoSuchKey, *types.NotFound, *types.NoSuchBucket, *types.NoSuchUpload:
			return true
		default:
			return false
		}
	}
	return false
}
