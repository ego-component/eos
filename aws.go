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
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/golang/snappy"
)

var _ Client = (*S3)(nil)

type S3 struct {
	ShardsBucket map[string]string
	BucketName   string
	client       *s3.Client
	cfg          *BucketConfig
	//compressor    Compressor
	presignClient *s3.PresignClient
}

func (a *S3) GetClient() any {
	return a.client
}

// 返回带prefix的key
func (a *S3) keyWithPrefix(key string) string {
	return a.cfg.Prefix + key
}

func (a *S3) Copy(ctx context.Context, srcKey, dstKey string, options ...CopyOption) error {
	cfg := DefaultCopyOptions()
	for _, opt := range options {
		opt(cfg)
	}
	var copySource = srcKey
	if !cfg.rawSrcKey {
		srcBucket, srcKey, err := a.getBucketAndKey(ctx, srcKey)
		if err != nil {
			return err
		}
		copySource = fmt.Sprintf("/%s/%s", srcBucket, srcKey)
	}
	bucketName, dstKey, err := a.getBucketAndKey(ctx, dstKey)
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
		//input.SetMetadataDirective("REPLACE")
		input.Metadata = make(map[string]string)
		cfg.metaKeysToCopy = append(cfg.metaKeysToCopy, "Content-Encoding") // always copy content-encoding
		metadata, err := a.Head(ctx, srcKey, cfg.metaKeysToCopy)
		if err != nil {
			return err
		}
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
	_, err = a.client.CopyObject(ctx, input)
	if err != nil {
		return err
	}
	return nil
}

func (a *S3) GetBucketName(ctx context.Context, key string) (string, error) {
	bucketName, _, err := a.getBucketAndKey(ctx, key)
	return bucketName, err
}

func (a *S3) getBucketAndKey(ctx context.Context, key string) (string, string, error) {
	key = a.keyWithPrefix(key)
	if a.ShardsBucket != nil && len(a.ShardsBucket) > 0 {
		keyLength := len(key)
		bucketName := a.ShardsBucket[strings.ToLower(key[keyLength-1:keyLength])]
		if bucketName == "" {
			return "", key, errors.New("shards can't find bucket")
		}

		return bucketName, key, nil
	}

	return a.BucketName, key, nil
}

// GetAsReader don't forget to call the close() method of the io.ReadCloser
func (a *S3) GetAsReader(ctx context.Context, key string, options ...GetOptions) (io.ReadCloser, error) {
	bucketName, key, err := a.getBucketAndKey(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("GetAsReader getBucketAndKey fail, err: %w", err)
	}

	input := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}
	setS3Options(ctx, options, input)

	result, err := a.client.GetObject(ctx, input)
	if err != nil {
		// todo 这里要处理 no such key
		//if aerr, ok := err.(awserr.Error); ok {
		//	if aerr.Code() == s3.ErrCodeNoSuchKey {
		//		return nil, nil
		//	}
		//}
		// https://aws.github.io/aws-sdk-go-v2/docs/migrating/
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			// handle NoSuchKey error
			return nil, nil
		}
		return nil, fmt.Errorf("GetAsReader GetObject fail, err: %w", err)
	}

	return result.Body, nil
}

// GetWithMeta don't forget to call the close() method of the io.ReadCloser
func (a *S3) GetWithMeta(ctx context.Context, key string, attributes []string, options ...GetOptions) (io.ReadCloser, map[string]string, error) {
	bucketName, key, err := a.getBucketAndKey(ctx, key)
	if err != nil {
		return nil, nil, err
	}

	input := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}
	setS3Options(ctx, options, input)

	result, err := a.client.GetObject(ctx, input)
	if err != nil {
		// todo 这里要处理 no such key
		//if aerr, ok := err.(awserr.Error); ok {
		//	if aerr.Code() == s3.ErrCodeNoSuchKey {
		//		return nil, nil, nil
		//	}
		//}
		// https://aws.github.io/aws-sdk-go-v2/docs/migrating/
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			// handle NoSuchKey error
			return nil, nil, nil
		}
		return nil, nil, fmt.Errorf("GetWithMeta GetObject fail, err: %w", err)
	}
	return result.Body, getS3Meta(ctx, attributes, mergeHttpStandardHeaders(&HeadGetObjectOutputWrapper{
		getObjectOutput: result,
	})), err
}

func (a *S3) Get(ctx context.Context, key string, options ...GetOptions) (string, error) {
	data, err := a.GetBytes(ctx, key, options...)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (a *S3) GetBytes(ctx context.Context, key string, options ...GetOptions) ([]byte, error) {
	result, err := a.get(ctx, key, options...)
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

func (a *S3) Range(ctx context.Context, key string, offset int64, length int64) (io.ReadCloser, error) {
	readRange := fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
	bucketName, key, err := a.getBucketAndKey(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("range getBucketAndKey fail, %w", err)
	}
	input := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Range:  &readRange,
	}
	r, err := a.client.GetObject(ctx, input)
	if err != nil {
		return nil, err
	}
	return r.Body, nil
}

func (a *S3) GetAndDecompress(ctx context.Context, key string) (string, error) {
	result, err := a.get(ctx, key)
	if err != nil {
		return "", err
	}
	if result == nil {
		return "", nil
	}

	body := result.Body
	defer func() {
		if body != nil {
			body.Close()
		}
	}()
	var buf bytes.Buffer
	_, err = io.Copy(&buf, body)
	if err != nil {
		return "", err
	}
	compressor := result.Metadata["Compressor"]
	if compressor != "" {
		if compressor != "snappy" {
			return "", errors.New("GetAndDecompress only supports snappy for now, got " + compressor)
		}

		//rawBytes, err := io.ReadAll(body)
		decodedBytes, err := snappy.Decode(nil, buf.Bytes())
		if err != nil {
			if errors.Is(err, snappy.ErrCorrupt) {
				reader := snappy.NewReader(bytes.NewReader(buf.Bytes()))
				data, err := io.ReadAll(reader)
				if err != nil {
					return "", err
				}

				return string(data), nil
			}
			return "", err
		}

		return string(decodedBytes), nil
	}
	//
	//data, err := io.ReadAll(body)
	//if err != nil {
	//	return "", err
	//}
	return buf.String(), nil
}

func (a *S3) GetAndDecompressAsReader(ctx context.Context, key string) (io.ReadCloser, error) {
	result, err := a.GetAndDecompress(ctx, key)
	if err != nil {
		return nil, err
	}

	return io.NopCloser(strings.NewReader(result)), nil
}

func (a *S3) Put(ctx context.Context, key string, reader io.Reader, meta map[string]string, options ...PutOptions) error {
	bucketName, key, err := a.getBucketAndKey(ctx, key)
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
	// todo 这里有性能问题，不能这么玩
	//if a.compressor != nil {
	//	wrapReader, l, err := WrapReader(input.Body)
	//	if err != nil {
	//		return err
	//	}
	//	if l > a.cfg.CompressLimit {
	//		input.Body, _, err = a.compressor.Compress(wrapReader)
	//		if err != nil {
	//			return err
	//		}
	//		encoding := a.compressor.ContentEncoding()
	//		input.ContentEncoding = &encoding
	//	} else {
	//		input.Body = wrapReader
	//	}
	//}

	err = retry.Do(func() error {
		_, err := a.client.PutObject(ctx, input)
		if err != nil && reader != nil {
			// Reset the body reader after the request since at this point it's already read
			// Note that it's safe to ignore the error here since the 0,0 position is always valid
			//_, _ = reader.Seek(0, 0)
		}
		return err
	}, retry.Attempts(3), retry.Delay(1*time.Second))

	return err
}

func (a *S3) PutAndCompress(ctx context.Context, key string, reader io.Reader, meta map[string]string, options ...PutOptions) error {
	//data, err := io.ReadAll(reader)
	//if err != nil {
	//	return err
	//}
	var buf bytes.Buffer
	_, err := io.Copy(&buf, reader)
	if err != nil {
		return err
	}
	if meta == nil {
		meta = make(map[string]string)
	}

	encodedBytes := snappy.Encode(nil, buf.Bytes())
	meta["Compressor"] = "snappy"

	return a.Put(ctx, key, bytes.NewReader(encodedBytes), meta, options...)
}

func (a *S3) Del(ctx context.Context, key string) error {
	bucketName, key, err := a.getBucketAndKey(ctx, key)
	if err != nil {
		return err
	}

	input := &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}

	_, err = a.client.DeleteObject(ctx, input)
	return err
}

func (a *S3) DelMulti(ctx context.Context, keys []string) error {
	bucketsNameKeys := make(map[string][]string)
	for _, k := range keys {
		bucketName, key, err := a.getBucketAndKey(ctx, k)
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

		_, err := a.client.DeleteObjects(ctx, input)
		if err != nil {
			return err
		}
	}

	return nil
}

func (a *S3) Head(ctx context.Context, key string, attributes []string) (map[string]string, error) {
	bucketName, key, err := a.getBucketAndKey(ctx, key)
	if err != nil {
		return nil, err
	}

	input := &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}

	result, err := a.client.HeadObject(ctx, input)
	if err != nil {
		// todo 这里要处理 404
		//if aerr, ok := err.(awserr.RequestFailure); ok {
		//	if aerr.StatusCode() == 404 {
		//		return nil, nil
		//	}
		//}
		return nil, err
	}
	return getS3Meta(ctx, attributes, mergeHttpStandardHeaders(&HeadGetObjectOutputWrapper{
		headObjectOutput: result,
	})), nil
}

func (a *S3) ListObject(ctx context.Context, key string, prefix string, marker string, maxKeys int, delimiter string) ([]string, error) {
	bucketName, _, err := a.getBucketAndKey(ctx, key)
	if err != nil {
		return nil, err
	}

	input := &s3.ListObjectsInput{
		Bucket: aws.String(bucketName),
	}
	input.Prefix = aws.String(a.cfg.Prefix)
	if prefix != "" {
		input.Prefix = aws.String(a.cfg.Prefix + prefix)
	}
	input.Marker = aws.String(a.cfg.Prefix)
	if marker != "" {
		input.Marker = aws.String(a.cfg.Prefix + marker)
	}
	if maxKeys > 0 {
		input.MaxKeys = aws.Int32(int32(maxKeys))
	}
	if delimiter != "" {
		input.Delimiter = aws.String(delimiter)
	}

	result, err := a.client.ListObjects(ctx, input)
	if err != nil {
		return nil, err
	}

	keys := make([]string, 0)
	for _, v := range result.Contents {
		keys = append(keys, *v.Key)
	}

	return keys, nil
}

func (a *S3) SignURL(ctx context.Context, key string, expired int64, options ...SignOptions) (string, error) {
	bucketName, key, err := a.getBucketAndKey(ctx, key)
	if err != nil {
		return "", err
	}
	input := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}
	signOptions := DefaultSignOptions()
	for _, opt := range options {
		opt(signOptions)
	}
	if signOptions.process != nil {
		panic("process option is not supported for s3")
	}
	req, err := a.presignClient.PresignGetObject(ctx, input, func(options *s3.PresignOptions) {
		options.Expires = time.Duration(expired) * time.Second
	})
	if err != nil {
		return "", err
	}
	//return req.Presign(time.Duration(expired) * time.Second)
	return req.URL, nil
}

func (a *S3) Exists(ctx context.Context, key string) (bool, error) {
	bucketName, key, err := a.getBucketAndKey(ctx, key)
	if err != nil {
		return false, err
	}

	input := &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}
	_, err = a.client.HeadObject(ctx, input)
	if err == nil {
		return true, nil
	}
	// todo 这里要处理 404
	//if aerr, ok := err.(awserr.RequestFailure); ok {
	//	if aerr.StatusCode() == 404 {
	//		return false, nil
	//	}
	//}
	return false, err
}

func (a *S3) get(ctx context.Context, key string, options ...GetOptions) (*s3.GetObjectOutput, error) {
	bucketName, key, err := a.getBucketAndKey(ctx, key)
	if err != nil {
		return nil, err
	}

	input := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}
	setS3Options(ctx, options, input)
	result, err := a.client.GetObject(ctx, input)
	if err != nil {
		// todo 这里要处理 404
		//if aerr, ok := err.(awserr.Error); ok {
		//	if aerr.Code() == s3.ErrCodeNoSuchKey {
		//		return nil, nil
		//	}
		//}
		return nil, err
	}

	return result, nil
}

func getS3Meta(ctx context.Context, attributes []string, metaData map[string]*string) map[string]string {
	// https://github.com/aws/aws-sdk-go-v2/issues/445
	// aws 会将 meta 的首字母大写，在这里需要转换下
	res := make(map[string]string)
	for _, v := range attributes {
		key := strings.Title(v)
		if metaData[key] != nil {
			res[v] = *metaData[key]
		}
		// v2新版本不会将其变成大写
		// 所以需要兼容这块
		if metaData[v] != nil {
			res[v] = *metaData[v]
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
