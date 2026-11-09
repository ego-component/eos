package eos

import (
	"context"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

// MultipartUploader 分段上传，用于事先不知道大小、不便整体放在内存中的对象。
// 目前只有 S3 实现，使用方按需做类型断言。
//
// 对象在 CompleteMultipartUpload 成功后才可见；放弃上传时应调用 AbortMultipartUpload，
// 否则已上传的分段会一直占用存储空间
type MultipartUploader interface {
	// CreateMultipartUpload 元数据和 PutOptions 与 Put 相同，返回 uploadID
	CreateMultipartUpload(ctx context.Context, key string, meta map[string]string, options ...PutOptions) (string, error)
	// UploadPart partNumber 从 1 开始；除最后一个分段外，每个分段不小于 5MB
	UploadPart(ctx context.Context, key, uploadID string, partNumber int, reader io.ReadSeeker) (CompletedPart, error)
	// CompleteMultipartUpload parts 按 partNumber 升序
	CompleteMultipartUpload(ctx context.Context, key, uploadID string, parts []CompletedPart) error
	AbortMultipartUpload(ctx context.Context, key, uploadID string) error
}

// CompletedPart 已上传的分段
type CompletedPart struct {
	PartNumber int
	ETag       string
}

var _ MultipartUploader = (*S3)(nil)

func (a *S3) CreateMultipartUpload(ctx context.Context, key string, meta map[string]string, options ...PutOptions) (string, error) {
	bucketName, key, err := a.getBucketAndKey(ctx, key)
	if err != nil {
		return "", err
	}
	putOptions := DefaultPutOptions()
	for _, opt := range options {
		opt(putOptions)
	}
	input := &s3.CreateMultipartUploadInput{
		Bucket:             aws.String(bucketName),
		Key:                aws.String(key),
		Metadata:           aws.StringMap(meta),
		ContentType:        aws.String(putOptions.contentType),
		ContentEncoding:    putOptions.contentEncoding,
		ContentDisposition: putOptions.contentDisposition,
		CacheControl:       putOptions.cacheControl,
		Expires:            putOptions.expires,
	}
	output, err := a.client.CreateMultipartUploadWithContext(ctx, input)
	if err != nil {
		return "", err
	}
	return aws.StringValue(output.UploadId), nil
}

func (a *S3) UploadPart(ctx context.Context, key, uploadID string, partNumber int, reader io.ReadSeeker) (CompletedPart, error) {
	bucketName, key, err := a.getBucketAndKey(ctx, key)
	if err != nil {
		return CompletedPart{}, err
	}
	output, err := a.client.UploadPartWithContext(ctx, &s3.UploadPartInput{
		Body:       reader,
		Bucket:     aws.String(bucketName),
		Key:        aws.String(key),
		UploadId:   aws.String(uploadID),
		PartNumber: aws.Int64(int64(partNumber)),
	})
	if err != nil {
		return CompletedPart{}, err
	}
	return CompletedPart{PartNumber: partNumber, ETag: aws.StringValue(output.ETag)}, nil
}

func (a *S3) CompleteMultipartUpload(ctx context.Context, key, uploadID string, parts []CompletedPart) error {
	bucketName, key, err := a.getBucketAndKey(ctx, key)
	if err != nil {
		return err
	}
	completed := make([]*s3.CompletedPart, len(parts))
	for i, p := range parts {
		completed[i] = &s3.CompletedPart{PartNumber: aws.Int64(int64(p.PartNumber)), ETag: aws.String(p.ETag)}
	}
	_, err = a.client.CompleteMultipartUploadWithContext(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(bucketName),
		Key:             aws.String(key),
		UploadId:        aws.String(uploadID),
		MultipartUpload: &s3.CompletedMultipartUpload{Parts: completed},
	})
	return err
}

func (a *S3) AbortMultipartUpload(ctx context.Context, key, uploadID string) error {
	bucketName, key, err := a.getBucketAndKey(ctx, key)
	if err != nil {
		return err
	}
	_, err = a.client.AbortMultipartUploadWithContext(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(bucketName),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
	})
	return err
}
