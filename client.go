package eos

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"os"
	"strings"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go/logging"
	"github.com/gotomicro/ego/core/elog"
	"go.opentelemetry.io/contrib/instrumentation/net/http/httptrace/otelhttptrace"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

const PackageName = "component.eos"

// Client object storage client interface
type Client interface {
	GetBucketName(ctx context.Context, key string) (string, error)
	Get(ctx context.Context, key string, options ...GetOptions) (string, error)
	GetBytes(ctx context.Context, key string, options ...GetOptions) ([]byte, error)
	GetAsReader(ctx context.Context, key string, options ...GetOptions) (io.ReadCloser, error)
	GetWithMeta(ctx context.Context, key string, attributes []string, options ...GetOptions) (io.ReadCloser, map[string]string, error)
	Put(ctx context.Context, key string, reader io.Reader, meta map[string]string, options ...PutOptions) error
	Del(ctx context.Context, key string) error
	DelMulti(ctx context.Context, keys []string) error
	Head(ctx context.Context, key string, meta []string) (map[string]string, error)
	ListObjects(ctx context.Context, continuationToken *string, options ...ListObjectsOption) (*ListObjectsResult, error)
	SignURL(ctx context.Context, key string, expired int64, options ...SignOptions) (string, error)
	Range(ctx context.Context, key string, offset int64, length int64) (io.ReadCloser, error)
	Exists(ctx context.Context, key string) (bool, error)
	Copy(ctx context.Context, srcKey, dstKey string, options ...CopyOption) error
	CreateMultipartUpload(ctx context.Context, key string) (*CreateMultipartUploadResult, error)
	CompleteMultipartUpload(ctx context.Context, key string, uploadID string, parts []MultiUploadCompletedPart) error
	SignUploadPartURL(ctx context.Context, key, uploadId string, partNumber int32, expired int64, options ...SignOptions) (*v4.PresignedHTTPRequest, error)
}

func newStorage(name string, cfg *BucketConfig, logger *elog.Component) (Client, error) {
	storageType := strings.ToLower(cfg.StorageType)
	switch storageType {
	case StorageTypeOSS:
		return newOSS(name, cfg, logger)
	case StorageTypeS3:
		return newS3(name, cfg, logger)
	case StorageTypeFile:
		return NewLocalFile(cfg.Endpoint)
	default:
		return nil, fmt.Errorf("unknown StorageType:\"%s\", only supports oss,s3", cfg.StorageType)
	}
}

func newS3(name string, cfg *BucketConfig, logger *elog.Component) (Client, error) {
	awsConfig := aws.Config{
		Region:      cfg.Region,
		Credentials: aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.AccessKeySecret, "")),
	}
	if cfg.Endpoint != "" {
		awsConfig.BaseEndpoint = aws.String(cfg.Endpoint)
	}
	if cfg.Debug {
		awsConfig.Logger = logging.NewStandardLogger(os.Stderr)
		awsConfig.ClientLogMode = aws.LogRetries | aws.LogRequest | aws.LogResponseWithBody
	}

	awsConfig.HTTPClient = newHttpClient(name, cfg, logger)
	service := s3.NewFromConfig(awsConfig, func(o *s3.Options) {
		o.UsePathStyle = cfg.S3ForcePathStyle
	})

	var s3Client *S3
	if len(cfg.Shards) > 0 {
		buckets := make(map[string]string)
		for _, v := range cfg.Shards {
			for i := 0; i < len(v); i++ {
				buckets[strings.ToLower(v[i:i+1])] = cfg.Bucket + "-" + v
			}
		}
		s3Client = &S3{
			ShardsBucket: buckets,
			client:       service,
		}
	} else {
		s3Client = &S3{
			BucketName: cfg.Bucket,
			client:     service,
		}
	}
	s3Client.cfg = cfg
	s3Client.presignClient = s3.NewPresignClient(service)

	return s3Client, nil
}

func newOSS(name string, cfg *BucketConfig, logger *elog.Component) (Client, error) {
	var opts = []oss.ClientOption{oss.HTTPClient(newHttpClient(name, cfg, logger))}
	if cfg.Debug {
		opts = append(opts, oss.SetLogLevel(oss.Debug))
	}
	client, err := oss.New(cfg.Endpoint, cfg.AccessKeyID, cfg.AccessKeySecret, opts...)
	if err != nil {
		return nil, err
	}

	var ossClient *OSS
	if len(cfg.Shards) > 0 {
		buckets := make(map[string]*oss.Bucket)
		for _, v := range cfg.Shards {
			bucket, err := client.Bucket(cfg.Bucket + "-" + v)
			if err != nil {
				return nil, err
			}
			for i := 0; i < len(v); i++ {
				buckets[strings.ToLower(v[i:i+1])] = bucket
			}
		}
		ossClient = &OSS{Shards: buckets}
	} else {
		bucket, err := client.Bucket(cfg.Bucket)
		if err != nil {
			return nil, err
		}
		ossClient = &OSS{Bucket: bucket}
	}
	ossClient.cfg = cfg

	return ossClient, nil
}

func newHttpClient(name string, cfg *BucketConfig, logger *elog.Component) *http.Client {
	httpCli := &http.Client{
		Timeout: time.Second * time.Duration(cfg.S3HttpTimeoutSecs),
	}
	var tp http.RoundTripper = createTransport(cfg)
	if cfg.EnableMetricInterceptor {
		tp = metricInterceptor(name, cfg, logger, tp)
	}
	if cfg.EnableTraceInterceptor {
		tp = traceLogReqIdInterceptor(name, cfg, logger, tp)
		if cfg.EnableClientTrace {
			tp = otelhttp.NewTransport(tp,
				otelhttp.WithClientTrace(func(ctx context.Context) *httptrace.ClientTrace {
					return otelhttptrace.NewClientTrace(ctx)
				}))
		} else {
			tp = otelhttp.NewTransport(tp)
		}
	}
	tp = fixedInterceptor(name, cfg, logger, tp)
	httpCli.Transport = tp

	return httpCli
}

func createTransport(config *BucketConfig) *http.Transport {
	dialer := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}

	return &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           dialer.DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          config.MaxIdleConns,
		IdleConnTimeout:       config.IdleConnTimeout,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DisableKeepAlives:     !config.EnableKeepAlives,
		MaxIdleConnsPerHost:   config.MaxIdleConnsPerHost,
	}
}

type Object struct {
	Key          string
	Size         int64
	LastModified time.Time
}

func newObjectFromS3(o *types.Object) *Object {
	res := &Object{}
	if o.Key != nil {
		res.Key = *o.Key
	}
	if o.Size != nil {
		res.Size = *o.Size
	}
	if o.LastModified != nil {
		res.LastModified = *o.LastModified
	}
	return res
}

func newObjectFromOss(o *oss.ObjectProperties) *Object {
	return &Object{
		Key:          o.Key,
		Size:         o.Size,
		LastModified: o.LastModified,
	}
}

type ListObjectsResult struct {
	Objects               []*Object
	NextContinuationToken *string
	IsTruncated           *bool
}

type CreateMultipartUploadResult struct {
	UploadId *string
}

func newCreateMultiUploadResFromS3(res *s3.CreateMultipartUploadOutput) *CreateMultipartUploadResult {
	return &CreateMultipartUploadResult{
		UploadId: res.UploadId,
	}
}

type MultiUploadCompletedPart struct {
	ETag       *string
	PartNumber *int32
}

func newMultiUploadCompletedPartsToS3(parts []MultiUploadCompletedPart) []types.CompletedPart {
	res := make([]types.CompletedPart, len(parts))
	for i, part := range parts {
		res[i] = types.CompletedPart{
			ETag:       part.ETag,
			PartNumber: part.PartNumber,
		}
	}
	return res
}
