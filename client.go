package eos

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptrace"
	"strings"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/gotomicro/ego/core/elog"
	"go.opentelemetry.io/contrib/instrumentation/net/http/httptrace/otelhttptrace"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

const PackageName = "component.eoss"

// Client object storage client interface
type Client interface {
	GetBucketName(ctx context.Context, key string) (string, error)
	Get(ctx context.Context, key string, options ...GetOptions) (string, error)
	GetBytes(ctx context.Context, key string, options ...GetOptions) ([]byte, error)
	GetAsReader(ctx context.Context, key string, options ...GetOptions) (io.ReadCloser, error)
	GetWithMeta(ctx context.Context, key string, attributes []string, options ...GetOptions) (io.ReadCloser, map[string]string, error)
	GetAndDecompress(ctx context.Context, key string) (string, error)
	GetAndDecompressAsReader(ctx context.Context, key string) (io.ReadCloser, error)
	Put(ctx context.Context, key string, reader io.ReadSeeker, meta map[string]string, options ...PutOptions) error
	PutAndCompress(ctx context.Context, key string, reader io.ReadSeeker, meta map[string]string, options ...PutOptions) error
	Del(ctx context.Context, key string) error
	DelMulti(ctx context.Context, keys []string) error
	Head(ctx context.Context, key string, meta []string) (map[string]string, error)
	ListObject(ctx context.Context, key string, prefix string, marker string, maxKeys int, delimiter string) ([]string, error)
	SignURL(ctx context.Context, key string, expired int64, options ...SignOptions) (string, error)
	Range(ctx context.Context, key string, offset int64, length int64) (io.ReadCloser, error)
	Exists(ctx context.Context, key string) (bool, error)
	Copy(ctx context.Context, srcKey, dstKey string, options ...CopyOption) error
}

func newStorage(name string, cfg *BucketConfig, logger *elog.Component) (Client, error) {
	storageType := strings.ToLower(cfg.StorageType)

	if storageType == StorageTypeOSS {
		var opts = []oss.ClientOption{oss.HTTPClient(newHttpClient(name, cfg, logger))}
		if cfg.Debug {
			opts = append(opts, oss.SetLogLevel(oss.Debug))
		}
		client, err := oss.New(cfg.Endpoint, cfg.AccessKeyID, cfg.AccessKeySecret, opts...)
		if err != nil {
			return nil, err
		}

		var ossClient *OSS
		if cfg.Shards != nil && len(cfg.Shards) > 0 {
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

			ossClient = &OSS{
				Shards: buckets,
			}
		} else {
			bucket, err := client.Bucket(cfg.Bucket)
			if err != nil {
				return nil, err
			}

			ossClient = &OSS{
				Bucket: bucket,
			}
		}
		ossClient.cfg = cfg
		if cfg.EnableCompressor {
			// 目前仅支持 gzip
			if comp, ok := compressors[cfg.CompressType]; ok {
				ossClient.compressor = comp
			} else {
				logger.Warn("unknown type", elog.String("name", cfg.CompressType))
			}
		}
		return ossClient, nil
	} else if storageType == StorageTypeS3 {
		var config *aws.Config

		// use minio
		if cfg.S3ForcePathStyle {
			config = &aws.Config{
				Region:           aws.String(cfg.Region),
				DisableSSL:       aws.Bool(!cfg.SSL),
				Credentials:      credentials.NewStaticCredentials(cfg.AccessKeyID, cfg.AccessKeySecret, ""),
				Endpoint:         aws.String(cfg.Endpoint),
				S3ForcePathStyle: aws.Bool(true),
			}
		} else {
			config = &aws.Config{
				Region:      aws.String(cfg.Region),
				DisableSSL:  aws.Bool(!cfg.SSL),
				Credentials: credentials.NewStaticCredentials(cfg.AccessKeyID, cfg.AccessKeySecret, ""),
			}
			if cfg.Endpoint != "" {
				config.Endpoint = aws.String(cfg.Endpoint)
			}
		}
		if cfg.Debug {
			config.LogLevel = aws.LogLevel(aws.LogDebugWithHTTPBody | aws.LogDebugWithSigning)
			slog.Default().Enabled(context.Background(), slog.LevelDebug)
		}

		config.HTTPClient = newHttpClient(name, cfg, logger)
		service := s3.New(session.Must(session.NewSession(config)))

		var s3Client *S3
		if cfg.Shards != nil && len(cfg.Shards) > 0 {
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
		if cfg.EnableCompressor {
			// 目前仅支持 gzip
			if comp, ok := compressors[cfg.CompressType]; ok {
				s3Client.compressor = comp
			} else {
				logger.Warn("unknown type", elog.String("name", cfg.CompressType))
			}
		}
		return s3Client, nil
	} else {
		return nil, fmt.Errorf("unknown StorageType:\"%s\", only supports oss,s3", cfg.StorageType)
	}
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
