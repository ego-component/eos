# EOSS: Wrapper For Aliyun OSS And Amazon S3

awos for node: https://github.com/shimohq/awos-js

## Features

- enable shards bucket
- add retry strategy
- avoid 404 status code:
  - `Get(objectName string) (string, error)` will return `"", nil` when object not exist
  - `Head(key string, meta []string) (map[string]string, error)` will return `nil, nil` when object not exist

## Installing

Use go get to retrieve the SDK to add it to your GOPATH workspace, or project's Go module dependencies.

```bash
go get github.com/ego-component/eos
```

## How to use
### config
```toml
[storage]
storageType = "oss" # oss|s3
accessKeyID = "xxx"
accessKeySecret = "xxx"
endpoint = "oss-cn-beijing.aliyuncs.com"
bucket = "aaa" # 定义默认storage实例
shards = []
  # 定义其他storage实例
  [storage.buckets.template] 
  bucket = "template-bucket"
  shards = []
  [storage.buckets.fileContent]
  bucket = "contents-bucket"
  shards = [
   "abcdefghijklmnopqr",
   "stuvwxyz0123456789"
  ]
```

```golang
import "github.com/ego-component/eos"

// 构建 os component
cmp := eoss.Load("storage").Build()
```

Available operations：

```golang
Get(ctx context.Context, key string, options ...GetOptions) (string, error)
GetBytes(ctx context.Context, key string, options ...GetOptions) ([]byte, error)
GetAsReader(ctx context.Context, key string, options ...GetOptions) (io.ReadCloser, error)
GetWithMeta(ctx context.Context, key string, attributes []string, options ...GetOptions) (io.ReadCloser, map[string]string, error)
Put(ctx context.Context, key string, reader io.ReadSeeker, meta map[string]string, options ...PutOptions) error
Del(ctx context.Context, key string) error
DelMulti(ctx context.Context, keys []string) error
Head(ctx context.Context, key string, meta []string) (map[string]string, error)
ListObject(ctx context.Context, key string, prefix string, marker string, maxKeys int, delimiter string) ([]string, error)
SignURL(ctx context.Context, key string, expired int64) (string, error)
GetAndDecompress(ctx context.Context, key string) (string, error)
GetAndDecompressAsReader(ctx context.Context, key string) (io.ReadCloser, error)
CompressAndPut(ctx context.Context, key string, reader io.ReadSeeker, meta map[string]string, options ...PutOptions) error
Range(ctx context.Context, key string, offset int64, length int64) (io.ReadCloser, error)
Exists(ctx context.Context, key string)(bool, error)
```

## Atomic create-only S3 Put

`PutIfAbsent()` sends a signed `If-None-Match: *` request and maps
`412 PreconditionFailed` to `ErrObjectAlreadyExists`. It is deliberately
fail-closed: S3 create-only is disabled unless the configured backend has
passed the atomic conditional-write probe.

```toml
[storage]
storageType = "s3"
s3CreateOnlySupported = true # enable only after the probe below passes
```

```go
err := storage.Put(ctx, key, body, metadata, eos.PutIfAbsent())
```

Compatibility and retry rules:

- Ordinary `Put` keeps its existing overwrite and retry behavior.
- `PutIfAbsent` makes exactly one HTTP attempt. Both eos retries and AWS SDK
  retries are disabled because replaying after a lost response can overwrite
  the winner on an incompatible S3 implementation.
- OSS and local-file clients return `ErrCreateOnlyUnsupported`.
- MinIO `RELEASE.2023-09-23T03-47-50Z` is known to ignore
  `If-None-Match: *`, report success, and overwrite the first object.
- MinIO support was added upstream by
  [minio/minio#19682](https://github.com/minio/minio/pull/19682). Do not infer
  safety from a version string alone: vendor rebuilds, gateways, or backports
  can change behavior. The explicit capability gate plus the live probe is the
  compatibility contract.

Run the opt-in probe against every endpoint, gateway path, bucket route, or
server image before setting `s3CreateOnlySupported = true`:

```bash
EOS_S3_INTEGRATION_ENDPOINT='http://127.0.0.1:9000' \
EOS_S3_INTEGRATION_ACCESS_KEY='...' \
EOS_S3_INTEGRATION_SECRET_KEY='...' \
EOS_S3_INTEGRATION_BUCKET='file-content' \
go test -run '^TestS3CreateOnlyIntegration$' -count=1 -v
```

The probe requires all of the following: first write succeeds; duplicate and
concurrent writes produce one winner plus `412`; the winner bytes remain
unchanged; ordinary `Put` still overwrites; probe objects are deleted and
verified absent. If any check fails, leave the capability disabled and do not
fall back to `Head`/`Exists` plus `Put`.
