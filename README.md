# EOS: Wrapper For Aliyun OSS And Amazon S3

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
bucket = "my-bucket" # 定义默认storage实例
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
func main() {
  cmp := eos.Load("storage").Build()

  res, err := cmp.Get(context.Background(), "key")
  if err != nil {
    log.Println("Get fail", err)
	return
  }
  
  fmt.Printf("res--------------->"+"%+v\n", res) 
}
```

Available operations：

```golang
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
```

https://aws.github.io/aws-sdk-go-v2/docs/migrating/