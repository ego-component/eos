package eos

// Put your environment configuration in ".env-oss"

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/gotomicro/ego/core/econf"
	"github.com/stretchr/testify/assert"
)

const (
	guid         = "test123"
	content      = "aaaaaa"
	expectLength = 6
	expectHead   = 1

	compressGUID    = "test123-snappy"
	compressContent = "snappy-contentsnappy-contentsnappy-contentsnappy-content"
)

var (
	ossCmp *Component
)

var ossConfs = `
[eos.oss]
debug = false
storageType = "oss"
s3HttpTransportMaxConnsPerHost = 100
s3HttpTransportIdleConnTimeout = "90s"
accessKeyID = "%s"
accessKeySecret = "%s"
endpoint = "%s"
bucket = "%s"
s3ForcePathStyle = false 
region = "%s"
ssl = false
shards = [%s]
compressLimit = 0
prefix = "abc-01"
enableCompressor = %t
compressType = "%s"
`

func init() {
	ossCmp = newOssCmp(os.Getenv("BUCKET"), "", true, "gzip")
}

func newOssCmp(bucket string, shards string, enableCompressor bool, compressType string) *Component {
	newConfs := fmt.Sprintf(ossConfs, os.Getenv("AK_ID"), os.Getenv("AK_SECRET"), os.Getenv("ENDPOINT"),
		bucket, os.Getenv("REGION"), shards, enableCompressor, compressType)
	if err := econf.LoadFromReader(strings.NewReader(newConfs), toml.Unmarshal); err != nil {
		panic(err)
	}
	cmp := Load("eos.oss").Build()
	return cmp
}

func TestOSS_GetBucketName(t *testing.T) {
	bucketShard := os.Getenv("BUCKET_SHARD")
	cmp := newOssCmp(bucketShard, `"abcdefghijklmnopqr", "stuvwxyz0123456789"`, true, "gzip")

	ctx := context.TODO()
	bn, err := cmp.GetBucketName(ctx, "fasdfsfsfsafsf")
	assert.NoError(t, err)
	assert.Equal(t, bucketShard+"-abcdefghijklmnopqr", bn)

	bn, err = cmp.GetBucketName(ctx, "19999999")
	assert.NoError(t, err)
	assert.Equal(t, bucketShard+"-stuvwxyz0123456789", bn)
}

func TestOSS_Put(t *testing.T) {
	ctx := context.TODO()
	meta := make(map[string]string)
	meta["head"] = strconv.Itoa(expectHead)
	meta["length"] = strconv.Itoa(expectLength)

	err := ossCmp.Put(ctx, guid, strings.NewReader(content), meta)
	assert.NoError(t, err)

	err = ossCmp.Put(ctx, guid, bytes.NewReader([]byte(content)), meta)
	assert.NoError(t, err)
}

func TestOSS_Head(t *testing.T) {
	ctx := context.TODO()
	attributes := make([]string, 0)
	attributes = append(attributes, "head")
	var res map[string]string
	var err error
	var head int
	var length int

	res, err = ossCmp.Head(ctx, guid, attributes)
	assert.NoError(t, err)

	head, err = strconv.Atoi(res["head"])
	assert.NoError(t, err)

	attributes = append(attributes, "length")
	attributes = append(attributes, "Content-Type")
	res, err = ossCmp.Head(ctx, guid, attributes)
	assert.NoError(t, err)

	head, err = strconv.Atoi(res["head"])
	assert.NoError(t, err)
	length, err = strconv.Atoi(res["length"])
	assert.NoError(t, err)
	assert.Equal(t, expectHead, head)
	assert.Equal(t, expectLength, length)
	assert.Equal(t, "text/plain", res["Content-Type"])
}

func TestOSS_Get(t *testing.T) {
	ctx := context.TODO()

	TestOSS_Put(t)
	res, err := ossCmp.Get(ctx, guid)
	assert.NoError(t, err)
	assert.Equal(t, content, res)

	res1, err := ossCmp.GetAsReader(ctx, guid)
	assert.NoError(t, err)
	defer res1.Close()

	byteRes, _ := ioutil.ReadAll(res1)
	assert.Equal(t, content, string(byteRes))

	opts := []GetOptions{}
	// TODO EnableCRCValidation()
	resBytes, err := ossCmp.GetBytes(ctx, guid, opts...)
	assert.NoError(t, err)
	assert.Equal(t, content, string(resBytes))

	res, err = ossCmp.Get(ctx, guid, opts...)
	assert.NoError(t, err)
	assert.Equal(t, content, res)
}

func TestOSS_GetWithMeta(t *testing.T) {
	ctx := context.TODO()
	attributes := make([]string, 0)
	attributes = append(attributes, "head")
	res, meta, err := ossCmp.GetWithMeta(ctx, guid, attributes)
	assert.NoError(t, err)
	defer res.Close()

	byteRes, _ := ioutil.ReadAll(res)
	assert.Equal(t, content, string(byteRes))

	head, err := strconv.Atoi(meta["head"])
	assert.NoError(t, err)
	assert.Equal(t, expectHead, head)
}

func TestOSS_SignURL(t *testing.T) {
	ctx := context.TODO()
	res, err := ossCmp.SignURL(ctx, guid, 60)
	assert.NoError(t, err)
	assert.NotEmpty(t, res)
}

func TestOSS_ListObject(t *testing.T) {
	ctx := context.TODO()
	res, err := ossCmp.ListObject(ctx, guid, guid[0:4], "", 10, "")
	assert.NoError(t, err)
	assert.NotEmpty(t, res)
}

func TestOSS_Del(t *testing.T) {
	ctx := context.TODO()
	err := ossCmp.Del(ctx, guid)
	assert.NoError(t, err)
}

func TestOSS_DelMulti(t *testing.T) {
	ctx := context.TODO()
	keys := []string{"aaa", "bb0", "ccc"}
	for _, key := range keys {
		err := ossCmp.Put(ctx, key, strings.NewReader("2333333"), nil)
		assert.NoError(t, err)
	}

	err := ossCmp.DelMulti(ctx, keys)
	assert.NoError(t, err)

	for _, key := range keys {
		res, err := ossCmp.Get(ctx, key)
		assert.NoError(t, err)
		assert.Empty(t, res)
	}
}

func TestOSS_GetNotExist(t *testing.T) {
	ctx := context.TODO()
	res1, err := ossCmp.Get(ctx, guid+"123")
	assert.NoError(t, err)
	assert.Empty(t, res1)

	attributes := make([]string, 0)
	attributes = append(attributes, "head")
	res2, err := ossCmp.Head(ctx, guid+"123", attributes)
	assert.NoError(t, err)
	assert.Empty(t, res2)
}

func TestOSS_Range(t *testing.T) {
	cmp := newOssCmp(os.Getenv("BUCKET"), "", false, "")

	ctx := context.TODO()
	cmp.Del(ctx, guid)
	meta := make(map[string]string)
	err := cmp.Put(ctx, guid, strings.NewReader("123456"), meta)
	assert.NoError(t, err)

	res, err := cmp.Range(ctx, guid, 3, 3)
	assert.NoError(t, err)

	byteRes, err := ioutil.ReadAll(res)
	assert.NoError(t, err)
	assert.Equal(t, "456", string(byteRes))
}

func TestOSS_Exists(t *testing.T) {
	ctx := context.TODO()
	meta := make(map[string]string)
	err := ossCmp.Put(ctx, guid, strings.NewReader("123456"), meta)
	assert.NoError(t, err)

	// test exists
	ok, err := ossCmp.Exists(ctx, guid)
	assert.NoError(t, err)
	assert.Equal(t, true, ok)

	err = ossCmp.Del(ctx, guid)
	assert.NoError(t, err)

	// test not exists
	ok, err = ossCmp.Exists(ctx, guid)
	assert.NoError(t, err)
	assert.Equal(t, false, ok)
}
