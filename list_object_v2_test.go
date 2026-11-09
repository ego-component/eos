package eos

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testListObjectV2 三种存储共用的 ListObjectV2 用例
func testListObjectV2(t *testing.T, client Client) {
	ctx := context.TODO()
	base := fmt.Sprintf("lov2-%d/", time.Now().UnixNano())
	keys := []string{base + "a/1", base + "a/2", base + "a/b/3", base + "c/4"}
	for _, k := range keys {
		require.NoError(t, client.Put(ctx, k, strings.NewReader("content"), nil))
	}
	t.Cleanup(func() {
		for _, k := range keys {
			_ = client.Del(ctx, k)
		}
	})

	objectKeys := func(res *ListObjectV2Result) []string {
		var out []string
		for _, o := range res.Object {
			out = append(out, o.Key)
		}
		return out
	}
	str := func(s string) *string { return &s }
	i64 := func(i int64) *int64 { return &i }

	t.Run("prefix only", func(t *testing.T) {
		res, err := client.ListObjectV2(ctx, base, base)
		require.NoError(t, err)
		// 返回的 key 不带 cfg.Prefix，可以直接用于 Get
		assert.Equal(t, keys, objectKeys(res))
		assert.Empty(t, res.CommonPrefixes)
		assert.False(t, res.IsTruncated)
		assert.Nil(t, res.ContinuationToken)
		for _, o := range res.Object {
			assert.Positive(t, o.Size)
			assert.False(t, o.LastModified.IsZero())
		}
		data, err := client.Get(ctx, res.Object[0].Key)
		require.NoError(t, err)
		assert.Equal(t, "content", data)
	})

	t.Run("delimiter", func(t *testing.T) {
		res, err := client.ListObjectV2(ctx, base, base, ListObjectWithDelimiter(str("/")))
		require.NoError(t, err)
		assert.Empty(t, res.Object)
		assert.Equal(t, []string{base + "a/", base + "c/"}, res.CommonPrefixes)

		res, err = client.ListObjectV2(ctx, base, base+"a/", ListObjectWithDelimiter(str("/")))
		require.NoError(t, err)
		assert.Equal(t, []string{base + "a/1", base + "a/2"}, objectKeys(res))
		assert.Equal(t, []string{base + "a/b/"}, res.CommonPrefixes)
	})

	t.Run("pagination", func(t *testing.T) {
		var got []string
		var token *string
		pages := 0
		for {
			opts := []ListObjectV2Option{ListObjectWithMaxKeys(i64(1))}
			if token != nil {
				opts = append(opts, ListObjectWithContinuationToken(token))
			}
			res, err := client.ListObjectV2(ctx, base, base, opts...)
			require.NoError(t, err)
			pages++
			require.LessOrEqual(t, pages, len(keys)+1, "pagination does not terminate")
			got = append(got, objectKeys(res)...)
			assert.Equal(t, res.IsTruncated, res.ContinuationToken != nil)
			if !res.IsTruncated {
				break
			}
			token = res.ContinuationToken
		}
		assert.Equal(t, keys, got)
	})

	t.Run("pagination with delimiter", func(t *testing.T) {
		var got []string
		var token *string
		for i := 0; i < 3; i++ {
			opts := []ListObjectV2Option{ListObjectWithMaxKeys(i64(1)), ListObjectWithDelimiter(str("/"))}
			if token != nil {
				opts = append(opts, ListObjectWithContinuationToken(token))
			}
			res, err := client.ListObjectV2(ctx, base, base, opts...)
			require.NoError(t, err)
			got = append(got, res.CommonPrefixes...)
			if !res.IsTruncated {
				break
			}
			token = res.ContinuationToken
		}
		assert.Equal(t, []string{base + "a/", base + "c/"}, got)
	})

	t.Run("start after", func(t *testing.T) {
		res, err := client.ListObjectV2(ctx, base, base, ListObjectWithStartAfter(str(base+"a/2")))
		require.NoError(t, err)
		assert.Equal(t, []string{base + "a/b/3", base + "c/4"}, objectKeys(res))
	})
}

func TestLocalFile_ListObjectV2(t *testing.T) {
	client, err := NewLocalFile(t.TempDir())
	require.NoError(t, err)
	testListObjectV2(t, client)
}

func TestS3_ListObjectV2(t *testing.T) {
	if os.Getenv("BUCKET") == "" {
		t.Skip("BUCKET not set")
	}
	testListObjectV2(t, awsCmp.DefaultClient())
}

func TestOSS_ListObjectV2(t *testing.T) {
	if os.Getenv("BUCKET") == "" {
		t.Skip("BUCKET not set")
	}
	testListObjectV2(t, ossCmp.DefaultClient())
}
