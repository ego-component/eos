package eos

import (
	"bytes"
	"context"
	"io"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type LocalFileTestSuite struct {
	suite.Suite
	oss  *LocalFile
	path string
}

func (s *LocalFileTestSuite) SetupSuite() {
	s.path = path.Join("./", "local_file_test")
	err := os.MkdirAll(s.path, os.ModePerm)
	require.NoError(s.T(), err)
	oss, err := NewLocalFile(s.path)
	require.NoError(s.T(), err)
	s.oss = oss
}

//func (s *LocalFileTestSuite) TearDownSuite() {
//	_ = os.RemoveAll(s.path)
//}

func (s *LocalFileTestSuite) TestCRUD() {
	testCases := []struct {
		name string
		// prepare data
		before    func(t *testing.T)
		key       string
		inputData string
		wantData  string
		wantErr   error
	}{
		{
			name: "new object",
			before: func(t *testing.T) {
				// do nothing
			},
			key:       "TestPut_New_OBJECT",
			inputData: "hello, this is a new object",
		},
		{
			name: "key existing",
			before: func(t *testing.T) {
				filename := path.Join(s.path, "TestPut_Key_EXISTING")
				f, err := os.OpenFile(filename, os.O_TRUNC|os.O_RDWR|os.O_CREATE, 0660)
				require.NoError(t, err)
				_, err = f.WriteString("hello, this is existing message")
				require.NoError(t, err)
			},
			key:       "TestPut_Key_EXISTING",
			inputData: "hello, this is new data",
		},
	}
	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			tc.before(t)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err := s.oss.Put(ctx, tc.key, bytes.NewReader([]byte(tc.inputData)), map[string]string{})
			require.NoError(t, err)
			data, err := s.oss.Get(ctx, tc.key)
			require.NoError(t, err)
			assert.Equal(t, data, tc.inputData)
			err = s.oss.Del(ctx, tc.key)
			require.NoError(t, err)
			_, err = s.oss.Get(ctx, tc.key)
			assert.Nil(t, err)
		})
	}
}

func (s *LocalFileTestSuite) TestPutAndGet() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	key1 := "TestDelMulti_KEY1"
	key2 := "TestDelMulti_KEY2"
	err := s.oss.Put(ctx, key1, bytes.NewReader([]byte("hello")), nil, nil)
	require.NoError(s.T(), err)
	err = s.oss.Put(ctx, key2, bytes.NewReader([]byte("hello")), nil, nil)
	require.NoError(s.T(), err)
	contentValue, err := s.oss.Get(ctx, key1)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), "hello", contentValue)
}

func (s *LocalFileTestSuite) TestPutAndGetWithMeta() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	key := "TestPutAndGetMetaKey1"
	err := s.oss.Put(ctx, key, bytes.NewReader([]byte("hello")), map[string]string{
		"hello": "askuy",
	}, nil)
	require.NoError(s.T(), err)
	contentValue, meta, err := s.oss.GetWithMeta(ctx, key, []string{"hello"})
	assert.NoError(s.T(), err)
	info, err := io.ReadAll(contentValue)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), "hello", string(info))
	assert.Equal(s.T(), map[string]string{
		"hello": "askuy",
	}, meta)
}

func (s *LocalFileTestSuite) TestPutAndGetMeta() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	key := "TestPutAndGetMetaKey1"
	err := s.oss.Put(ctx, key, bytes.NewReader([]byte("hello")), map[string]string{
		"hello": "askuy",
	}, nil)
	require.NoError(s.T(), err)
	meta, err := s.oss.Head(ctx, key, []string{"hello"})
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), map[string]string{
		"hello": "askuy",
	}, meta)
}

func (s *LocalFileTestSuite) TestDelMulti() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	key1 := "TestDelMulti_KEY1"
	key2 := "TestDelMulti_KEY2"
	err := s.oss.Put(ctx, key1, bytes.NewReader([]byte("hello")), nil, nil)
	require.NoError(s.T(), err)
	err = s.oss.Put(ctx, key2, bytes.NewReader([]byte("hello")), nil, nil)
	require.NoError(s.T(), err)
	err = s.oss.DelMulti(ctx, []string{key1, key2})
	require.NoError(s.T(), err)
}

func (s *LocalFileTestSuite) TestGetBucketName() {
	assert.Panics(s.T(), func() {
		s.oss.GetBucketName(context.Background(), "key")
	})
}

func (s *LocalFileTestSuite) TestGetXX() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	key1 := "TestGetXX_KEY1"
	rd, meta, err := s.oss.GetWithMeta(ctx, key1, []string{"hello"})
	require.NoError(s.T(), err)
	assert.Equal(s.T(), map[string]string{"hello": "world"}, meta)
	data, err := io.ReadAll(rd)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), "hello", string(data))

	data, err = s.oss.GetBytes(ctx, key1)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), "hello", string(data))

	rd, err = s.oss.GetAsReader(ctx, key1)
	data, err = io.ReadAll(rd)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), "hello", string(data))

}

func TestLocalFile(t *testing.T) {
	suite.Run(t, new(LocalFileTestSuite))
}
