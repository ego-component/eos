package eos

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"

	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"go.uber.org/multierr"
	"google.golang.org/protobuf/proto"

	"github.com/ego-component/eos/eospb"
)

var _ Client = (*LocalFile)(nil)

// LocalFile is the implementation based on local files.
// For desktop APP or test.
type LocalFile struct {
	// path is the content root
	// all files are stored here.
	path string
	//nolint:unused
	l sync.Mutex
}

func NewLocalFile(path string) (*LocalFile, error) {
	err := os.MkdirAll(path, os.ModePerm)
	return &LocalFile{
		path: path,
	}, err
}

func (l *LocalFile) GetBucketName(ctx context.Context, key string) (string, error) {
	panic("implement me")
}

func (l *LocalFile) Get(ctx context.Context, key string, options ...GetOptions) (string, error) {
	data, err := l.GetBytes(ctx, key, options...)
	if err != nil {
		return "", err
	}
	return string(data), err
}

func (l *LocalFile) GetBytes(ctx context.Context, key string, options ...GetOptions) ([]byte, error) {
	rd, err := l.GetAsReader(ctx, key, options...)
	if err != nil || rd == nil {
		return nil, err
	}
	defer rd.Close()
	var buf bytes.Buffer
	_, err = io.Copy(&buf, rd)

	fileBytes := buf.Bytes()
	buffer := writeBuffer()
	buffer.Put(buf.Bytes())
	// 获取内容长度
	headerLength := buffer.Get32ByOffset(0)
	contentLength := buffer.Get32ByOffset(4)
	return fileBytes[8+headerLength : 8+headerLength+contentLength], err
}

// GetAsReader returns reader which you need to close it.
func (l *LocalFile) GetAsReader(ctx context.Context, key string, options ...GetOptions) (io.ReadCloser, error) {
	filename := l.initDir(key)
	file, err := os.Open(filename)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	return file, err
}

func (l *LocalFile) GetWithMeta(ctx context.Context, key string, attributes []string, options ...GetOptions) (io.ReadCloser, map[string]string, error) {
	rd, err := l.GetAsReader(ctx, key)
	if err != nil {
		return nil, nil, err
	}
	defer rd.Close()
	var buf bytes.Buffer
	if _, err = io.Copy(&buf, rd); err != nil {
		return nil, nil, err
	}

	fileBytes := buf.Bytes()
	buffer := writeBuffer()
	buffer.Put(buf.Bytes())
	// 获取内容长度
	headerLength := buffer.Get32ByOffset(0)
	contentLength := buffer.Get32ByOffset(4)

	metaBytes := buffer.GetByOffsetAndLength(8, headerLength)
	headerProto := eospb.LocalFileSegment{}
	if err = proto.Unmarshal(metaBytes, &headerProto); err != nil {
		return nil, nil, err
	}

	meta := make(map[string]string)
	for _, v := range attributes {
		meta[v] = headerProto.Header[v]
	}

	return io.NopCloser(bytes.NewBuffer(fileBytes[8+headerLength : 8+headerLength+contentLength])), meta, nil
}

// Put override the file
// It will create two files, one for content, one for meta.
func (l *LocalFile) Put(ctx context.Context, key string, reader io.Reader, meta map[string]string, options ...PutOptions) error {
	filename := l.initDir(key)
	f, err := os.OpenFile(filename, os.O_TRUNC|os.O_RDWR|os.O_CREATE, 0660)
	if err != nil {
		return err
	}
	defer f.Close()

	bufferWriter := writeBuffer()
	header := &eospb.LocalFileSegment{
		Header: meta,
	}
	headerBytes, err := proto.Marshal(header)
	if err != nil {
		return err
	}
	var contentBytes bytes.Buffer
	_, err = io.Copy(&contentBytes, reader)
	if err != nil {
		return err
	}

	bufferWriter.Put32(uint32(len(headerBytes)))
	bufferWriter.Put32(uint32(len(contentBytes.Bytes())))
	bufferWriter.Put(headerBytes)
	bufferWriter.Put(contentBytes.Bytes())
	_, err = f.Write(bufferWriter.Buffer())
	return err
}

func (l *LocalFile) Del(ctx context.Context, key string) error {
	filename := l.initDir(key)
	return os.Remove(filename)
}

func (l *LocalFile) DelMulti(ctx context.Context, keys []string) error {
	var err error
	for _, key := range keys {
		err = l.Del(ctx, key)
		if err != nil {
			err = multierr.Append(err, fmt.Errorf("faile to delete file, key %s, %w", key, err))
		}
	}
	return err
}

func (l *LocalFile) Head(ctx context.Context, key string, attributes []string) (map[string]string, error) {
	rd, err := l.GetAsReader(ctx, key)
	if err != nil || rd == nil {
		return nil, err
	}
	defer rd.Close()
	var buf bytes.Buffer

	if _, err = io.Copy(&buf, rd); err != nil {
		return nil, err
	}

	buffer := writeBuffer()
	buffer.Put(buf.Bytes())
	// 获取内容长度
	headerLength := buffer.Get32ByOffset(0)
	metaBytes := buffer.GetByOffsetAndLength(8, headerLength)
	headerProto := eospb.LocalFileSegment{}
	if err = proto.Unmarshal(metaBytes, &headerProto); err != nil {
		return nil, err
	}

	meta := make(map[string]string)
	for _, v := range attributes {
		meta[v] = headerProto.Header[v]
	}
	return meta, nil
}

func (l *LocalFile) ListObjects(ctx context.Context, continuationToken *string, options ...ListObjectsOption) (*ListObjectsResult, error) {
	panic("implement me")
}

func (l *LocalFile) SignURL(ctx context.Context, key string, expired int64, options ...SignOptions) (string, error) {
	panic("implement me")
}

func (l *LocalFile) Range(ctx context.Context, key string, offset int64, length int64) (io.ReadCloser, error) {
	rd, err := l.GetAsReader(ctx, key)
	if err != nil || rd == nil {
		return nil, err
	}
	defer rd.Close()
	var buf bytes.Buffer
	if _, err = io.Copy(&buf, rd); err != nil {
		return nil, err
	}

	fileBytes := buf.Bytes()
	buffer := writeBuffer()
	buffer.Put(buf.Bytes())
	// 获取内容长度
	headerLength := buffer.Get32ByOffset(0)
	contentLength := buffer.Get32ByOffset(4)
	uint32Length := uint32(length)
	if uint32Length > contentLength {
		uint32Length = contentLength
	}

	return io.NopCloser(bytes.NewBuffer(fileBytes[8+headerLength+uint32(offset) : 8+headerLength+uint32(offset)+uint32Length])), nil
}

func (l *LocalFile) Exists(ctx context.Context, key string) (bool, error) {
	panic("implement me")
}

func (l *LocalFile) Copy(ctx context.Context, srcKey, dstKey string, options ...CopyOption) error {
	srcPath := l.initDir(srcKey)
	srcFile, err := os.OpenFile(srcPath, os.O_TRUNC|os.O_RDWR|os.O_CREATE, 0660)
	if err != nil {
		return err
	}
	dstPath := l.initDir(dstKey)
	dstFile, err := os.OpenFile(dstPath, os.O_TRUNC|os.O_RDWR|os.O_CREATE, 0660)
	if err != nil {
		return err
	}
	_, err = io.Copy(dstFile, srcFile)
	return err
}

func (l *LocalFile) CreateMultipartUpload(ctx context.Context, key string) (*CreateMultipartUploadResult, error) {
	return nil, errors.New("not implemented")
}

func (l *LocalFile) CompleteMultipartUpload(ctx context.Context, key string, uploadID string, parts []MultiUploadCompletedPart) error {
	return errors.New("not implemented")
}

func (l *LocalFile) SignUploadPartURL(ctx context.Context, key, uploadId string, partNumber int32, expired int64, options ...SignOptions) (*v4.PresignedHTTPRequest, error) {
	return nil, errors.New("not implemented")
}

// initDir returns the entire path
func (l *LocalFile) initDir(key string) string {
	// compatible with Windows
	segs := strings.Split(key, "/")
	res := path.Join(segs...)
	res = path.Join(l.path, res)
	// it should never error
	_ = os.MkdirAll(filepath.Dir(res), os.ModePerm)
	return res
}
