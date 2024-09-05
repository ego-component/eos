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

	"go.uber.org/multierr"
)

// LocalFile is the implementation based on local files.
// For desktop APP or test.
type LocalFile struct {
	// path is the content root
	// all files are stored here.
	path string
	l    sync.Mutex
	// store in memory
	// TODO persistent
	meta map[string]map[string]string
}

func (l *LocalFile) GetClient() any {
	//TODO implement me
	return nil
}

func NewLocalFile(path string) (*LocalFile, error) {
	err := os.MkdirAll(path, os.ModePerm)
	return &LocalFile{
		path: path,
		meta: make(map[string]map[string]string),
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
	return buf.Bytes(), err
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
	data, err := l.GetAsReader(ctx, key)
	if err != nil {
		return nil, nil, err
	}
	meta, err := l.Head(ctx, key, attributes)
	if err != nil {
		return nil, nil, err
	}
	return data, meta, nil
}

func (l *LocalFile) GetAndDecompress(ctx context.Context, key string) (string, error) {
	return l.Get(ctx, key)
}

func (l *LocalFile) GetAndDecompressAsReader(ctx context.Context, key string) (io.ReadCloser, error) {
	return l.GetAsReader(ctx, key)
}

// Put override the file
// It will create two files, one for content, one for meta.
func (l *LocalFile) Put(ctx context.Context, key string, reader io.Reader, meta map[string]string, options ...PutOptions) error {
	filename := l.initDir(key)
	l.l.Lock()
	l.meta[key] = meta
	l.l.Unlock()
	f, err := os.OpenFile(filename, os.O_TRUNC|os.O_RDWR|os.O_CREATE, 0660)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, reader)
	return err
}

func (l *LocalFile) PutAndCompress(ctx context.Context, key string, reader io.Reader, meta map[string]string, options ...PutOptions) error {
	return l.Put(ctx, key, reader, meta)
}

func (l *LocalFile) Del(ctx context.Context, key string) error {
	filename := l.initDir(key)
	l.l.Lock()
	delete(l.meta, key)
	l.l.Unlock()
	return os.Remove(filename)
}

func (l *LocalFile) DelMulti(ctx context.Context, keys []string) error {
	var res error
	for _, key := range keys {
		err := l.Del(ctx, key)
		if err != nil {
			err = multierr.Append(res, fmt.Errorf("faile to delete file, key %s, %w", key, err))
		}
	}
	return res
}

func (l *LocalFile) Head(ctx context.Context, key string, attributes []string) (map[string]string, error) {
	l.l.Lock()
	defer l.l.Unlock()
	fileMeta, ok := l.meta[key]
	if !ok {
		return map[string]string{}, nil
	}
	meta := make(map[string]string)
	for _, v := range attributes {
		meta[v] = fileMeta[v]
	}
	return meta, nil
}

func (l *LocalFile) ListObject(ctx context.Context, key string, prefix string, marker string, maxKeys int, delimiter string) ([]string, error) {
	panic("implement me")
}

func (l *LocalFile) SignURL(ctx context.Context, key string, expired int64, options ...SignOptions) (string, error) {
	panic("implement me")
}

func (l *LocalFile) Range(ctx context.Context, key string, offset int64, length int64) (io.ReadCloser, error) {
	panic("implement me")
}

func (l *LocalFile) Exists(ctx context.Context, key string) (bool, error) {
	l.l.Lock()
	defer l.l.Unlock()
	_, ok := l.meta[key]
	return ok, nil
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
