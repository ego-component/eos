package eos

import (
	"io"
	"os"
	"runtime"
	"testing"
)

// TestCompress_gzip
// src_path 原始文件
// target_path 生成的目标文件
// 测试读取文件后压缩 输出到指定path 对比是否正常
func TestCompress_gzip(t *testing.T) {
	path := os.Getenv("src_path")
	source, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	//reader, _, err := DefaultGzipCompressor.Compress(source)
	//if err != nil {
	//	panic(err)
	//}
	targetPath := os.Getenv("target_path")
	_, err = os.Stat(targetPath)
	if err == nil {
		os.Remove(targetPath)
	}
	target, err := os.Create(targetPath)
	if err != nil {
		panic(err)
	}
	defer target.Close()
	defer source.Close()
	_, err = io.Copy(target, source)
	if err != nil {
		panic(err)
	}
}

func TestGetLength(t *testing.T) {
	path := os.Getenv("src_path")
	source, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	t.Logf("内存分配 (HeapAlloc): %v bytes\n", m.HeapAlloc)
	t.Logf("内存总分配 (HeapSys): %v bytes\n", m.HeapSys)
	t.Logf("内存被释放但还未被重新分配 (HeapIdle): %v bytes\n", m.HeapIdle)
	t.Logf("内存仍然被使用 (HeapInuse): %v bytes\n", m.HeapInuse)
	//length, err := GetReaderLength(source)
	t.Logf("-----------------------------")
	runtime.ReadMemStats(&m)
	t.Logf("内存分配 (HeapAlloc): %v bytes\n", m.HeapAlloc)
	t.Logf("内存总分配 (HeapSys): %v bytes\n", m.HeapSys)
	t.Logf("内存被释放但还未被重新分配 (HeapIdle): %v bytes\n", m.HeapIdle)
	t.Logf("内存仍然被使用 (HeapInuse): %v bytes\n", m.HeapInuse)
	if err != nil {
		panic(err)
	}
	//t.Logf("length %d", length)

	//seeker, _, _ := DefaultGzipCompressor.Compress(source)
	targetPath := os.Getenv("target_path")
	_, err = os.Stat(targetPath)
	if err == nil {
		os.Remove(targetPath)
	}
	target, err := os.Create(targetPath)
	if err != nil {
		panic(err)
	}
	defer target.Close()
	_, err = io.Copy(target, source)
	if err != nil {
		panic(err)
	}
}
