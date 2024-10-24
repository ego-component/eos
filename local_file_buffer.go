package eos

import (
	"encoding/binary"
)

// Buffer is a buffer to read/write integers.
type Buffer struct {
	endian binary.ByteOrder
	offset int
	buf    []byte
}

// WriteBuffer utility to create *Buffer
func writeBuffer() *Buffer {
	return &Buffer{
		endian: binary.BigEndian,
		buf:    make([]byte, 0),
	}
}

// Len returns length of buffer
func (b *Buffer) Len() uint32 {
	return uint32(len(b.buf))
}

// Buffer returns
func (b *Buffer) Buffer() []byte {
	return b.buf
}

// Grow 简单点，先按成倍翻倍
func (b *Buffer) Grow(i int) {
	// 这里还可以优化
	//b.buf = append(b.buf, make([]byte, 2*len(b.buf)+i)...)
	b.buf = append(b.buf, make([]byte, i)...)
}

// Put32 appends uint32 to Buffer
func (b *Buffer) Put32(v uint32) {
	b.Grow(4)
	b.endian.PutUint32(b.buf[b.offset:b.offset+4], v)
	b.offset += 4
}

// Put appends slice of byte to Buffer
func (b *Buffer) Put(v []byte) (offset uint32, length uint32) {
	length = uint32(len(v))
	b.Grow(int(length))
	copy(b.buf[b.offset:b.offset+int(length)], v)
	offset = uint32(b.offset)
	b.offset += int(length)
	return
}

// Get32ByOffset returns uint32
func (b *Buffer) Get32ByOffset(offset uint32) uint32 {
	v := b.endian.Uint32(b.buf[offset : offset+4])
	return v
}

// GetByOffsetAndLength returns uint32
func (b *Buffer) GetByOffsetAndLength(offset uint32, length uint32) []byte {
	return b.buf[offset : offset+length]
}
