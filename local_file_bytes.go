package eos

//nolint:unused
type localFileBytes struct {
	HeaderLength  uint32
	ContentLength uint32
	HeaderValue   []byte
	ContentValue  []byte
}
