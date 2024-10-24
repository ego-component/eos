package eos

//lint:ignore U1000
type localFileBytes struct {
	HeaderLength  uint32
	ContentLength uint32
	HeaderValue   []byte
	ContentValue  []byte
}
