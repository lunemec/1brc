//go:build !amd64 || !goexperiment.simd

package main

func supportsAVX2Masks() bool {
	return false
}

func semicolonMask64AVX2(data []byte) uint64 {
	return semicolonMask64(data)
}
