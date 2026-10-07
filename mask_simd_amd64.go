//go:build amd64 && goexperiment.simd

package main

import "simd/archsimd"

func supportsAVX2Masks() bool {
	return archsimd.X86.AVX2()
}

func semicolonMask64AVX2(data []byte) uint64 {
	_ = data[63]
	delimiter := archsimd.BroadcastUint8x32(';')
	low := archsimd.LoadUint8x32(data).Equal(delimiter).ToBits()
	high := archsimd.LoadUint8x32(data[32:]).Equal(delimiter).ToBits()
	return uint64(low) | uint64(high)<<32
}
