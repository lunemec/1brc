package main

import "testing"

func TestMaskPoolScalarFallbackOwnership(t *testing.T) {
	testMaskPoolImmediateReuse(t, chunkReaderScalar)
	testMaskPoolAllShortEOFViews(t, chunkReaderScalar)
}
