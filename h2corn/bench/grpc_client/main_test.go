package main

import (
	"testing"
	"time"

	"google.golang.org/grpc/metadata"
)

func TestHistogramReportsQuantileUpperBounds(t *testing.T) {
	h := histogram{}
	for range 100 {
		h.add(time.Millisecond)
	}
	for name, seconds := range h.percentiles() {
		if seconds < .001 || seconds > .00101 {
			t.Fatalf("%s = %g is outside the 1%% bucket containing 1 ms", name, seconds)
		}
	}
}

func TestTerminalMetadataIsRequired(t *testing.T) {
	headers := metadata.Pairs("x-bench-case", "U32")
	if err := validateMetadata(headers, nil, "U32"); err == nil {
		t.Fatal("accepted a response with no terminal benchmark metadata")
	}
	if err := validateMetadata(headers, metadata.Pairs("x-bench-finished", "yes"), "U32"); err != nil {
		t.Fatal(err)
	}
}

func TestFinitePlanRejectsAnUnknownShapeBeforeReadingDescriptors(t *testing.T) {
	_, err := run(plan{Concurrency: 1, Messages: 1, Drain: 1, Requests: 1, Shape: "unknown"})
	if err == nil || err.Error() != "unknown shape or mode" {
		t.Fatalf("expected unknown shape rejection, got %v", err)
	}
}
