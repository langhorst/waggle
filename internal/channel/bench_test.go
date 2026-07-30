package channel

import (
	"context"
	"testing"
)

// BenchmarkPipeline measures one message through the full in-memory
// pipeline: parse, transform serialization, one destination send.
func BenchmarkPipeline(b *testing.B) {
	out := &fakeOut{}
	ch, src := newTestChannel(NewMemoryRecorder(), &Destination{ID: "d1", OutType: hl7Type(), Adapter: out})
	if err := ch.Start(context.Background()); err != nil {
		b.Fatal(err)
	}
	defer ch.Stop()

	raw := []byte(sampleHL7)
	b.SetBytes(int64(len(raw)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rec, err := src.deliver(context.Background(), raw, nil)
		if err != nil {
			b.Fatal(err)
		}
		<-rec.Done
	}
}
