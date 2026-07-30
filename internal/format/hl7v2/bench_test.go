package hl7v2

import "testing"

func BenchmarkParse(b *testing.B) {
	raw := sampleADT()
	b.SetBytes(int64(len(raw)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := dt.Parse(raw); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSerialize(b *testing.B) {
	root, err := dt.Parse(sampleADT())
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := dt.Serialize(root); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkResolve(b *testing.B) {
	root, err := dt.Parse(sampleADT())
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := dt.Resolve(root, "PID-5.1"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFlatten(b *testing.B) {
	root, err := dt.Parse(sampleADT())
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		dt.Flatten(root)
	}
}
