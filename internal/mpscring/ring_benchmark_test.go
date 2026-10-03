package mpscring

import (
	"fmt"
	"math/bits"
	"runtime"
	"sync"
	"testing"
	"unsafe"
)

const (
	benchmarkRingCapacity  = 4_096
	benchmarkProducerCount = 4
)

var benchmarkChecksum uint64

// BenchmarkMPSCRingValueVsInPlace prints one row for each payload size and API
// variant. Run it with:
//
//	go test ./internal/mpscring -run '^$' \
//		-bench '^BenchmarkMPSCRingValueVsInPlace$' -benchmem
func BenchmarkMPSCRingValueVsInPlace(b *testing.B) {
	benchmarkPayloadSize[[1]uint64](b)
	benchmarkPayloadSize[[2]uint64](b)
	benchmarkPayloadSize[[3]uint64](b)
	benchmarkPayloadSize[[4]uint64](b)
	benchmarkPayloadSize[[5]uint64](b)
	benchmarkPayloadSize[[6]uint64](b)
	benchmarkPayloadSize[[7]uint64](b)
	benchmarkPayloadSize[[8]uint64](b)
	benchmarkPayloadSize[[9]uint64](b)
	benchmarkPayloadSize[[10]uint64](b)
	benchmarkPayloadSize[[11]uint64](b)
	benchmarkPayloadSize[[12]uint64](b)
	benchmarkPayloadSize[[13]uint64](b)
	benchmarkPayloadSize[[14]uint64](b)
	benchmarkPayloadSize[[15]uint64](b)
	benchmarkPayloadSize[[16]uint64](b)
	benchmarkPayloadSize[[17]uint64](b)
	benchmarkPayloadSize[[18]uint64](b)
	benchmarkPayloadSize[[19]uint64](b)
	benchmarkPayloadSize[[20]uint64](b)
	benchmarkPayloadSize[[21]uint64](b)
	benchmarkPayloadSize[[22]uint64](b)
	benchmarkPayloadSize[[23]uint64](b)
	benchmarkPayloadSize[[24]uint64](b)
	benchmarkPayloadSize[[25]uint64](b)
	benchmarkPayloadSize[[26]uint64](b)
	benchmarkPayloadSize[[27]uint64](b)
	benchmarkPayloadSize[[28]uint64](b)
	benchmarkPayloadSize[[29]uint64](b)
	benchmarkPayloadSize[[30]uint64](b)
	benchmarkPayloadSize[[31]uint64](b)
	benchmarkPayloadSize[[32]uint64](b)
}

func benchmarkPayloadSize[T any](b *testing.B) {
	b.Helper()
	size := int(unsafe.Sizeof(*new(T)))
	b.Run(fmt.Sprintf("bytes=%03d/value", size), benchmarkRingByValue[T])
	b.Run(fmt.Sprintf("bytes=%03d/in_place", size), benchmarkRingInPlace[T])
}

func benchmarkRingByValue[T any](b *testing.B) {
	b.Helper()
	queue := NewNotifying[T](benchmarkRingCapacity)
	start := make(chan struct{})
	consumerDone := make(chan uint64, 1)
	var ready sync.WaitGroup
	var producers sync.WaitGroup
	ready.Add(benchmarkProducerCount + 1)

	go func() {
		ready.Done()
		<-start
		var checksum uint64
		for consumed := 0; consumed < b.N; {
			value, ok := queue.TryDequeue()
			if !ok {
				<-queue.Ready()
				continue
			}
			checksum ^= consumeBenchmarkPayload(&value)
			consumed++
		}
		consumerDone <- checksum
	}()

	for producer := range benchmarkProducerCount {
		first, count := benchmarkProducerRange(b.N, producer)
		producers.Go(func() {
			ready.Done()
			<-start
			for sequence := first; sequence < first+count; sequence++ {
				var value T
				fillBenchmarkPayload(&value, uint64(sequence)) //nolint:gosec // sequence is nonnegative.
				for !queue.TryEnqueue(value) {
					runtime.Gosched()
				}
			}
		})
	}

	b.SetBytes(int64(unsafe.Sizeof(*new(T)))) //nolint:gosec // Benchmarked payloads are at most 256 bytes.
	b.ReportAllocs()
	ready.Wait()
	b.ResetTimer()
	close(start)
	producers.Wait()
	checksum := <-consumerDone
	b.StopTimer()

	benchmarkChecksum = checksum
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "items/s")
}

func benchmarkRingInPlace[T any](b *testing.B) {
	b.Helper()
	queue := NewNotifying[T](benchmarkRingCapacity)
	start := make(chan struct{})
	consumerDone := make(chan uint64, 1)
	var ready sync.WaitGroup
	var producers sync.WaitGroup
	ready.Add(benchmarkProducerCount + 1)

	go func() {
		ready.Done()
		<-start
		var checksum uint64
		for consumed := 0; consumed < b.N; {
			value := queue.TryStartDequeue()
			if value == nil {
				<-queue.Ready()
				continue
			}
			checksum ^= consumeBenchmarkPayload(value)
			queue.FinishDequeue()
			consumed++
		}
		consumerDone <- checksum
	}()

	for producer := range benchmarkProducerCount {
		first, count := benchmarkProducerRange(b.N, producer)
		producers.Go(func() {
			ready.Done()
			<-start
			for sequence := first; sequence < first+count; sequence++ {
				for {
					value, ticket := queue.TryStartEnqueue()
					if value == nil {
						runtime.Gosched()
						continue
					}
					fillBenchmarkPayload(value, uint64(sequence)) //nolint:gosec // sequence is nonnegative.
					queue.FinishEnqueue(ticket)
					break
				}
			}
		})
	}

	b.SetBytes(int64(unsafe.Sizeof(*new(T)))) //nolint:gosec // Benchmarked payloads are at most 256 bytes.
	b.ReportAllocs()
	ready.Wait()
	b.ResetTimer()
	close(start)
	producers.Wait()
	checksum := <-consumerDone
	b.StopTimer()

	benchmarkChecksum = checksum
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "items/s")
}

func benchmarkProducerRange(total, producer int) (int, int) {
	count := total / benchmarkProducerCount
	first := producer * count
	if producer < total%benchmarkProducerCount {
		count++
		first += producer
	} else {
		first += total % benchmarkProducerCount
	}
	return first, count
}

func fillBenchmarkPayload[T any](value *T, sequence uint64) {
	words := benchmarkPayloadWords(value)
	state := sequence + 0x9e3779b97f4a7c15
	for index := range words {
		state ^= state >> 12
		state ^= state << 25
		state ^= state >> 27
		words[index] = bits.RotateLeft64(state*0x2545f4914f6cdd1d, index&63)
	}
}

func consumeBenchmarkPayload[T any](value *T) uint64 {
	words := benchmarkPayloadWords(value)
	checksum := uint64(len(words)) ^ 0xcbf29ce484222325
	for index, word := range words {
		checksum ^= bits.RotateLeft64(word, (index*11+7)&63)
		checksum *= 0x100000001b3
	}
	return checksum
}

func benchmarkPayloadWords[T any](value *T) []uint64 {
	wordCount := int(unsafe.Sizeof(*value) / unsafe.Sizeof(uint64(0)))
	return unsafe.Slice((*uint64)(unsafe.Pointer(value)), wordCount) //nolint:gosec // Every benchmark T is a uint64 array.
}
