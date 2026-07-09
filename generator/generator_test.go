package generator

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestGenerateNumbers(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := Generate(ctx)
	got := make([]int, 0, 5)
	for i := 0; i < 5; i++ {
		n, ok := <-ch
		if !ok {
			t.Fatalf("ожидали %d элементов, получили %d", 5, len(got))
		}
		got = append(got, n)
	}
	for i, v := range got {
		if v != i {
			t.Fatalf("ожидали %d, получили %d", i, v)
		}
	}
}

func TestGenerateCancel(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ch := Generate(ctx)
	if _, ok := <-ch; ok {
		t.Fatal("канал должен закрыться после отмены")
	}
}

func TestGenerateSequential(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := Generate(ctx)
	expected := 0
	for i := 0; i < 100; i++ {
		n, ok := <-ch
		if !ok {
			t.Fatalf("channel closed unexpectedly at iteration %d", i)
		}
		if n != expected {
			t.Fatalf("expected %d, got %d", expected, n)
		}
		expected++
	}
}

func TestGenerateCancelAfterRead(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	ch := Generate(ctx)

	for i := 0; i < 10; i++ {
		n, ok := <-ch
		if !ok {
			t.Fatalf("channel closed unexpectedly at iteration %d", i)
		}
		if n != i {
			t.Fatalf("expected %d, got %d", i, n)
		}
	}

	cancel()

	_, ok := <-ch
	if ok {
		t.Fatal("channel should be closed after cancellation")
	}
}

func TestGenerateTimeout(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	ch := Generate(ctx)
	count := 0

	for {
		select {
		case _, ok := <-ch:
			if !ok {
				if count == 0 {
					t.Fatal("expected to receive some values before timeout")
				}
				return
			}
			count++
		case <-time.After(200 * time.Millisecond):
			t.Fatal("timeout waiting for channel closure")
		}
	}
}

func TestGenerateConcurrentReads(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := Generate(ctx)
	var wg sync.WaitGroup
	const numReaders = 5

	results := make([][]int, numReaders)

	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			values := make([]int, 0)
			for j := 0; j < 10; j++ {
				n, ok := <-ch
				if !ok {
					t.Errorf("reader %d: channel closed unexpectedly", idx)
					return
				}
				values = append(values, n)
			}
			results[idx] = values
		}(i)
	}

	wg.Wait()

	seen := make(map[int]bool)
	for _, values := range results {
		for _, v := range values {
			if seen[v] {
				t.Errorf("duplicate value %d", v)
			}
			seen[v] = true
		}
	}
}

func TestGenerateCancelImmediately(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ch := Generate(ctx)
	_, ok := <-ch
	if ok {
		t.Fatal("channel should be closed when context is already canceled")
	}
}

func TestGenerateLargeSequence(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := Generate(ctx)
	count := 0
	for i := 0; i < 1000; i++ {
		n, ok := <-ch
		if !ok {
			t.Fatalf("channel closed unexpectedly at iteration %d", i)
		}
		if n != i {
			t.Fatalf("expected %d, got %d", i, n)
		}
		count++
	}

	if count != 1000 {
		t.Fatalf("expected 1000 values, got %d", count)
	}
}

func TestGenerateMultipleInstances(t *testing.T) {
	t.Parallel()
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	ch1 := Generate(ctx1)
	ch2 := Generate(ctx2)

	for i := 0; i < 10; i++ {
		n1, ok1 := <-ch1
		n2, ok2 := <-ch2

		if !ok1 || !ok2 {
			t.Fatal("channels should not be closed")
		}
		if n1 != i || n2 != i {
			t.Fatalf("expected both %d, got %d and %d", i, n1, n2)
		}
	}
}

func TestGenerateChannelClosure(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	ch := Generate(ctx)

	for i := 0; i < 5; i++ {
		<-ch
	}

	cancel()

	time.Sleep(10 * time.Millisecond)

	_, ok := <-ch
	if ok {
		t.Fatal("channel should be closed after cancellation")
	}
}

func BenchmarkGenerate(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch := Generate(ctx)
		count := 0
		for range ch {
			count++
			if count >= 100 {
				break
			}
		}
	}
}
