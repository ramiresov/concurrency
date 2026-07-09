package initonce

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestInitOnce(t *testing.T) {
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			Init()
			wg.Done()
		}()
	}
	wg.Wait()
	if !Initialized() {
		t.Fatal("ресурс не был инициализирован: после вызова Init() из 100 горутин состояние Initialized() должно быть true. Это указывает на проблему с механизмом инициализации или sync.Once")
	}
}

func TestInitializedMultiple(t *testing.T) {
	Init()
	if !Initialized() {
		t.Fatal("ожидалось истинное значение инициализации")
	}
	if !Initialized() {
		t.Fatal("ожидалось истинное значение инициализации")
	}
}

func TestInitOnceSingleExecution(t *testing.T) {
	var callCount int32

	initialized = false
	once = sync.Once{}

	var wg sync.WaitGroup
	const numGoroutines = 1000

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			Init()
			atomic.AddInt32(&callCount, 1)
		}()
	}

	wg.Wait()

	if !Initialized() {
		t.Fatal("ресурс не был инициализирован: после вызова Init() из 1000 горутин состояние Initialized() должно быть true. Даже при множественных параллельных вызовах инициализация должна произойти хотя бы один раз. Это указывает на проблему с sync.Once или механизмом установки флага")
	}

	if atomic.LoadInt32(&callCount) != numGoroutines {
		t.Errorf("неверное количество вызовов: ожидали %d вызовов Init() (каждая горутина должна вызвать функцию), получили %d. Все горутины должны выполнить вызов Init(), даже если инициализация происходит только один раз", numGoroutines, callCount)
	}
}

func TestInitOnceRaceCondition(t *testing.T) {
	initialized = false
	once = sync.Once{}

	const numGoroutines = 100
	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			Init()
		}()
	}

	wg.Wait()

	if !Initialized() {
		t.Fatal("ресурс не был инициализирован: после вызова Init() из 100 горутин (проверка условий гонки) состояние Initialized() должно быть true. При параллельных вызовах инициализация должна произойти ровно один раз. Это указывает на проблему с синхронизацией или механизмом sync.Once")
	}
}

func TestInitializedState(t *testing.T) {
	initialized = false
	once = sync.Once{}

	if Initialized() {
		t.Fatal("неверное начальное состояние: ресурс не должен быть инициализирован до вызова Init(). Состояние Initialized() должно возвращать false до первой инициализации. Это указывает на проблему с начальным значением флага")
	}

	Init()

	if !Initialized() {
		t.Fatal("ресурс не был инициализирован: после вызова Init() состояние Initialized() должно быть true. Функция Init() должна установить флаг инициализации. Это указывает на проблему с механизмом установки флага")
	}
}

func TestInitOnceMultipleCalls(t *testing.T) {
	initialized = false
	once = sync.Once{}

	Init()
	Init()
	Init()

	if !Initialized() {
		t.Fatal("ресурс не был инициализирован: после множественных последовательных вызовов Init() состояние Initialized() должно быть true. Несмотря на то, что Init() вызывается три раза, инициализация должна произойти хотя бы один раз. Это указывает на проблему с sync.Once или механизмом установки флага")
	}
}

func TestInitOnceConcurrentCalls(t *testing.T) {
	initialized = false
	once = sync.Once{}

	const numCalls = 500
	var wg sync.WaitGroup

	for i := 0; i < numCalls; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			Init()
		}()
	}

	wg.Wait()

	if !Initialized() {
		t.Fatal("ресурс не был инициализирован: после параллельных вызовов Init() из 500 горутин состояние Initialized() должно быть true. При параллельных вызовах инициализация должна произойти ровно один раз. Это указывает на проблему с потокобезопасностью или механизмом sync.Once")
	}
}

func TestInitOnceNoRace(t *testing.T) {
	initialized = false
	once = sync.Once{}

	var wg sync.WaitGroup
	const numGoroutines = 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			Init()
			if !Initialized() {
				t.Error("ресурс не инициализирован после вызова Init(): каждая горутина должна видеть, что ресурс инициализирован сразу после вызова Init(). Это указывает на проблему с порядком операций или синхронизацией")
			}
		}()
	}

	wg.Wait()

	if !Initialized() {
		t.Fatal("ресурс не был инициализирован: после вызова Init() из 100 горутин состояние Initialized() должно быть true. Даже при параллельных вызовах инициализация должна произойти. Это указывает на проблему с механизмом инициализации")
	}
}

func TestInitOnceTiming(t *testing.T) {
	initialized = false
	once = sync.Once{}

	start := time.Now()

	var wg sync.WaitGroup
	const numCalls = 1000
	for i := 0; i < numCalls; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			Init()
		}()
	}

	wg.Wait()
	duration := time.Since(start)

	if !Initialized() {
		t.Fatal("ресурс не был инициализирован: после вызова Init() из 1000 горутин состояние Initialized() должно быть true. При большом количестве параллельных вызовов инициализация должна произойти. Это указывает на проблему с производительностью или синхронизацией")
	}

	if duration > 5*time.Second {
		t.Errorf("инициализация заняла слишком много времени: %v. При параллельных вызовах sync.Once должен обеспечивать быструю инициализацию без блокировок. Это указывает на проблему с производительностью или дедлоком", duration)
	}
}

func BenchmarkInitOnce(b *testing.B) {
	initialized = false
	once = sync.Once{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Init()
	}
}

func BenchmarkInitOnceConcurrent(b *testing.B) {
	initialized = false
	once = sync.Once{}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			Init()
		}
	})
}
