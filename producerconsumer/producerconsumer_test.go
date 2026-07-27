package producerconsumer

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestProducerConsumer(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	Run(&buf)
	output := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(output) != 10 {
		t.Fatalf("неверное количество чисел в выводе: ожидали 10 чисел (от 1 до 10), получили %d. Producer должен генерировать числа от 1 до 10, Consumer должен их все обработать. Это указывает на проблему с синхронизацией или закрытием канала", len(output))
	}
	for i, line := range output {
		expected := i + 1
		if line != fmt.Sprint(expected) {
			t.Errorf("неверное значение на строке %d: ожидали %d, получили %s. Последовательность должна быть строго от 1 до 10 по порядку. Это указывает на проблему с порядком обработки или синхронизацией", i, expected, line)
		}
	}
}

func TestProducerConsumerFirstLast(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	Run(&buf)
	output := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(output) > 0 {
		if output[0] != "1" || output[len(output)-1] != "10" {
			t.Fatal("последовательность должна начинаться с 1 и заканчиваться 10")
		}
	}
}

func TestProducerConsumerCompleteSequence(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	Run(&buf)
	output := strings.Split(strings.TrimSpace(buf.String()), "\n")

	expected := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	if len(output) != len(expected) {
		t.Fatalf("неверное количество чисел в полной последовательности: ожидали %d чисел (от 1 до 10), получили %d. Producer должен сгенерировать все числа, Consumer должен их все обработать в правильном порядке. Это указывает на проблему с завершением работы или синхронизацией", len(expected), len(output))
	}

	for i, line := range output {
		num, err := strconv.Atoi(line)
		if err != nil {
			t.Errorf("строка %d: неверный формат числа '%s'. Все строки должны содержать целые числа. Это указывает на проблему с форматированием вывода", i, line)
			continue
		}
		if num != expected[i] {
			t.Errorf("строка %d: неверное значение последовательности, ожидали %d, получили %d. Последовательность должна быть строго от 1 до 10 по порядку без пропусков. Это указывает на проблему с порядком обработки", i, expected[i], num)
		}
	}
}

func TestProducerConsumerNoDuplicates(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	Run(&buf)
	output := strings.Split(strings.TrimSpace(buf.String()), "\n")

	seen := make(map[int]bool)
	for i, line := range output {
		num, err := strconv.Atoi(line)
		if err != nil {
			t.Errorf("строка %d: неверный формат числа '%s'. Все строки должны содержать целые числа. Это указывает на проблему с форматированием вывода", i, line)
			continue
		}
		if seen[num] {
			t.Errorf("обнаружено дублирование числа %d на строке %d. Каждое число должно встречаться только один раз в последовательности от 1 до 10. Это указывает на проблему с обработкой или синхронизацией", num, i)
		}
		seen[num] = true
	}

	if len(seen) != 10 {
		t.Errorf("неверное количество уникальных чисел: ожидали 10 уникальных чисел (от 1 до 10), получили %d. Все числа от 1 до 10 должны быть обработаны ровно один раз. Это указывает на проблему с обработкой или пропуском значений", len(seen))
	}
}

func TestProducerConsumerCompletes(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	done := make(chan struct{})

	go func() {
		Run(&buf)
		close(done)
	}()

	select {
	case <-done:
		output := strings.Split(strings.TrimSpace(buf.String()), "\n")
		if len(output) != 10 {
			t.Errorf("неверное количество чисел после завершения: ожидали 10 чисел после завершения работы Producer-Consumer, получили %d. Функция должна завершиться после обработки всех чисел. Это указывает на проблему с завершением работы или синхронизацией", len(output))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("producer-consumer did not complete (possible deadlock)")
	}
}

func TestProducerConsumerConcurrentRuns(t *testing.T) {
	t.Parallel()
	const numRuns = 10
	var wg sync.WaitGroup

	for i := 0; i < numRuns; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var buf bytes.Buffer
			Run(&buf)
			output := strings.Split(strings.TrimSpace(buf.String()), "\n")
			if len(output) != 10 {
				t.Errorf("параллельный запуск: неверное количество чисел, ожидали 10 чисел, получили %d. При параллельных запусках каждый экземпляр должен обработать полную последовательность. Это указывает на проблему с изоляцией экземпляров или синхронизацией", len(output))
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent runs did not complete")
	}
}

func TestProducerConsumerSynchronization(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	start := time.Now()
	Run(&buf)
	duration := time.Since(start)

	if duration > 1*time.Second {
		t.Errorf("Producer-Consumer занял слишком много времени: %v. Функция должна завершиться быстро при правильной синхронизации. Это указывает на проблему с блокировкой или синхронизацией", duration)
	}

	output := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(output) != 10 {
		t.Errorf("неверное количество чисел после синхронизации: ожидали 10 чисел, получили %d. Producer-Consumer должен завершиться быстро и обработать все числа. Это указывает на проблему с синхронизацией или блокировкой", len(output))
	}
}

func TestProducerConsumerChannelClosure(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	Run(&buf)

	output := strings.Split(strings.TrimSpace(buf.String()), "\n")
	count := 0
	for range output {
		count++
	}

	if count != 10 {
		t.Errorf("неверное количество обработанных значений: ожидали 10 обработанных значений, получили %d. Все значения от Producer должны быть потреблены Consumer. Это указывает на проблему с закрытием канала или обработкой значений", count)
	}
}
