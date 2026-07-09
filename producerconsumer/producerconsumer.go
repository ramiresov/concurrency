package producerconsumer

import (
	"fmt"
	"io"
	"sync"
)

func Run(w io.Writer) {
	ch := make(chan int)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()

		for i := 1; i <= 10; i++ {
			ch <- i
		}

		close(ch)
	}()

	go func() {
		defer wg.Done()

		for num := range ch {
			fmt.Fprintln(w, num)
		}
	}()
	wg.Wait()
}
