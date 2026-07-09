package generator

import "context"

func Generate(ctx context.Context) <-chan int {
	ch := make(chan int)

	go func() {
		defer close(ch)

		i := 0

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			select {
			case ch <- i:
				i++
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch
}
