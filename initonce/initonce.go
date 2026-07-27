package initonce

import (
	"sync"
)

var (
	once        sync.Once
	initialized bool
)

func Init() {
	once.Do(func() {
		initialized = true
	})
}

func Initialized() bool {
	return initialized
}
