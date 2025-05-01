package main

import (
	"fmt"
	"sync"

	"github.com/1995parham-learning/cmq-1/pkg/cmq"
)

func syncMapLen(m *sync.Map) int {
	i := 0
	m.Range(func(key, value any) bool {
		i++
		return true
	})

	return i
}

func main() {
	mmq := cmq.NewMockMessageQueue[int]()

	var wg sync.WaitGroup

	_ = mmq.Register("s1", "numbers", 10)
	_ = mmq.Register("s2", "numbers", 10)

	var s1 sync.Map
	var s2 sync.Map

	wg.Add(3)

	go func() {
		sc, _ := mmq.Subscribe("s1", "numbers")
		defer sc.Close()

		for syncMapLen(&s1) != 3 {
			i := <-sc.Channel()
			s1.Store(i, true)
			fmt.Printf("[s1]: %d\n", i)
		}

		wg.Done()
	}()

	go func() {
		sc, _ := mmq.Subscribe("s2", "numbers")
		defer sc.Close()

		for syncMapLen(&s2) != 3 {
			i := <-sc.Channel()
			s2.Store(i, true)
			fmt.Printf("[s2]: %d\n", i)
		}

		wg.Done()
	}()

	go func() {
		sc, _ := mmq.Subscribe("s1", "numbers")
		defer sc.Close()

		for syncMapLen(&s1) != 3 {
			i := <-sc.Channel()
			s1.Store(i, true)
			fmt.Printf("[s1]: %d\n", i)
		}

		wg.Done()
	}()

	_ = mmq.Publish("numbers", 10)
	_ = mmq.Publish("numbers", 20)
	_ = mmq.Publish("numbers", 30)

	wg.Wait()
}
