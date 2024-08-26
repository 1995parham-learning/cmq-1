package main

import (
	"fmt"
	"sync"

	"github.com/1995parham-learning/cmq-1/pkg/cmq"
)

func main() {
	mmq := cmq.NewMockMessageQueue[int]()

	var wg sync.WaitGroup

	mmq.Register("s1", "numbers", 10)
	mmq.Register("s2", "numbers", 10)

	wg.Add(3)

	go func() {
		ch, _ := mmq.Subscribe("s1", "numbers")
		for i := range ch {
			fmt.Println(i)
		}
		wg.Done()
	}()

	go func() {
		ch, _ := mmq.Subscribe("s2", "numbers")
		for i := range ch {
			fmt.Println(i)
		}
		wg.Done()
	}()

	go func() {
		ch, _ := mmq.Subscribe("s1", "numbers")
		for i := range ch {
			fmt.Println(i)
		}
		wg.Done()
	}()

	mmq.Publish("numbers", 10)
	mmq.Publish("numbers", 20)
	mmq.Publish("numbers", 30)

	wg.Wait()
}
