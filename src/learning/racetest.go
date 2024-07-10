package main

import (
	"time"

	"math/rand"
)

func main() {
	//rand.Seed(time.Now().UnixNano())

	// rand.Seed() is deprecated as of Go 1.20
	// rand automatically seeds randomly now
	// if you need to seed a specific number, use this format

	// rand.New(rand.NewSource(20))

	count := 0
	finished := 0

	for i := 0; i < 10; i++ {
		go func() {
			vote := requestVote()
			if vote {
				count++
			}
			finished++
		}()
	}

	for count < 5 && finished != 10 {
		// wait
	}

	if count >= 5 {
		println("received 5+ votes")
	} else {
		println("lost election")
	}
}

func requestVote() bool {
	time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	return rand.Intn(2) == 1
}