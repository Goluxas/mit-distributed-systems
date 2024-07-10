package main

import (
	"sync"
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
	/*
		Using a mutex lock/unlock prevents the race condition
		when accessing the variables count and finished that are used in both this main scope
		and all the go thread scopes
	*/
	var mu sync.Mutex
	cond := sync.NewCond(&mu)

	for i := 0; i < 10; i++ {
		go func() {
			vote := requestVote()

			mu.Lock()
			defer mu.Unlock()

			if vote {
				count++
			}
			finished++
			cond.Broadcast()
		}()
	}

	/* While Loop to wait, using only a mutex
	for {
		mu.Lock()
		if count >= 5 || finished == 10 {
			break
		}
		mu.Unlock()
	}
	*/

	/* While Loop to wait using Cond
	 */
	mu.Lock()
	for count < 5 && finished != 10 {
		/* cond.Wait
		1. Unlocks the mu, freeing the shared variables
		2. waits for a cond.Broadcast
		3. Locks the mu, proceeds back through the while loop chec
		4. repeats
		*/
		cond.Wait()
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
