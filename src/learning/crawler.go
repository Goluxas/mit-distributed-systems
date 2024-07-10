package main

import (
	"fmt"
	"sync"
)

type fetchState struct {
	mu      sync.Mutex
	fetched map[string]bool
}

func buildState() *fetchState {
	/*
		returns a pointer because the Mutex must be
		passed by reference.
	*/
	return &fetchState{fetched: make(map[string]bool)}
}

func ConcurrentMutex(url string, fetcher Fetcher, f *fetchState) {
	f.mu.Lock()
	seen := f.fetched[url]
	f.fetched[url] = true
	f.mu.Unlock()

	if seen {
		return
	}

	urls, err := fetcher.Fetch(url)
	if err != nil {
		// the Fetcher prints a message about the missing URL
		//fmt.Fprintf(os.Stderr, "Error fetching URL %v\n", url)
		return
	}

	var done sync.WaitGroup
	for _, next_url := range urls {
		done.Add(1)
		go func(u string) {
			ConcurrentMutex(u, fetcher, f)
			done.Done()
		}(next_url)
	}
	done.Wait()
	return
}

func ConcurrentChannel(url string, fetcher Fetcher) {
	newUrls := make(chan []string)

	visited := make(map[string]bool)

	/*
		kickstart the channel
		with a thread because it sleeps until the value is consumed
	*/
	go func() {
		newUrls <- []string{url}
	}()

	/*
		n represents the number of worker returns left to process
		even if the return is nothing
	*/
	n := 1

	/*
		Each item in the newUrls channel is the url list from a fetch call
		or an empty list if it failed
	*/
	for urls := range newUrls {
		// Loop over the previous results, if any
		for _, nextUrl := range urls {
			if visited[nextUrl] {
				continue
			}

			visited[nextUrl] = true

			// New URL, new worker result to expect
			n += 1

			go func(u string, newUrls chan []string, fetcher Fetcher) {
				urls, err := fetcher.Fetch(u)
				if err != nil {
					newUrls <- []string{}
				} else {
					newUrls <- urls
				}
			}(nextUrl, newUrls, fetcher)
		}

		// One result passed to channel has been processed
		n -= 1
		if n == 0 {
			// Break needed because we don't specify a close channel operation anywhere
			break
		}
	}

}

func main() {
	golang := "http://golang.org/"
	// Concurrently fetch each URL using a Mutex to prevent race conditions
	//ConcurrentMutex(golang, fetcher, buildState())

	ConcurrentChannel(golang, fetcher)
}

// Given Mock Fetcher

//
// Fetcher
//

type Fetcher interface {
	// Fetch returns a slice of URLs found on the page.
	Fetch(url string) (urls []string, err error)
}

// fakeFetcher is Fetcher that returns canned results.
type fakeFetcher map[string]*fakeResult

type fakeResult struct {
	body string
	urls []string
}

func (f fakeFetcher) Fetch(url string) ([]string, error) {
	if res, ok := f[url]; ok {
		fmt.Printf("found:   %s\n", url)
		return res.urls, nil
	}
	fmt.Printf("missing: %s\n", url)
	return nil, fmt.Errorf("not found: %s", url)
}

// fetcher is a populated fakeFetcher.
var fetcher = fakeFetcher{
	"http://golang.org/": &fakeResult{
		"The Go Programming Language",
		[]string{
			"http://golang.org/pkg/",
			"http://golang.org/cmd/",
		},
	},
	"http://golang.org/pkg/": &fakeResult{
		"Packages",
		[]string{
			"http://golang.org/",
			"http://golang.org/cmd/",
			"http://golang.org/pkg/fmt/",
			"http://golang.org/pkg/os/",
		},
	},
	"http://golang.org/pkg/fmt/": &fakeResult{
		"Package fmt",
		[]string{
			"http://golang.org/",
			"http://golang.org/pkg/",
		},
	},
	"http://golang.org/pkg/os/": &fakeResult{
		"Package os",
		[]string{
			"http://golang.org/",
			"http://golang.org/pkg/",
		},
	},
}
