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

func main() {
	// Concurrently fetch each URL using a Mutex to prevent race conditions
	ConcurrentMutex("http://golang.org/", fetcher, buildState())
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
