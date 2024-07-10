package main

import (
	"fmt"
	"os"
	"plugin"
	"sort"

	"6.5840/mr"
)

// This creates an object wrapper for the KeyValue list that
// implements the interface for sort.Sort.
// Later versions of go recommend using slices.SortFunc which is much more readable
type ByValueAlpha []mr.KeyValue

func (a ByValueAlpha) Len() int           { return len(a) }
func (a ByValueAlpha) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByValueAlpha) Less(i, j int) bool { return a[i].Value[0] < a[j].Value[0] }

func main() {

	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stdout, "Usage: <filename> <word>\n")
		os.Exit(1)
	}
	arg := os.Args[1]

	fmt.Fprintf(os.Stdout, "Hello %v\n", arg)

	for index, val := range os.Args {
		fmt.Printf("Arg %v -> %v\n", index, val)
	}

	/*
		// Manual build
		things := Thing{}
		things = append(things, arg)
		things = append(things, "test")
	*/

	// Build with file
	filename := os.Args[1]
	contents := loadInputFile(filename)
	processf := loadPlugin("plugin.so")
	filelines := processf(contents)

	fmt.Printf("File Lines: %v\n", filelines)

	// Sort alphabetically
	// The sort.Sort function from the course is complicated and deprecated
	// but I may redo it just to have the knowledge

	/*
		// Declaring and using functions like this requires Go 1.18
		// which the course is not using (1.15)
			sortfunc := func(a, b mr.KeyValue) int {
				return cmp.Compare(a.Value[0], b.Value[0])
			}

		// Sorts in place -- compiler gives No Value error when trying to assign
		slices.SortFunc(filelines, sortfunc)
	*/
	sort.Sort(ByValueAlpha(filelines))

	fmt.Printf("Sorted Lines: %v\n", filelines)

	// Output file
	outname := "output.txt"
	outfile, err := os.Create(outname)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot write to %v", outname)
	}
	defer outfile.Close()

	for _, line := range filelines {
		fmt.Fprintf(outfile, "%v\n", line.Value)
	}
}

func loadInputFile(filename string) string {
	/*
		// Pre Go 1.16 method
		file, err := os.Open(filename)
		if err != nil {
			fmt.Fprintf(os.Stderr, "cannot open %v", filename)
			os.Exit(1)
		}

		content, err := io.ReadAll(file)
		if err != nil {
			fmt.Fprintf(os.Stderr, "cannot read %v", filename)
		}

		file.Close()
	*/

	// Post Go 1.16 method
	content, err := os.ReadFile(filename)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot read %v", filename)
	}

	return string(content)
}

func loadPlugin(filename string) func(string) []mr.KeyValue {
	p, err := plugin.Open(filename)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Cannot load plugin %v", filename)
		os.Exit(1)
	}

	xprocessf, err := p.Lookup("Process")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Cannot find function Process in %v", filename)
		os.Exit(1)
	}
	processf := xprocessf.(func(string) []mr.KeyValue)

	return processf
}
