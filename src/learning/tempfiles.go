package main

import (
	"fmt"
	"log"
	"os"
)

func main() {
	tempFile, err := os.CreateTemp("", "")
	if err != nil {
		log.Fatal("uhoh")
	}

	fmt.Fprintf(tempFile, "Oh holy moly we sure are writing to a temporary file")

	os.Rename(tempFile.Name(), "used_to_be_a_tempfile.txt")
}
