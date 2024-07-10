package main

import (
	"strconv"
	"strings"

	"6.5840/mr"
)

func Process(content string) []mr.KeyValue {
	output := []mr.KeyValue{}
	for i, line := range strings.Split(content, "\n") {
		output = append(output, mr.KeyValue{strconv.Itoa(i), line})
	}

	return output
}
