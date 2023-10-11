package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// Input Buffer Struct
type InputBuffer struct {
	buffer       string
	input_length int
}

func new_input_buffer() *InputBuffer {
	input_buffer := &InputBuffer{}
	input_buffer.buffer = ""
	input_buffer.input_length = 0

	return input_buffer
}

func print_prompt() {
	fmt.Printf("db > ")
}

func read_input(buffer *InputBuffer) {
	// reading input
	reader := bufio.NewReader(os.Stdin)
	input, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("Error reading input")
		os.Exit(1)
	}

	// ignore trailing newline
	input = strings.TrimSpace(strings.TrimSuffix(input, "\n"))

	buffer.buffer = input
	buffer.input_length = len(input)
}

func main() {
	inputBuffer := &InputBuffer{}

	for {
		print_prompt()
		read_input(inputBuffer)

		if inputBuffer.buffer == ".exit" {
			os.Exit(0)
		} else {
			fmt.Printf("Unrecognized command '%s'. \n", inputBuffer.buffer)
		}
	}
}
