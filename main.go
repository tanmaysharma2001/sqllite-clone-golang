package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

type MetaCommandResult int

const (
	META_COMMAND_SUCCESS MetaCommandResult = iota
	META_COMMAND_UNRECOGNIZED_COMMAND
)

type PrepareResult int

const (
	PREPARE_SUCCESS PrepareResult = iota
	PREPARE_UNRECOGNIZED_STATEMENT
)

type StatementType int

const (
	STATEMENT_INSERT StatementType = iota
	STATEMENT_SELECT
)

type Statement struct {
	Type StatementType
}

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

func doMetaCommand(buffer *InputBuffer) MetaCommandResult {
	if buffer.buffer == ".exit" {
		os.Exit(0)
		return META_COMMAND_SUCCESS
	} else {
		return META_COMMAND_UNRECOGNIZED_COMMAND
	}
}

func prepare_statement(buffer *InputBuffer, statement *Statement) PrepareResult {
	if buffer.buffer == "insert" {
		statement.Type = STATEMENT_INSERT
		return PREPARE_SUCCESS
	}

	if buffer.buffer == "select" {
		statement.Type = STATEMENT_SELECT
		return PREPARE_SUCCESS
	}

	return PREPARE_UNRECOGNIZED_STATEMENT
}

func execute_statement(statement *Statement) {
	switch statement.Type {
	case STATEMENT_INSERT:
		fmt.Printf("This is where we would do an insert.\n")
		break
	case STATEMENT_SELECT:
		fmt.Printf("This is where we would do a select.\n")
		break
	}
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

		//if inputBuffer.buffer == ".exit" {
		//	os.Exit(0)
		//} else {
		//	fmt.Printf("Unrecognized command '%s'. \n", inputBuffer.buffer)
		//}

		if inputBuffer.buffer[0] == '.' {
			switch doMetaCommand(inputBuffer) {
			case META_COMMAND_SUCCESS:
				continue
			case META_COMMAND_UNRECOGNIZED_COMMAND:
				fmt.Printf("Unrecognized command '%s'\n", inputBuffer.buffer)
				continue
			}
		}

		var statement Statement
		switch prepare_statement(inputBuffer, &statement) {
		case PREPARE_SUCCESS:
			break
		case PREPARE_UNRECOGNIZED_STATEMENT:
			fmt.Printf("Unrecognized keyword at start of '%s'.\n")
			continue
		}

		execute_statement(&statement)
		fmt.Println("Executed.")
	}
}
