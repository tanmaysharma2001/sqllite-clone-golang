package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// ------------- INPUT AND OUTPUT --------------

func printPrompt() {
	fmt.Printf("db > ")
}

type InputBuffer struct {
	buffer      string
	inputLength int
}

func newInputBuffer() *InputBuffer {
	inputBuffer := &InputBuffer{}
	inputBuffer.buffer = ""
	inputBuffer.inputLength = 0

	return inputBuffer
}

func readInput(buffer *InputBuffer) {
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
	buffer.inputLength = len(input)
}

func main() {

	if len(os.Args) < 2 {
		fmt.Println("Must supply a database filename.")
		os.Exit(1)
	}

	filename := os.Args[1]

	inputBuffer := newInputBuffer()
	table := DB_OPEN(filename)
	//table := newTable()

	for {
		printPrompt()
		readInput(inputBuffer)

		if inputBuffer.buffer[0] == '.' {
			switch DoMetaCommand(inputBuffer, table) {
			case META_COMMAND_SUCCESS:
				continue
			case META_COMMAND_UNRECOGNIZED_COMMAND:
				fmt.Printf("Unrecognized command '%s'.\n", inputBuffer.buffer)
				continue
			}
		}

		statement := &Statement{}
		switch PrepareStatement(inputBuffer, statement) {
		case PREPARE_SUCCESS:
			break
		case PREPARE_SYNTAX_ERROR:
			fmt.Println("Syntax error could not parse statement.")
			continue
		case PREPARE_UNRECOGNIZED_STATEMENT:
			fmt.Printf("Unrecognized keyword at start of '%s'.\n", inputBuffer.buffer)
			continue
		case PREPARE_NEGATIVE_ID:
			fmt.Println("Parsing Error: Negative ID passed")
			continue
		default:
			continue
		}

		switch executeStatement(statement, table) {
		case EXECUTE_SUCCESS:
			fmt.Println("Executed")
			break
		case EXECUTE_TABLE_FULL:
			fmt.Println("Table is full!")
			break
		case EXECUTE_FAIL:
			fmt.Println("Executing the command failed.")
		}
	}
}
