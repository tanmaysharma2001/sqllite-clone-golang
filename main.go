package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"unsafe"
)

// All the commands Flags

type ExecuteResult int

const (
	EXECUTE_SUCCESS ExecuteResult = iota
	EXECUTE_TABLE_FULL
	EXECUTE_FAIL
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
	PREPARE_SYNTAX_ERROR
)

type StatementType int

const (
	STATEMENT_INSERT StatementType = iota
	STATEMENT_SELECT
)

// Row Definition: will change later according to tables

type Row struct {
	id       int
	username string
	email    string
}

const (
	ID_SIZE       = int(unsafe.Sizeof(Row{}.id))
	USERNAME_SIZE = int(unsafe.Sizeof(Row{}.username))
	EMAIL_SIZE    = int(unsafe.Sizeof(Row{}.email))

	ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE
)

// !IMPORTANT!: table composition characteristics
// Note: here a page size is equivalent to 4kb the usual page size in the memory
const (
	PAGE_SIZE       int = 4096
	TABLE_MAX_PAGES int = 100
	ROWS_PER_PAGE   int = PAGE_SIZE / ROW_SIZE
	TABLE_MAX_ROWS  int = ROWS_PER_PAGE * TABLE_MAX_PAGES
)

type Page struct {
	rows         [ROWS_PER_PAGE]*Row
	numberOfRows int
}

type Table struct {
	numRows int
	pages   [TABLE_MAX_PAGES]*Page
}

func newTable() *Table {
	table := &Table{}
	table.numRows = 0
	for i := 0; i < TABLE_MAX_PAGES; i++ {
		table.pages[i] = &Page{}
	}
	return table
}

// ------------- Row Functions ----------------

func (table *Table) rowSlot(numRows int) (int, int) {
	pageNum := numRows / ROWS_PER_PAGE
	currentPage := table.pages[pageNum]

	if currentPage == nil {
		fmt.Println("Something went wrong!")
		os.Exit(0)
	}

	rowOffset := numRows % ROWS_PER_PAGE
	return pageNum, rowOffset
}

func (table *Table) serializeRow(row *Row, currentPage int, currentRow int) {
	page := table.pages[currentPage]
	page.rows[currentRow] = row
	table.numRows += 1
}

func (table *Table) deserializeRow(currentPage int, currentRow int) *Row {
	page := table.pages[currentPage]
	row := page.rows[currentRow]
	return row
}

func printRow(row *Row) {
	fmt.Printf("%d %s %s\n", row.id, row.email, row.username)
}

// ----------- Statement Functions -----------------

type Statement struct {
	Type        StatementType
	rowToInsert Row
}

func doMetaCommand(buffer *InputBuffer) MetaCommandResult {
	if buffer.buffer == ".exit" {
		os.Exit(0)
		return META_COMMAND_SUCCESS
	} else {
		return META_COMMAND_UNRECOGNIZED_COMMAND
	}
}

func prepareStatement(buffer *InputBuffer, statement *Statement) PrepareResult {

	arguments := strings.Fields(buffer.buffer)

	if arguments[0] == "insert" {
		statement.Type = STATEMENT_INSERT

		if len(arguments) < 4 {
			return PREPARE_SYNTAX_ERROR
		}

		rowID, _ := strconv.Atoi(arguments[1])

		row := Row{id: rowID, username: arguments[2], email: arguments[3]}

		statement.rowToInsert = row

		return PREPARE_SUCCESS
	}

	if arguments[0] == "select" {
		statement.Type = STATEMENT_SELECT
		return PREPARE_SUCCESS
	}

	return PREPARE_UNRECOGNIZED_STATEMENT
}

func (table *Table) executeInsert(statement *Statement) ExecuteResult {
	if table.numRows >= TABLE_MAX_ROWS {
		return EXECUTE_TABLE_FULL
	} else {
		rowToInsert := &statement.rowToInsert

		currentPage, currentRow := table.rowSlot(table.numRows)

		table.serializeRow(rowToInsert, currentPage, currentRow)

		return EXECUTE_SUCCESS
	}
}

func (table *Table) executeSelect(statement *Statement) ExecuteResult {
	for i := 0; i < table.numRows; i++ {
		row := table.deserializeRow(table.rowSlot(i))
		printRow(row)
	}
	return EXECUTE_SUCCESS
}

func executeStatement(statement *Statement, table *Table) ExecuteResult {
	switch statement.Type {
	case STATEMENT_INSERT:
		return table.executeInsert(statement)
	case STATEMENT_SELECT:
		return table.executeSelect(statement)
	}

	return EXECUTE_FAIL
}

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
	inputBuffer := newInputBuffer()
	table := newTable()

	for {
		printPrompt()
		readInput(inputBuffer)

		if inputBuffer.buffer[0] == '.' {
			switch doMetaCommand(inputBuffer) {
			case META_COMMAND_SUCCESS:
				continue
			case META_COMMAND_UNRECOGNIZED_COMMAND:
				fmt.Printf("Unrecognized command '%s'.\n", inputBuffer.buffer)
				continue
			}
		}

		statement := &Statement{}
		switch prepareStatement(inputBuffer, statement) {
		case PREPARE_SUCCESS:
			break
		case PREPARE_SYNTAX_ERROR:
			fmt.Println("Syntax error could not parse statement.")
			continue
		case PREPARE_UNRECOGNIZED_STATEMENT:
			fmt.Printf("Unrecognized keyword at start of '%s'.\n")
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
