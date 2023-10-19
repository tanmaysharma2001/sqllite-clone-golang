package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// ----------- Statement Functions -----------------

type Statement struct {
	Type        StatementType
	rowToInsert Row
}

func DoMetaCommand(buffer *InputBuffer, table *Table) MetaCommandResult {
	if buffer.buffer == ".exit" {
		DB_CLOSE(table)
		os.Exit(0)
		return META_COMMAND_SUCCESS
	} else {
		return META_COMMAND_UNRECOGNIZED_COMMAND
	}
}

func prepareInsert(buffer *InputBuffer, statement *Statement) PrepareResult {
	arguments := strings.Fields(buffer.buffer)

	statement.Type = STATEMENT_INSERT

	if len(arguments) < 4 {
		return PREPARE_SYNTAX_ERROR
	}

	if arguments[1] == " " || arguments[2] == " " || arguments[3] == " " {
		return PREPARE_SYNTAX_ERROR
	}

	rowID, err := strconv.Atoi(arguments[1])
	if err != nil {
		fmt.Println("Error parsing the ID field")
		return PREPARE_SYNTAX_ERROR
	}

	if rowID < 0 {

		return PREPARE_NEGATIVE_ID
	}

	row := Row{id: rowID, username: arguments[2], email: arguments[3]}

	statement.rowToInsert = row

	return PREPARE_SUCCESS

}

func PrepareStatement(buffer *InputBuffer, statement *Statement) PrepareResult {

	arguments := strings.Fields(buffer.buffer)

	if arguments[0] == "insert" {
		return prepareInsert(buffer, statement)
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

		cursor := table.table_end()

		currentPage, currentRow := table.cursorValue(cursor)

		table.serializeRow(rowToInsert, currentPage, currentRow)

		return EXECUTE_SUCCESS
	}
}

func (table *Table) executeSelect(statement *Statement) ExecuteResult {

	cursor := table.table_start()

	for !cursor.end_of_table {
		row := table.deserializeRow(table.cursorValue(cursor))
		printRow(row)
		cursor.advance()
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
