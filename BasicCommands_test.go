package main

import (
	"testing"
)

func TestBasicInsertion(t *testing.T) {
	buffer := newInputBuffer()
	table := newTable()

	buffer.buffer = "insert 1 csstack t@gmail.com"
	statement := &Statement{}

	switch prepareStatement(buffer, statement) {
	case PREPARE_SUCCESS:
		break
	default:
		t.Fatalf("Preparing statement for insertion failed: '%s'\n", buffer.buffer)
	}

	switch executeStatement(statement, table) {
	case EXECUTE_SUCCESS:
		break
	default:
		t.Fatalf("Insertion test failed for this: '%s'\n", buffer.buffer)
	}

	// check if it is in the table
	buffer.buffer = "select"
	statement = &Statement{}

	switch prepareStatement(buffer, statement) {
	case PREPARE_SUCCESS:
		break
	default:
		t.Fatalf("Preparing statement for selection failed in TestBasicInsertion.")
	}

	switch executeStatement(statement, table) {
	case EXECUTE_SUCCESS:
		break
	default:
		t.Fatalf("Selection test failed for this: '%s'\n", buffer.buffer)
	}

}
