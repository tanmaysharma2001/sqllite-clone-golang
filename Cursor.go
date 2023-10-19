package main

type Cursor struct {
	table        *Table
	row_num      int
	end_of_table bool
}

func (table *Table) table_start() *Cursor {
	cursor := &Cursor{}
	cursor.table = table
	cursor.row_num = 0
	cursor.end_of_table = (table.numRows == 0)

	return cursor
}

func (table *Table) table_end() *Cursor {
	cursor := &Cursor{}
	cursor.table = table
	cursor.row_num = table.numRows
	cursor.end_of_table = true

	return cursor
}

func (cursor *Cursor) advance() {
	cursor.row_num += 1
	if cursor.row_num == cursor.table.numRows {
		cursor.end_of_table = true
	}
}
