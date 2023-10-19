package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

// Row Definition: will change later according to tables

const COLUMN_USERNAME_SIZE = 32
const COLUMN_EMAIL_SIZE = 255

type Row struct {
	id       int
	username string
	email    string
}

const (
	ID_SIZE       = int(unsafe.Sizeof(Row{}.id))
	USERNAME_SIZE = int(unsafe.Sizeof(Row{}.username))
	EMAIL_SIZE    = int(unsafe.Sizeof(Row{}.email))

	ROW_SIZE = ID_SIZE + COLUMN_USERNAME_SIZE + COLUMN_EMAIL_SIZE + 2
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
	rows         []byte
	numberOfRows int
	num          uint64
}

type Table struct {
	numRows int
	pager   *Pager
}

func pager_open(fileName string) *Pager {
	file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0644) // 0644 represents user read/write permission
	if err != nil {
		fmt.Println("Unable to open file")
		os.Exit(1)
	}

	fileInfo, err := file.Stat()
	if err != nil {
		fmt.Print("Unable to get file info")
		file.Close()
		os.Exit(1)
	}

	fileSizeInBytes := fileInfo.Size()

	// Read the header to get the number of pages
	//var numPages uint32
	//err = binary.Read(file, binary.LittleEndian, &numPages)
	//if err != nil {
	//	// If error occurs while reading the header, assume the database is empty
	//	numPages = 0
	//}

	pager := &Pager{
		File:           file,
		fileDescriptor: int(file.Fd()),
		//numPages:       int(numPages),
		fileLength: int(fileSizeInBytes),
	}

	for i := 0; i < TABLE_MAX_PAGES; i++ {
		pager.pages[i] = nil
	}

	return pager
}

func DB_OPEN(fileName string) *Table {
	pager := pager_open(fileName)
	table := &Table{}
	table.numRows = pager.fileLength / ROW_SIZE
	table.pager = pager
	return table
}

func DB_CLOSE(table *Table) {
	pager := table.pager
	num_full_pages := table.numRows / ROWS_PER_PAGE

	for i := 0; i < num_full_pages; i++ {
		if pager.pages[i] == nil {
			continue
		}
		pager_flush(pager, i, PAGE_SIZE)
		pager.pages[i] = nil
	}

	num_additional_rows := table.numRows % ROWS_PER_PAGE
	if num_additional_rows > 0 {
		page_num := num_full_pages
		if table.pager.pages[page_num] != nil {
			pager_flush(table.pager, page_num, num_additional_rows*ROW_SIZE)
			table.pager.pages[page_num] = nil
		}
	}

	defer table.pager.File.Close()
}

// ------------- Row Functions ----------------

func (table *Table) getPage(pageNum int) *Page {
	if pageNum > TABLE_MAX_PAGES {
		fmt.Printf("Tried to fetch page number out of bounds. %d", pageNum)
		os.Exit(0)
	}

	if table.pager.pages[pageNum] == nil {
		// a cache miss, lets allocate a page struct pointer at this location
		// and load from file?
		page := &Page{
			rows: make([]byte, PAGE_SIZE),
		}
		//table.pager.pages[pageNum] = page
		num_pages := table.pager.fileLength / PAGE_SIZE

		if table.pager.fileLength%PAGE_SIZE >= 1 {
			num_pages += 1
		}

		if pageNum <= num_pages {
			//offset := pageNum * PAGE_SIZE
			offset, err := syscall.Seek(syscall.Handle(table.pager.fileDescriptor), int64(pageNum*PAGE_SIZE), os.SEEK_SET)
			if err != nil {
				fmt.Printf("Error seeking: %v\n", err)
				os.Exit(1)
			}

			if offset == -1 {
				fmt.Printf("Error seeking: %v\n", err)
				os.Exit(1)
			}

			//_, err = table.pager.File.ReadAt(page.rows, int64(offset))
			//
			//if err != nil {
			//	fmt.Printf("Error reading page from file: %v\n", err)
			//	os.Exit(0)
			//}

			_, err = syscall.Read(syscall.Handle(table.pager.fileDescriptor), page.rows)
			if err != nil {
				fmt.Printf("Error reading file: %v\n", err)
				os.Exit(1)
			}

			//if int64(bytesRead) != PAGE_SIZE {
			//	fmt.Println("Error: Read was incomplete")
			//	os.Exit(1)
			//}
		}

		table.pager.pages[pageNum] = page

		return table.pager.pages[pageNum]

	}

	return table.pager.pages[pageNum]

}

func pager_flush(pager *Pager, page_num int, PAGE_SIZE int) {
	if pager.pages[page_num] == nil {
		fmt.Println("Tried to flush null page\n")
		os.Exit(0)
	}

	currentPage := pager.pages[page_num]

	offset := int64(page_num) * int64(PAGE_SIZE)

	_, err := pager.File.WriteAt(currentPage.rows[:PAGE_SIZE], offset)
	if err != nil {
		fmt.Println("Error flushing page!")
		os.Exit(0)
	}

}

func (table *Table) rowSlot(numRows int) (int, int) {
	pageNum := numRows / ROWS_PER_PAGE
	currentPage := table.getPage(pageNum)

	if currentPage == nil {
		fmt.Println("Something went wrong!")
		os.Exit(0)
	}

	rowOffset := numRows % ROWS_PER_PAGE
	return pageNum, rowOffset
}

func (table *Table) serializeRow(row *Row, currentPage int, currentRow int) {
	page := table.pager.pages[currentPage]
	current_row_index := currentRow * ROW_SIZE
	fmt.Printf("Current Page: %v and Current Row: %v\n", currentPage, currentRow)
	data := make([]byte, ROW_SIZE)
	binary.LittleEndian.PutUint32(data[:4], uint32(row.id))
	copy(data[4:4+COLUMN_USERNAME_SIZE+1], []byte(row.username))
	copy(data[4+COLUMN_USERNAME_SIZE+1:4+COLUMN_USERNAME_SIZE+1+COLUMN_EMAIL_SIZE+1], []byte(row.email))
	copy(page.rows[current_row_index:current_row_index+ROW_SIZE], data)
	table.numRows += 1
}

func (table *Table) deserializeRow(currentPage int, currentRow int) *Row {
	page := table.pager.pages[currentPage]
	current_row_index := currentRow * ROW_SIZE
	data := make([]byte, ROW_SIZE)
	copy(data, page.rows[current_row_index:current_row_index+ROW_SIZE])
	id := int(binary.LittleEndian.Uint32(data[:4]))
	username := string(data[4 : 4+COLUMN_USERNAME_SIZE+1])
	email := string(data[4+COLUMN_USERNAME_SIZE+1 : 4+COLUMN_USERNAME_SIZE+1+COLUMN_EMAIL_SIZE+1])
	return &Row{id: id, username: username, email: email}
}

func printRow(row *Row) {
	fmt.Printf("(%d, %s, %s)\n", row.id, row.username, row.email)
}
