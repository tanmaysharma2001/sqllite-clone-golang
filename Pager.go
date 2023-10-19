package main

import "os"

type Pager struct {
	File           *os.File
	fileDescriptor int
	numPages       int
	fileLength     int
	pages          [TABLE_MAX_PAGES]*Page
}
