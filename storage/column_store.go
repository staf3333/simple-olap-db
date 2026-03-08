package storage

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

// TODO: Implement ColumnStore
//
// Your job: create a struct that implements the ColumnStore interface.
// It should write each column to its own file and read back only
// the columns requested.
//
// Think about:
//   - One file per column? What naming convention?
//   - What on-disk format for each column? (one value per line? binary encoding?)
//   - ReadColumns should ONLY read the files for requested columns —
//     that's the whole point of column storage!
//
// Start simple — get it working, then optimize.

type SimpleColumnStore struct {
	Directory string
}

// this function should write columns to disk in simple text file format
func (s *SimpleColumnStore) Write(schema Schema, columns []Column) error {
	// given directory, need to write a series of files where the file name matches the column name 
	for _,  col := range(columns) {
		// create file with this columns same in the given directory
		filePath := filepath.Join(s.Directory, col.Name+".col")
		var columnBytes []byte
		switch col.Type {
		case TypeInt64:
			for _,val := range(col.IntData) {
				line := fmt.Sprintf("%v\n", val)
				columnBytes = append(columnBytes, []byte(line)...)
			}
		case TypeFloat64:
			for _,val := range(col.FloatData) {
				line := fmt.Sprintf("%v\n", val)
				columnBytes = append(columnBytes, []byte(line)...)
			}
		case TypeString:
			for _,val := range(col.StringData) {
				line := fmt.Sprintf("%v\n", val)
				columnBytes = append(columnBytes, []byte(line)...)
			}
		}
		err := os.WriteFile(filePath, columnBytes, 0644)
		if err != nil {
			return err
		}
	}

	return nil
}

// this function reads files from disk into array of columns
func (s *SimpleColumnStore) ReadColumns(schema Schema, columnNames []string) ([]Column, error) {
	// for given directory, open each file and collect into column, store all the columns in a slice
	// know the file paths from column names
	var columns []Column

	for _, colName := range(columnNames) {
		colFilePath := filepath.Join(s.Directory, colName+".col")

		colFile, err := os.Open(colFilePath)
		if err != nil {
			return nil, err
		}
		
		var colType string
		for _, schemaCol := range(schema.Columns) {
			if schemaCol.Name == colName {
				colType = schemaCol.Type
			}
		}
		col := Column{Name: colName, Type: colType}
		scanner := bufio.NewScanner(colFile)
		for scanner.Scan() {
			line := scanner.Text()
			switch col.Type {
			case TypeInt64:
				val, err := strconv.ParseInt(line, 10, 64)
				if err != nil {
					return nil, err
				}
				col.IntData = append(col.IntData, val)
			case TypeFloat64:
				val, err := strconv.ParseFloat(line, 64)
				if err != nil {
					return nil, err
				}
				col.FloatData = append(col.FloatData, val)
			case TypeString:
				col.StringData = append(col.StringData, line)
			}
		}
		colFile.Close()

		columns = append(columns, col)
	}

	return columns, nil
}
