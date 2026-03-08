package storage

import (
	"fmt"
	"os"
	"path/filepath"
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

// this function reads 
func (s *SimpleColumnStore) ReadColumns(schema Schema, columnNames []string) ([]Column, error) {
	return nil, nil
}
