package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
)
type JsonRowStore struct {
	FilePath string
}

// this function should write all the rows to disk
// a single row is a map of column name to value
// given an array of rows
func (s *JsonRowStore) Write(schema Schema, rows []Row) error {
	allJsonLineBytes := []byte{}
	for _, row := range rows {
		jsonRowBytes, err := json.Marshal(row)
		if err != nil {
			return err
		}
		jsonRowBytes = append(jsonRowBytes, '\n')
		allJsonLineBytes = append(allJsonLineBytes, jsonRowBytes...)
	}

	return os.WriteFile(s.FilePath, allJsonLineBytes, 0644)
}

// this function shoould read all rows from the disk
func (s *JsonRowStore) ReadAll(schema Schema) ([]Row, error) {
	file, err := os.Open(s.FilePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	rows := []Row{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		var row Row
		err = json.Unmarshal([]byte(line), &row)
		if err != nil {
			return nil, err
		}
		// json.Unmarshal defaults to setting all numbers as float64 with any (interface{}) type, so we have to figure out if this row needs 
		// a type conversion
		for _, col := range(schema.Columns) {
			if col.Type == TypeInt64 {
				row[col.Name] = int64(row[col.Name].(float64)) 
			}
		}
		rows = append(rows, row)
	}

	return rows, nil
}

func (s *JsonRowStore) SUM(schema Schema, columnName string) (float64, error) {
	rows, err := s.ReadAll(schema)
	if err != nil {
		return 0, err
	}

	// check that the column name is in schema before iterating, because we wont know if it exists in map
	var isColInSchema bool
	var colType string
	for _, schemaCol := range(schema.Columns) {
		if columnName == schemaCol.Name {
			colType = schemaCol.Type
			if colType != TypeInt64 && colType != TypeFloat64 {
				return 0, fmt.Errorf("Cannot sum column of type %s", schemaCol.Type)
			}
			isColInSchema = true
			break
		}
	}
	if !isColInSchema {
		return 0, fmt.Errorf("col %s does not exist in schema", columnName)
	}

	var sum float64
	for _, row := range(rows) {
		switch colType {
		case TypeInt64:
			sum += float64(row[columnName].(int64))
		case TypeFloat64:
			sum += row[columnName].(float64)
		}
	}
	return sum, nil
}
