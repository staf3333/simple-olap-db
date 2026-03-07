package storage

import "fmt"

// Supported column types
const (
	TypeInt64   = "int64"
	TypeFloat64 = "float64"
	TypeString  = "string"
)

// Column holds a single column's data. Only one of the typed slices
// is populated, determined by Type.
type Column struct {
	Name       string
	Type       string
	IntData    []int64
	FloatData  []float64
	StringData []string
}

// Len returns the number of values in the column.
func (c *Column) Len() int {
	switch c.Type {
	case TypeInt64:
		return len(c.IntData)
	case TypeFloat64:
		return len(c.FloatData)
	case TypeString:
		return len(c.StringData)
	default:
		return 0
	}
}

// ColumnDef describes a column's name and type (used in schemas).
type ColumnDef struct {
	Name string
	Type string
}

// Schema defines the structure of a table.
type Schema struct {
	TableName string
	Columns   []ColumnDef
}

// Row is a single row represented as a map of column name to value.
type Row map[string]interface{}

// RowStore is the interface you'll implement for row-oriented storage.
type RowStore interface {
	// Write writes all rows to disk.
	Write(schema Schema, rows []Row) error
	// ReadAll reads all rows back from disk.
	ReadAll(schema Schema) ([]Row, error)
}

// ColumnStore is the interface you'll implement for column-oriented storage.
type ColumnStore interface {
	// Write writes columns to disk.
	Write(schema Schema, columns []Column) error
	// ReadColumns reads specific columns back from disk.
	ReadColumns(schema Schema, columnNames []string) ([]Column, error)
}

// SalesSchema returns the schema for the sample sales dataset.
func SalesSchema() Schema {
	return Schema{
		TableName: "sales",
		Columns: []ColumnDef{
			{Name: "id", Type: TypeInt64},
			{Name: "product", Type: TypeString},
			{Name: "category", Type: TypeString},
			{Name: "price", Type: TypeFloat64},
			{Name: "quantity", Type: TypeInt64},
			{Name: "region", Type: TypeString},
		},
	}
}

// RowsToColumns converts row-oriented data to column-oriented data.
// This is provided as a helper — the conversion logic isn't the interesting part.
func RowsToColumns(schema Schema, rows []Row) ([]Column, error) {
	columns := make([]Column, len(schema.Columns))
	n := len(rows)

	for i, def := range schema.Columns {
		col := Column{Name: def.Name, Type: def.Type}
		switch def.Type {
		case TypeInt64:
			col.IntData = make([]int64, 0, n)
		case TypeFloat64:
			col.FloatData = make([]float64, 0, n)
		case TypeString:
			col.StringData = make([]string, 0, n)
		}
		columns[i] = col
	}

	for rowIdx, row := range rows {
		for i, def := range schema.Columns {
			val, ok := row[def.Name]
			if !ok {
				return nil, fmt.Errorf("row %d missing column %q", rowIdx, def.Name)
			}
			switch def.Type {
			case TypeInt64:
				v, ok := val.(int64)
				if !ok {
					return nil, fmt.Errorf("row %d column %q: expected int64", rowIdx, def.Name)
				}
				columns[i].IntData = append(columns[i].IntData, v)
			case TypeFloat64:
				v, ok := val.(float64)
				if !ok {
					return nil, fmt.Errorf("row %d column %q: expected float64", rowIdx, def.Name)
				}
				columns[i].FloatData = append(columns[i].FloatData, v)
			case TypeString:
				v, ok := val.(string)
				if !ok {
					return nil, fmt.Errorf("row %d column %q: expected string", rowIdx, def.Name)
				}
				columns[i].StringData = append(columns[i].StringData, v)
			}
		}
	}

	return columns, nil
}
