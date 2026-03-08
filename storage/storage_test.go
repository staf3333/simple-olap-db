package storage_test

import (
	"os"
	"testing"
	"reflect"

	"github.com/staf/simple-olap-db/storage"
)

// Small dataset for unit tests
func testRows() []storage.Row {
	return []storage.Row{
		{"id": int64(1), "product": "Widget", "category": "Tools", "price": 9.99, "quantity": int64(10), "region": "North"},
		{"id": int64(2), "product": "Gadget", "category": "Electronics", "price": 14.50, "quantity": int64(5), "region": "South"},
		{"id": int64(3), "product": "Doohickey", "category": "Tools", "price": 3.25, "quantity": int64(100), "region": "North"},
		{"id": int64(4), "product": "Thingamajig", "category": "Electronics", "price": 99.99, "quantity": int64(2), "region": "East"},
		{"id": int64(5), "product": "Whatchamacallit", "category": "Tools", "price": 7.50, "quantity": int64(25), "region": "West"},
	}
}

func testSchema() storage.Schema {
	return storage.SalesSchema()
}

// TestRowStoreRoundTrip verifies that rows survive a write→read cycle.
func TestRowStoreRoundTrip(t *testing.T) {
	dir, err := os.MkdirTemp("", "rowstore-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	schema := testSchema()
	rows := testRows()

	filepath := dir + "/sales.jsonl"
	rowStore := &storage.JsonRowStore{FilePath: filepath}
	err = rowStore.Write(schema, rows)
	if err != nil {
		t.Fatal(err)
	}

	readRows, err := rowStore.ReadAll(schema)
	if err != nil {
		t.Fatal(err)
	}

	// if you get this error, it is likely that you are missing type conversion
	if !reflect.DeepEqual(readRows, rows) {
		t.Errorf("rows don't match! \nwant: %#v\ngot:%#v", rows, readRows)
		t.Logf("original id type: %T, readback id type: %T", rows[0]["id"], readRows[0]["id"])

	}
}

func TestColumnStoreWriteOnly(t *testing.T) {
	dir, err := os.MkdirTemp("", "colstore-test-*")
	if err != nil {
		t.Fatal(err)
	}

	schema := testSchema()
	rows := testRows()
	columns, err := storage.RowsToColumns(schema, rows)
	if err != nil {
		t.Fatal(err)
	}

	columnStore := &storage.SimpleColumnStore{Directory: dir}
	err = columnStore.Write(schema, columns)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("ColumnStore here: %s", dir) 
}

// TestColumnStoreRoundTrip verifies that columns survive a write→read cycle.
func TestColumnStoreRoundTrip(t *testing.T) {
	dir, err := os.MkdirTemp("", "colstore-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	schema := testSchema()
	rows := testRows()
	columns, err := storage.RowsToColumns(schema, rows)
	if err != nil {
		t.Fatal(err)
	}

	columnStore := &storage.SimpleColumnStore{Directory: dir}
	err = columnStore.Write(schema, columns)
	if err != nil {
		t.Fatal(err)
	}

	// get column names
	var colNames []string
	for _, col := range(columns) {
		colNames = append(colNames, col.Name)
	}
	readColumns, err := columnStore.ReadColumns(schema, colNames)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(readColumns, columns) {
		t.Errorf("columns don't match! \nwant: %#v\ngot:%#v", columns, readColumns)
	}
}

// TestColumnStoreSelectiveRead verifies that ReadColumns only returns
// the requested columns (the whole point of column storage!).
func TestColumnStoreSelectiveRead(t *testing.T) {
	dir, err := os.MkdirTemp("", "colstore-selective-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	schema := testSchema()
	rows := testRows()
	columns, err := storage.RowsToColumns(schema, rows)
	if err != nil {
		t.Fatal(err)
	}

	columnStore := &storage.SimpleColumnStore{Directory: dir}
	err = columnStore.Write(schema, columns)
	if err != nil {
		t.Fatal(err)
	}

	// get column names
	colsToRead := []string {"price", "quantity"}
	cols, err := columnStore.ReadColumns(schema, colsToRead)
	if err != nil {
		t.Fatal(err)
	}

	if len(cols) != 2 {
		t.Errorf("reading selective number of cols did not work as expected! wanted %d cols, got %d cols. Cols: %v", len(colsToRead), len(cols), cols)
	}

	expected := []storage.Column{columns[3], columns[4]}
	if !reflect.DeepEqual(cols, expected) {
	       t.Errorf("data from selectively read columns do not match!")
	}
}

// TestStoreSumsAsExpected verify that the SUM functions on each type of store return expected value
func TestStoreSumsAsExpected(t *testing.T) {
	dir, err := os.MkdirTemp("", "both-store-sum-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	schema := testSchema()
	rows := testRows()
	columns, err := storage.RowsToColumns(schema, rows)
	if err != nil {
		t.Fatal(err)
	}

	// create column store
	columnStore := &storage.SimpleColumnStore{Directory: dir}
	err = columnStore.Write(schema, columns)
	if err != nil {
		t.Fatal(err)
	}

	//create row store
	filepath := dir + "/sales.jsonl"
	rowStore := &storage.JsonRowStore{FilePath: filepath}
	err = rowStore.Write(schema, rows)
	if err != nil {
		t.Fatal(err)
	}

	colToSum := "price"
	rowSum, err := rowStore.SUM(schema, colToSum)
	if err != nil {
		t.Fatal(err)
	}

	colSum, err := columnStore.SUM(schema, colToSum)
	if err != nil {
		t.Fatal(err)
	}

	expectedSum := float64(135.23)
	if rowSum != colSum || rowSum != expectedSum {
		t.Errorf("Unexpected sum... expected %v rowSum=%v, colSum=%v", expectedSum, rowSum, colSum)
	}
}
