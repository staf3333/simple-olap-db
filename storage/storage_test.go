package storage_test

import (
	"os"
	"testing"

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
	t.Skip("TODO: implement RowStore, then remove this skip")

	dir, err := os.MkdirTemp("", "rowstore-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	schema := testSchema()
	rows := testRows()

	// TODO: create your RowStore, write rows, read them back,
	// and verify they match.
	_ = dir
	_ = schema
	_ = rows
}

// TestColumnStoreRoundTrip verifies that columns survive a write→read cycle.
func TestColumnStoreRoundTrip(t *testing.T) {
	t.Skip("TODO: implement ColumnStore, then remove this skip")

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

	// TODO: create your ColumnStore, write columns, read them back,
	// and verify they match.
	_ = dir
	_ = columns
}

// TestColumnStoreSelectiveRead verifies that ReadColumns only returns
// the requested columns (the whole point of column storage!).
func TestColumnStoreSelectiveRead(t *testing.T) {
	t.Skip("TODO: implement ColumnStore, then remove this skip")

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

	// TODO: write all columns, then read back ONLY "price" and "quantity".
	// Verify you get exactly 2 columns with the correct data.
	_ = dir
	_ = columns
}
