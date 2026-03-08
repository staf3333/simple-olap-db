package storage_test

import (
	"os"
	"testing"

	"github.com/staf/simple-olap-db/datagen"
	"github.com/staf/simple-olap-db/storage"
)

var result float64

func BenchmarkRowStoreSUM(b *testing.B) {
	dir, err := os.MkdirTemp("", "row-store-sum-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	schema := testSchema()
	rows := datagen.GenerateSalesRows(1_000_000, 42)

	//create row store
	filepath := dir + "/sales.jsonl"
	rowStore := &storage.JsonRowStore{FilePath: filepath}
	err = rowStore.Write(schema, rows)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		colToSum := "price"
		result, err = rowStore.SUM(schema, colToSum)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkColumnStoreSUM(b *testing.B) {
	dir, err := os.MkdirTemp("", "column-store-sum-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	schema := testSchema()
	rows := datagen.GenerateSalesRows(1_000_000, 42)
	columns, err := storage.RowsToColumns(schema, rows)
	if err != nil {
		b.Fatal(err)
	}

	// create column store
	columnStore := &storage.SimpleColumnStore{Directory: dir}
	err = columnStore.Write(schema, columns)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		colToSum := "price"
		result, err = columnStore.SUM(schema, colToSum)
		if err != nil {
			b.Fatal(err)
		}
	}
}
