package storage

// TODO: Implement RowStore
//
// Your job: create a struct that implements the RowStore interface.
// It should write all rows to a single file and read them back.
//
// Think about:
//   - What on-disk format will you use? (JSON lines, CSV, binary?)
//   - How do you handle different column types when reading back?
//
// Start simple — get it working, then optimize.
// row store is a list of rows where each row is a map of columns to their values 
type JsonRowStore struct {
	FilePath string
}

// this function should write all the rows to disk
func (s *JsonRowStore) Write(schema Schema, rows []Row) error {
	return nil
}

// this function shoould read all rows from the disk
func (s *JsonRowStore) ReadAll(schema Schema) ([]Row, error) {
	return nil, nil
}
