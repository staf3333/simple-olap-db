package storage

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
