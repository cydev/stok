// Package stok implements fast storage for small files, optimized for zero allocations
// and fast reads.
//
// Trade-offs: space consumption fully relies on vacuum algorithm, writes and deletes are slower than reads.
package stok
