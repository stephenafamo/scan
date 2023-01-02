package scan

type ICursor[T any] interface {
	// Close the underlying rows
	Close() error
	// Prepare the next row
	Next() bool
	// Get the values of the current row
	Get() (T, error)
	// Return any error with the underlying rows
	Err() error
}

type cursor[T any] struct {
	v      *Row
	before func(*Row) (any, error)
	after  func(any) (T, error)
}

func (c *cursor[T]) Close() error {
	return c.v.r.Close()
}

func (c *cursor[T]) Err() error {
	return c.v.r.Err()
}

func (c *cursor[T]) Next() bool {
	return c.v.r.Next()
}

func (c *cursor[T]) Get() (T, error) {
	return scanOneRow(c.v, c.before, c.after)
}
