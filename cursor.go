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
	r      Rows
	v      *Values
	before func(*Values) (any, error)
	after  func(any) (T, error)
}

func (c *cursor[T]) Close() error {
	return c.r.Close()
}

func (c *cursor[T]) Err() error {
	return c.r.Err()
}

func (c *cursor[T]) Next() bool {
	return c.r.Next()
}

func (c *cursor[T]) Get() (T, error) {
	return scanOneRow(c.v, c.r, c.before, c.after)
}
