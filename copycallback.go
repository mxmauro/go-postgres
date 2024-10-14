package postgres

import (
	"context"
	"errors"
)

// -----------------------------------------------------------------------------

type copyWithCallback struct {
	ctx     context.Context
	cb      CopyCallback
	counter int
	data    []interface{}
	err     error
}

// -----------------------------------------------------------------------------

func (c *copyWithCallback) Next() bool {
	var err error

	if c.err != nil || c.counter < 0 {
		return false
	}

	c.data, err = c.cb(c.ctx, c.counter)
	if err != nil {
		c.err = newError(err, "")
		c.data = nil
		return false
	}

	if c.data == nil {
		c.counter = -1
		return false
	}

	c.counter += 1
	return true
}

func (c *copyWithCallback) Values() ([]interface{}, error) {
	if c.err != nil {
		return nil, c.err
	}
	if c.data == nil {
		return nil, errors.New("unexpected call to copyWithCallback.Values")
	}
	data := c.data
	c.data = nil
	return data, nil
}

func (c *copyWithCallback) Err() error {
	return c.err
}
