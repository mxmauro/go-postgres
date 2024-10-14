// See the LICENSE file for license details.

package postgres

// -----------------------------------------------------------------------------

var errNoRows = &NoRowsError{}

// -----------------------------------------------------------------------------

func (db *Database) handleError(err error) error {
	isOurs := true
	switch TypeOfError(err) {
	case ErrorTypeNone:
	case ErrorTypeNoRows:
		isOurs = false
	}

	// Only deal with fatal database errors. Cancellation, timeouts and empty result sets are not considered fatal.
	db.err.mutex.Lock()
	defer db.err.mutex.Unlock()

	if err != nil && isOurs {
		if db.err.last == nil {
			db.err.last = err
			if db.err.handler != nil {
				db.err.handler(err)
			}
		}
	} else {
		if db.err.last != nil {
			db.err.last = nil
			if db.err.handler != nil {
				db.err.handler(nil)
			}
		}
	}

	// Done
	return err
}
