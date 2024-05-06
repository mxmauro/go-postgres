package postgres

import (
	"context"
	"errors"
	"hash/fnv"
	"math"
	"strings"
	"unicode/utf8"
)

// -----------------------------------------------------------------------------

// MigrationStep contains details about the SQL sentence to execute in this step.
// Pass an empty struct to indicate the end.
type MigrationStep struct {
	// Name is a user defined name for this migration step. I.e.: "v1->v2"
	Name string

	// The index of the SQL sentence within a named block.
	SequenceNo int

	// Actual SQL sentence to execute in this migration step.
	Sql string
}

// MigrationStepCallback is called to get the migration step details at stepIdx position (starting from 1)
type MigrationStepCallback func(ctx context.Context, stepIdx int) (MigrationStep, error)

// -----------------------------------------------------------------------------

// CreateMigrationStepsFromSqlContent creates an array of migration steps based on the provided content
//
// The expected format is the following:
// # a comment with the step name (starting and ending spaces and dashes will be removed)
// A single SQL sentence
// (extra comment/sql sentence pairs)
func CreateMigrationStepsFromSqlContent(content string) ([]MigrationStep, error) {
	steps := make([]MigrationStep, 0)

	currentName := ""
	currentSeqNo := 1

	// Parse content
	contentLen := len(content)
	for ofs := 0; ofs < contentLen; {
		// Check if we can ignore the current line
		deltaOfs := shouldIgnoreLine(content[ofs:])
		if deltaOfs > 0 {
			ofs += deltaOfs
			continue
		}

		// Is it a comment at the beginning of the line?
		if content[ofs] == '#' {
			// Yes, assume new zone if we are not in the middle of an sql sentence
			startOfs := ofs
			ofs += findEol(content[ofs:])

			currentName = truncStrBytes(strings.Trim(content[startOfs:ofs], " \t-=#"), 255)
			if len(currentName) == 0 {
				return nil, errors.New("empty start of block comment")
			}
			currentSeqNo = 1

			continue
		}

		// At this point we start to parse an SQL sentence
		if len(currentName) == 0 {
			return nil, errors.New("SQL sentence found outside a block")
		}

		currentSql := strings.Builder{}
		addSpace := false
		for ofs < contentLen {
			deltaOfs = skipSpaces(content[ofs:])
			if deltaOfs > 0 {
				addSpace = true
				ofs += deltaOfs
				continue
			}
			deltaOfs = skipEol(content[ofs:])
			if deltaOfs > 0 {
				addSpace = true
				ofs += deltaOfs
				continue
			}

			if content[ofs] == '#' {
				// We find a comment, skip until EOL
				addSpace = true
				ofs += 1
				ofs += findEol(content[ofs:])
				continue
			}

			if content[ofs] == ';' {
				// Reached the end of the SQL sentence
				if currentSql.Len() > 0 {
					currentSql.WriteRune(';')

					steps = append(steps, MigrationStep{
						Name:       currentName,
						SequenceNo: currentSeqNo,
						Sql:        currentSql.String(),
					})

					// Reset
					currentSql = strings.Builder{}
					currentSeqNo += 1
				}

				addSpace = false
				ofs += 1
				break
			}

			if content[ofs] == '\'' {
				// Start of a single-quote string
				startOfs := ofs
				ofs += 1

				for {
					if ofs >= contentLen {
						// Open string found
						return nil, errors.New("invalid SQL content (open string)")
					}

					r, rSize := utf8.DecodeRuneInString(content[ofs:])
					if r == utf8.RuneError || rSize == 0 {
						return nil, errors.New("invalid SQL content (invalid char)")
					}
					ofs += rSize

					// Reached the end of the string or double single-quotes?
					if r == '\'' {
						if ofs >= contentLen || content[ofs] != '\'' {
							break // End of string
						}
						// Double single-quotes
						ofs += 1
					}
				}

				if addSpace {
					currentSql.WriteRune(' ')
					addSpace = false
				}
				currentSql.WriteString(content[startOfs:ofs])
				continue
			}

			if content[ofs] == '"' {
				// Start of a double-quotes string
				startOfs := ofs
				ofs += 1

				escapedCharacter := false
				for {
					if ofs >= contentLen {
						// Open string found
						return nil, errors.New("invalid SQL content (open string)")
					}

					r, rSize := utf8.DecodeRuneInString(content[ofs:])
					if r == utf8.RuneError || rSize == 0 {
						return nil, errors.New("invalid SQL content (invalid char)")
					}
					ofs += rSize

					if escapedCharacter {
						escapedCharacter = false
						continue
					}

					// Reached the end of the string?
					if r == '"' {
						break
					}

					// Escaped character?
					if r == '\\' {
						escapedCharacter = true
					}
				}

				if addSpace {
					currentSql.WriteRune(' ')
					addSpace = false
				}
				currentSql.WriteString(content[startOfs:ofs])
				continue
			}

			if content[ofs] == '$' {
				// Dollar tag
				startOfs := ofs
				ofs += 1

				for {
					if ofs >= contentLen {
						return nil, errors.New("invalid SQL content (dollar tag)")
					}
					if content[ofs] == '$' {
						ofs += 1
						break
					}
					if !(content[ofs] == '_' || (content[ofs] >= '0' && content[ofs] <= '9') ||
						(content[ofs] >= 'A' && content[ofs] <= 'Z') ||
						(content[ofs] >= 'a' && content[ofs] <= 'z')) {
						return nil, errors.New("invalid SQL content (dollar tag)")
					}
					ofs += 1
				}
				tag := content[startOfs:ofs]

				// Find the next tag
				deltaOfs = strings.Index(content[ofs:], tag)
				if deltaOfs < 0 {
					return nil, errors.New("invalid SQL content (open dollar tag)")
				}
				ofs += deltaOfs + len(tag)

				if addSpace {
					currentSql.WriteRune(' ')
					addSpace = false
				}
				currentSql.WriteString(content[startOfs:ofs])
				continue
			}

			// If we reached here, it is a single character of an sql sentence
			r, rSize := utf8.DecodeRuneInString(content[ofs:])
			if r == utf8.RuneError || rSize == 0 {
				return nil, errors.New("invalid SQL content (invalid char)")
			}
			ofs += rSize

			if addSpace {
				currentSql.WriteRune(' ')
				addSpace = false
			}
			currentSql.WriteRune(r)
		}

		// At this point we are at the end of the content or reached the end of an SQL sentence
		if currentSql.Len() > 0 {
			currentSql.WriteRune(';')

			steps = append(steps, MigrationStep{
				Name:       currentName,
				SequenceNo: currentSeqNo,
				Sql:        currentSql.String(),
			})

			// Reset
			currentSql = strings.Builder{}
			currentSeqNo += 1
		}
	}

	// Done
	return steps, nil
}

// -----------------------------------------------------------------------------

func (db *Database) RunMigrations(ctx context.Context, tableName string, cb MigrationStepCallback) error {
	// Lock concurrent access from multiple instances/threads
	lockId := db.getMigrationLockId(tableName)

	// Quote table name
	tableName = quoteIdentifier(tableName)

	// We must execute migrations within a single connection
	return db.WithinConn(ctx, func(ctx context.Context, conn Conn) error {
		var stepIdx int32

		_, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", lockId)
		if err != nil {
			return err
		}
		defer func() {
			_, _ = conn.Exec(ctx, "SELECT pg_advisory_unlock($1)", lockId)
		}()

		// Create migration table if it does not exist
		_, err = conn.Exec(ctx,
			`CREATE TABLE IF NOT EXISTS `+tableName+` (
				id         int NOT NULL PRIMARY KEY,
				name       varchar(255) NOT NULL,
				sequence   int NOT NULL,
				executedAt timestamp NOT NULL
		)`)
		if err != nil {
			return err
		}

		// Calculate the next step index to execute based on the last stored
		row := conn.QueryRow(ctx, `SELECT id FROM `+tableName+` ORDER BY id DESC LIMIT 1`)
		err = row.Scan(&stepIdx)
		if err == nil {
			stepIdx += 1
		} else {
			if !IsNoRowsError(err) {
				return err
			}
			stepIdx = 1
		}

		// Run migrations
		for {
			var stepInfo MigrationStep

			stepInfo, err = cb(ctx, int(stepIdx))
			if err != nil {
				return err
			}
			// If no name or sql sentence was provided, assume we finished
			if len(stepInfo.Name) == 0 {
				break
			}

			// Execute step
			err = conn.WithinTx(ctx, func(ctx context.Context, tx Tx) error {
				_, stepErr := tx.Exec(ctx, stepInfo.Sql)
				if stepErr == nil {
					_, stepErr = tx.Exec(
						ctx,
						`INSERT INTO `+tableName+` (id, name, sequence, executedAt) VALUES ($1, $2, $3, NOW());`,
						stepIdx, stepInfo.Name, stepInfo.SequenceNo,
					)
				}
				// Done
				return stepErr
			})
			if err != nil {
				return err
			}

			// Increment index
			stepIdx += 1
		}

		// Done
		return nil
	})
}

func (db *Database) getMigrationLockId(tableName string) int64 {
	h := fnv.New64a()
	_, _ = h.Write(db.nameHash[:])
	_, _ = h.Write([]byte(tableName))
	return int64(h.Sum64() & math.MaxInt64)
}

func truncStrBytes(s string, maxBytes int) string {
	if len(s) <= maxBytes {
		return s
	}
	truncated := s[:maxBytes]
	l := maxBytes
	for l > 0 {
		// Decode last rune. If it's invalid, we'll move back until we find a valid one.
		r, size := utf8.DecodeLastRuneInString(truncated)
		if r != utf8.RuneError {
			break
		}
		if size == 0 {
			return ""
		}
		// If the last rune is invalid, trim the string byte by byte until we get a valid rune.
		l -= 1
		truncated = truncated[:l]
	}
	return truncated
}

// This function returns > 0 if the line must be ignored. Ignored
func shouldIgnoreLine(s string) int {
	ofs := skipSpaces(s)
	if ofs >= len(s) {
		return ofs // Yes
	}
	// End of line?
	if s[ofs] == '\r' || s[ofs] == '\n' {
		ofs += skipEol(s[ofs:])
		return ofs // Yes
	}
	// Comment not at the beginning?
	if s[ofs] == '#' && ofs > 0 {
		ofs += findEol(s[ofs:])
		ofs += skipEol(s[ofs:])
		return ofs // Yes
	}
	// Do not skip this line
	return 0
}

func findEol(s string) int {
	eolOfs := strings.IndexByte(s, '\n')
	if eolOfs < 0 {
		eolOfs = len(s)
	}
	eolOfs2 := strings.IndexByte(s, '\r')
	if eolOfs2 >= 0 && eolOfs2 < eolOfs {
		eolOfs = eolOfs2
	}
	return eolOfs
}

func skipSpaces(s string) int {
	count := 0
	l := len(s)
	for count < l && (s[count] == ' ' || s[count] == '\t') {
		count += 1
	}
	return count
}

func skipEol(s string) int {
	count := 0
	l := len(s)
	for count < l && (s[count] == '\r' || s[count] == '\n') {
		count += 1
	}
	return count
}
