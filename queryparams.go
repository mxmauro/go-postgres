package postgres

// -----------------------------------------------------------------------------

type QueryParams struct {
	sql  string
	args []interface{}
}

// -----------------------------------------------------------------------------

func NewQueryParams(sql string, args ...interface{}) QueryParams {
	return QueryParams{
		sql:  sql,
		args: args,
	}
}
