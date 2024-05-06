package postgres_test

import (
	"context"
	"flag"
	"fmt"
	"testing"

	"github.com/randlabs/go-postgres"
)

// -----------------------------------------------------------------------------

func TestMigration(t *testing.T) {
	var db *postgres.Database
	var err error

	// Parse and check command-line parameters
	flag.Parse()
	checkSettings(t)

	ctx := context.Background()

	// Create database driver
	if len(pgUrl) > 0 {
		db, err = postgres.NewFromURL(ctx, pgUrl)
	} else {
		db, err = postgres.New(ctx, postgres.Options{
			Host:     pgHost,
			Port:     uint16(pgPort),
			User:     pgUsername,
			Password: pgPassword,
			Name:     pgDatabaseName,
		})
	}
	if err != nil {
		t.Fatal(err.Error())
	}
	defer db.Close()

	// t.Log("Run migration test")
	err = runMigrationTest(ctx, db)
	if err != nil {
		t.Fatal(err.Error())
	}
}

func TestMigrationStepParser(t *testing.T) {
	steps, err := postgres.CreateMigrationStepsFromSqlContent(`
# Simple table with single quotes in the default values
CREATE TABLE "Employee" (
	"EmployeeID" SERIAL PRIMARY KEY,
	"FirstName" VARCHAR(100) NOT NULL,
	"LastName" VARCHAR(100) NOT NULL,
	"DateOfBirth" DATE NOT NULL DEFAULT '1990-01-01'
);

# Table with special characters in column names
CREATE TABLE "Order-Details" (
	"Order_ID" INT,
	"Product_Name" VARCHAR(255) DEFAULT 'unknown',
	"Unit_Price" NUMERIC(10, 2) DEFAULT '0.00',
	"Quantity" INT DEFAULT '1',
	PRIMARY KEY ("Order_ID", "Product_Name")
);

# Creating an index on the FirstName and LastName columns
CREATE INDEX "idx_employee_name" ON "Employee" ("FirstName", "LastName");

# Creating a unique index using a function
CREATE UNIQUE INDEX "idx_lower_last_name" ON "Employee" (LOWER("LastName"));

# A function to calculate age from the DateOfBirth
CREATE FUNCTION "calculate_age" ("dob" DATE) RETURNS INT AS $$
BEGIN
    RETURN DATE_PART('year', AGE("dob"));
END;
$$ LANGUAGE plpgsql;

# A function to concatenate first and last name with a space between
CREATE FUNCTION "full_name" ("first" TEXT, "last" TEXT) RETURNS TEXT AS $tag$
BEGIN
    RETURN "first" || ' ' || "last";
END;
$tag$ LANGUAGE plpgsql;
`)
	if err != nil {
		t.Fatal(err.Error())
	}
	if len(steps) != 6 {
		t.Fatalf("Wrong number of steps: %d", len(steps))
	}
}

// -----------------------------------------------------------------------------

func runMigrationTest(ctx context.Context, db *postgres.Database) error {
	var stepIdx int

	// Destroy old test tables if exists
	_, err := db.Exec(ctx, `DROP TABLE IF EXISTS migrations_test`)
	if err == nil {
		_, err = db.Exec(ctx, `DROP TABLE IF EXISTS migrations`)
	}
	if err != nil {
		return fmt.Errorf("unable to drop tables [err=%v]", err.Error())
	}

	// Run migrations
	err = db.RunMigrations(ctx, "migrations", func(ctx context.Context, stepIdx int) (postgres.MigrationStep, error) {
		switch stepIdx {
		case 1:
			return postgres.MigrationStep{
				Name:       "v1",
				SequenceNo: 1,
				Sql:        `CREATE TABLE migrations_test (id int NOT NULL PRIMARY KEY, name varchar(255) NOT NULL);`,
			}, nil

		case 2:
			return postgres.MigrationStep{
				Name:       "v1",
				SequenceNo: 2,
				Sql:        `ALTER TABLE migrations_test ADD COLUMN description TEXT;`,
			}, nil
		}
		return postgres.MigrationStep{}, nil
	})
	if err != nil {
		return fmt.Errorf("unable to run migrations [err=%v]", err.Error())
	}

	// Check last step
	row := db.QueryRow(ctx, `SELECT id FROM migrations ORDER BY id DESC LIMIT 1`)
	err = row.Scan(&stepIdx)
	if err != nil {
		return fmt.Errorf("unable to get last migration step [err=%v]", err.Error())
	}
	if stepIdx != 2 {
		return fmt.Errorf("last migration step mismatch [got=%v] [expected=2]", stepIdx)
	}

	// Run more migrations
	err = db.RunMigrations(ctx, "migrations", func(ctx context.Context, stepIdx int) (postgres.MigrationStep, error) {
		if stepIdx != 3 {
			return postgres.MigrationStep{}, fmt.Errorf("migration step mismatch [got=%v] [expected=3]", stepIdx)
		}
		return postgres.MigrationStep{}, nil
	})
	if err != nil {
		return fmt.Errorf("unable to run more migrations [err=%v]", err.Error())
	}

	// Done
	return nil
}
