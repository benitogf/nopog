package main

import (
	"database/sql"
	_ "embed"
	"flag"
	"fmt"
	"log"

	_ "github.com/lib/pq"
)

//go:embed migrate.sql
var migrationSQL string

func main() {
	host := flag.String("host", "localhost", "PostgreSQL host")
	port := flag.String("port", "5432", "PostgreSQL port")
	user := flag.String("user", "postgres", "PostgreSQL user")
	password := flag.String("password", "postgres", "PostgreSQL password")
	dbname := flag.String("dbname", "postgres", "PostgreSQL database name")
	sslmode := flag.String("sslmode", "disable", "PostgreSQL SSL mode")
	flag.Parse()

	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		*host, *port, *user, *password, *dbname, *sslmode)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}

	log.Println("Connected to database, applying migration...")

	_, err = db.Exec(migrationSQL)
	if err != nil {
		log.Fatalf("Migration failed: %v", err)
	}

	log.Println("Migration completed successfully!")
	log.Println("Changes applied:")
	log.Println("  - Created monotonic_clock table for monotonic timestamps")
	log.Println("  - Created monotonic_now() function")
	log.Println("  - Created valid() function for key validation")
	log.Println("  - Created create_table(name) function to create table pairs")
	log.Println("  - Created drop_table(name) function to drop table pairs")
	log.Println("  - Created nopog_get(table, key) function")
	log.Println("  - Created nopog_peek(table, key) function")
	log.Println("  - Created nopog_set(table, key, value) function")
	log.Println("  - Created nopog_del(table, key) function")
}
