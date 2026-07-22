package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"

	"github.com/benitogf/nopog"
	_ "github.com/lib/pq"
)

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

	if err = db.Ping(); err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}

	log.Println("Connected to database, applying nopog schema...")

	if _, err = db.Exec(nopog.SchemaSQL); err != nil {
		log.Fatalf("Schema install failed: %v", err)
	}

	log.Println("Schema installed successfully. Objects created:")
	log.Println("  - nopog_now() microsecond timestamp function")
	log.Println("  - valid() key validation function")
	log.Println("  - create_table(name) / drop_table(name)")
	log.Println("  - nopog_set / nopog_set_batch / nopog_set_meta / nopog_import")
	log.Println("  - nopog_get / nopog_get_range / nopog_get_range_segment / nopog_scan")
	log.Println("  - nopog_peek / nopog_peek_range")
	log.Println("  - nopog_del")
	log.Println("  - nopog_get_by_json / nopog_get_by_field")
}
