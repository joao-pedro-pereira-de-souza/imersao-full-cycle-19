package main

import (
	"database/sql"
	"fmt"
	"imersaofc/internal/converter"
	"log/slog"
	"os"

	_ "github.com/lib/pq"
)

func connectPostgres() (*sql.DB, error) {
	user := getEnvOrDefault("POSTGRES_USER", "user")
	password := getEnvOrDefault("POSTGRES_PASSWORD", "password")
	dbname := getEnvOrDefault("POSTGRES_DB", "converter")

	host := getEnvOrDefault("POSTGRES_HOST", "postgres")
	sslmode := getEnvOrDefault("POSTGRES_SSLMODE", "disable")

	connStr := fmt.Sprintf("user=%s password=%s dbname=%s host=%s sslmode=%s", user, password, dbname, host, sslmode)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		slog.Error("failed to connect", slog.String("connStr", connStr))

		return nil, err
	}

	err = db.Ping()
	if err != nil {
		slog.Error("failed to connect", slog.String("connStr", connStr))
		return nil, err
	}

	slog.Info("connection to postgres succeeded")

	return db, nil
}

func getEnvOrDefault(key, defaultValue string) string {

	if value, exists := os.LookupEnv(key); exists {
		return value
	}

	return defaultValue
}
func main() {

	db, err := connectPostgres()
	if err != nil {
		panic(err)
	}
	vc := converter.NewVideoConverter(db)

	vc.Hendle([]byte(`{"video_id": 1, "path": "media/uploads/1"}`))
}
