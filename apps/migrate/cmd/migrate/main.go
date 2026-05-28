package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/your-org/notification-control-plane/libs/core/config"
)

type migrationFile struct {
	Version string
	Path    string
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	databaseURL := config.MustGetEnv("DATABASE_URL")
	migrationsDir := config.GetEnv("MIGRATIONS_DIR", "/app/migrations")

	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		log.Fatalf("open database: %v", err)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		log.Fatalf("ping database: %v", err)
	}

	files, err := loadMigrationFiles(migrationsDir)
	if err != nil {
		log.Fatalf("load migration files: %v", err)
	}

	if err := ensureSchemaMigrationsTable(ctx, db); err != nil {
		log.Fatalf("ensure schema_migrations table: %v", err)
	}

	applied, err := listAppliedMigrations(ctx, db)
	if err != nil {
		log.Fatalf("list applied migrations: %v", err)
	}

	appliedCount := 0
	for _, file := range files {
		if applied[file.Version] {
			continue
		}
		if err := applyMigration(ctx, db, file); err != nil {
			log.Fatalf("apply migration %s: %v", file.Version, err)
		}
		appliedCount++
		log.Printf("applied migration %s", file.Version)
	}

	log.Printf("migration run complete: %d applied, %d total known", appliedCount, len(files))
}

func loadMigrationFiles(dir string) ([]migrationFile, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	files := make([]migrationFile, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".sql") {
			continue
		}
		version := strings.TrimSuffix(name, filepath.Ext(name))
		files = append(files, migrationFile{
			Version: version,
			Path:    filepath.Join(dir, name),
		})
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].Version < files[j].Version
	})

	return files, nil
}

func ensureSchemaMigrationsTable(ctx context.Context, db *sql.DB) error {
	const query = `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`
	_, err := db.ExecContext(ctx, query)
	return err
}

func listAppliedMigrations(ctx context.Context, db *sql.DB) (map[string]bool, error) {
	rows, err := db.QueryContext(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	applied := map[string]bool{}
	for rows.Next() {
		var version string
		if err := rows.Scan(&version); err != nil {
			return nil, err
		}
		applied[version] = true
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return applied, nil
}

func applyMigration(ctx context.Context, db *sql.DB, file migrationFile) error {
	contents, err := os.ReadFile(file.Path)
	if err != nil {
		return err
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	sqlText := strings.TrimSpace(string(contents))
	if sqlText == "" {
		return errors.New("migration file is empty")
	}

	if _, err = tx.ExecContext(ctx, sqlText); err != nil {
		return fmt.Errorf("execute sql: %w", err)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO schema_migrations(version) VALUES ($1)`, file.Version); err != nil {
		return fmt.Errorf("record migration: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit migration: %w", err)
	}
	return nil
}
