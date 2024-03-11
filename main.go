package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	db       *pgxpool.Pool
	upgrader = websocket.Upgrader{}
)

func main() {
	dbPool, err := pgxpool.New(context.Background(), "postgres://postgres@localhost:5432")
	if err != nil {
		slog.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	db = dbPool

	if err := migrate(); err != nil {
		slog.Error("failed to migrate database", "error", err)
		os.Exit(1)
	}

	if err := serve(); err != nil {
		slog.Error("failed to serve", "error", err)
		os.Exit(1)
	}
}

func migrate() error {
	ctx := context.Background()
	if _, err := db.Exec(ctx, `CREATE TABLE IF NOT EXISTS events (
id uuid PRIMARY KEY,
message jsonb NOT NULL,
created_at timestamp with time zone DEFAULT now()
)`); err != nil {
		return fmt.Errorf("create events table: %w", err)
	}
	if _, err := db.Exec(ctx, `CREATE OR REPLACE FUNCTION notify_event() RETURNS TRIGGER AS $$
DECLARE
BEGIN
  PERFORM pg_notify('new_events', NEW.id::text);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql`); err != nil {
		return fmt.Errorf("create notify_event function: %w", err)
	}
	if _, err := db.Exec(ctx, `CREATE OR REPLACE TRIGGER notify_event_trigger
AFTER INSERT ON events
FOR EACH ROW EXECUTE FUNCTION notify_event()`); err != nil {
		return fmt.Errorf("create notify_event_trigger trigger: %w", err)
	}
	return nil
}

func serve() error {
	http.HandleFunc("/ws", serveWebSocket)
	if err := http.ListenAndServe(":8080", nil); err != nil {
		return fmt.Errorf("listen and serve: %w", err)
	}
	return nil
}

func serveWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = fmt.Fprintf(w, "failed to upgrade connection: %v", err)
		slog.Error("failed to upgrade connection", "error", err)
		return
	}
	defer conn.Close()
	go func() {
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					slog.Error("unexpected close error", "error", err)
				}
				break
			}
			var msgJSON []byte
			if err := json.Unmarshal(msg, &msgJSON); err != nil {
				slog.Error("failed to unmarshal message", "error", err)
				continue
			}
			if _, err := db.Exec(context.Background(), `INSERT INTO events (id, message)
VALUES ($1, $2)`, uuid.NewString(), string(msgJSON)); err != nil {
				slog.Error("failed to insert event", "error", err)
				continue
			}
		}
	}()
}
