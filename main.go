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
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgxlisten"
)

type (
	ClientID        string
	MessageID       string
	RegisterRequest struct {
		ClientID ClientID
		Ch       chan<- MessageID
	}
)

var (
	db           *pgxpool.Pool
	upgrader     = websocket.Upgrader{}
	registerCh   = make(chan RegisterRequest)
	unregisterCh = make(chan ClientID)
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

	if err := broadcast(); err != nil {
		slog.Error("failed to broadcast", "error", err)
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
	slog.Info("server started")
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
	slog.Info("client connected", "remote_addr", r.RemoteAddr)
	go func() {
		defer conn.Close()
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					slog.Error("unexpected close error", "error", err)
				}
				break
			}

			slog.Info("received message", "message", string(msg))

			var data any
			if err := json.Unmarshal(msg, &data); err != nil {
				slog.Error("failed to unmarshal message", "error", err)
				continue
			}

			if _, err := db.Exec(context.Background(), `INSERT INTO events (id, message)
VALUES ($1, $2)`, uuid.NewString(), msg); err != nil {
				slog.Error("failed to insert event", "error", err)
				continue
			}
		}
	}()
	go func() {
		clientID := ClientID(uuid.NewString())
		receiveCh := make(chan MessageID)
		registerCh <- RegisterRequest{ClientID: clientID, Ch: receiveCh}
		defer func() {
			unregisterCh <- clientID
			close(receiveCh)
		}()
		for {
			messageID := <-receiveCh
			var msg json.RawMessage
			if err := db.QueryRow(context.Background(), `SELECT message FROM events WHERE id = $1`, messageID).Scan(&msg); err != nil {
				slog.Error("failed to query event", "error", err)
				continue
			}
			if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				slog.Error("failed to write message", "error", err)
				break
			}
		}
	}()
}

func broadcast() error {
	listener := &pgxlisten.Listener{
		Connect: func(ctx context.Context) (*pgx.Conn, error) {
			c, err := db.Acquire(ctx)
			if err != nil {
				return nil, err
			}
			return c.Conn(), nil
		},
	}
	notifyCh := make(chan MessageID)
	listener.Handle("new_events", pgxlisten.HandlerFunc(
		func(ctx context.Context, notification *pgconn.Notification, conn *pgx.Conn) error {
			slog.Info("received notification", "payload", notification.Payload)
			notifyCh <- MessageID(notification.Payload)
			return nil
		}),
	)

	go func() {
		slog.Info("listening for notifications")
		if err := listener.Listen(context.Background()); err != nil {
			slog.Error("failed to listen", "error", err)
		}
	}()

	clientMap := make(map[ClientID]chan<- MessageID)
	go func() {
		for {
			select {
			case messageID := <-notifyCh:
				slog.Info("broadcasting message", "message_id", messageID)
				for _, ch := range clientMap {
					ch <- messageID
				}
			case registerReq := <-registerCh:
				slog.Info("client registered", "client_id", registerReq.ClientID)
				clientMap[registerReq.ClientID] = registerReq.Ch
			case clientID := <-unregisterCh:
				slog.Info("client unregistered", "client_id", clientID)
				delete(clientMap, clientID)
			}
		}
	}()
	return nil
}
