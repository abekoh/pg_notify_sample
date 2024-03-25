package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgxlisten"
)

type (
	RoomID          string
	ClientID        string
	EventID         string
	RegisterRequest struct {
		RoomID   RoomID
		ClientID ClientID
		Ch       chan<- EventID
	}
	UnregisterRequest struct {
		RoomID   RoomID
		ClientID ClientID
	}
	NewEventsPayload struct {
		EventID  EventID  `json:"event_id"`
		RoomID   RoomID   `json:"room_id"`
		ClientID ClientID `json:"client_id"`
	}
)

var (
	db           *pgxpool.Pool
	upgrader     = websocket.Upgrader{}
	registerCh   = make(chan RegisterRequest)
	unregisterCh = make(chan UnregisterRequest)
)

func main() {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	ctx, cancel := context.WithCancel(context.Background())
	if err := execute(ctx); err != nil {
		slog.Error("failed to execute", "error", err)
		os.Exit(1)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	<-sigCh
	cancel()
}

func execute(ctx context.Context) error {
	dbPool, err := pgxpool.New(ctx, "postgres://postgres@localhost:5432")
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	db = dbPool

	if err := migrate(ctx); err != nil {
		return fmt.Errorf("failed to migrate database: %w", err)
	}

	if err := listenAndNotify(ctx); err != nil {
		return fmt.Errorf("failed to listenAndNotify: %w", err)
	}

	if err := serve(ctx); err != nil {
		return fmt.Errorf("failed to serve: %w", err)
	}
	return nil
}

func migrate(ctx context.Context) error {
	if _, err := db.Exec(ctx, `CREATE TABLE IF NOT EXISTS events (
event_id uuid PRIMARY KEY,
room_id uuid NOT NULL,
client_id uuid NOT NULL,
message jsonb NOT NULL,
created_at timestamp with time zone DEFAULT now()
)`); err != nil {
		return fmt.Errorf("create events table: %w", err)
	}
	if _, err := db.Exec(ctx, `CREATE OR REPLACE FUNCTION notify_event() RETURNS TRIGGER AS $$
DECLARE
BEGIN
  PERFORM PG_NOTIFY('new_events', JSON_BUILD_OBJECT('event_id', NEW.event_id, 'room_id', NEW.room_id, 'client_id', NEW.client_id)::text);
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

func serve(ctx context.Context) error {
	srv := &http.Server{Addr: ":8080"}
	http.HandleFunc("/ws", handleWebSocket)
	slog.Info("server started")
	go func() {
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			slog.Error("failed to listen and serve", "error", err)
		}
	}()
	go func() {
		<-ctx.Done()
		slog.Info("server stopping")
		if err := srv.Shutdown(context.Background()); err != nil {
			slog.Error("failed to shutdown server", "error", err)
		}
	}()
	return nil
}

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	roomIDStr := strings.ToLower(r.URL.Query().Get("room_id"))
	if err := uuid.Validate(roomIDStr); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = fmt.Fprintf(w, "invalid room_id: %v", err)
		slog.Error("invalid room_id", "error", err)
		return
	}
	roomID := RoomID(roomIDStr)

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = fmt.Fprintf(w, "failed to upgrade connection: %v", err)
		slog.Error("failed to upgrade connection", "error", err)
		return
	}

	clientID := ClientID(uuid.NewString())
	slog.Info("client connected", "remote_addr", r.RemoteAddr, "client_id", clientID)

	receiveCh := make(chan EventID, 100)
	registerCh <- RegisterRequest{RoomID: roomID, ClientID: clientID, Ch: receiveCh}

	ctx, cancel := context.WithCancel(context.Background())

	// Writer
	go func() {
		defer func() {
			cancel()
			conn.Close()
			close(receiveCh)
			unregisterCh <- UnregisterRequest{RoomID: roomID, ClientID: clientID}
			slog.Info("client disconnected", "client_id", clientID)
		}()
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					slog.Error("unexpected close error", "error", err)
				}
				break
			}

			var data any
			if err := json.Unmarshal(msg, &data); err != nil {
				slog.Error("failed to unmarshal message", "error", err)
				continue
			}

			eventID := EventID(uuid.NewString())
			slog.Debug("write event", "event_id", eventID, "client_id", clientID, "room_id", roomID)

			if _, err := db.Exec(ctx, `INSERT INTO events (event_id, room_id, client_id, message) VALUES ($1, $2, $3, $4)`,
				eventID, roomID, clientID, msg); err != nil {
				slog.Error("failed to insert event", "error", err, "event_id", eventID, "client_id", clientID, "room_id", roomID)
				continue
			}
		}
	}()
	// Reader
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case eventID, ok := <-receiveCh:
				if !ok {
					break
				}
				var msg json.RawMessage
				if err := db.QueryRow(ctx, `SELECT message FROM events WHERE event_id = $1`, eventID).Scan(&msg); err != nil {
					slog.Error("failed to query event", "error", err, "event_id", eventID, "client_id", clientID, "room_id", roomID)
					continue
				}
				slog.Debug("read message", "event_id", eventID, "client_id", clientID, "room_id", roomID)
				if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
					slog.Error("failed to write message", "error", err, "event_id", eventID, "client_id", clientID, "room_id", roomID)
					break
				}
			}
		}
	}()
}

func listenAndNotify(ctx context.Context) error {
	listener := &pgxlisten.Listener{
		Connect: func(ctx context.Context) (*pgx.Conn, error) {
			c, err := db.Acquire(ctx)
			if err != nil {
				return nil, err
			}
			return c.Conn(), nil
		},
	}
	newEventCh := make(chan NewEventsPayload, 10)
	listener.Handle("new_events", pgxlisten.HandlerFunc(
		func(ctx context.Context, notification *pgconn.Notification, conn *pgx.Conn) error {
			var payload NewEventsPayload
			if err := json.Unmarshal([]byte(notification.Payload), &payload); err != nil {
				return fmt.Errorf("unmarshal payload: %w", err)
			}
			newEventCh <- payload
			return nil
		}),
	)

	// Listener
	go func() {
		slog.Info("listening from new_events channel")
		if err := listener.Listen(ctx); err != nil {
			slog.Error("failed to listen", "error", err)
		}
	}()

	roomClientMap := make(map[RoomID]map[ClientID]chan<- EventID)
	// Dispatcher
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case registerReq := <-registerCh:
				slog.Info("client registered", "client_id", registerReq.ClientID, "room_id", registerReq.RoomID)
				clientMap, ok := roomClientMap[registerReq.RoomID]
				if !ok {
					roomClientMap[registerReq.RoomID] = make(map[ClientID]chan<- EventID)
					clientMap = roomClientMap[registerReq.RoomID]
				}
				clientMap[registerReq.ClientID] = registerReq.Ch
			case unregisterReq := <-unregisterCh:
				slog.Info("client unregistered", "client_id", unregisterReq.ClientID, "room_id", unregisterReq.RoomID)
				clientMap, ok := roomClientMap[unregisterReq.RoomID]
				if !ok {
					continue
				}
				delete(clientMap, unregisterReq.ClientID)
				if len(clientMap) == 0 {
					delete(roomClientMap, unregisterReq.RoomID)
				}
			case payload := <-newEventCh:
				slog.Info("notify event", "event_id", payload.EventID, "client_id", payload.ClientID, "room_id", payload.RoomID)
				clientMap, ok := roomClientMap[payload.RoomID]
				if !ok {
					continue
				}
				for clientID, ch := range clientMap {
					if clientID != payload.ClientID {
						select {
						case ch <- payload.EventID:
						default:
							slog.Warn("failed to send event to client", "event_id", payload.EventID, "client_id", clientID, "room_id", payload.RoomID)
						}
					}
				}
			}
		}
	}()
	return nil
}
