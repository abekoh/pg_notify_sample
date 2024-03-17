package main

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

const (
	roomNum           = 10
	readClientNum     = 10
	writeClientNum    = 10
	writeNumPerClient = 10
)

func TestMain(m *testing.M) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	execute(ctx)

	m.Run()
}

func TestExecute(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	startCh := make(chan struct{})
	readyCh := make(chan struct{})
	resultCh := make(chan int)
	roomID := uuid.NewString()

	for i := 0; i < readClientNum; i++ {
		go readClient(t, ctx, roomID, readyCh, resultCh)
	}
	readyCount := 0
	for range readyCh {
		readyCount++
		if readyCount == readClientNum {
			break
		}
	}
	var wg sync.WaitGroup
	for i := 0; i < writeClientNum; i++ {
		wg.Add(1)
		go func() {
			writeClient(t, ctx, roomID, startCh)
			wg.Done()
		}()
	}

	close(startCh)

	wg.Wait()
	time.Sleep(3 * time.Second)
	cancel()

	gotCount := 0
	for msgCount := range resultCh {
		gotCount++
		if msgCount != writeNumPerClient*writeClientNum {
			t.Errorf("expected %d messages, but got %d", writeNumPerClient*writeClientNum, msgCount)
		}
		if gotCount == readClientNum {
			break
		}
	}
}
func getConn(t *testing.T, ctx context.Context, roomID string) *websocket.Conn {
	t.Helper()

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, "ws://localhost:8080/ws?room_id="+roomID, nil)
	if err != nil {
		t.Fatal("failed to connect to server:", err)
	}
	return conn
}

func readClient(t *testing.T, ctx context.Context, roomID string, readyCh chan<- struct{}, resultCh chan<- int) {
	t.Helper()

	conn := getConn(t, ctx, roomID)
	go func() {
		<-ctx.Done()
		conn.Close()
	}()

	msgCount := 0
	readyCh <- struct{}{}
	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			break
		}
		msgCount++
	}
	resultCh <- msgCount
}

func writeClient(t *testing.T, ctx context.Context, roomID string, startCh <-chan struct{}) {
	t.Helper()

	conn := getConn(t, ctx, roomID)
	defer conn.Close()

	<-startCh

	for i := 0; i < writeNumPerClient; i++ {
		if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"type": "ping"}`)); err != nil {
			t.Fatal("failed to write message:", err)
		}
	}
}
