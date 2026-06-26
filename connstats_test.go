package nsq

import (
	"net"
	"sync/atomic"
	"testing"
)

// TestConnStatsReportsPerConnRDYAndInFlight verifies ConnStats returns the
// per-nsqd RDY and in-flight counts for each connection.
func TestConnStatsReportsPerConnRDYAndInFlight(t *testing.T) {
	config := NewConfig()
	config.MaxInFlight = 10
	consumer, err := NewConsumer("test", "ch", config)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	close(consumer.exitChan) // quiesce rdyLoop

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	mk := func(addr string, rdy, inflight int64) {
		conn := NewConn(addr, &consumer.config, &consumerConnDelegate{r: consumer})
		conn.SetRDY(rdy)
		atomic.StoreInt64(&conn.messagesInFlight, inflight)
		consumer.mtx.Lock()
		consumer.connections[addr] = conn
		consumer.mtx.Unlock()
	}
	mk("10.0.0.1:4150", 3, 2)
	mk("10.0.0.2:4150", 0, 0)

	got := map[string]ConnStat{}
	for _, s := range consumer.ConnStats() {
		got[s.Address] = s
	}
	if len(got) != 2 {
		t.Fatalf("want 2 conn stats, got %d", len(got))
	}
	if s := got["10.0.0.1:4150"]; s.RDY != 3 || s.InFlight != 2 {
		t.Fatalf("conn1: want RDY 3 inflight 2, got RDY %d inflight %d", s.RDY, s.InFlight)
	}
	if s := got["10.0.0.2:4150"]; s.RDY != 0 || s.InFlight != 0 {
		t.Fatalf("conn2: want RDY 0 inflight 0, got RDY %d inflight %d", s.RDY, s.InFlight)
	}
}
