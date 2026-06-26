package nsq

import (
	"net"
	"sync/atomic"
	"testing"
)

// TestRebalanceRDYRaisesStarvedConn verifies that when a connection set is at
// its budget (existing connections hold all of MaxInFlight) and one connection
// is under its fair share, rebalanceRDY lowers the over-holders and raises the
// starved one to its perConnMaxInFlight share — the fresh-nsqd-after-roll case.
func TestRebalanceRDYRaisesStarvedConn(t *testing.T) {
	const numConns = 6
	config := NewConfig()
	config.MaxInFlight = 18 // perConnMaxInFlight = 18/6 = 3
	consumer, err := NewConsumer("test", "ch", config)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	close(consumer.exitChan) // quiesce rdyLoop

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) { _, _ = c.Read(make([]byte, 4096)) }(c)
		}
	}()

	mk := func(addr string, rdy int64) *Conn {
		c := NewConn(addr, &consumer.config, &consumerConnDelegate{r: consumer})
		tcp, derr := net.Dial("tcp", ln.Addr().String())
		if derr != nil {
			t.Fatalf("dial: %v", derr)
		}
		t.Cleanup(func() { tcp.Close() })
		c.conn = tcp.(*net.TCPConn)
		c.r = tcp
		c.w = tcp
		c.SetRDY(rdy)
		consumer.mtx.Lock()
		consumer.connections[addr] = c
		consumer.mtx.Unlock()
		atomic.AddInt64(&consumer.totalRdyCount, rdy)
		return c
	}

	// 5 established connections each over the fair share (RDY 4, total 20 > 18),
	// and a freshly-connected one starved at RDY 0 — budget exhausted.
	for i := 0; i < numConns-1; i++ {
		mk("10.0.0."+string(rune('1'+i))+":4150", 4)
	}
	starved := mk("10.0.0.9:4150", 0)

	consumer.rebalanceRDY()

	if got := starved.RDY(); got != 3 {
		t.Fatalf("starved conn: want RDY 3 (perConnMaxInFlight), got %d (totalRdy=%d)",
			got, atomic.LoadInt64(&consumer.totalRdyCount))
	}
	if total := atomic.LoadInt64(&consumer.totalRdyCount); total != 18 {
		t.Fatalf("want totalRdyCount 18 (6*3), got %d", total)
	}
}
