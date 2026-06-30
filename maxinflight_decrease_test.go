package nsq

import (
	"net"
	"sync/atomic"
	"testing"
)

// TestMaxInFlightDecreaseLowersRDY reproduces the budget over-commitment bug.
//
// Six connections share MaxInFlight=18 at their fair share (RDY 3 each,
// totalRdyCount=18). The manager then lowers MaxInFlight to 12. RDY must follow
// down to perConn=2 each (totalRdyCount=12).
//
// It does NOT, because updateRDY refuses every call with count>0 once
// totalRdyCount > MaxInFlight — including calls that *lower* a connection. So
// rebalanceRDY's lower-pass is refused on every connection, totalRdyCount stays
// pinned at 18 (above the new budget of 12), and the over-commitment can never
// unwind. A connection added afterwards is then refused RDY and starves.
//
// This test asserts the correct behaviour, so it fails on the current code
// (reproducing the bug) and passes once the lowering path is allowed.
func TestMaxInFlightDecreaseLowersRDY(t *testing.T) {
	const numConns = 6
	config := NewConfig()
	config.MaxInFlight = 18 // perConnMaxInFlight = 18/6 = 3
	consumer, err := NewConsumer("test", "ch", config)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	close(consumer.exitChan) // drive RDY changes manually

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

	// Six connections balanced at the full budget: 6 * 3 == 18 == MaxInFlight.
	conns := make([]*Conn, numConns)
	for i := 0; i < numConns; i++ {
		conns[i] = mk("10.0.0."+string(rune('1'+i))+":4150", 3)
	}
	if got := consumer.TotalRDY(); got != 18 {
		t.Fatalf("setup: want totalRdyCount 18, got %d", got)
	}

	// The manager scales the consumer down. RDY must track the smaller budget.
	consumer.ChangeMaxInFlight(12) // perConnMaxInFlight now 12/6 = 2

	if total := consumer.TotalRDY(); total != 12 {
		t.Fatalf("after MaxInFlight 18->12, want totalRdyCount 12, got %d "+
			"(RDY not lowered: updateRDY refuses to reduce a connection while "+
			"totalRdyCount > MaxInFlight)", total)
	}
	for i, c := range conns {
		if c.RDY() != 2 {
			t.Fatalf("conn %d: want RDY lowered to perConnMaxInFlight 2, got %d", i, c.RDY())
		}
	}
}
