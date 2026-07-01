package nsq

import (
	"net"
	"strconv"
	"sync/atomic"
	"testing"
)

// newRDYTestConsumer returns a consumer with its rdyLoop quiesced, plus an
// addConn(addr, rdy) helper that attaches a live, drained connection at the
// given RDY and accounts it in totalRdyCount.
//
// It's enough to drive updateRDY / ChangeMaxInFlight in white-box RDY-budget
// tests without standing up a full nsqd: a real TCP socket is needed only
// because Conn.Write sets a write deadline on it, so connections dial a local
// listener that just drains whatever is written.
func newRDYTestConsumer(t *testing.T, maxInFlight int) (consumer *Consumer, addConn func(addr string, rdy int64) *Conn) {
	t.Helper()

	config := NewConfig()
	config.MaxInFlight = maxInFlight
	consumer, err := NewConsumer("test", "ch", config)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	close(consumer.exitChan) // we drive RDY changes directly, not via rdyLoop

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) { _, _ = conn.Read(make([]byte, 4096)) }(conn)
		}
	}()

	addConn = func(addr string, rdy int64) *Conn {
		c := NewConn(addr, &consumer.config, &consumerConnDelegate{r: consumer})
		tcp, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Fatalf("dial: %v", err)
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
	return consumer, addConn
}

// TestMaxInFlightDecreaseLowersRDY verifies that lowering MaxInFlight lowers the
// per-connection RDY so totalRdyCount tracks the reduced budget back down.
//
// Six connections share MaxInFlight=18 at their fair share (RDY 3 each,
// totalRdyCount=18). Lowering MaxInFlight to 12 must bring each connection down
// to perConn=2 (totalRdyCount=12).
//
// Previously it did not: updateRDY rejected any RDY change with count>0 while
// totalRdyCount > MaxInFlight. That guard is meant to block only raises that
// would exceed the budget, but it also blocked the reductions ChangeMaxInFlight
// applies to each connection when the budget shrinks. totalRdyCount then stayed
// pinned above the reduced budget, the over-commitment never unwound, and a
// connection added afterwards was refused RDY and starved.
func TestMaxInFlightDecreaseLowersRDY(t *testing.T) {
	const numConns = 6

	// Six connections balanced at the full budget: 6 * 3 == 18 == MaxInFlight.
	consumer, addConn := newRDYTestConsumer(t, 18) // perConnMaxInFlight = 18/6 = 3
	conns := make([]*Conn, numConns)
	for i := 0; i < numConns; i++ {
		conns[i] = addConn("10.0.0."+strconv.Itoa(i+1)+":4150", 3)
	}
	if got := atomic.LoadInt64(&consumer.totalRdyCount); got != 18 {
		t.Fatalf("setup: want totalRdyCount 18, got %d", got)
	}

	// MaxInFlight is lowered; RDY must track the smaller budget down.
	consumer.ChangeMaxInFlight(12) // perConnMaxInFlight now 12/6 = 2

	if total := atomic.LoadInt64(&consumer.totalRdyCount); total != 12 {
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
