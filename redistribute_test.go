package nsq

import (
	"io"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// startRDYDrainListener returns a loopback TCP listener that accepts
// connections and discards everything written to them. It lets test Conns
// issue RDY writes (via WriteCommand) without a real nsqd on the other end.
func startRDYDrainListener(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go io.Copy(io.Discard, c)
		}
	}()
	return ln
}

// addTestConn builds a Conn backed by a real (draining) loopback socket so RDY
// writes succeed, seeds its RDY count, and registers it on the consumer.
func addTestConn(t *testing.T, r *Consumer, ln net.Listener, addr string, rdy int64) *Conn {
	t.Helper()
	conn := NewConn(addr, &r.config, &consumerConnDelegate{r: r})

	tcp, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { tcp.Close() })
	conn.conn = tcp.(*net.TCPConn)
	conn.r = tcp
	conn.w = tcp
	conn.SetRDY(rdy)

	r.mtx.Lock()
	r.connections[addr] = conn
	r.mtx.Unlock()
	atomic.AddInt64(&r.totalRdyCount, rdy)
	return conn
}

// TestRedistributeRDYRestoresStarvedConn is a regression test for dangling
// nsqd connections left stuck at RDY 0 after a rotation.
//
// Sequence reproduced: while an nsqd rolls, the consumer briefly holds more
// connections than MaxInFlight, so go-nsq trims an idle connection to RDY 0.
// When the extra connection drops away (count back to MaxInFlight), a single
// redistribution pass runs and must hand the one free RDY slot to the trimmed
// connection. Before the fix the pass chose a random connection — usually one
// that already held RDY — wasting the freed slot and leaving the trimmed
// connection stranded at RDY 0, with no further redistribution scheduled
// (needRDYRedistributed only re-arms while len(conns) > maxInFlight).
//
// Models the production setup: 6 nsqds (MaxInFlight 6), five busy and one
// starved, with exactly one slot of budget free.
func TestRedistributeRDYRestoresStarvedConn(t *testing.T) {
	const numConns = 6

	config := NewConfig()
	config.MaxInFlight = numConns

	consumer, err := NewConsumer("test", "ch", config)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	// Quiesce the background rdyLoop so the only redistribution is the one we
	// invoke explicitly below.
	close(consumer.exitChan)

	ln := startRDYDrainListener(t)
	t.Cleanup(func() { ln.Close() })

	// Five busy connections at RDY 1 and one starved at RDY 0: total RDY 5,
	// one slot (6-5) free — the post-rotation steady state.
	var starved *Conn
	now := time.Now().UnixNano()
	for i := 0; i < numConns; i++ {
		addr := "10.0.0." + strconv.Itoa(i+1) + ":4150"
		if i == numConns-1 {
			starved = addTestConn(t, consumer, ln, addr, 0)
			continue
		}
		busy := addTestConn(t, consumer, ln, addr, 1)
		// Pretend each busy connection just received a message so
		// redistributeRDY does not reap its RDY as idle.
		atomic.StoreInt64(&busy.lastMsgTimestamp, now)
	}

	// Arm redistribution the way onConnClose does when the rotation's extra
	// connection drops away and the count returns to MaxInFlight.
	atomic.StoreInt32(&consumer.needRDYRedistributed, 1)

	consumer.redistributeRDY()

	if got := starved.RDY(); got <= 0 {
		t.Fatalf("starved connection left at RDY %d after redistribution; "+
			"want > 0 (totalRdyCount=%d, maxInFlight=%d)",
			got, atomic.LoadInt64(&consumer.totalRdyCount), consumer.getMaxInFlight())
	}
}
