package xgb

import (
	"bytes"
	"net"
	"testing"
)

// rawEvent keeps the bytes the reader handed to the constructor, so a test can see
// whether trailing protocol data arrived with them.
type rawEvent struct{ b []byte }

func (e rawEvent) Bytes() []byte  { return e.b }
func (e rawEvent) String() string { return "rawEvent" }

const plainEventCode = 122

// A generic event carries protocol data after its 32 header bytes, counted by the
// length field. A reader that keeps only the header parses the rest as events and
// never recovers: a client selecting XInput 2 raw motion sees one event and then
// garbage, which for the connection means a dead stream.
func TestGenericEventTrailingData(t *testing.T) {
	prevGeneric := NewEventFuncs[genericEvent]
	prevPlain := NewEventFuncs[plainEventCode]
	NewEventFuncs[genericEvent] = func(buf []byte) Event {
		return rawEvent{b: append([]byte(nil), buf...)}
	}
	NewEventFuncs[plainEventCode] = func(buf []byte) Event {
		return rawEvent{b: append([]byte(nil), buf...)}
	}
	defer func() {
		NewEventFuncs[genericEvent] = prevGeneric
		NewEventFuncs[plainEventCode] = prevPlain
	}()

	client, server := net.Pipe()
	defer client.Close()
	c, err := postNewConn(&Conn{conn: client, setupResourceIdBase: 0, setupResourceIdMask: 1})
	if err != nil {
		t.Fatalf("connection: %v", err)
	}
	defer c.Close()

	// One generic event with two units of trailing data, then a plain event that
	// has to arrive intact: its presence proves the stream stayed aligned.
	head := make([]byte, 32)
	head[0] = genericEvent
	head[2], head[3] = 1, 0 // sequence 1
	head[4] = 2             // length: two 4 byte units follow
	head[8] = 17            // the value a consumer would read as the XI2 event type
	tail := []byte{0xde, 0xad, 0xbe, 0xef, 0x01, 0x02, 0x03, 0x04}
	plain := make([]byte, 32)
	plain[0] = plainEventCode
	plain[2], plain[3] = 2, 0 // sequence 2

	go func() {
		_, _ = server.Write(append(append(head, tail...), plain...))
	}()

	ev, err := c.WaitForEvent()
	if err != nil {
		t.Fatalf("first event: %v", err)
	}
	got, ok := ev.(rawEvent)
	if !ok {
		t.Fatalf("first event is %T, want rawEvent", ev)
	}
	want := append(append([]byte(nil), head...), tail...)
	if !bytes.Equal(got.b, want) {
		t.Fatalf("first event is %d bytes (% x), want %d (% x)", len(got.b), got.b, len(want), want)
	}

	ev, err = c.WaitForEvent()
	if err != nil {
		t.Fatalf("second event: %v", err)
	}
	got, ok = ev.(rawEvent)
	if !ok {
		t.Fatalf("second event is %T, want rawEvent", ev)
	}
	if !bytes.Equal(got.b, plain) {
		t.Fatalf("second event is %d bytes (% x), want % x", len(got.b), got.b, plain)
	}
}
