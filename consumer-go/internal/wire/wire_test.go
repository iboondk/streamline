package wire

import (
	"bytes"
	"errors"
	"testing"
)

func TestParseValid(t *testing.T) {
	frame := []byte{0, 0, 0, 1, 2, 0xAA, 0xBB}
	id, payload, err := Parse(frame)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id != 258 {
		t.Fatalf("schema id = %d, want 258", id)
	}
	if !bytes.Equal(payload, []byte{0xAA, 0xBB}) {
		t.Fatalf("payload = %x, want aabb", payload)
	}
}

func TestParseEmptyPayload(t *testing.T) {
	id, payload, err := Parse([]byte{0, 0, 0, 0, 7})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id != 7 || len(payload) != 0 {
		t.Fatalf("got id=%d payload=%x", id, payload)
	}
}

func TestParseShort(t *testing.T) {
	for _, b := range [][]byte{nil, {0}, {0, 0, 0, 1}} {
		if _, _, err := Parse(b); !errors.Is(err, ErrBadMagic) {
			t.Fatalf("Parse(%x) err = %v, want ErrBadMagic", b, err)
		}
	}
}

func TestParseWrongMagic(t *testing.T) {
	if _, _, err := Parse([]byte{1, 0, 0, 0, 1, 9}); !errors.Is(err, ErrBadMagic) {
		t.Fatalf("err = %v, want ErrBadMagic", err)
	}
}
