package dlq

import "testing"

func TestTopic(t *testing.T) {
	cases := map[string]string{
		"shop.orders.v1":    "shop.orders.dlq",
		"shop.inventory.v1": "shop.inventory.dlq",
		"plain":             "plain.dlq",
	}
	for in, want := range cases {
		if got := Topic(in); got != want {
			t.Errorf("Topic(%q) = %q, want %q", in, got, want)
		}
	}
}
