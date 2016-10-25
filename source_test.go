package mqtt

import (
	"testing"
)

func TestAdjustOldBrokerURL(t *testing.T) {
	cases := []struct {
		value    string
		expected string // empty if same as value
		fail     bool
	}{
		{"tcp://host:1234", "", false},
		{"ws://host:1234", "", false},
		{"://host:1234", "", true},
		{"hostonly", "tcp://hostonly:1883", false},
		{"host:1234", "tcp://host:1234", false},
		{"host:", "", true},
		{":1234", "", true},
	}

	for _, c := range cases {
		if c.expected == "" {
			c.expected = c.value
		}

		res, err := adjustOldBrokerURL(c.value)
		if c.fail {
			if err == nil {
				t.Errorf(`"%v" should fail (returned "%v")`, c.value, res)
			}
			continue
		}

		if c.expected != res {
			t.Errorf(`"%v": expected "%v", actual "%v"`, c.value, c.expected, res)
		}
	}
}
