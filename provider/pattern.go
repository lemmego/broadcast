package provider

import "strings"

// Params are the brace placeholders from a matched channel pattern.
type Params map[string]string

// Get returns one placeholder.
func (p Params) Get(name string) string { return p[name] }

// rule is one authorization pattern and its callback.
type rule struct {
	segments  []string
	authorize func(c authContext, params Params) error
}

// match reports whether a channel name fits a pattern, and what the
// placeholders captured.
//
// Patterns are matched segment by segment on ".", not as regular
// expressions, and that is a security property rather than a simplification.
// A pattern compiled as an unanchored regular expression matches more than it
// names — "a.b" would also match "x.a.b.y" — and in an authorization matcher
// that *grants* access rather than merely skipping a check. There is no
// regular expression here to get wrong.
func match(pattern []string, channel string) (Params, bool) {
	segments := strings.Split(channel, ".")
	if len(segments) != len(pattern) {
		return nil, false
	}

	params := Params{}
	for i, want := range pattern {
		if strings.HasPrefix(want, "{") && strings.HasSuffix(want, "}") {
			name := want[1 : len(want)-1]
			if name == "" || segments[i] == "" {
				return nil, false
			}
			params[name] = segments[i]
			continue
		}
		if want != segments[i] {
			return nil, false
		}
	}
	return params, true
}
