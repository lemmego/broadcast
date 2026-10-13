package provider

import (
	"strings"
	"testing"
)

func TestChannelPatternsMatchBySegment(t *testing.T) {
	cases := []struct {
		pattern string
		channel string
		match   bool
		params  map[string]string
		why     string
	}{
		{
			pattern: "private-user.{subject}",
			channel: "private-user.42",
			match:   true,
			params:  map[string]string{"subject": "42"},
		},
		{
			pattern: "private-post.{id}",
			channel: "private-post.7",
			match:   true,
			params:  map[string]string{"id": "7"},
		},
		{
			pattern: "private-user.{subject}",
			channel: "private-user.42.extra",
			match:   false,
			why: "a longer channel must not match a shorter pattern — this is " +
				"the case a 'contains' matcher would wrongly grant",
		},
		{
			pattern: "private-user.{subject}",
			channel: "private-user",
			match:   false,
			why:     "a shorter channel must not match",
		},
		{
			pattern: "private-user.{subject}",
			channel: "private-users.42",
			match:   false,
			why:     "a literal segment is compared exactly, not by prefix",
		},
		{
			pattern: "private-user.{subject}",
			channel: "xprivate-user.42",
			match:   false,
			why: "api's CSRF ExcludePatterns were matched with " +
				"regexp.MatchString, which is an unanchored contains, so " +
				"\"/api\" also excluded \"/x/api/y\". Here that bug would grant access",
		},
		{
			pattern: "private-user.{subject}",
			channel: "private-user.",
			match:   false,
			why:     "an empty placeholder captures nothing and must not match",
		},
		{
			pattern: "private-team.{team}.{room}",
			channel: "private-team.9.general",
			match:   true,
			params:  map[string]string{"team": "9", "room": "general"},
		},
	}

	for _, one := range cases {
		t.Run(one.pattern+" vs "+one.channel, func(t *testing.T) {
			params, ok := match(split(one.pattern), one.channel)
			if ok != one.match {
				t.Fatalf("match=%v, want %v (%s)", ok, one.match, one.why)
			}
			if !one.match {
				return
			}
			for name, want := range one.params {
				if got := params.Get(name); got != want {
					t.Fatalf("placeholder %s captured %q, want %q", name, got, want)
				}
			}
		})
	}
}

func split(pattern string) []string { return strings.Split(pattern, ".") }
