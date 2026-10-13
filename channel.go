// Package broadcast delivers events to clients over a live connection.
//
// It is named for the capability, with the wire protocol as a driver —
// matching cache/store/* and tasker/driver/* — because "websocket" names a
// protocol, which is the mistake calling the cache module "redis" would have
// been. Server-sent events and WebSocket are two transports behind one
// contract, and both ship together so the contract is fitted against two
// implementations rather than against one by accident.
//
// The root package is standard library only. Both transports, both buses and
// the application wiring are subpackages, so a module that only publishes —
// lemmego/notify — depends on nothing it does not use.
package broadcast

import (
	"errors"
	"fmt"
	"strings"
)

// Channel is a subscription target.
//
// A name prefixed "private-" is authorized before a client may subscribe to
// it; anything else is public, and public is refused unless an application
// says otherwise. The prefix is part of the name rather than a separate field
// because the name is what crosses the wire, becomes a bus channel and gets
// typed into a frontend call — a privacy marker that can be dropped in
// transit is not a marker.
type Channel string

// PrivatePrefix marks a channel that must be authorized.
const PrivatePrefix = "private-"

// Private assembles a private channel from parts, joined with ".".
//
//	broadcast.Private("post", "42")  // private-post.42
func Private(parts ...string) Channel {
	return Channel(PrivatePrefix + strings.Join(parts, "."))
}

// UserChannel is the per-subject private channel, private-user.<subject>.
//
// A function so every module that agrees to use it agrees on the spelling.
// lemmego/notify publishes here by default; nothing else should have to know
// what the string looks like.
func UserChannel(subject string) Channel {
	return Private("user", subject)
}

// IsPrivate reports whether this channel requires authorization.
func (c Channel) IsPrivate() bool {
	return strings.HasPrefix(string(c), PrivatePrefix)
}

func (c Channel) String() string { return string(c) }

// MaxChannelBytes is the longest a channel name may be.
//
// 190 rather than a round number: a bus prefixes the name to build a key, and
// past this the result stops fitting in the places those keys go.
const MaxChannelBytes = 190

// ErrBadChannel reports an unusable channel name.
var ErrBadChannel = errors.New("broadcast: unusable channel name")

// Validate reports why a channel name cannot be used.
//
// The rules are the intersection of what every transport and bus can carry:
// one to MaxChannelBytes bytes of [A-Za-z0-9._:@-]. The exclusions are
// load-bearing rather than tidy. A name is written into an event stream,
// where "\n" ends a field and a blank line ends a frame, so a name carrying
// either would let one subscriber inject frames at every other subscriber on
// that connection. A "*" would be read as a wildcard by a bus that pattern
// subscribes. Validating here rather than in each transport is what stops one
// of them forgetting.
func (c Channel) Validate() error {
	name := string(c)
	switch {
	case name == "":
		return fmt.Errorf("%w: it is empty", ErrBadChannel)
	case len(name) > MaxChannelBytes:
		return fmt.Errorf("%w: %d bytes, and the limit is %d",
			ErrBadChannel, len(name), MaxChannelBytes)
	}

	for i := 0; i < len(name); i++ {
		if !channelByteAllowed(name[i]) {
			return fmt.Errorf("%w: %q contains %q, and a name may hold only "+
				"letters, digits and . _ - : @",
				ErrBadChannel, name, string(name[i]))
		}
	}
	return nil
}

func channelByteAllowed(b byte) bool {
	switch {
	case b >= 'a' && b <= 'z', b >= 'A' && b <= 'Z', b >= '0' && b <= '9':
		return true
	case b == '.' || b == '_' || b == '-' || b == ':' || b == '@':
		return true
	default:
		return false
	}
}
