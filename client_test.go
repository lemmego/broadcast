package broadcast

import (
	"os"
	"strings"
	"testing"
)

// The TypeScript client has to type-check on a machine that is not this one.
//
// It did not. tsconfig.json carried a "paths" block and a "typeRoots" entry
// pointing at /Users/<somebody>/code/<some other project>/node_modules, so
// `tsc` resolved react's types out of whichever application happened to be
// checked out beside this module — and resolved nothing at all for everybody
// else, CI included. The client shipped that way in v0.2.1.
//
// It is the same failure this repo already paid for once, in Go: apptest
// carried a `replace` directive naming an absolute path on one machine, and a
// consumer's `go mod tidy` failed with "Repository not found". A module that
// builds only where it was written is not a module.
//
// So the configuration declares its own dev dependency (@types/react in
// client/package.json) and names no path outside its own directory. This test
// is cheap and the failure it guards is silent, which is the whole argument
// for it.
func TestTheClientConfigNamesNoPathOutsideItself(t *testing.T) {
	for _, name := range []string{"client/tsconfig.json", "client/package.json"} {
		contents, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("reading %s: %v", name, err)
		}
		text := string(contents)

		// An absolute POSIX path in a JSON string, and the Windows shape
		// alongside it, because a drive letter is just as unportable.
		for _, bad := range []string{`"/`, `":/`, `"\\`, `/Users/`, `/home/`, `C:\`} {
			if strings.Contains(text, bad) {
				t.Errorf("%s contains %q, so it resolves on one machine only:\n%s",
					name, bad, text)
			}
		}
	}
}

// The client is two files and the hook is the only one that needs React, so
// react is an *optional* peer dependency: an application using the plain
// connection should not be told it is missing a UI library. The manifest has
// to keep saying so, because the obvious edit — moving react to dependencies
// to make a typecheck pass — would bundle a second copy of React into every
// consumer that already has one.
func TestReactIsAnOptionalPeerDependency(t *testing.T) {
	contents, err := os.ReadFile("client/package.json")
	if err != nil {
		t.Fatalf("reading the manifest: %v", err)
	}
	manifest := string(contents)

	if !strings.Contains(manifest, `"peerDependencies"`) {
		t.Error("react is not declared as a peer dependency")
	}
	if !strings.Contains(manifest, `"peerDependenciesMeta"`) {
		t.Error("the react peer dependency is not marked optional")
	}
	// A "dependencies" block would make it a hard, duplicated install.
	// "devDependencies" and "peerDependencies" both contain the substring, so
	// the check is anchored to the start of a line.
	for _, line := range strings.Split(manifest, "\n") {
		if strings.TrimSpace(line) == `"dependencies": {` {
			t.Error("the client declares runtime dependencies; react belongs in peerDependencies")
		}
	}
}
