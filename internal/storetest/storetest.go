// Package storetest forwards to the public conformance suite in
// github.com/justusschock/go-object-storage/storetest.
//
// It exists only for the nested backend modules, whose go.mod files still
// require a root release (v0.3.0) in which the suite was internal. Once they
// require a root release that contains the public storetest package, their
// tests import it directly and this package is deleted.
package storetest

import (
	"testing"

	"github.com/justusschock/go-object-storage/storetest"
)

// Factory is storetest.Factory.
type Factory = storetest.Factory

// StreamingSize is storetest.StreamingSize.
const StreamingSize = storetest.StreamingSize

// Run calls storetest.Run.
func Run(t *testing.T, factory Factory) {
	t.Helper()
	storetest.Run(t, factory)
}
