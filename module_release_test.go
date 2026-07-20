package objectstore_test

import (
	"os"
	"strings"
	"testing"
)

func TestReleaseModuleChecksums(t *testing.T) {
	tests := []struct {
		path    string
		modules []string
	}{
		{
			path: "go.sum",
			modules: []string{
				"github.com/justusschock/go-cache",
				"github.com/justusschock/go-concurrency",
				"github.com/justusschock/go-distributed-coordination",
				"github.com/justusschock/go-lifecycle",
			},
		},
		{
			path:    "backend/s3/go.sum",
			modules: []string{"github.com/justusschock/go-object-storage"},
		},
	}

	for _, test := range tests {
		data, err := os.ReadFile(test.path)
		if err != nil {
			t.Errorf("read %s: %v", test.path, err)
			continue
		}
		for _, module := range test.modules {
			if !strings.Contains(string(data), module+" v0.2.0 h1:") {
				t.Errorf("%s is missing the %s v0.2.0 module checksum", test.path, module)
			}
		}
	}
}
