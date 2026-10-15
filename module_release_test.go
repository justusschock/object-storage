package objectstore_test

import (
	"os"
	"strings"
	"testing"
)

// rootRelease is the root module version that backend/s3 requires; it is
// tagged together with backend/s3 from one commit.
const rootRelease = "v0.3.0"

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
				"github.com/justusschock/go-messaging",
			},
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
			if !strings.Contains(string(data), module+" v0.2.0/go.mod h1:") {
				t.Errorf("%s is missing the %s v0.2.0 go.mod checksum", test.path, module)
			}
		}

		checkPrivateVersions(t, test.path, string(data))
	}

	data, err := os.ReadFile("backend/s3/go.sum")
	if err != nil {
		t.Fatalf("read backend/s3/go.sum: %v", err)
	}
	for _, suffix := range []string{" h1:", "/go.mod h1:"} {
		if !strings.Contains(string(data), "github.com/justusschock/go-object-storage "+rootRelease+suffix) {
			t.Errorf("backend/s3/go.sum is missing the root %s%s checksum", rootRelease, suffix)
		}
	}
	checkPrivateVersions(t, "backend/s3/go.sum", string(data))
	module, err := os.ReadFile("backend/s3/go.mod")
	if err != nil {
		t.Fatalf("read backend/s3/go.mod: %v", err)
	}
	if !strings.Contains(string(module), "github.com/justusschock/go-object-storage "+rootRelease+"\n") {
		t.Errorf("backend/s3/go.mod does not require the root %s release", rootRelease)
	}
}

// checkPrivateVersions allows sibling private modules only at v0.2.0 and
// this repository's root module only at rootRelease.
func checkPrivateVersions(t *testing.T, path, data string) {
	t.Helper()
	for _, line := range strings.Split(data, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 || !strings.HasPrefix(fields[0], "github.com/justusschock/go-") {
			continue
		}
		version := strings.TrimSuffix(fields[1], "/go.mod")
		want := "v0.2.0"
		if fields[0] == "github.com/justusschock/go-object-storage" {
			want = rootRelease
		}
		if version != want {
			t.Errorf("%s contains private dependency %s %s, want %s", path, fields[0], fields[1], want)
		}
	}
}
