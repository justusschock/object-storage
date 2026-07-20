package objectstore_test

import (
	"os"
	"strings"
	"testing"
)

func TestCICompatibilityContract(t *testing.T) {
	data, err := os.ReadFile(".github/workflows/ci.yml")
	if err != nil {
		t.Fatal(err)
	}
	workflow := string(data)
	for _, required := range []string{
		"'1.22.x'", "'1.23.x'", "'1.24.x'", "'1.25.x'", "'1.26.x'",
		"secrets.GO_MODULES_TOKEN", "${HOME}/.netrc", "GOPRIVATE", "GONOSUMDB",
		"go mod download", "go mod verify", "go mod tidy", "git diff --exit-code -- go.mod go.sum",
		"go test ./...", "go test -race ./...", "go test -tags=integration ./...", "go vet ./...",
		"working-directory: backend/s3", "github.com/justusschock/go-", "v0.2.0", "aws-sdk",
		"./scripts/verify-release-modules.sh",
	} {
		if !strings.Contains(workflow, required) {
			t.Errorf("CI workflow is missing %q", required)
		}
	}
	for _, forbidden := range []string{"set -x", `echo "${GO_MODULES_TOKEN}"`, "replace github.com/justusschock"} {
		if strings.Contains(workflow, forbidden) {
			t.Errorf("CI workflow contains unsafe or forbidden fragment %q", forbidden)
		}
	}

	info, err := os.Stat("scripts/verify-release-modules.sh")
	if err != nil {
		t.Fatalf("stat release module verifier: %v", err)
	}
	if info.Mode()&0o111 == 0 {
		t.Error("release module verifier is not executable")
	}
	script, err := os.ReadFile("scripts/verify-release-modules.sh")
	if err != nil {
		t.Fatalf("read release module verifier: %v", err)
	}
	if !strings.Contains(string(script), "git cat-file -t") {
		t.Error("release module verifier does not require an annotated tag")
	}
	if strings.Contains(string(script), `go list -m -json "${root_module}@v0.2.0"`) {
		t.Error("release module verifier may select an ambient cached root release instead of the exact candidate")
	}
}
