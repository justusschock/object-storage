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
		"'1.25.x'", "'1.26.x'", "'1.27.x'",
		"secrets.GO_MODULES_TOKEN", "${HOME}/.netrc", "GOPRIVATE", "GONOSUMDB",
		"go mod download", "go mod verify", "go mod tidy", "git diff --exit-code -- go.mod go.sum",
		"go test ./...", "go test -race ./...", "go test -tags=integration ./...", "go vet ./...",
		"working-directory: backend/s3", "github.com/justusschock/go-", "v0.2.0", "aws-sdk",
		"./scripts/verify-release-modules.sh", "../../scripts/with-root-candidate.sh go test ./...",
		"GOOS=${target%/*}", "windows/amd64",
	} {
		if !strings.Contains(workflow, required) {
			t.Errorf("CI workflow is missing %q", required)
		}
	}
	for _, forbidden := range []string{"set -x", `echo "${GO_MODULES_TOKEN}"`, "replace github.com/justusschock", "'1.22.x'", "'1.24.x'"} {
		if strings.Contains(workflow, forbidden) {
			t.Errorf("CI workflow contains unsafe or forbidden fragment %q", forbidden)
		}
	}

	for _, path := range []string{"scripts/verify-release-modules.sh", "scripts/with-root-candidate.sh"} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat %s: %v", path, err)
		}
		if info.Mode()&0o111 == 0 {
			t.Errorf("%s is not executable", path)
		}
	}
	verifier, err := os.ReadFile("scripts/verify-release-modules.sh")
	if err != nil {
		t.Fatalf("read release module verifier: %v", err)
	}
	if !strings.Contains(string(verifier), "scripts/with-root-candidate.sh") {
		t.Error("release module verifier does not verify nested modules against the root release candidate")
	}
	script, err := os.ReadFile("scripts/with-root-candidate.sh")
	if err != nil {
		t.Fatalf("read release candidate runner: %v", err)
	}
	for _, required := range []string{
		"git cat-file -t", "ls-remote --exit-code --tags origin",
		`export GOMODCACHE=${temporary_directory}/gomodcache`, "go clean -modcache", "backend/s3/go.mod",
	} {
		if !strings.Contains(string(script), required) {
			t.Errorf("release candidate runner is missing %q", required)
		}
	}
	if strings.Contains(string(script)+string(verifier), `go list -m -json "${root_module}@v0.2.0"`) {
		t.Error("release verification may select an ambient cached root release instead of the exact candidate")
	}
}
