#!/usr/bin/env bash

set -euo pipefail

repo_root=$(git rev-parse --show-toplevel)
private_prefix=${PRIVATE_MODULE_PREFIX:-github.com/justusschock/go-}
private_version=v0.2.0
root_module=github.com/justusschock/go-object-storage

export GOPRIVATE=${GOPRIVATE:-github.com/justusschock/*}
export GONOSUMDB=${GONOSUMDB:-github.com/justusschock/*}

# verify_module checks one module in isolation: checksums, tidiness, exact
# private dependency versions, and tests with a read-only module graph.
# Sibling private modules must be at ${private_version}; this repository's
# root module may only appear at the version backend/s3 requires.
verify_module() {
	local directory=$1
	local root_version

	cd "${directory}"
	root_version=$(awk -v module="${root_module}" '$1 == module { print $2; exit }' "${repo_root}/backend/s3/go.mod")
	GOWORK=off go mod download
	GOWORK=off go mod verify
	GOWORK=off go mod tidy
	git diff --exit-code -- go.mod go.sum
	GOWORK=off go list -m all | awk -v prefix="${private_prefix}" -v version="${private_version}" -v root="${root_module}" -v root_version="${root_version}" '
		index($1, prefix) == 1 && NF > 1 && $2 != version && !($1 == root && $2 == root_version) {
			print
			bad = 1
		}
		END { exit bad }
	'
	GOWORK=off go test -mod=readonly ./...
}

if [[ "${1:-}" == "--module" ]]; then
	verify_module "$2"
	exit
fi

verify_module "${repo_root}"
"${repo_root}/scripts/with-root-candidate.sh" "${repo_root}/scripts/verify-release-modules.sh" --module "${repo_root}/backend/s3"
