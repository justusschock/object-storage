#!/usr/bin/env bash

set -euo pipefail

repo_root=$(git rev-parse --show-toplevel)
private_prefix=${PRIVATE_MODULE_PREFIX:-github.com/justusschock/go-}
root_module=github.com/justusschock/go-object-storage

export GOPRIVATE=${GOPRIVATE:-github.com/justusschock/*}
export GONOSUMDB=${GONOSUMDB:-github.com/justusschock/*}

verify_module() {
	local directory=$1

	cd "${directory}"
	GOWORK=off go mod download
	GOWORK=off go mod verify
	GOWORK=off go mod tidy
	git diff --exit-code -- go.mod go.sum
	GOWORK=off go list -m all | awk -v prefix="${private_prefix}" '
		index($1, prefix) == 1 && NF > 1 && $2 != "v0.2.0" {
			print
			bad = 1
		}
		END { exit bad }
	'
	GOWORK=off go test -mod=readonly ./...
}

verify_module "${repo_root}"

if GOWORK=off go list -m -json "${root_module}@v0.2.0" >/dev/null 2>&1; then
	verify_module "${repo_root}/backend/s3"
	exit 0
fi

temporary_directory=$(mktemp -d)
trap 'rm -rf "${temporary_directory}"' EXIT
mirror=${temporary_directory}/repository

git clone --quiet --no-hardlinks "${repo_root}" "${mirror}"
if tag_commit=$(git -C "${mirror}" rev-parse -q --verify 'refs/tags/v0.2.0^{}'); then
	test "${tag_commit}" = "$(git -C "${mirror}" rev-parse HEAD)"
else
	git -C "${mirror}" \
		-c user.name='go-object-storage release verifier' \
		-c user.email='release-verifier@localhost' \
		tag -a v0.2.0 -m 'go-object-storage v0.2.0 release verification'
fi

export GIT_CONFIG_COUNT=2
export GIT_CONFIG_KEY_0="url.file://${mirror}/.insteadOf"
export GIT_CONFIG_VALUE_0="https://github.com/justusschock/go-object-storage"
export GIT_CONFIG_KEY_1=protocol.file.allow
export GIT_CONFIG_VALUE_1=always

verify_module "${repo_root}/backend/s3"
