#!/usr/bin/env bash

# Runs a command so that the root module version required by backend/s3 (and
# by every other nested backend module, which require the same version)
# resolves correctly before and after that version is published.
#
# When the required tag exists on origin, the command runs unchanged against
# the published release. Otherwise the current commit is the release
# candidate: it is cloned into an isolated mirror, tagged there with an
# annotated tag, and served in place of the GitHub module through Git URL
# rewriting with a private, read-only module cache.

set -euo pipefail

repo_root=$(git rev-parse --show-toplevel)
root_module=github.com/justusschock/go-object-storage

export GOPRIVATE=${GOPRIVATE:-github.com/justusschock/*}
export GONOSUMDB=${GONOSUMDB:-github.com/justusschock/*}

root_version=$(awk -v module="${root_module}" '
	$1 == module { print $2; exit }
	$1 == "require" && $2 == module { print $3; exit }
' "${repo_root}/backend/s3/go.mod")
test -n "${root_version}"

if git -C "${repo_root}" ls-remote --exit-code --tags origin "refs/tags/${root_version}" >/dev/null 2>&1; then
	exec "$@"
fi

temporary_directory=$(mktemp -d)
trap 'GOMODCACHE="${temporary_directory}/gomodcache" go clean -modcache; rm -rf "${temporary_directory}"' EXIT
mirror=${temporary_directory}/repository
export GOMODCACHE=${temporary_directory}/gomodcache

git clone --quiet --no-hardlinks "${repo_root}" "${mirror}"
if tag_commit=$(git -C "${mirror}" rev-parse -q --verify "refs/tags/${root_version}^{}"); then
	tag_type=$(GIT_DIR="${mirror}/.git" git cat-file -t "refs/tags/${root_version}")
	test "${tag_type}" = tag
	test "${tag_commit}" = "$(git -C "${mirror}" rev-parse HEAD)"
else
	git -C "${mirror}" \
		-c user.name='go-object-storage release verifier' \
		-c user.email='release-verifier@localhost' \
		tag -a "${root_version}" -m "go-object-storage ${root_version} release verification"
fi

export GIT_CONFIG_COUNT=2
export GIT_CONFIG_KEY_0="url.file://${mirror}/.insteadOf"
export GIT_CONFIG_VALUE_0="https://${root_module}"
export GIT_CONFIG_KEY_1=protocol.file.allow
export GIT_CONFIG_VALUE_1=always

"$@"
