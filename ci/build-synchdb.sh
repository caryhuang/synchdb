#!/bin/bash

# make bash behave
set -euo pipefail
IFS=$'\n\t'

# shellcheck disable=SC1091
source ci/ci_helpers.sh

# read pg major version, error if not provided
PG_MAJOR=${PG_MAJOR:?please provide the postgres major version}

# get codename from release file
. /etc/os-release
codename=${VERSION#*(}
codename=${codename%)*}

# we'll do everything with absolute paths
basedir="$(pwd)"

# get the project and clear out the git repo (reduce workspace size
rm -rf "${basedir}/.git"
