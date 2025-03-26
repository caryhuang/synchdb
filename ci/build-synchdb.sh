#!/bin/bash

# make bash behave
set -euo pipefail
IFS=$'\n\t'

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

function build_synchdb()
{
	pg_major="$1"

	installdir="${basedir}/build-${pg_major}"
	echo "Beginning build for PostgreSQL ${pg_major}..." >&2
	#mkdir -p "${builddir}" && cd "${builddir}"
	export USE_PGXS=1
	make build_dbz PG_CONFIG=/usr/lib/postgresql/${pg_major}/bin/pg_config
	make PG_CONFIG=/usr/lib/postgresql/${pg_major}/bin/pg_config

	sudo USE_PGXS=1 make install_dbz libdir=/usr/lib/postgresql/${pg_major}/lib  PG_CONFIG=/usr/lib/postgresql/${pg_major}/bin/pg_config
	sudo USE_PGXS=1 make install PG_CONFIG=/usr/lib/postgresql/${pg_major}/bin/pg_config

}

build_synchdb "${PG_MAJOR}"
