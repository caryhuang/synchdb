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

	#sudo apt install -y postgresql-common
	#sudo /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh
	#sudo apt install -y postgresql-${pg_major} postgresql-server-dev-${pg_major}

	builddir="${basedir}/build-${pg_major}"
	echo "Beginning build for PostgreSQL ${pg_major}..." >&2
	#mkdir -p "${builddir}" && cd "${builddir}"
	make build_dbz
	#CFLAGS=-Werror "${basedir}/configure" PG_CONFIG="/usr/lib/postgresql/${pg_major}/bin/pg_config" --enable-coverage --with-security-flags
	#installdir="${builddir}/install"	
	
	#make -j$(nproc) build_dbz && mkdir -p "${installdir}" && { make DESTDIR="${installdir}" install-all || make DESTDIR="${installdir}" install ; }

}

build_synchdb "${PG_MAJOR}"
