#!/bin/bash

# make bash behave
set -euo pipefail
IFS=$'\n\t'

# read pg major version, error if not provided
PG_MAJOR=${PG_MAJOR:?please provide the postgres major version}
PG_BRANCH=${PG_BRANCH:?please provide the postgres branch}

# get codename from release file
. /etc/os-release
codename=${VERSION#*(}
codename=${codename%)*}

# we'll do everything with absolute paths
basedir="$(pwd)"

function build_synchdb()
{
	pg_major="$1"

	echo $PG_BRANCH
	installdir="${basedir}/synchdb-install-${pg_major}"
	mkdir -p $installdir
	echo "Beginning build for PostgreSQL ${pg_major}..." >&2

	exit 1
	#mkdir -p "${builddir}" && cd "${builddir}"
	
	
	#export USE_PGXS=1
	#make build_dbz PG_CONFIG=/usr/lib/postgresql/${pg_major}/bin/pg_config
	#make PG_CONFIG=/usr/lib/postgresql/${pg_major}/bin/pg_config

	#sudo USE_PGXS=1 make install DESTDIR=${installdir} PG_CONFIG=/usr/lib/postgresql/${pg_major}/bin/pg_config
	#sudo USE_PGXS=1 make install_dbz pkglibdir=${installdir}/usr/lib/postgresql/${pg_major}/lib  PG_CONFIG=/usr/lib/postgresql/${pg_major}/bin/pg_config

	cd $installdir
	ls
	tar czvf synchdb-install-${pg_major}.tar.gz *
	mv synchdb-install-${pg_major}.tar.gz $basedir
}

build_synchdb "${PG_MAJOR}"
