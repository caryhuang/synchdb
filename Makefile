# contrib/synchdb/Makefile

MODULE_big = synchdb

EXTENSION = synchdb
DATA = synchdb--1.0.sql
PGFILEDESC = "synchdb - allows logical replication with heterogeneous databases"
REGRESS = synchdb

OBJS = synchdb.o

PG_CFLAGS = -I/usr/include/jdk-22.0.1/include -I/usr/include/jdk-22.0.1/include/linux
PG_LDFLAGS= -L/usr/lib/jdk-22.0.1/lib/server -ljvm

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/synchdb
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
