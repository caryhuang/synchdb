# contrib/synchdb/Makefile

MODULE_big = synchdb

EXTENSION = synchdb
DATA = synchdb--1.0.sql
PGFILEDESC = "synchdb - allows logical replication with heterogeneous databases"

REGRESS = synchdb

OBJS = synchdb.o \
       format_converter.o \
       replication_agent.o

# Dynamically set JDK paths
JAVA_PATH := $(shell which java)
JDK_HOME_PATH := $(shell readlink -f $(JAVA_PATH) | sed 's:bin/java::')
JDK_INCLUDE_PATH := $(JDK_HOME_PATH)/include
JDK_INCLUDE_PATH_LINUX := $(JDK_INCLUDE_PATH)/linux
JDK_LIB_PATH := $(JDK_HOME_PATH)/lib/server

PG_CFLAGS = -I$(JDK_INCLUDE_PATH) -I$(JDK_INCLUDE_PATH_LINUX)
PG_LDFLAGS = -L$(JDK_LIB_PATH) -ljvm

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

# Target that checks JDK paths
check_jdk:
	@if [ ! -d $(JDK_INCLUDE_PATH) ]; then \
	  echo "Error: JDK include path $(JDK_INCLUDE_PATH) not found"; \
	  exit 1; \
	fi
	@if [ ! -d $(JDK_INCLUDE_PATH_LINUX) ]; then \
	  echo "Error: JDK include path for Linux $(JDK_INCLUDE_PATH_LINUX) not found"; \
	  exit 1; \
	fi
	@if [ ! -d $(JDK_LIB_PATH) ]; then \
	  echo "Error: JDK lib path $(JDK_LIB_PATH) not found"; \
	  exit 1; \
	fi
