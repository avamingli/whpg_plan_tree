EXTENSION  = whpg_plan_tree
MODULES    = whpg_plan_tree
DATA       = whpg_plan_tree--1.0.0.sql
REGRESS    = whpg_plan_tree

# Needs gp_enable_query_metrics=on and whpg_plan_tree in
# shared_preload_libraries on the target cluster already (both
# PGC_POSTMASTER) -- see sql/whpg_plan_tree.sql's own header comment.
PG_CPPFLAGS = -I$(libpq_srcdir)

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/whpg_plan_tree
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
