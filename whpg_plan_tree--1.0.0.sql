-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION whpg_plan_tree" to load this file. \quit

--------------------------------------------------------------------------------
--  Real-plan-tree shmem capture (CapturePlanTree, whpg_plan_tree.c)          --
--------------------------------------------------------------------------------

CREATE SCHEMA whpg_plan_tree;
SET search_path = whpg_plan_tree;

--------------------------------------------------------------------------------
-- @function:
--        plan_detail_f_on_master / _on_segments
--
-- @doc:
--        UDF wrapping plan_tree_detail() (whpg_plan_tree.c) -- one row per
--        (query, segment, plan node): the real plan_node_id/parent/label
--        captured at METRICS_PLAN_NODE_INITIALIZE time, keyed the same way
--        as gp_instrument_shmem_detail (tmid/ssid/ccnt/segid), so the two
--        can be joined on (tmid, ssid, ccnt, segid, nid) without a client
--        re-running EXPLAIN or re-deriving plan_node_id numbering.
--------------------------------------------------------------------------------

CREATE FUNCTION plan_detail_f_on_master()
RETURNS SETOF record
AS '$libdir/whpg_plan_tree', 'plan_tree_detail'
LANGUAGE C VOLATILE EXECUTE ON COORDINATOR;

GRANT EXECUTE ON FUNCTION plan_detail_f_on_master() TO public;

CREATE FUNCTION plan_detail_f_on_segments()
RETURNS SETOF record
AS '$libdir/whpg_plan_tree', 'plan_tree_detail'
LANGUAGE C VOLATILE EXECUTE ON ALL SEGMENTS;

GRANT EXECUTE ON FUNCTION plan_detail_f_on_segments() TO public;

CREATE VIEW plan_detail AS
WITH all_entries AS (
  SELECT C.* FROM plan_detail_f_on_master() AS C (
    tmid int4, ssid int4, ccnt int2, segid int2, pid int4,
    nid int2, parent_nid int2, node_type text, parallel_aware bool,
    strategy text, partial_mode text, operation text,
    motion_senders int2, motion_receivers int2,
    relname text, plan_rows float8,
    startup_cost float8, total_cost float8, plan_width int4
  )
  UNION ALL
  SELECT C.* FROM plan_detail_f_on_segments() AS C (
    tmid int4, ssid int4, ccnt int2, segid int2, pid int4,
    nid int2, parent_nid int2, node_type text, parallel_aware bool,
    strategy text, partial_mode text, operation text,
    motion_senders int2, motion_receivers int2,
    relname text, plan_rows float8,
    startup_cost float8, total_cost float8, plan_width int4
  ))
SELECT * FROM all_entries ORDER BY segid, nid;

GRANT SELECT ON plan_detail TO public;

SET search_path TO DEFAULT;

--------------------------------------------------------------------------------
-- @view:
--        whpg_plan_tree.instrument_detail
--
-- @doc:
--        Thin SQL wrapper around the kernel-owned per-node row-counter ring
--        that gp_enable_query_metrics already fills ($libdir/gp_instrument_shmem,
--        C symbol gp_instrument_shmem_detail). One row per (running query,
--        segment, plan node) with tuplecount/nloops/ntuples, keyed by
--        (tmid, ssid, ccnt, segid, nid) -- exactly the same key as
--        whpg_plan_tree.plan_detail above, so the two views join into
--        "live plan tree + row progress" with no extra bookkeeping:
--
--          SELECT s.nid, s.node_type, s.relname, s.plan_rows AS estimated,
--                 COALESCE(m.ntuples, 0) AS actual, m.nloops
--            FROM whpg_plan_tree.plan_detail       s
--            LEFT JOIN whpg_plan_tree.instrument_detail m
--                   USING (tmid, ssid, ccnt, segid, nid);
--
--        Split into _on_master/_on_segments the same way plan_detail_f_* is:
--        wraps the same kernel C symbol twice with different EXECUTE ON
--        routing so both coordinator- and segment-resident slots surface
--        without needing external-web-table dispatch tricks.
--
--        Bundled here so one CREATE EXTENSION gets both sides of the join --
--        prior to this, operators had to hand-run a matching DDL to expose
--        the kernel .so at SQL. Wrapped in a DO/EXCEPTION block so the
--        outer CREATE EXTENSION still succeeds on a fork that doesn't ship
--        $libdir/gp_instrument_shmem: the block is a subtransaction, so on
--        failure nothing new is left behind and whpg_plan_tree.plan_detail
--        keeps working on its own. pg_dash already probes the view at the
--        capability layer, so a missing view degrades Watch to "hidden"
--        rather than "error".
--------------------------------------------------------------------------------
DO $bootstrap$
BEGIN
	EXECUTE $ddl$
		CREATE FUNCTION whpg_plan_tree.instrument_detail_f_on_master()
		RETURNS SETOF record
		AS '$libdir/gp_instrument_shmem', 'gp_instrument_shmem_detail'
		LANGUAGE C VOLATILE EXECUTE ON COORDINATOR
	$ddl$;

	EXECUTE 'GRANT EXECUTE ON FUNCTION whpg_plan_tree.instrument_detail_f_on_master() TO public';

	EXECUTE $ddl$
		CREATE FUNCTION whpg_plan_tree.instrument_detail_f_on_segments()
		RETURNS SETOF record
		AS '$libdir/gp_instrument_shmem', 'gp_instrument_shmem_detail'
		LANGUAGE C VOLATILE EXECUTE ON ALL SEGMENTS
	$ddl$;

	EXECUTE 'GRANT EXECUTE ON FUNCTION whpg_plan_tree.instrument_detail_f_on_segments() TO public';

	EXECUTE $ddl$
		CREATE VIEW whpg_plan_tree.instrument_detail AS
		WITH all_entries AS (
		  SELECT C.* FROM whpg_plan_tree.instrument_detail_f_on_master() AS C (
		    tmid int4, ssid int4, ccnt int2, segid int2, pid int4,
		    nid int2, tuplecount int8, nloops int8, ntuples int8
		  )
		  UNION ALL
		  SELECT C.* FROM whpg_plan_tree.instrument_detail_f_on_segments() AS C (
		    tmid int4, ssid int4, ccnt int2, segid int2, pid int4,
		    nid int2, tuplecount int8, nloops int8, ntuples int8
		  ))
		SELECT * FROM all_entries ORDER BY segid, nid
	$ddl$;

	EXECUTE 'GRANT SELECT ON whpg_plan_tree.instrument_detail TO public';
EXCEPTION WHEN OTHERS THEN
	RAISE NOTICE 'whpg_plan_tree: skipped bootstrapping whpg_plan_tree.instrument_detail (%). The kernel gp_instrument_shmem library is likely absent on this target; whpg_plan_tree.plan_detail is still available.', SQLERRM;
END
$bootstrap$;
