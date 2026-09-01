-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION whpg_plan_tree" to load this file. \quit

--------------------------------------------------------------------------------
--  Real-plan-tree shmem capture (CapturePlanTree, whpg_plan_tree.c)          --
--------------------------------------------------------------------------------

CREATE SCHEMA plan_tree;
SET search_path = plan_tree;

--------------------------------------------------------------------------------
-- @function:
--        plan_tree_detail_f_on_master / _on_segments
--
-- @doc:
--        UDF wrapping plan_tree_detail() (whpg_plan_tree.c) -- one row per
--        (query, segment, plan node): the real plan_node_id/parent/label
--        captured at METRICS_PLAN_NODE_INITIALIZE time, keyed the same way
--        as gp_instrument_shmem_detail (tmid/ssid/ccnt/segid), so the two
--        can be joined on (tmid, ssid, ccnt, segid, nid) without a client
--        re-running EXPLAIN or re-deriving plan_node_id numbering.
--------------------------------------------------------------------------------

CREATE FUNCTION plan_tree_detail_f_on_master()
RETURNS SETOF record
AS '$libdir/whpg_plan_tree', 'plan_tree_detail'
LANGUAGE C VOLATILE EXECUTE ON COORDINATOR;

GRANT EXECUTE ON FUNCTION plan_tree_detail_f_on_master() TO public;

CREATE FUNCTION plan_tree_detail_f_on_segments()
RETURNS SETOF record
AS '$libdir/whpg_plan_tree', 'plan_tree_detail'
LANGUAGE C VOLATILE EXECUTE ON ALL SEGMENTS;

GRANT EXECUTE ON FUNCTION plan_tree_detail_f_on_segments() TO public;

CREATE VIEW plan_tree_detail AS
WITH all_entries AS (
  SELECT C.* FROM plan_tree_detail_f_on_master() AS C (
    tmid int4, ssid int4, ccnt int2, segid int2, pid int4,
    nid int2, parent_nid int2, node_type text, parallel_aware bool,
    strategy text, partial_mode text, operation text,
    motion_senders int2, motion_receivers int2,
    relname text, plan_rows float8,
    startup_cost float8, total_cost float8, plan_width int4
  )
  UNION ALL
  SELECT C.* FROM plan_tree_detail_f_on_segments() AS C (
    tmid int4, ssid int4, ccnt int2, segid int2, pid int4,
    nid int2, parent_nid int2, node_type text, parallel_aware bool,
    strategy text, partial_mode text, operation text,
    motion_senders int2, motion_receivers int2,
    relname text, plan_rows float8,
    startup_cost float8, total_cost float8, plan_width int4
  ))
SELECT * FROM all_entries ORDER BY segid, nid;

GRANT SELECT ON plan_tree_detail TO public;

SET search_path TO DEFAULT;
