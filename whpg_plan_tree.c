/*-------------------------------------------------------------------------
 *
 * whpg_plan_tree.c
 *    Capture the real, already-planned plan tree into shared memory once
 *    per query per backend, and expose it over SQL -- entirely as a
 *    loadable module, zero core changes required on any target -- built
 *    primarily for WHPG, also runs unmodified across GPDB-lineage cores
 *    spanning PG12 through PG19.
 *
 * WHY: gp_enable_query_metrics already ships a shared-memory ring of live
 * per-node row counters (InstrumentationSlot, src/backend/executor/
 * instrument.c), keyed by tmid/ssid/ccnt/segid/nid. Nothing reading it from
 * outside the backend can get the real plan structure that nid refers to,
 * though: EXPLAIN never prints plan_node_id in any format, so an external
 * monitor has to re-run EXPLAIN on the same SQL text and re-derive the
 * numbering itself by re-implementing setrefs.c's pre-order rule -- a
 * reconstruction that is usually right but isn't the query that's actually
 * running.
 *
 * HOW: this module preloads (shared_preload_libraries) and, in _PG_init(),
 * registers itself against query_info_collect_hook (utils/metrics_utils.h)
 * -- already fires unconditionally at METRICS_PLAN_NODE_INITIALIZE in
 * execMain.c's InitPlan(), right after queryDesc->planstate is built and,
 * on the QD, strictly before CdbDispatchPlan. Each QE reaches the identical
 * call site independently via exec_mpp_query's standard PortalStart/
 * ExecutorStart path, with its own local copy of the same plan_node_id-
 * numbered tree (deserializeNode() sends the whole PlannedStmt, not just
 * the local slice). Verified byte-identical (typedef, enum values, header
 * location) across all three target trees.
 *
 * PORTABILITY: the one part of this module that genuinely differs across
 * PG cores from PG12 through PG19 is how an extension requests and
 * attaches its own shared memory -- three real eras of core API,
 * confirmed by reading each target's own storage/ipc.h and storage/
 * lwlock.h rather than assumed from version numbers:
 *
 *   PG_VERSION_NUM < 150000 (WHPG7/GPDB7, PG12):
 *     no shmem_request_hook exists yet (added upstream in PG15) --
 *     RequestAddinShmemSpace()/RequestNamedLWLockTranche() are called
 *     directly inside _PG_init() instead, exactly like contrib/
 *     pg_stat_statements does on this same tree.
 *   150000 <= PG_VERSION_NUM < 190000 (a later GPDB-lineage core, PG16):
 *     classic shmem_request_hook/shmem_startup_hook chaining, again
 *     mirroring pg_stat_statements on this tree.
 *   PG_VERSION_NUM >= 190000 (WHPG's own PG19 core):
 *     RegisterShmemCallbacks()/ShmemRequestStruct() -- the newer
 *     structured callback registry this fork's core has migrated onto
 *     (src/backend/storage/ipc/ipci.c). 190000 is unambiguous here since
 *     no real upstream PostgreSQL has reached major version 19; it is
 *     this fork's own numbering, not a claim about a real PG threshold.
 *
 * The two older eras attach via RequestNamedLWLockTranche()+
 * GetNamedLWLockTranche(), so PlanTreeHeader.lock is a *pointer* there
 * (core owns the LWLock); the new era's ShmemRequestStruct .ptr-wiring
 * works with an LWLock embedded directly in our own struct, initialized
 * via that era's also-new one-arg LWLockNewTrancheId(). See
 * WHPG_PLAN_TREE_NEW_CALLBACKS below.
 *
 * A second, smaller portability seam: three NodeTags this module's label
 * switch needs (T_IncrementalSort, T_Memoize, T_TidRangeScan) don't exist
 * at all on WHPG7/GPDB7 (PG12 predates all three upstream: PG13, PG14 and
 * PG14 respectively) -- confirmed by grepping nodes.h directly rather than
 * assumed from the PG12 version number alone, since NodeTag coverage on a
 * GPDB-lineage fork isn't guaranteed to track upstream 1:1 (some later
 * cores carry all three despite forking their own planner). Gated the
 * same way, by the real upstream introduction version.
 *
 * Exposed as whpg_plan_tree.plan_detail (own schema, deliberately not
 * gp_-prefixed -- GPDB reserves that prefix for system schemas). This
 * extension's own script also creates whpg_plan_tree.instrument_detail,
 * a thin wrapper over the kernel's $libdir/gp_instrument_shmem so both
 * sides of "plan structure + row progress" surface under one schema.
 *
 * Captured per node: nid, parent_nid, node type, and the same
 * strategy/partial-mode/operation/motion-type/relname/plan_rows fields
 * EXPLAIN (FORMAT JSON) exposes as separate properties for
 * Agg/SetOp/ModifyTable/ForeignScan/Motion/Scan nodes. A client can build
 * the exact tree whpg_plan_tree.instrument_detail's nid refers to by
 * joining on (tmid, ssid, ccnt, segid, nid) -- no re-EXPLAIN, no
 * client-side renumbering.
 *
 * Shmem layout and slot lifecycle mirror InstrumentationSlot as closely
 * as possible: same free-list-in-shmem design (PATTERN-filled free slots,
 * next-pointer stashed in the slot's own trailing bytes), same
 * gp_enable_query_metrics gate, and same ResourceOwner-based recycle
 * timing (RESOURCE_RELEASE_AFTER_LOCKS) -- a slot returns to the free
 * list the instant its query's resource owner releases, exactly like
 * InstrumentationSlot. GUC whpg_plan_tree.size (KB, PGC_POSTMASTER,
 * default 8MB) sizes the ring; nodes beyond a 128-per-query cap are
 * dropped with a truncated flag, same MAX_SCAN_ON_SHMEM-style bound
 * already used for scan-node instrumentation.
 *
 * GUC whpg_plan_tree.capture (bool, PGC_USERSET, default true) is a
 * second, per-backend gate checked after gp_enable_query_metrics: default
 * on keeps today's "capture everything" behavior; an operator who flips
 * the server-wide default off can have individual sessions opt back in
 * with a plain SET, trading away the ability to Watch an arbitrary
 * already-running query nobody pre-selected for capture (that query's
 * own capture decision already happened before anyone could react).
 *
 * Requires: shared_preload_libraries = 'whpg_plan_tree' (for the hook and
 * shmem) plus CREATE EXTENSION whpg_plan_tree (for the SQL-visible
 * plan_detail_f_on_master/_on_segments functions and the plan_detail
 * view -- same .so either way). This extension is entirely
 * self-contained: it does not depend on, and is not bundled inside,
 * gp_internal_tools.
 *
 * Portions Copyright (c) 2026, Zhang Mingli (avamingli)
 *
 *-------------------------------------------------------------------------
*/
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "cdb/cdbvars.h"
#include "executor/executor.h"
#include "executor/instrument.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/metrics_utils.h"
#include "utils/resowner.h"

PG_MODULE_MAGIC;

/*
 * See the PORTABILITY section of the file header comment. 190000 is this
 * fork's own numbering, not a real upstream PG threshold.
 */
#if PG_VERSION_NUM >= 190000
#define WHPG_PLAN_TREE_NEW_CALLBACKS 1
#else
#define WHPG_PLAN_TREE_NEW_CALLBACKS 0
#endif

/*
 * Hard cap on captured nodes per query, same philosophy as
 * MAX_SCAN_ON_SHMEM in instrument.h: bound shmem usage per slot instead of
 * a variable-size allocator. A plan with more nodes than this is captured
 * up to the cap and flagged 'truncated'; callers should treat a truncated
 * capture as "structure incomplete, fall back to EXPLAIN".
 */
#define WHPG_PLAN_TREE_MAX_NODES 128
#define WHPG_PLAN_TREE_NAMEDATALEN 64
#define WHPG_PLAN_TREE_STRLEN 16

typedef struct PlanTreeNodeEntry
{
	int16		nid;			/* real plan_node_id, no client-side guessing */
	int16		parent_nid;		/* -1 for the root node */
	int32		node_type;		/* NodeTag of the Plan node */
	int16		motion_type;	/* MotionType when node_type == T_Motion, else -1 --
								 * explain.c's "Node Type"/sname for Motion is
								 * motionType-specific ("Gather Motion" vs
								 * "Redistribute Motion" vs ...), so the bare
								 * NodeTag alone can't produce the right label. */
	bool		parallel_aware;

	/*
	 * Mirrors exactly the extra properties EXPLAIN (FORMAT JSON) exposes
	 * separately from "Node Type" for these node kinds (Strategy, Partial
	 * Mode, Operation) -- so a client's existing label-reconstruction
	 * logic can drive off real data unchanged.
	 */
	char		strategy[WHPG_PLAN_TREE_STRLEN];		/* Agg/SetOp */
	char		partial_mode[WHPG_PLAN_TREE_STRLEN];	/* Agg */
	char		operation[WHPG_PLAN_TREE_STRLEN];		/* ModifyTable/ForeignScan */
	int16		motion_senders;		/* Motion; -1 if not a Motion node */
	int16		motion_receivers;

	char		relname[WHPG_PLAN_TREE_NAMEDATALEN];	/* Scan nodes only */
	double		plan_rows;			/* optimizer's per-segment row estimate */
	double		startup_cost;
	double		total_cost;
	int32		plan_width;
} PlanTreeNodeEntry;

typedef struct PlanTreeSlot
{
	uint32		magic;			/* PLAN_TREE_MAGIC_FREE while on the free
								 * list (natural result of PATTERN-fill,
								 * also what the recycle callback sets),
								 * stays FREE through the "allocated, being
								 * populated" window (writer skips memset to
								 * avoid a per-query 20KB store), then flips
								 * to PLAN_TREE_MAGIC_VALID under LW_EXCLUSIVE
								 * as the publish barrier. Readers observe
								 * only VALID slots, so a mid-populate or
								 * error-aborted slot is invisible. */
	int32		pid;
	int32		tmid;
	int32		ssid;
	int32		ccnt;
	int16		segid;
	int16		nnodes;
	bool		truncated;
	PlanTreeNodeEntry nodes[WHPG_PLAN_TREE_MAX_NODES];
} PlanTreeSlot;

typedef struct PlanTreeHeader
{
#if WHPG_PLAN_TREE_NEW_CALLBACKS
	LWLock		lock;			/* embedded; new framework wires the whole
								 * struct's memory to us in one shot */
#else
	LWLock	   *lock;			/* core-owned, obtained via
								 * GetNamedLWLockTranche() at startup */
#endif
	void	   *head;
	int			free;
} PlanTreeHeader;

#if WHPG_PLAN_TREE_NEW_CALLBACKS
#define PLAN_TREE_LOCK (&PlanTreeGlobal->lock)
#else
#define PLAN_TREE_LOCK (PlanTreeGlobal->lock)
#endif

typedef struct PlanTreeResownerSet
{
	PlanTreeSlot *slot;
	ResourceOwner owner;
	struct PlanTreeResownerSet *next;
} PlanTreeResownerSet;

/*
 * Same free-list-in-place trick as instrument.h's SlotIsEmpty/
 * GetInstrumentNext: a free slot is PATTERN-filled, and its last 8 bytes
 * (the last node entry's plan_rows, a full double) double as the "next
 * free slot" pointer while unused.
 *
 * "Is this slot free?" is checked via slot->magic == PLAN_TREE_MAGIC_FREE
 * rather than "the first 8 bytes are all pattern": magic is only ever
 * assigned one of three specific values, so there is no way for a real
 * capture's header bytes (pid/tmid) to accidentally collide with the
 * pattern -- eliminating the tiny theoretical false-positive InstrumentSlot
 * inherits from its raw-pattern check.
 *
 * "Is this slot safe for a reader to consume?" is a stricter check --
 * readers filter on magic == PLAN_TREE_MAGIC_VALID. A slot in the
 * transient "popped from freelist, being populated" state still has
 * magic == FREE (the writer doesn't clear it until publish), which the
 * VALID-only filter naturally excludes.
 */
#define PLAN_TREE_PATTERN 0xd5
#define PLAN_TREE_MAGIC_FREE  0xd5d5d5d5U		/* what PATTERN-fill leaves
												 * in .magic; unreachable
												 * from any live capture */
#define PLAN_TREE_MAGIC_VALID 0xc0de1234U		/* final "publish" store
												 * done under LW_EXCLUSIVE
												 * after populate -- readers
												 * key on this */
#define PlanTreeSlotIsEmpty(slot) ((slot)->magic == PLAN_TREE_MAGIC_FREE)
#define GetPlanTreeNext(slot) (*((PlanTreeSlot **)((slot) + 1) - 1))

typedef struct PlanCaptureContext
{
	PlanTreeSlot *slot;
	int16		parent_nid;
} PlanCaptureContext;

/* GUC backing vars, module-local -- no core cdbvars.h extern needed. */
static int	whpg_plan_tree_size = 8192;

/*
 * PGC_USERSET, unlike whpg_plan_tree.size -- shmem sizing is a
 * postmaster-time decision (see the file header comment), but "should
 * *this* backend bother capturing" has no such constraint: it's a plain
 * per-backend variable this module already reads on every query, so the
 * standard GUC context ladder (server-wide default via postgresql.conf/
 * ALTER SYSTEM + reload, no restart needed; any session can SET its own
 * override; SET LOCAL for just one transaction) applies for free. Default
 * true so enabling gp_enable_query_metrics keeps behaving exactly like it
 * does today -- capture everything -- unless an operator deliberately
 * flips the server-wide default to capture only sessions that explicitly
 * opt in (trading away the ability to Watch an arbitrary already-running
 * query someone didn't plan to watch ahead of time, since the capture
 * decision for a query already has to have been made before that query
 * started -- no GUC context can retroactively capture a statement whose
 * METRICS_PLAN_NODE_INITIALIZE has already fired and passed).
 */
static bool whpg_plan_tree_capture = true;

static PlanTreeHeader *PlanTreeGlobal = NULL;
#if WHPG_PLAN_TREE_NEW_CALLBACKS
static int	plan_tree_tranche_id = -1;
#endif
static int	planTreeNumSlots = -1;
static bool planTreeResownerCallbackRegistered = false;
static PlanTreeResownerSet *planSlotsOccupied = NULL;

static query_info_collect_hook_type prev_query_info_collect_hook = NULL;

static void planTreeRecycleCallback(ResourceReleasePhase phase, bool isCommit,
									  bool isTopLevel, void *arg);
static bool plan_capture_walker(PlanState *planstate, void *context);
static void capture_one_node(PlanState *planstate, PlanCaptureContext *ctx);
static void CapturePlanTree(QueryDesc *queryDesc);
static void plan_tree_query_info_hook(QueryMetricsStatus status, void *args);
static Size PlanTreeComputeSize(void);

#if WHPG_PLAN_TREE_NEW_CALLBACKS
static void PlanTreeRequestCallback(void *arg);
static void PlanTreeInitCallback(void *arg);

static const ShmemCallbacks PlanTreeCallbacks = {
	.request_fn = PlanTreeRequestCallback,
	.init_fn = PlanTreeInitCallback,
};
#else
static void PlanTreeRequest(void);
static void PlanTreeStartup(void);

#if PG_VERSION_NUM >= 150000
static shmem_request_hook_type prev_shmem_request_hook = NULL;
#endif
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
#endif

void		_PG_init(void);
void		_PG_fini(void);

/* Calculate number of slots from whpg_plan_tree.size */
static Size
PlanTreeNumSlots(void)
{
	if (planTreeNumSlots < 0)
	{
		planTreeNumSlots = (int) (whpg_plan_tree_size * 1024 - sizeof(PlanTreeHeader)) / sizeof(PlanTreeSlot);
		planTreeNumSlots = (planTreeNumSlots < 0) ? 0 : planTreeNumSlots;
	}
	return planTreeNumSlots;
}

/*
 * Byte footprint of the plan-shmem free list. Returns 0 (feature disabled)
 * under the exact same conditions the per-node instrumentation ring does
 * -- tied to the same gp_enable_query_metrics GUC, so the two features
 * are always enabled/disabled together.
 */
static Size
PlanTreeComputeSize(void)
{
	Size		number_slots;

	if (Gp_role == GP_ROLE_UTILITY)
		return 0;

	if (!gp_enable_query_metrics || whpg_plan_tree_size <= 0)
		return 0;

	number_slots = PlanTreeNumSlots();

	if (number_slots <= 0)
		return 0;

	return add_size(sizeof(PlanTreeHeader),
					 mul_size(number_slots, sizeof(PlanTreeSlot)));
}

/*
 * Build the free list over an already-allocated, PATTERN-filled block --
 * shared by both eras' init paths (only the "how did we get this pointer,
 * and how many times will this run" part differs between them).
 */
static void
PlanTreeBuildFreeList(void)
{
	Size		number_slots;
	PlanTreeSlot *slot;
	int			i;

	memset(PlanTreeGlobal, PLAN_TREE_PATTERN, PlanTreeComputeSize());

	number_slots = PlanTreeNumSlots();
	slot = (PlanTreeSlot *) (PlanTreeGlobal + 1);

	PlanTreeGlobal->head = slot;
	PlanTreeGlobal->free = number_slots;

	for (i = 0; i < number_slots - 1; i++)
		GetPlanTreeNext(&slot[i]) = &slot[i + 1];
	GetPlanTreeNext(&slot[i]) = NULL;
}

#if WHPG_PLAN_TREE_NEW_CALLBACKS

/*
 * ShmemCallbacks request/init pair -- the same RegisterShmemCallbacks()
 * mechanism contrib/pg_stat_statements' pgss_shmem_request()/
 * pgss_shmem_init() use on this tree, called from our own _PG_init()
 * instead of core's RegisterBuiltinShmemCallbacks(). The framework
 * guarantees init_fn runs exactly once, before any backend forks
 * (pg_stat_statements asserts !IsUnderPostmaster in its own init_fn for
 * the same reason), so no manual "first backend through" guard is needed
 * here, unlike the older shmem_startup_hook path below.
 */
static void
PlanTreeRequestCallback(void *arg)
{
	Size		size = PlanTreeComputeSize();

	if (size == 0)
		return;

	ShmemRequestStruct(.name = "whpg_plan_tree",
						.size = size,
						.ptr = (void **) &PlanTreeGlobal);
}

static void
PlanTreeInitCallback(void *arg)
{
	Size		size = PlanTreeComputeSize();

	if (size == 0)
		return;

	Assert(PlanTreeGlobal != NULL);

	/* PlanTreeBuildFreeList() PATTERN-fills the header too; carve the
	 * header's real fields in afterwards. */
	PlanTreeBuildFreeList();

	plan_tree_tranche_id = LWLockNewTrancheId("whpg_plan_tree");
	LWLockInitialize(&PlanTreeGlobal->lock, plan_tree_tranche_id);

	if (!planTreeResownerCallbackRegistered)
	{
		RegisterResourceReleaseCallback(planTreeRecycleCallback, NULL);
		planTreeResownerCallbackRegistered = true;
	}
}

#else							/* !WHPG_PLAN_TREE_NEW_CALLBACKS */

/*
 * Older two eras: request the shmem/lock tranche -- either directly from
 * _PG_init() (PG12, no shmem_request_hook exists yet) or chained through
 * shmem_request_hook (PG15+) -- exactly like pg_stat_statements does on
 * each of those trees.
 */
static void
PlanTreeRequest(void)
{
	Size		size;

#if PG_VERSION_NUM >= 150000
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();
#endif

	size = PlanTreeComputeSize();
	if (size == 0)
		return;

	RequestAddinShmemSpace(size);
	RequestNamedLWLockTranche("whpg_plan_tree", 1);
}

/*
 * Every backend (not just the first) runs its own shmem_startup_hook to
 * attach to the already-allocated block -- the "if (!found)" guard below
 * is what makes sure only the first one through actually builds the free
 * list, exactly mirroring pg_stat_statements' own pgss_shmem_startup() on
 * this same tree.
 */
static void
PlanTreeStartup(void)
{
	bool		found;
	Size		size;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	size = PlanTreeComputeSize();
	if (size == 0)
		return;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	PlanTreeGlobal = (PlanTreeHeader *) ShmemInitStruct("whpg_plan_tree", size, &found);

	if (!found)
	{
		/*
		 * Build the free list (PATTERN-fills the whole struct, header
		 * included) *before* touching any header field of our own --
		 * setting `lock` first would just get clobbered by the memset.
		 */
		PlanTreeBuildFreeList();
		PlanTreeGlobal->lock = &(GetNamedLWLockTranche("whpg_plan_tree"))->lock;
	}

	LWLockRelease(AddinShmemInitLock);

	if (!planTreeResownerCallbackRegistered)
	{
		RegisterResourceReleaseCallback(planTreeRecycleCallback, NULL);
		planTreeResownerCallbackRegistered = true;
	}
}

#endif							/* WHPG_PLAN_TREE_NEW_CALLBACKS */

/*
 * Node-type label, matching explain.c's "sname" (the string EXPLAIN
 * (FORMAT JSON) puts in "Node Type") for every node kind explain.c knows
 * about. Kept as our own switch rather than reusing print.c's
 * plannode_type() -- that helper is a debug-log shorthand (all-caps,
 * "UNKNOWN" default) missing several common node types (ModifyTable,
 * IndexOnlyScan, GatherMerge, ...), not meant to be authoritative here.
 *
 * T_IncrementalSort/T_Memoize/T_TidRangeScan are gated on their real
 * upstream introduction versions (PG13/PG14/PG14) rather than assumed
 * present -- WHPG7/GPDB7 (PG12) predates all three and doesn't have the
 * NodeTag at all, confirmed directly against nodes.h rather than inferred.
 */
static const char *
PlanTreeNodeTypeName(NodeTag tag)
{
	switch (tag)
	{
		case T_Result: return "Result";
		case T_ProjectSet: return "ProjectSet";
		case T_ModifyTable: return "ModifyTable";
		case T_Append: return "Append";
		case T_MergeAppend: return "Merge Append";
		case T_RecursiveUnion: return "Recursive Union";
		case T_Sequence: return "Sequence";
		case T_BitmapAnd: return "BitmapAnd";
		case T_BitmapOr: return "BitmapOr";
		case T_NestLoop: return "Nested Loop";
		case T_MergeJoin: return "Merge Join";
		case T_HashJoin: return "Hash Join";
		case T_SeqScan: return "Seq Scan";
		case T_DynamicSeqScan: return "Dynamic Seq Scan";
		case T_SampleScan: return "Sample Scan";
		case T_Gather: return "Gather";
		case T_GatherMerge: return "Gather Merge";
		case T_IndexScan: return "Index Scan";
		case T_DynamicIndexScan: return "Dynamic Index Scan";
		case T_DynamicIndexOnlyScan: return "Dynamic Index Only Scan";
		case T_IndexOnlyScan: return "Index Only Scan";
		case T_BitmapIndexScan: return "Bitmap Index Scan";
		case T_DynamicBitmapIndexScan: return "Dynamic Bitmap Index Scan";
		case T_BitmapHeapScan: return "Bitmap Heap Scan";
		case T_DynamicBitmapHeapScan: return "Dynamic Bitmap Heap Scan";
		case T_TidScan: return "Tid Scan";
#if PG_VERSION_NUM >= 140000
		case T_TidRangeScan: return "Tid Range Scan";
#endif
		case T_SubqueryScan: return "Subquery Scan";
		case T_FunctionScan: return "Function Scan";
		case T_TableFuncScan: return "Table Function Scan";
		case T_TableFunctionScan: return "Table Function Scan"; /* GPDB: distinct NodeTag from T_TableFuncScan, same explain.c label */
		case T_ValuesScan: return "Values Scan";
		case T_CteScan: return "CTE Scan";
		case T_NamedTuplestoreScan: return "Named Tuplestore Scan";
		case T_WorkTableScan: return "WorkTable Scan";
		case T_ShareInputScan: return "Shared Scan";
		case T_ForeignScan: return "Foreign Scan";
		case T_DynamicForeignScan: return "Dynamic Foreign Scan";
		case T_CustomScan: return "Custom Scan";
		case T_Material: return "Materialize";
#if PG_VERSION_NUM >= 170000
		case T_Memoize: return "Memoize";
#endif
		case T_Sort: return "Sort";
#if PG_VERSION_NUM >= 130000
		case T_IncrementalSort: return "Incremental Sort";
#endif
		case T_TupleSplit: return "TupleSplit";
		case T_Agg: return "Aggregate";
		case T_WindowAgg: return "WindowAgg";
		case T_Unique: return "Unique";
		case T_SetOp: return "SetOp";
		case T_LockRows: return "LockRows";
		case T_Limit: return "Limit";
		case T_Hash: return "Hash";
		case T_Motion: return "Motion";
		case T_SplitUpdate: return "Split";
		case T_AssertOp: return "Assert";
		case T_PartitionSelector: return "Partition Selector";
		default: return "???";
	}
}

/*
 * Motion's "Node Type"/sname in EXPLAIN is motionType-specific, unlike
 * every other node kind PlanTreeNodeTypeName() covers with a plain
 * NodeTag switch -- mirrors explain.c's own T_Motion case
 * (src/backend/commands/explain.c) exactly, string for string.
 */
static const char *
MotionTypeName(int16 motion_type)
{
	switch ((MotionType) motion_type)
	{
		case MOTIONTYPE_GATHER: return "Gather Motion";
		case MOTIONTYPE_GATHER_SINGLE: return "Explicit Gather Motion";
		case MOTIONTYPE_HASH: return "Redistribute Motion";
		case MOTIONTYPE_BROADCAST: return "Broadcast Motion";
		case MOTIONTYPE_EXPLICIT: return "Explicit Redistribute Motion";
		default: return "???"; /* MOTIONTYPE_OUTER_QUERY (and any other
								 * target-specific MotionType value gated
								 * behind its own feature flag): same
								 * fallback explain.c uses */
	}
}

/*
 * Fill in the node-kind-specific extra fields, mirroring exactly the extra
 * ExplainProperty*() calls explain.c makes for these node kinds in JSON
 * format (Strategy/Partial Mode/Operation/motionType).
 */
static void
fill_node_extras(PlanState *planstate, PlanTreeNodeEntry *entry)
{
	Plan	   *plan = planstate->plan;

	memset(entry->strategy, 0, sizeof(entry->strategy));
	memset(entry->partial_mode, 0, sizeof(entry->partial_mode));
	memset(entry->operation, 0, sizeof(entry->operation));
	entry->motion_senders = -1;
	entry->motion_receivers = -1;
	entry->motion_type = -1;
	memset(entry->relname, 0, sizeof(entry->relname));

	switch (nodeTag(plan))
	{
		case T_Agg:
			{
				Agg		   *agg = (Agg *) plan;

				switch (agg->aggstrategy)
				{
					case AGG_PLAIN: strlcpy(entry->strategy, "Plain", sizeof(entry->strategy)); break;
					case AGG_SORTED: strlcpy(entry->strategy, "Sorted", sizeof(entry->strategy)); break;
					case AGG_HASHED: strlcpy(entry->strategy, "Hashed", sizeof(entry->strategy)); break;
					case AGG_MIXED: strlcpy(entry->strategy, "Mixed", sizeof(entry->strategy)); break;
				}
				if (DO_AGGSPLIT_SKIPFINAL(agg->aggsplit))
					strlcpy(entry->partial_mode, "Partial", sizeof(entry->partial_mode));
				else if (DO_AGGSPLIT_COMBINE(agg->aggsplit))
					strlcpy(entry->partial_mode, "Finalize", sizeof(entry->partial_mode));
				else
					strlcpy(entry->partial_mode, "Simple", sizeof(entry->partial_mode));
				break;
			}
		case T_SetOp:
			{
				SetOp	   *setop = (SetOp *) plan;

				strlcpy(entry->strategy,
						setop->strategy == SETOP_HASHED ? "Hashed" : "Sorted",
						sizeof(entry->strategy));
				break;
			}
		case T_ModifyTable:
			{
				ModifyTable *mt = (ModifyTable *) plan;

				switch (mt->operation)
				{
					case CMD_INSERT: strlcpy(entry->operation, "Insert", sizeof(entry->operation)); break;
					case CMD_UPDATE: strlcpy(entry->operation, "Update", sizeof(entry->operation)); break;
					case CMD_DELETE: strlcpy(entry->operation, "Delete", sizeof(entry->operation)); break;
					default: break;
				}
				break;
			}
		case T_ForeignScan:
		case T_DynamicForeignScan:
			{
				ForeignScan *fs = (ForeignScan *) plan;

				switch (fs->operation)
				{
					case CMD_SELECT: strlcpy(entry->operation, "Select", sizeof(entry->operation)); break;
					case CMD_INSERT: strlcpy(entry->operation, "Insert", sizeof(entry->operation)); break;
					case CMD_UPDATE: strlcpy(entry->operation, "Update", sizeof(entry->operation)); break;
					case CMD_DELETE: strlcpy(entry->operation, "Delete", sizeof(entry->operation)); break;
					default: break;
				}
				break;
			}
		case T_Motion:
			{
				Motion	   *motion = (Motion *) plan;

				/*
				 * Real per-slice segment counts aren't reachable from a
				 * bare Plan node post-hoc (that lives in the EState's
				 * SliceTable while executing) -- leave senders/receivers
				 * as "unknown" (-1) for this v1 and let the client fall
				 * back to what it already derives from EXPLAIN for the
				 * Motion N:M annotation.
				 */
				entry->motion_type = (int16) motion->motionType;
				break;
			}
		default:
			break;
	}

	/* Scan target relation name, for the common single-relation scan kinds. */
	switch (nodeTag(plan))
	{
		case T_SeqScan:
		case T_DynamicSeqScan:
		case T_SampleScan:
		case T_IndexScan:
		case T_DynamicIndexScan:
		case T_IndexOnlyScan:
		case T_DynamicIndexOnlyScan:
		case T_BitmapHeapScan:
		case T_DynamicBitmapHeapScan:
		case T_TidScan:
#if PG_VERSION_NUM >= 140000
		case T_TidRangeScan:
#endif
			{
				/*
				 * All of these embed a "Scan scan;" as their literal first
				 * struct member (see plannodes.h), so the cast is safe.
				 */
				Scan	   *scan = (Scan *) plan;
				RangeTblEntry *rte = exec_rt_fetch(scan->scanrelid,
													planstate->state);
				char	   *relname = OidIsValid(rte->relid) ?
					get_rel_name(rte->relid) : NULL;

				if (relname)
					strlcpy(entry->relname, relname, sizeof(entry->relname));
				break;
			}
		default:
			break;
	}
}

static void
capture_one_node(PlanState *planstate, PlanCaptureContext *ctx)
{
	PlanTreeSlot *slot = ctx->slot;
	Plan	   *plan = planstate->plan;
	PlanTreeNodeEntry *entry;
	int16		my_nid;
	int16		saved_parent;

	if (slot->nnodes >= WHPG_PLAN_TREE_MAX_NODES)
	{
		slot->truncated = true;
		return;
	}

	entry = &slot->nodes[slot->nnodes++];
	my_nid = (int16) plan->plan_node_id;

	entry->nid = my_nid;
	entry->parent_nid = ctx->parent_nid;
	entry->node_type = nodeTag(plan);
	entry->parallel_aware = plan->parallel_aware;
	entry->plan_rows = plan->plan_rows;
	entry->startup_cost = plan->startup_cost;
	entry->total_cost = plan->total_cost;
	entry->plan_width = plan->plan_width;
	fill_node_extras(planstate, entry);

	saved_parent = ctx->parent_nid;
	ctx->parent_nid = my_nid;
	planstate_tree_walker(planstate, plan_capture_walker, ctx);
	ctx->parent_nid = saved_parent;
}

static bool
plan_capture_walker(PlanState *planstate, void *context)
{
	capture_one_node(planstate, (PlanCaptureContext *) context);
	return false;				/* keep walking the whole tree */
}

/*
 * Pick a free slot, walk queryDesc->planstate once, and populate it with
 * the real plan tree -- called once per query per backend from
 * query_info_collect_hook's METRICS_PLAN_NODE_INITIALIZE event, i.e.
 * after the whole PlanState tree is built and, on the QD, strictly before
 * CdbDispatchPlan. QE backends reach the same call site independently via
 * exec_mpp_query's standard PortalStart/ExecutorStart path, with their
 * own local copy of the same plan_node_id-numbered tree.
 *
 * Concurrency contract:
 *   - LW_EXCLUSIVE is held only across two short critical sections: the
 *     free-list pop, and a single-store publish at the end. The plan-tree
 *     walk (bounded to WHPG_PLAN_TREE_MAX_NODES=128, plus a syscache
 *     lookup per scan for the relname) runs *lock-free* between them --
 *     so every backend's METRICS_PLAN_NODE_INITIALIZE does not serialize
 *     on this one cluster-wide lock. This matters: every query on the
 *     cluster hits this path.
 *   - Ownership discipline replaces continuous locking: a popped slot is
 *     exclusively owned by this backend from pop until either publish or
 *     resowner-recycle; no other backend can touch it because it's off
 *     the free list. So the lock-free populate is race-free on its own
 *     terms (nobody else writes this slot), and readers that iterate the
 *     whole slot array simply skip it via the magic filter.
 *   - slot->magic starts at PLAN_TREE_MAGIC_FREE (on the free list,
 *     tagged by the recycle callback) and stays FREE throughout the
 *     lock-free populate -- the writer deliberately does not clear the
 *     slot to zero, saving a 20KB memset per capture. It flips to
 *     PLAN_TREE_MAGIC_VALID inside the second LWLock section as the
 *     sole publish store. The LWLockAcquire before it is a full memory
 *     barrier, so all the lock-free populate stores above happen-before
 *     the magic store; a reader that later takes LW_SHARED and observes
 *     magic == VALID is guaranteed to see the fully-populated slot.
 *     Readers filter on magic == VALID and skip anything else -- so a
 *     slot mid-populate (still FREE) or an aborted populate (still
 *     FREE, then recycled) is invisible.
 *   - The resowner tracking entry is registered *before* the walk begins:
 *     if the walk throws, PG's abort processing runs the
 *     RESOURCE_RELEASE_AFTER_LOCKS callback, which recycles the slot
 *     back to the free list. Without this ordering a throwing walk would
 *     permanently leak the slot. planSlotsOccupied is backend-local, so
 *     the linked-list splice needs no lock.
 */
static void
CapturePlanTree(QueryDesc *queryDesc)
{
	PlanTreeSlot *slot = NULL;
	PlanTreeResownerSet *item;
	PlanCaptureContext ctx;
	MemoryContext oldcontext;

	if (Gp_role == GP_ROLE_UTILITY)
		return;

	if (!gp_enable_query_metrics || NULL == PlanTreeGlobal)
		return;

	/*
	 * Per-backend opt-out, checked *after* the shared, cluster-wide
	 * gp_enable_query_metrics gate above -- see whpg_plan_tree_capture's
	 * own comment for why this is safe as a plain GUC read with no shared
	 * state or locking involved.
	 */
	if (!whpg_plan_tree_capture)
		return;

	if (queryDesc == NULL || queryDesc->planstate == NULL)
		return;

	/*
	 * Pre-allocate the resowner tracking entry outside the LWLock so an
	 * ENOMEM here (backend-local, no shmem touched) is a clean no-op
	 * rather than a shmem slot orphaned off the free list.
	 */
	oldcontext = MemoryContextSwitchTo(TopMemoryContext);
	item = (PlanTreeResownerSet *) palloc0(sizeof(PlanTreeResownerSet));
	MemoryContextSwitchTo(oldcontext);

	/*
	 * Section 1: pop a free slot under LW_EXCLUSIVE, then release. Held
	 * only across the linked-list unlink and counter decrement.
	 */
	LWLockAcquire(PLAN_TREE_LOCK, LW_EXCLUSIVE);
	slot = PlanTreeGlobal->head;
	if (NULL != slot && PlanTreeSlotIsEmpty(slot))
	{
		PlanTreeGlobal->head = GetPlanTreeNext(slot);
		PlanTreeGlobal->free--;
	}
	else
		slot = NULL;
	LWLockRelease(PLAN_TREE_LOCK);

	if (slot == NULL)
	{
		pfree(item);
		return;					/* no free slot; skip capture, degrade gracefully */
	}

	/*
	 * Lock-free populate. This slot is off the free list, so no other
	 * backend can touch it. magic stays at PLAN_TREE_MAGIC_FREE (untouched
	 * from the recycle callback's cheap FREE tag) throughout populate;
	 * readers filter on magic == VALID and skip anything else, so an
	 * in-flight populate is invisible.
	 *
	 * Deliberately no memset of the slot: capture_one_node writes every
	 * field of each PlanTreeNodeEntry it fills (fill_node_extras zeros the
	 * conditional extras up front -- see its own head), and entries at
	 * indices >= nnodes are never read by any consumer. A 20KB per-query
	 * memset would add up under load for no observable benefit.
	 */
	slot->segid = (int16) GpIdentity.segindex;
	slot->pid = MyProcPid;
	gp_gettmid(&(slot->tmid));
	slot->ssid = gp_session_id;
	slot->ccnt = gp_command_count;
	slot->nnodes = 0;
	slot->truncated = false;

	/*
	 * Register with the current ResourceOwner *before* walking so an
	 * ereport() out of fill_node_extras' get_rel_name lookup (unlikely at
	 * this point but not statically excluded) still gets the slot back to
	 * the free list at abort -- see the function header comment.
	 * planSlotsOccupied is backend-local, no shmem lock needed.
	 */
	item->owner = CurrentResourceOwner;
	item->slot = slot;
	item->next = planSlotsOccupied;
	planSlotsOccupied = item;

	ctx.slot = slot;
	ctx.parent_nid = -1;
	capture_one_node(queryDesc->planstate, &ctx);

	/*
	 * Section 2: publish under LW_EXCLUSIVE, held only across a single
	 * store. The lock acquire is a full memory barrier, so the lock-free
	 * populate stores above are visible before magic = VALID; any reader
	 * that later takes LW_SHARED and sees magic == VALID observes the
	 * fully-populated slot. Contention here is negligible -- a single
	 * store's worth of hold time, no cross-backend gating on the walk.
	 */
	LWLockAcquire(PLAN_TREE_LOCK, LW_EXCLUSIVE);
	slot->magic = PLAN_TREE_MAGIC_VALID;
	LWLockRelease(PLAN_TREE_LOCK);
}

/*
 * Recycle plan-shmem slots. Timing is copied deliberately from the
 * per-node instrumentation ring's own recycle callback: a slot is freed
 * the instant its query's ResourceOwner releases
 * (RESOURCE_RELEASE_AFTER_LOCKS) -- no separate/longer lifecycle for the
 * plan capture, on purpose (this is a monitoring aid, not history; a
 * finished query's slot should free up for the next one promptly).
 *
 * Two-phase, keep LW_EXCLUSIVE hold time to O(N) tiny stores:
 *   1. Partition planSlotsOccupied into "expiring here" (matches current
 *      resowner) and "still live" (belongs to an outer scope). Backend-local
 *      linked-list splicing, no lock.
 *   2. Under LW_EXCLUSIVE, per expiring slot: flip magic to FREE, splice
 *      into shmem freelist, bump counter. No memset -- setting magic makes
 *      the slot invisible to readers (they filter on magic == VALID), and
 *      any stale bytes in nodes[] are never observed because the next
 *      populate re-writes every entry up to its own nnodes and readers
 *      only look at [0..nnodes-1]. A 20KB per-recycle memset under the
 *      cluster-wide lock was serialising every query end unnecessarily.
 *   3. Free the backend-local tracking items after releasing the lock.
 */
static void
planTreeRecycleCallback(ResourceReleasePhase phase, bool isCommit, bool isTopLevel, void *arg)
{
	PlanTreeResownerSet *next;
	PlanTreeResownerSet *curr;
	PlanTreeResownerSet *toRecycle = NULL;
	PlanTreeResownerSet *kept = NULL;
	PlanTreeSlot *slot;

	if (NULL == PlanTreeGlobal || NULL == planSlotsOccupied || phase != RESOURCE_RELEASE_AFTER_LOCKS)
		return;

	/* Phase 1: lock-free partition. */
	next = planSlotsOccupied;
	planSlotsOccupied = NULL;
	while (next)
	{
		curr = next;
		next = curr->next;
		if (curr->owner == CurrentResourceOwner)
		{
			curr->next = toRecycle;
			toRecycle = curr;
		}
		else
		{
			curr->next = kept;
			kept = curr;
		}
	}
	planSlotsOccupied = kept;

	if (toRecycle == NULL)
		return;

	/* Phase 2: brief lock hold, per-slot O(2 stores). */
	LWLockAcquire(PLAN_TREE_LOCK, LW_EXCLUSIVE);
	for (curr = toRecycle; curr != NULL; curr = curr->next)
	{
		slot = curr->slot;
		slot->magic = PLAN_TREE_MAGIC_FREE;
		GetPlanTreeNext(slot) = PlanTreeGlobal->head;
		PlanTreeGlobal->head = slot;
		PlanTreeGlobal->free++;
	}
	LWLockRelease(PLAN_TREE_LOCK);

	/* Phase 3: free tracking items outside the lock. */
	while (toRecycle)
	{
		curr = toRecycle;
		toRecycle = curr->next;
		pfree(curr);
	}
}

/*
 * query_info_collect_hook wrapper -- the standard, already-existing GPDB
 * hook this module rides on (utils/metrics_utils.h). Chains to whatever
 * was registered before us, in case another extension also uses it.
 */
static void
plan_tree_query_info_hook(QueryMetricsStatus status, void *args)
{
	if (status == METRICS_PLAN_NODE_INITIALIZE)
		CapturePlanTree((QueryDesc *) args);

	if (prev_query_info_collect_hook)
		(*prev_query_info_collect_hook) (status, args);
}

#define GET_PLAN_TREE_SLOT_BY_INDEX(index) ((PlanTreeSlot*)(PlanTreeGlobal + 1) + (index))

typedef struct PlanTreeDetailCtx
{
	int32		nSlots;			/* # populated slots in the local snapshot */
	int32		slotIndex;
	int32		nodeIndex;
	PlanTreeSlot *slots;		/* backend-local snapshot taken once at
								 * SRF first-call, under LW_SHARED; every
								 * per-call iteration below then touches
								 * only this local array. Two reasons:
								 * (1) SRF calls can be paused arbitrarily
								 * by the consumer, so holding the shmem
								 * lock across them would gate every
								 * writer on a slow SELECT; (2) reading a
								 * slot lock-free while another backend's
								 * recycle memset(PATTERN)s it would tear
								 * -- e.g. relname without a NUL, causing
								 * CStringGetTextDatum's strlen to overrun. */
} PlanTreeDetailCtx;

/*
 * Advance to the next (slot, node) pair in the backend-local snapshot.
 * The snapshot was materialized under LW_SHARED in SRF first-call, so
 * this walks only local memory -- no lock, no shmem access, no torn read.
 */
static bool
next_plan_tree_slot_node(PlanTreeDetailCtx *ctx, PlanTreeSlot **outSlot, PlanTreeNodeEntry **outEntry)
{
	while (ctx->slotIndex < ctx->nSlots)
	{
		PlanTreeSlot *slot = &ctx->slots[ctx->slotIndex];

		if (ctx->nodeIndex < slot->nnodes)
		{
			*outSlot = slot;
			*outEntry = &slot->nodes[ctx->nodeIndex];
			ctx->nodeIndex++;
			return true;
		}
		ctx->slotIndex++;
		ctx->nodeIndex = 0;
	}
	return false;
}

Datum		plan_tree_detail(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(plan_tree_detail);

/*
 * Interface to plan_tree_detail C function. Wrapped from SQL by both
 * whpg_plan_tree.plan_detail_f_on_master() and _on_segments() -- same
 * .so entry point, different EXECUTE ON routing:
 *
 *   CREATE FUNCTION whpg_plan_tree.plan_detail_f_on_master()
 *     RETURNS TABLE ( tmid int4, ssid int4, ccnt int2, segid int2, pid int4
 *                    ,nid int2, parent_nid int2, node_type text
 *                    ,parallel_aware bool
 *                    ,strategy text, partial_mode text, operation text
 *                    ,motion_senders int2, motion_receivers int2
 *                    ,relname text, plan_rows float8
 *                    ,startup_cost float8, total_cost float8, plan_width int4
 *                  )
 *     AS '$libdir/whpg_plan_tree', 'plan_tree_detail'
 *     LANGUAGE C VOLATILE EXECUTE ON COORDINATOR;
 */
Datum
plan_tree_detail(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	PlanTreeDetailCtx *ctx;

#define WHPG_PLAN_TREE_DETAIL_NATTR 19

	if (SRF_IS_FIRSTCALL())
	{
		funcctx = SRF_FIRSTCALL_INIT();

		MemoryContext oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		TupleDesc	tupdesc = CreateTemplateTupleDesc(WHPG_PLAN_TREE_DETAIL_NATTR);

		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "tmid", INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "ssid", INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "ccnt", INT2OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "segid", INT2OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "pid", INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 6, "nid", INT2OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 7, "parent_nid", INT2OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 8, "node_type", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 9, "parallel_aware", BOOLOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 10, "strategy", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 11, "partial_mode", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 12, "operation", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 13, "motion_senders", INT2OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 14, "motion_receivers", INT2OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 15, "relname", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 16, "plan_rows", FLOAT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 17, "startup_cost", FLOAT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 18, "total_cost", FLOAT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 19, "plan_width", INT4OID, -1, 0);

		/* TupleDescFinalize() is this fork's own addition -- confirmed
		 * absent on the two older PG cores this file also targets;
		 * BlessTupleDesc() alone is sufficient on those. */
#if PG_VERSION_NUM >= 190000
		TupleDescFinalize(tupdesc);
#endif
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		ctx = (PlanTreeDetailCtx *) palloc0(sizeof(PlanTreeDetailCtx));
		funcctx->user_fctx = ctx;

		/*
		 * Snapshot every published (magic == VALID) slot under LW_SHARED,
		 * then release the lock. Per-call iteration below reads the local
		 * copy only. Sized by PlanTreeGlobal->free (read under the same
		 * lock) so we never over-palloc, and defensive-bounded by
		 * ctx->nSlots < maxSnap in case a race between free-counter and
		 * per-slot magic happens to have transient slack.
		 */
		if (PlanTreeGlobal != NULL)
		{
			int32		nSlotsTotal;
			int32		maxSnap;

			LWLockAcquire(PLAN_TREE_LOCK, LW_SHARED);

			nSlotsTotal = (int32) PlanTreeNumSlots();
			maxSnap = nSlotsTotal - PlanTreeGlobal->free;

			if (maxSnap > 0)
			{
				int32	i;

				ctx->slots = (PlanTreeSlot *) palloc(maxSnap * sizeof(PlanTreeSlot));

				for (i = 0; i < nSlotsTotal && ctx->nSlots < maxSnap; i++)
				{
					PlanTreeSlot *src = GET_PLAN_TREE_SLOT_BY_INDEX(i);

					if (src->magic != PLAN_TREE_MAGIC_VALID)
						continue;
					memcpy(&ctx->slots[ctx->nSlots++], src, sizeof(PlanTreeSlot));
				}
			}

			LWLockRelease(PLAN_TREE_LOCK);
		}

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	ctx = (PlanTreeDetailCtx *) funcctx->user_fctx;

	PlanTreeSlot *slot;
	PlanTreeNodeEntry *entry;

	if (!next_plan_tree_slot_node(ctx, &slot, &entry))
		SRF_RETURN_DONE(funcctx);

	Datum		values[WHPG_PLAN_TREE_DETAIL_NATTR];
	bool		nulls[WHPG_PLAN_TREE_DETAIL_NATTR];

	memset(nulls, 0, sizeof(nulls));

	values[0] = Int32GetDatum(slot->tmid);
	values[1] = Int32GetDatum(slot->ssid);
	values[2] = Int16GetDatum(slot->ccnt);
	values[3] = Int16GetDatum(slot->segid);
	values[4] = Int32GetDatum(slot->pid);
	values[5] = Int16GetDatum(entry->nid);
	values[6] = Int16GetDatum(entry->parent_nid);
	values[7] = CStringGetTextDatum(
		(NodeTag) entry->node_type == T_Motion
			? MotionTypeName(entry->motion_type)
			: PlanTreeNodeTypeName((NodeTag) entry->node_type));
	values[8] = BoolGetDatum(entry->parallel_aware);

	if (entry->strategy[0])
		values[9] = CStringGetTextDatum(entry->strategy);
	else
		nulls[9] = true;

	if (entry->partial_mode[0])
		values[10] = CStringGetTextDatum(entry->partial_mode);
	else
		nulls[10] = true;

	if (entry->operation[0])
		values[11] = CStringGetTextDatum(entry->operation);
	else
		nulls[11] = true;

	if (entry->motion_senders >= 0)
		values[12] = Int16GetDatum(entry->motion_senders);
	else
		nulls[12] = true;

	if (entry->motion_receivers >= 0)
		values[13] = Int16GetDatum(entry->motion_receivers);
	else
		nulls[13] = true;

	if (entry->relname[0])
		values[14] = CStringGetTextDatum(entry->relname);
	else
		nulls[14] = true;

	values[15] = Float8GetDatum(entry->plan_rows);
	values[16] = Float8GetDatum(entry->startup_cost);
	values[17] = Float8GetDatum(entry->total_cost);
	values[18] = Int32GetDatum(entry->plan_width);

	HeapTuple	tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
	Datum		result = HeapTupleGetDatum(tuple);

	SRF_RETURN_NEXT(funcctx, result);
}

/*
 * Module load callback -- mirrors contrib/pg_stat_statements/
 * pg_stat_statements.c's _PG_init() structure on each of the three eras
 * this file targets.
 */
void
_PG_init(void)
{
	/*
	 * In order to create our shared memory area, we have to be loaded via
	 * shared_preload_libraries. If not, fall out without hooking into
	 * anything -- the plan_detail_f_on_master/_on_segments SQL functions
	 * can still be created (they just return nothing, PlanTreeGlobal
	 * stays NULL).
	 */
	if (!process_shared_preload_libraries_in_progress)
		return;

	DefineCustomIntVariable("whpg_plan_tree.size",
							 "Sets the size of shmem allocated for real-plan-tree capture.",
							 "Only takes effect when gp_enable_query_metrics is on.",
							 &whpg_plan_tree_size,
							 8192, 0, 131072,
							 PGC_POSTMASTER,
							 GUC_UNIT_KB,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable("whpg_plan_tree.capture",
							 "Sets whether this backend's queries get captured.",
							 "Only takes effect when gp_enable_query_metrics is on. "
							 "Change the server-wide default to turn capture off "
							 "everywhere except sessions that explicitly SET this "
							 "on for themselves.",
							 &whpg_plan_tree_capture,
							 true,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	/*
	 * EmitWarningsOnPlaceholders is the pre-PG15 name; every tree this
	 * file targets still accepts it (PG15+ keeps it as a #define alias for
	 * MarkGUCPrefixReserved), so one spelling works everywhere -- no #if
	 * needed here.
	 */
	EmitWarningsOnPlaceholders("whpg_plan_tree");

#if WHPG_PLAN_TREE_NEW_CALLBACKS
	RegisterShmemCallbacks(&PlanTreeCallbacks);
#else
#if PG_VERSION_NUM >= 150000
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = PlanTreeRequest;
#else
	/* PG12: no shmem_request_hook exists at all -- request directly. */
	PlanTreeRequest();
#endif
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = PlanTreeStartup;
#endif

	prev_query_info_collect_hook = query_info_collect_hook;
	query_info_collect_hook = plan_tree_query_info_hook;
}

void
_PG_fini(void)
{
	query_info_collect_hook = prev_query_info_collect_hook;

#if !WHPG_PLAN_TREE_NEW_CALLBACKS
	shmem_startup_hook = prev_shmem_startup_hook;
#if PG_VERSION_NUM >= 150000
	shmem_request_hook = prev_shmem_request_hook;
#endif
#endif

	/*
	 * No public API exists to unregister a RegisterShmemCallbacks entry
	 * or a RegisterResourceReleaseCallback entry; those stay wired for
	 * the process lifetime. Not observable in practice: a
	 * shared_preload_libraries module is never actually unloaded from a
	 * running postmaster -- _PG_fini is a formality here, kept only so
	 * the hooks we *can* restore are restored symmetrically.
	 */
}
