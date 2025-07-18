/* -------------------------------------------------------------------------
 *
 * pgactive_commandfilter.c
 *		prevent execution of utility commands not yet or never supported
 *
 *
 * Copyright (C) 2012-2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		pgactive_commandfilter.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "pgactive.h"
#include "pgactive_locks.h"

#include "fmgr.h"
#include "miscadmin.h"

#include "access/genam.h"
#include "access/heapam.h"

#include "catalog/namespace.h"

#include "commands/dbcommands.h"
#include "commands/event_trigger.h"
#include "commands/extension.h"
#include "commands/defrem.h"
#include "commands/tablecmds.h"

/* For the client auth filter */
#include "libpq/auth.h"

#include "parser/parse_utilcmd.h"

#include "replication/origin.h"

#include "storage/standby.h"

#include "tcop/utility.h"

#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

/*
* pgactive_commandfilter.c: a ProcessUtility_hook to prevent a cluster from running
* commands that pgactive does not yet support.
*/

static ProcessUtility_hook_type next_ProcessUtility_hook = NULL;

static ClientAuthentication_hook_type next_ClientAuthentication_hook = NULL;

/* GUCs */
/* replaced by pgactive_skip_ddl_replication for now
bool           pgactive_permit_unsafe_commands = false; */

#if PG_VERSION_NUM >= 120000
static bool default_with_oids = false;
#endif

static void error_unsupported_command(const char *cmdtag);

static int	pgactive_ddl_nestlevel = 0;
bool		pgactive_in_extension = false;

/*
* Check the passed rangevar, locking it and looking it up in the cache
* then determine if the relation requires logging to WAL. If it does, then
* right now pgactive won't cope with it and we must reject the operation that
* touches this relation.
*/
static void
error_on_persistent_rv(RangeVar *rv,
					   const char *cmdtag,
					   LOCKMODE lockmode,
					   bool missing_ok)
{
	bool		needswal;
	Relation	rel;

	if (rv == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("unqualified command %s is unsafe with pgactive active",
						cmdtag)));

	rel = table_openrv_extended(rv, lockmode, missing_ok);

	if (rel != NULL)
	{
		needswal = RelationNeedsWAL(rel);
		table_close(rel, lockmode);
		if (needswal)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("%s may only affect UNLOGGED or TEMPORARY tables " \
							"when pgactive is active; %s is a regular table",
							cmdtag, rv->relname)));
	}
}

static void
error_unsupported_command(const char *cmdtag)
{
	/*
	 * replace pgactive_permit_unsafe_commands by
	 * pgactive_skip_ddl_replication for now
	 */
	if (pgactive_skip_ddl_replication)
		return;

	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("%s is not supported when pgactive is active",
					cmdtag)));
}

static bool
ispermanent(const char persistence)
{
	/* In case someone adds a new type we don't know about */
	Assert(persistence == RELPERSISTENCE_TEMP
		   || persistence == RELPERSISTENCE_UNLOGGED
		   || persistence == RELPERSISTENCE_PERMANENT);

	return persistence == RELPERSISTENCE_PERMANENT;
}

static void
filter_CreateStmt(Node *parsetree,
				  const char *completionTag)
{
	CreateStmt *stmt;
	ListCell   *cell;
	bool		with_oids = default_with_oids;

	stmt = (CreateStmt *) parsetree;

	/*
	 * replace pgactive_permit_unsafe_commands by
	 * pgactive_skip_ddl_replication for now
	 */
	if (pgactive_skip_ddl_replication)
		return;

	if (stmt->ofTypename != NULL)
		error_unsupported_command("CREATE TABLE ... OF TYPE");

	/* verify WITH options */
	foreach(cell, stmt->options)
	{
		DefElem    *def = (DefElem *) lfirst(cell);

		/* reject WITH OIDS */
		if (def->defnamespace == NULL &&
			pg_strcasecmp(def->defname, "oids") == 0)
		{
			with_oids = defGetBoolean(def);
		}
	}

	if (with_oids)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("tables WITH OIDs are not supported with pgactive")));
	}

	/* verify table elements */
	foreach(cell, stmt->tableElts)
	{
		Node	   *element = lfirst(cell);

		if (nodeTag(element) == T_Constraint)
		{
			Constraint *con = (Constraint *) element;

			if (con->contype == CONSTR_EXCLUSION &&
				ispermanent(stmt->relation->relpersistence))
			{
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("EXCLUDE constraints are unsafe with pgactive active")));
			}
		}
	}
}

static void
filter_AlterTableStmt(Node *parsetree,
					  const char *completionTag,
					  const char *queryString,
					  pgactiveLockType * lock_type)
{
	AlterTableStmt *astmt;
	ListCell   *cell1;
	bool		hasInvalid;
#if PG_VERSION_NUM >= 130000
	AlterTableStmt *stmts = makeNode(AlterTableStmt);
	List	   *beforeStmts;
	List	   *afterStmts;
#else
	ListCell   *cell;
	List	   *stmts;
#endif
	Oid			relid;
	LOCKMODE	lockmode;

	/*
	 * replace pgactive_permit_unsafe_commands by
	 * pgactive_skip_ddl_replication for now
	 */
	if (pgactive_skip_ddl_replication)
		return;

	astmt = (AlterTableStmt *) parsetree;
	hasInvalid = false;

	/*
	 * Can't use AlterTableGetLockLevel(astmt->cmds); Otherwise we deadlock
	 * between the global DDL locks and DML replay. ShareUpdateExclusiveLock
	 * should be enough to block DDL but not DML.
	 */
	lockmode = ShareUpdateExclusiveLock;
	relid = AlterTableLookupRelation(astmt, lockmode);

	/* XXX Do we need to take care of beforeStmts and afterStmts? */
	stmts = transformAlterTableStmtpgactive(relid, astmt, queryString);

#if PG_VERSION_NUM >= 130000
	foreach(cell1, stmts->cmds)
	{
		AlterTableCmd *stmt;
		Node	   *node = (Node *) lfirst(cell1);

		/*
		 * We ignore all nodes which are not AlterTableCmd statements since
		 * the standard utility hook will recurse and thus call our handler
		 * again.
		 */
		if (!IsA(node, AlterTableCmd))
			continue;

		stmt = (AlterTableCmd *) lfirst(cell1);
#else
	foreach(cell, stmts)
	{
		Node	   *node = (Node *) lfirst(cell);
		AlterTableStmt *at_stmt;

		/*
		 * We ignore all nodes which are not AlterTableCmd statements since
		 * the standard utility hook will recurse and thus call our handler
		 * again.
		 */
		if (!IsA(node, AlterTableStmt))
			continue;

		at_stmt = (AlterTableStmt *) node;

		foreach(cell1, at_stmt->cmds)
		{
			AlterTableCmd *stmt = (AlterTableCmd *) lfirst(cell1);
#endif

			switch (stmt->subtype)
			{
					/*
					 * allowed for now:
					 */
				case AT_AddColumn:
					{
						ColumnDef  *def = (ColumnDef *) stmt->def;
						ListCell   *cell;

						/*
						 * Error out if there's a default for the new column,
						 * that requires a table rewrite which might be
						 * nondeterministic.
						 */
						if (def->raw_default != NULL ||
							def->cooked_default != NULL)
						{
							error_on_persistent_rv(
												   astmt->relation,
												   "ALTER TABLE ... ADD COLUMN ... DEFAULT",
												   lockmode,
												   astmt->missing_ok);
						}

						/*
						 * Column defaults can also be represented as
						 * constraints.
						 */
						foreach(cell, def->constraints)
						{
							Constraint *con;

							Assert(IsA(lfirst(cell), Constraint));
							con = (Constraint *) lfirst(cell);

							if (con->contype == CONSTR_DEFAULT)
								error_on_persistent_rv(
													   astmt->relation,
													   "ALTER TABLE ... ADD COLUMN ... DEFAULT",
													   lockmode,
													   astmt->missing_ok);
						}
					}
					/* FALLTHROUGH */
				case AT_AddIndex:	/* produced by for example ALTER TABLE …
									 * ADD CONSTRAINT … PRIMARY KEY */
					{
						/*
						 * Any ADD CONSTRAINT that creates an index is
						 * tranformed into an AT_AddIndex by
						 * transformAlterTableStmt in parse_utilcmd.c, before
						 * we see it. We can't look at the AT_AddConstraint
						 * because there isn't one anymore.
						 */
						IndexStmt  *index = (IndexStmt *) stmt->def;

						*lock_type = pgactive_LOCK_DDL;

						if (index->excludeOpNames != NIL)
						{
							error_on_persistent_rv(astmt->relation,
												   "ALTER TABLE ... ADD CONSTRAINT ... EXCLUDE",
												   lockmode,
												   astmt->missing_ok);
						}

					}
					/* FALLTHROUGH */
				case AT_DropColumn:
				case AT_DropNotNull:
				case AT_SetNotNull:
				case AT_ColumnDefault:	/* ALTER COLUMN DEFAULT */

				case AT_ClusterOn:	/* CLUSTER ON */
				case AT_DropCluster:	/* SET WITHOUT CLUSTER */
				case AT_ChangeOwner:
				case AT_SetStorage:
					*lock_type = pgactive_LOCK_DDL;
					break;

				case AT_SetRelOptions:	/* SET (...) */
				case AT_ResetRelOptions:	/* RESET (...) */
				case AT_ReplaceRelOptions:	/* replace reloption list */
				case AT_ReplicaIdentity:
					break;

				case AT_DropConstraint:
					break;

				case AT_SetTableSpace:
					break;

				case AT_AddConstraint:
#if PG_VERSION_NUM < 130000
				case AT_ProcessedConstraint:
#endif
					if (IsA(stmt->def, Constraint))
					{
						Constraint *con = (Constraint *) stmt->def;

						/*
						 * This won't be hit on current Pg; see the handling
						 * of AT_AddIndex above. But we check for it anyway to
						 * defend against future change.
						 */
						if (con->contype == CONSTR_EXCLUSION)
							error_on_persistent_rv(astmt->relation,
												   "ALTER TABLE ... ADD CONSTRAINT ... EXCLUDE",
												   lockmode,
												   astmt->missing_ok);
					}
					break;

				case AT_ValidateConstraint: /* VALIDATE CONSTRAINT */
					*lock_type = pgactive_LOCK_DDL;
					break;

				case AT_AlterConstraint:
					error_on_persistent_rv(astmt->relation,
										   "ALTER TABLE ... ALTER CONSTRAINT",
										   lockmode,
										   astmt->missing_ok);
					break;

				case AT_AddIndexConstraint:
					error_on_persistent_rv(astmt->relation,
										   "ALTER TABLE ... ADD CONSTRAINT USING INDEX",
										   lockmode,
										   astmt->missing_ok);
					break;

				case AT_AlterColumnType:
					error_on_persistent_rv(astmt->relation,
										   "ALTER TABLE ... ALTER COLUMN TYPE",
										   lockmode,
										   astmt->missing_ok);
					break;

				case AT_AlterColumnGenericOptions:
					error_on_persistent_rv(astmt->relation,
										   "ALTER TABLE ... ALTER COLUMN OPTIONS",
										   lockmode,
										   astmt->missing_ok);
					break;

#if PG_VERSION_NUM < 120000
				case AT_AddOids:
#endif
				case AT_DropOids:
					error_on_persistent_rv(astmt->relation,
										   "ALTER TABLE ... SET WITH[OUT] OIDS",
										   lockmode,
										   astmt->missing_ok);
					break;

				case AT_EnableTrig:
				case AT_DisableTrig:
				case AT_EnableTrigUser:
				case AT_DisableTrigUser:

					/*
					 * It's safe to ALTER TABLE ... ENABLE|DISABLE TRIGGER
					 * without blocking concurrent writes.
					 */
					*lock_type = pgactive_LOCK_DDL;
					break;

				case AT_EnableAlwaysTrig:
				case AT_EnableReplicaTrig:
				case AT_EnableTrigAll:
				case AT_DisableTrigAll:

					/*
					 * Since we might fire replica triggers later and that
					 * could affect replication, continue to take a write-lock
					 * for them.
					 */
					break;

				case AT_EnableRule:
				case AT_EnableAlwaysRule:
				case AT_EnableReplicaRule:
				case AT_DisableRule:
					error_on_persistent_rv(astmt->relation,
										   "ALTER TABLE ... ENABLE|DISABLE [ALWAYS|REPLICA] RULE",
										   lockmode,
										   astmt->missing_ok);
					break;

				case AT_AddInherit:
				case AT_DropInherit:
					error_on_persistent_rv(astmt->relation,
										   "ALTER TABLE ... [NO] INHERIT",
										   lockmode,
										   astmt->missing_ok);
					break;

				case AT_AddOf:
				case AT_DropOf:
					error_on_persistent_rv(astmt->relation,
										   "ALTER TABLE ... [NOT] OF",
										   lockmode,
										   astmt->missing_ok);
					break;

				case AT_SetStatistics:
					break;
				case AT_SetOptions:
				case AT_ResetOptions:
					error_on_persistent_rv(astmt->relation,
										   "ALTER TABLE ... ALTER COLUMN ... SET STATISTICS|(...)",
										   lockmode,
										   astmt->missing_ok);
					break;

				case AT_GenericOptions:
					error_on_persistent_rv(astmt->relation,
										   "ALTER TABLE ... SET (...)",
										   lockmode,
										   astmt->missing_ok);
					break;

				default:
					hasInvalid = true;
					break;
			}
		}
#if PG_VERSION_NUM < 130000
	}
#endif

	if (hasInvalid)
		error_on_persistent_rv(astmt->relation,
							   "This variant of ALTER TABLE",
							   lockmode,
							   astmt->missing_ok);
}

static void
filter_CreateTableAs(Node *parsetree)
{
	CreateTableAsStmt *stmt;

	stmt = (CreateTableAsStmt *) parsetree;

	/*
	 * replace pgactive_permit_unsafe_commands by
	 * pgactive_skip_ddl_replication for now
	 */
	if (pgactive_skip_ddl_replication)
		return;

	if (ispermanent(stmt->into->rel->relpersistence))
		error_unsupported_command(GetCommandTagName(CreateCommandTag(parsetree)));
}

static bool
statement_affects_only_nonpermanent(Node *parsetree)
{
	switch (nodeTag(parsetree))
	{
		case T_CreateTableAsStmt:
			{
				CreateTableAsStmt *stmt = (CreateTableAsStmt *) parsetree;

				return !ispermanent(stmt->into->rel->relpersistence);
			}
		case T_CreateStmt:
			{
				CreateStmt *stmt = (CreateStmt *) parsetree;

				return !ispermanent(stmt->relation->relpersistence);
			}
		case T_DropStmt:
			{
				DropStmt   *stmt = (DropStmt *) parsetree;
				ListCell   *cell;

				/*
				 * It doesn't make any sense to drop temporary tables
				 * concurrently.
				 */
				if (stmt->concurrent)
					return false;

				/* Figure out if only temporary objects are affected. */

				/*
				 * Only do this for temporary relations and indexes, not other
				 * objects for now.
				 */
				switch (stmt->removeType)
				{
					case OBJECT_INDEX:
					case OBJECT_TABLE:
					case OBJECT_SEQUENCE:
					case OBJECT_VIEW:
					case OBJECT_MATVIEW:
					case OBJECT_FOREIGN_TABLE:
						break;
					default:
						return false;

				}

				/* Now check each dropped relation. */
				foreach(cell, stmt->objects)
				{
					Oid			relOid;
					RangeVar   *rv = makeRangeVarFromNameList((List *) lfirst(cell));
					Relation	rel;
					bool		istemp;

					relOid = RangeVarGetRelid(rv,
											  AccessExclusiveLock,
											  stmt->missing_ok);
					if (relOid == InvalidOid)
						continue;

					/*
					 * If a schema name is not provided, check to see if the
					 * session's temporary namespace is first in the
					 * search_path and if a relation with the same Oid is in
					 * the current session's "pg_temp" schema. If so, we can
					 * safely assume that the DROP statement will refer to
					 * this object, since the pg_temp schema is
					 * session-private.
					 */
					if (rv->schemaname == NULL)
					{
						Oid			tempNamespaceOid,
									tempRelOid;
						List	   *searchPath;
						bool		foundtemprel;

						foundtemprel = false;
						tempNamespaceOid = LookupExplicitNamespace("pg_temp", true);
						if (tempNamespaceOid == InvalidOid)
							return false;
						searchPath = fetch_search_path(true);
						if (searchPath != NULL)
						{
							ListCell   *i;

							foreach(i, searchPath)
							{
								if (lfirst_oid(i) != tempNamespaceOid)
									break;
								tempRelOid = get_relname_relid(rv->relname, tempNamespaceOid);
								if (tempRelOid != relOid)
									break;
								foundtemprel = true;
								break;
							}
							list_free(searchPath);
						}
						if (!foundtemprel)
							return false;
					}

					if (stmt->removeType != OBJECT_INDEX)
					{
						rel = relation_open(relOid, AccessExclusiveLock);
						istemp = !ispermanent(rel->rd_rel->relpersistence);
						relation_close(rel, NoLock);
					}
					else
					{
						rel = index_open(relOid, AccessExclusiveLock);
						istemp = !ispermanent(rel->rd_rel->relpersistence);
						index_close(rel, NoLock);
					}

					if (!istemp)
						return false;
				}
				return true;
				break;
			}
		case T_IndexStmt:
			{
				IndexStmt  *stmt = (IndexStmt *) parsetree;

				return !ispermanent(stmt->relation->relpersistence);
			}
			/* FIXME: Add more types of statements */
		default:
			break;
	}
	return false;
}

static bool
allowed_on_read_only_node(Node *parsetree, CommandTag *tag)
{
	/*
	 * This list is copied verbatim from check_xact_readonly we only do
	 * different action on it.
	 *
	 * Note that check_xact_readonly handles COPY elsewhere. We capture it
	 * here so don't delete it from this list if you update it. Make sure to
	 * check other callsites of PreventCommandIfReadOnly too.
	 *
	 * pgactive handles plannable statements in pgactiveExecutorStart, not
	 * here.
	 */
	switch (nodeTag(parsetree))
	{
		case T_AlterDatabaseStmt:
		case T_AlterDatabaseSetStmt:
		case T_AlterDomainStmt:
		case T_AlterFunctionStmt:
		case T_AlterRoleStmt:
		case T_AlterRoleSetStmt:
		case T_AlterObjectSchemaStmt:
		case T_AlterOwnerStmt:
		case T_AlterSeqStmt:
		case T_AlterTableMoveAllStmt:
		case T_AlterTableStmt:
		case T_RenameStmt:
		case T_CommentStmt:
		case T_DefineStmt:
		case T_CreateCastStmt:
		case T_CreateEventTrigStmt:
		case T_AlterEventTrigStmt:
		case T_CreateConversionStmt:
		case T_CreatedbStmt:
		case T_CreateDomainStmt:
		case T_CreateFunctionStmt:
		case T_CreateRoleStmt:
		case T_IndexStmt:
		case T_CreatePLangStmt:
		case T_CreateOpClassStmt:
		case T_CreateOpFamilyStmt:
		case T_AlterOpFamilyStmt:
		case T_RuleStmt:
		case T_CreateSchemaStmt:
		case T_CreateSeqStmt:
		case T_CreateStmt:
		case T_CreateTableAsStmt:
		case T_RefreshMatViewStmt:
		case T_CreateTableSpaceStmt:
		case T_CreateTrigStmt:
		case T_CompositeTypeStmt:
		case T_CreateEnumStmt:
		case T_CreateRangeStmt:
		case T_AlterEnumStmt:
		case T_ViewStmt:
		case T_DropStmt:
		case T_DropdbStmt:
		case T_DropTableSpaceStmt:
		case T_DropRoleStmt:
		case T_GrantStmt:
		case T_GrantRoleStmt:
		case T_AlterDefaultPrivilegesStmt:
		case T_TruncateStmt:
		case T_DropOwnedStmt:
		case T_ReassignOwnedStmt:
		case T_AlterTSDictionaryStmt:
		case T_AlterTSConfigurationStmt:
		case T_CreateExtensionStmt:
		case T_AlterExtensionStmt:
		case T_AlterExtensionContentsStmt:
		case T_CreateFdwStmt:
		case T_AlterFdwStmt:
		case T_CreateForeignServerStmt:
		case T_AlterForeignServerStmt:
		case T_CreateUserMappingStmt:
		case T_AlterUserMappingStmt:
		case T_DropUserMappingStmt:
		case T_AlterTableSpaceOptionsStmt:
		case T_CreateForeignTableStmt:
		case T_SecLabelStmt:
			{
				*tag = CreateCommandTag(parsetree);
				return statement_affects_only_nonpermanent(parsetree);
			}
			/* Pg checks this in DoCopy not check_xact_readonly */
		case T_CopyStmt:
			{
#if PG_VERSION_NUM < 130000
				*tag = "COPY FROM";
#else
				*tag = CMDTAG_COPY_FROM;
#endif
				return !((CopyStmt *) parsetree)->is_from || statement_affects_only_nonpermanent(parsetree);
			}
		default:
			/* do nothing */
			break;
	}

	return true;
}

static void
pgactive_commandfilter_dbname(const char *dbname)
{
	/*
	 * replace pgactive_permit_unsafe_commands by
	 * pgactive_skip_ddl_replication for now
	 */
	if (pgactive_skip_ddl_replication)
		return;

	if (strcmp(dbname, pgactive_SUPERVISOR_DBNAME) == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_RESERVED_NAME),
				 errmsg("pgactive extension reserves the database name "
						pgactive_SUPERVISOR_DBNAME " for its own use"),
				 errhint("Use a different database name.")));
	}
}

static void
prevent_drop_extension_pgactive(DropStmt *stmt)
{
	ListCell   *cell;
	Oid			pgactive_oid;

	/*
	 * replace pgactive_permit_unsafe_commands by
	 * pgactive_skip_ddl_replication for now
	 *
	 */

	/* Only interested in DROP EXTENSION */
	if (stmt->removeType != OBJECT_EXTENSION)
		return;

	pgactive_oid = get_extension_oid("pgactive", false);

	/* Check to see if the pgactive extension is being dropped */
	foreach(cell, stmt->objects)
	{
#if PG_VERSION_NUM < 150000
		Value	   *objname = lfirst(cell);
#else
		String	   *objname = lfirst_node(String, cell);
#endif
		Oid			ext_oid;

		ext_oid = get_extension_oid(strVal(objname), false);

		if (pgactive_oid == ext_oid)
			ereport(ERROR,
					(errmsg("dropping the pgactive extension is prohibited while pgactive is active"),
					 errhint("Detach this node with pgactive.detach_by_node_names(...) first, or if appropriate use pgactive.pgactive_remove(...).")));
	}
}

/*
 * We disallow creating an external logical replication extension when pgactive is
 * active. Technically, pgactive has nothing to do with such external logical
 * replication extensions, however, we disallow them for now to not have any
 * possible data divergence issues and conflicts on nodes within the pgactive group.
 * For instance, when a pgactive node pulls in changes from a non-pgactive node using
 * any of external logical replication extensions, then, the node can easily
 * diverge from the other nodes in pgactive group, and may cause conflicts.
 *
 * XXX: We might have to leave all of these to the user and allow such
 * extensions at some point.
 */
static void
prevent_disallowed_extension_creation(CreateExtensionStmt *stmt)
{

	if (pg_strncasecmp(stmt->extname, "pglogical", 9) == 0)
		ereport(ERROR,
				(errmsg("cannot create an external logical replication extension when pgactive is active")));
}

#if PG_VERSION_NUM >= 140000
static void
pgactive_commandfilter(PlannedStmt *pstmt,
					   const char *queryString,
					   bool readOnlyTree,
					   ProcessUtilityContext context,
					   ParamListInfo params,
					   QueryEnvironment *queryEnv,
					   DestReceiver *dest,
					   QueryCompletion *qc)
#elif PG_VERSION_NUM >= 130000
static void
pgactive_commandfilter(PlannedStmt *pstmt,
					   const char *queryString,
					   ProcessUtilityContext context,
					   ParamListInfo params,
					   QueryEnvironment *queryEnv,
					   DestReceiver *dest,
					   QueryCompletion *qc)
#else
static void
pgactive_commandfilter(PlannedStmt *pstmt,
					   const char *queryString,
					   ProcessUtilityContext context,
					   ParamListInfo params,
					   QueryEnvironment *queryEnv,
					   DestReceiver *dest,
					   char *completionTag)
#endif
{
	Node	   *parsetree = pstmt->utilityStmt;
	bool		incremented_nestlevel = false;
	bool		affects_only_nonpermanent;
	bool		entered_extension = false;
	bool		altering_pgactive_nid_func = false;

	/* take strongest lock by default. */
	pgactiveLockType lock_type = pgactive_LOCK_WRITE;

	/*
	 * Only pgactive can create/drop/alter pgactive node identifier getter
	 * function on local node i.e. no replication to other pgactive members.
	 */
	switch (nodeTag(parsetree))
	{
		case T_CreateFunctionStmt:	/* CREATE FUNCTION */
			if (is_pgactive_nid_getter_function_create((CreateFunctionStmt *) parsetree))
			{
				if (is_pgactive_creating_nid_getter_function() ||
					pgactive_permit_node_identifier_getter_function_creation)
					goto done;
				else
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("creation of pgactive node identifier getter function is not allowed")));
			}
			break;
		case T_DropStmt:		/* DROP FUNCTION */
			if (is_pgactive_nid_getter_function_drop((DropStmt *) parsetree))
			{
				if (is_pgactive_creating_nid_getter_function() ||
					pgactive_permit_node_identifier_getter_function_creation)
					goto done;
				else
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("dropping of pgactive node identifier getter function is not allowed")));
			}
			/* prevent DROP EXTENSION pgactive; */
			if (pgactive_is_pgactive_activated_db(MyDatabaseId))
				prevent_drop_extension_pgactive((DropStmt *) parsetree);
			break;
		case T_AlterFunctionStmt:	/* ALTER FUNCTION */
			altering_pgactive_nid_func =
				is_pgactive_nid_getter_function_alter((AlterFunctionStmt *) parsetree);
			break;
		case T_AlterOwnerStmt:	/* ALTER FUNCTION OWNER TO */
			altering_pgactive_nid_func =
				is_pgactive_nid_getter_function_alter_owner((AlterOwnerStmt *) parsetree);
			break;
		case T_RenameStmt:		/* ALTER FUNCTION RENAME TO */
			altering_pgactive_nid_func =
				is_pgactive_nid_getter_function_alter_rename((RenameStmt *) parsetree);
			break;
		default:
			break;
	}

	if (altering_pgactive_nid_func)
	{
		if (is_pgactive_creating_nid_getter_function() ||
			pgactive_permit_node_identifier_getter_function_creation)
			goto done;
		else
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("altering of pgactive node identifier getter function is not allowed")));
	}

	/*
	 * If DDL replication is disabled, let's call the next process utility
	 * hook (if any) or the standard one. The reason is that there is no
	 * reason to filter anything in such a case.
	 */
	if (pgactive_skip_ddl_replication)
	{
		if (next_ProcessUtility_hook)
			next_ProcessUtility_hook(pstmt, queryString,
#if PG_VERSION_NUM >= 140000
									 readOnlyTree,
									 context, params, queryEnv,
									 dest, qc);
		else
			standard_ProcessUtility(pstmt, queryString,
									readOnlyTree,
									context, params, queryEnv,
									dest, qc);
#elif PG_VERSION_NUM >= 130000
									 context, params, queryEnv,
									 dest, qc);
		else
			standard_ProcessUtility(pstmt, queryString,
									context, params, queryEnv,
									dest, qc);
#else
									 context, params, queryEnv,
									 dest, completionTag);
		else
			standard_ProcessUtility(pstmt, queryString,
									context, params, queryEnv,
									dest, completionTag);
#endif

		return;
	}

	elog(DEBUG2, "processing %s: %s in statement %s",
		 context == PROCESS_UTILITY_TOPLEVEL ? "toplevel" : "query",
		 GetCommandTagName(CreateCommandTag(parsetree)), queryString);

	/* don't filter in single user mode */
	if (!IsUnderPostmaster)
		goto done;

	/* Permit only VACUUM on the supervisordb, if it exists */
	if (pgactiveSupervisorDbOid == InvalidOid)
		pgactiveSupervisorDbOid = pgactive_get_supervisordb_oid(true);

	if (pgactiveSupervisorDbOid != InvalidOid
		&& MyDatabaseId == pgactiveSupervisorDbOid
		&& nodeTag(parsetree) != T_VacuumStmt)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("no commands may be run on the pgactive supervisor database")));
	}

	/*
	 * Extension contents aren't individually replicated. While postgres sets
	 * creating_extension for create/alter extension, it doesn't set it for
	 * drop extension. To ensure we don't replicate anything for drop
	 * extension, we use pgactive_in_extension that was set when pgactive
	 * first sees drop extension.
	 */
	if (creating_extension || pgactive_in_extension)
		goto done;

	/* don't perform filtering while replaying */
	if (replorigin_session_origin != InvalidRepOriginId)
		goto done;

	/*
	 * Skip transaction control commands first as the following function calls
	 * might require transaction access.
	 */
	if (nodeTag(parsetree) == T_TransactionStmt)
	{
		TransactionStmt *stmt = (TransactionStmt *) parsetree;

		if (in_pgactive_replicate_ddl_command &&
			(stmt->kind == TRANS_STMT_COMMIT ||
			 stmt->kind == TRANS_STMT_ROLLBACK ||
			 stmt->kind == TRANS_STMT_PREPARE))
		{
			/*
			 * It's unsafe to let pgactive_replicate_ddl_command run
			 * transaction control commands via SPI that might end the current
			 * xact, since it's being called from the fmgr/executor who'll
			 * expect a valid transaction context on return.
			 */
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot COMMIT, ROLLBACK or PREPARE TRANSACTION in pgactive_replicate_ddl_command")));
		}
		goto done;
	}

	/* don't filter if this database isn't using pgactive */
	if (!pgactive_is_pgactive_activated_db(MyDatabaseId))
		goto done;

	/* check for read-only mode */
	{
		CommandTag	tag;

		if (pgactive_local_node_read_only()

		/*
		 * replace pgactive_permit_unsafe_commands by
		 * pgactive_skip_ddl_replication for now
		 */
			&& !pgactive_skip_ddl_replication
			&& !allowed_on_read_only_node(parsetree, &tag))
			ereport(ERROR,
					(errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),
					 errmsg("cannot run %s on read-only pgactive node", GetCommandTagName(tag))));
	}

	/* commands we skip (for now) */
	switch (nodeTag(parsetree))
	{
			/* These are purely local and don't need replication */
		case T_PlannedStmt:
		case T_ClosePortalStmt:
		case T_FetchStmt:
		case T_PrepareStmt:
		case T_DeallocateStmt:
		case T_NotifyStmt:
		case T_ListenStmt:
		case T_UnlistenStmt:
		case T_LoadStmt:
		case T_ExplainStmt:
		case T_VariableSetStmt:
		case T_VariableShowStmt:
		case T_DiscardStmt:
		case T_LockStmt:
		case T_ConstraintsSetStmt:
		case T_CheckPointStmt:
		case T_ReindexStmt:
		case T_VacuumStmt:
		case T_ClusterStmt:
			goto done;

			/*
			 * We'll replicate the results of a DO block, not the block its
			 * self
			 */
		case T_DoStmt:
			goto done;

			/*
			 * Tablespaces can differ over nodes and aren't replicated.
			 * They're global objects anyway.
			 */
		case T_CreateTableSpaceStmt:
		case T_DropTableSpaceStmt:
		case T_AlterTableSpaceOptionsStmt:
			goto done;

			/*
			 * We treat properties of the database its self as node-specific
			 * and don't try to replicate GUCs set on the database, etc.
			 */
		case T_AlterDatabaseStmt:
		case T_AlterDatabaseSetStmt:
		case T_CreateEventTrigStmt:
		case T_AlterEventTrigStmt:
			goto done;

			/* Handled by truncate triggers elsewhere */
		case T_TruncateStmt:
			goto done;

			/* We replicate the rows changed, not the statements, for these */
		case T_ExecuteStmt:
			goto done;

			/*
			 * for COPY we'll replicate the rows changed and don't care about
			 * the statement. It cannot UPDATE or DELETE so we don't need a PK
			 * check. We already checked read-only mode.
			 */
		case T_CopyStmt:
			goto done;

			/*
			 * These affect global objects, which we don't replicate changes
			 * to.
			 *
			 * The ProcessUtility_hook runs on all DBs, but we have no way to
			 * enqueue such statements onto the DDL command queue. We'd also
			 * have to make sure they replicated only once if there was more
			 * than one local node.
			 *
			 * This may be possible using generic logical WAL messages,
			 * writing a message from one DB that's replayed on another DB,
			 * but only if a new variant of LogLogicalMessage is added to
			 * allow the target db oid to be specified.
			 */
		case T_GrantRoleStmt:
		case T_AlterSystemStmt:
		case T_CreateRoleStmt:
		case T_AlterRoleStmt:
		case T_AlterRoleSetStmt:
		case T_DropRoleStmt:
			goto done;

		case T_DeclareCursorStmt:
			goto done;
		default:
			break;
	}

	/*
	 * We stop people from creating a DB named pgactive_SUPERVISOR_DBNAME if
	 * the pgactive extension is installed because we reserve that name, even
	 * if pgactive isn't actually active.
	 *
	 */
	switch (nodeTag(parsetree))
	{
		case T_CreatedbStmt:
			pgactive_commandfilter_dbname(((CreatedbStmt *) parsetree)->dbname);
			goto done;
		case T_DropdbStmt:
			pgactive_commandfilter_dbname(((DropdbStmt *) parsetree)->dbname);
			goto done;
		case T_RenameStmt:

			/*
			 * ALTER DATABASE ... RENAME TO ... is actually a RenameStmt not
			 * an AlterDatabaseStmt. It's handled here for the database target
			 * only then falls through for the other rename object type.
			 */
			if (((RenameStmt *) parsetree)->renameType == OBJECT_DATABASE)
			{
				pgactive_commandfilter_dbname(((RenameStmt *) parsetree)->subname);
				pgactive_commandfilter_dbname(((RenameStmt *) parsetree)->newname);
				goto done;
			}

		default:
			break;
	}

	/* statements handled directly in standard_ProcessUtility */
	switch (nodeTag(parsetree))
	{
		case T_DropStmt:
			prevent_drop_extension_pgactive((DropStmt *) parsetree);
			break;
		case T_AlterOwnerStmt:
			lock_type = pgactive_LOCK_DDL;
			break;
		default:
			break;
	}

	/* all commands handled by ProcessUtilitySlow() */
	switch (nodeTag(parsetree))
	{
		case T_CreateSchemaStmt:
			lock_type = pgactive_LOCK_DDL;
			break;

		case T_CreateStmt:
#if PG_VERSION_NUM >= 130000
			filter_CreateStmt(parsetree, GetCommandTagName(CreateCommandTag(parsetree)));
#else
			filter_CreateStmt(parsetree, completionTag);
#endif
			lock_type = pgactive_LOCK_DDL;
			break;

		case T_CreateForeignTableStmt:
			lock_type = pgactive_LOCK_DDL;
			break;

		case T_AlterTableStmt:
#if PG_VERSION_NUM >= 130000
			filter_AlterTableStmt(parsetree, GetCommandTagName(CreateCommandTag(parsetree)), queryString, &lock_type);
#else
			filter_AlterTableStmt(parsetree, completionTag, queryString, &lock_type);
#endif
			break;

		case T_AlterDomainStmt:
			/* XXX: we could support this */
			error_unsupported_command(GetCommandTagName(CreateCommandTag(parsetree)));
			break;

		case T_DefineStmt:
			{
				DefineStmt *stmt = (DefineStmt *) parsetree;

				switch (stmt->kind)
				{
					case OBJECT_AGGREGATE:
					case OBJECT_OPERATOR:
					case OBJECT_TYPE:
						break;

					default:
						error_unsupported_command(GetCommandTagName(CreateCommandTag(parsetree)));
						break;
				}

				lock_type = pgactive_LOCK_DDL;
				break;
			}

		case T_IndexStmt:
			{
				IndexStmt  *stmt;

				stmt = (IndexStmt *) parsetree;

				/*
				 * Only allow CONCURRENTLY when not wrapped in
				 * pgactive.replicate_ddl_command; see
				 * 2ndQuadrant/pgactive-private#124 for details and linked
				 * issues.
				 *
				 * We can permit it but not replicate it otherwise. To ensure
				 * that users aren't confused, only permit it when
				 * pgactive.skip_ddl_replication is set.
				 */

				/*
				 * replace pgactive_permit_unsafe_commands by
				 * pgactive_skip_ddl_replication for now
				 */
				if (stmt->concurrent && !pgactive_skip_ddl_replication)
				{
					if (in_pgactive_replicate_ddl_command)
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("CREATE INDEX CONCURRENTLY is not supported in pgactive.replicate_ddl_command"),
								 errhint("Run CREATE INDEX CONCURRENTLY on each node individually with pgactive.skip_ddl_replication set.")));

					if (!pgactive_skip_ddl_replication)
						error_on_persistent_rv(stmt->relation,
											   "CREATE INDEX CONCURRENTLY without pgactive.skip_ddl_replication set",
											   AccessExclusiveLock, false);
				}

				/*
				 * replace pgactive_permit_unsafe_commands by
				 * pgactive_skip_ddl_replication for now
				 */
				if (stmt->whereClause && stmt->unique && !pgactive_skip_ddl_replication)
					error_on_persistent_rv(stmt->relation,
										   "CREATE UNIQUE INDEX ... WHERE",
										   AccessExclusiveLock, false);

				/*
				 * Non-unique concurrently built indexes can be done in
				 * parallel with writing.
				 */
				if (!stmt->unique && stmt->concurrent)
					lock_type = pgactive_LOCK_DDL;

				break;
			}
		case T_CreateExtensionStmt:
			prevent_disallowed_extension_creation((CreateExtensionStmt *) parsetree);
			break;

		case T_AlterExtensionStmt:
			/* XXX: we could support some of these */
			error_unsupported_command(GetCommandTagName(CreateCommandTag(parsetree)));
			break;

		case T_AlterExtensionContentsStmt:
			error_unsupported_command(GetCommandTagName(CreateCommandTag(parsetree)));
			break;

			/*
			 * We disallow a pgactive node from being a subscriber in postgres
			 * logical replication when pgactive is active. Technically,
			 * pgactive has nothing to do with postgres logical replication,
			 * however, we disallow subscriptions on a pgactive node for now
			 * to not have any possible data divergence issues and conflicts
			 * on nodes within the pgactive group. For instance, when a
			 * pgactive node pulls in changes from a non-pgactive publisher
			 * using postgres logical replication, then, the node can easily
			 * diverge from the other nodes in pgactive group, and may cause
			 * conflicts.
			 *
			 * However, we have no problem if a pgactive node is a publisher
			 * in postgres logical replication. Meaning, a non-pgactive node
			 * can still pull in changes from a pgactive node.
			 *
			 * XXX: We might have to leave all of these to the user and allow
			 * subscriptions on pgactive nodes.
			 */
		case T_CreateSubscriptionStmt:
		case T_AlterSubscriptionStmt:
		case T_DropSubscriptionStmt:
			error_unsupported_command(GetCommandTagName(CreateCommandTag(parsetree)));
			break;

		case T_CreatePublicationStmt:
		case T_AlterPublicationStmt:
			lock_type = pgactive_LOCK_DDL;
			break;

		case T_CreateFdwStmt:
		case T_AlterFdwStmt:
			/* XXX: we should probably support all of these at some point */
			error_unsupported_command(GetCommandTagName(CreateCommandTag(parsetree)));
			break;

			/*
			 * Execute the following only on local node i.e. no replication to
			 * other pgactive members.
			 */
		case T_CreateForeignServerStmt:
		case T_AlterForeignServerStmt:
		case T_CreateUserMappingStmt:
		case T_AlterUserMappingStmt:
		case T_DropUserMappingStmt:
			goto done;

		case T_CompositeTypeStmt:	/* CREATE TYPE (composite) */
		case T_CreateEnumStmt:	/* CREATE TYPE AS ENUM */
		case T_CreateRangeStmt: /* CREATE TYPE AS RANGE */
			lock_type = pgactive_LOCK_DDL;
			break;

		case T_ViewStmt:		/* CREATE VIEW */
		case T_CreateFunctionStmt:	/* CREATE FUNCTION */
			lock_type = pgactive_LOCK_DDL;
			break;

		case T_AlterEnumStmt:
		case T_AlterFunctionStmt:	/* ALTER FUNCTION */
		case T_RuleStmt:		/* CREATE RULE */
			break;

		case T_CreateSeqStmt:
			break;

		case T_AlterSeqStmt:
			break;

		case T_CreateTableAsStmt:
			filter_CreateTableAs(parsetree);
			break;

		case T_RefreshMatViewStmt:
			/* XXX: might make sense to support or not */
			error_unsupported_command(GetCommandTagName(CreateCommandTag(parsetree)));
			break;

		case T_CreateTrigStmt:
			break;

		case T_CreatePLangStmt:
			error_unsupported_command(GetCommandTagName(CreateCommandTag(parsetree)));
			break;

		case T_CreateDomainStmt:
			lock_type = pgactive_LOCK_DDL;
			break;

		case T_CreateConversionStmt:
			error_unsupported_command(GetCommandTagName(CreateCommandTag(parsetree)));
			break;

		case T_CreateCastStmt:
		case T_CreateOpClassStmt:
		case T_CreateOpFamilyStmt:
		case T_AlterOpFamilyStmt:
			error_unsupported_command(GetCommandTagName(CreateCommandTag(parsetree)));
			break;

		case T_AlterTSDictionaryStmt:
		case T_AlterTSConfigurationStmt:
			error_unsupported_command(GetCommandTagName(CreateCommandTag(parsetree)));
			break;

		case T_DropStmt:

			/*
			 * DROP INDEX CONCURRENTLY is currently only safe when run outside
			 * pgactive.replicate_ddl_command, and only with
			 * pgactive.skip_ddl_replication set. See
			 * 2ndQuadrant/pgactive-private#124 and linked issues.
			 */
			{
				DropStmt   *stmt = (DropStmt *) parsetree;

				/*
				 * replace pgactive_permit_unsafe_commands by
				 * pgactive_skip_ddl_replication for now
				 */
				if (stmt->removeType == OBJECT_INDEX && stmt->concurrent && !pgactive_skip_ddl_replication)
				{
					if (in_pgactive_replicate_ddl_command)
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("DROP INDEX CONCURRENTLY is not supported in pgactive.replicate_ddl_command"),
								 errhint("Run DROP INDEX CONCURRENTLY on each node individually with pgactive.skip_ddl_replication set.")));

					if (!pgactive_skip_ddl_replication && !statement_affects_only_nonpermanent(parsetree))
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("DROP INDEX CONCURRENTLY is not supported without pgactive.skip_ddl_replication set")));
				}

				/*
				 * Execute the DROP SERVER only on local node i.e. no
				 * replication to other pgactive members.
				 */
				if (stmt->removeType == OBJECT_FOREIGN_SERVER)
					goto done;
			}
			break;

		case T_RenameStmt:
			{
				RenameStmt *n = (RenameStmt *) parsetree;

				switch (n->renameType)
				{
					case OBJECT_AGGREGATE:
					case OBJECT_COLLATION:
					case OBJECT_CONVERSION:
					case OBJECT_OPCLASS:
					case OBJECT_OPFAMILY:
						error_unsupported_command(GetCommandTagName(CreateCommandTag(parsetree)));
						break;

					default:
						break;
				}
			}
			break;

		case T_AlterObjectSchemaStmt:
			error_unsupported_command(GetCommandTagName(CreateCommandTag(parsetree)));
			break;

		case T_AlterOwnerStmt:
			/* local only for now */
			break;

		case T_DropOwnedStmt:
			error_unsupported_command(GetCommandTagName(CreateCommandTag(parsetree)));
			break;

		case T_AlterDefaultPrivilegesStmt:
			lock_type = pgactive_LOCK_DDL;
			break;

		case T_SecLabelStmt:
			{
				SecLabelStmt *sstmt;

				sstmt = (SecLabelStmt *) parsetree;

				if (sstmt->provider == NULL ||
					strcmp(sstmt->provider, "pgactive") == 0)
					break;
				error_unsupported_command(GetCommandTagName(CreateCommandTag(parsetree)));
				break;
			}

		case T_CommentStmt:
		case T_ReassignOwnedStmt:
			lock_type = pgactive_LOCK_NOLOCK;
			break;

		case T_GrantStmt:
			break;

		default:

			/*
			 * It's not practical to let the compiler yell about missing cases
			 * here as there are just too many node types that can never
			 * appear as ProcessUtility targets. So just ERROR if we missed
			 * one.
			 */

			/*
			 * replace pgactive_permit_unsafe_commands by
			 * pgactive_skip_ddl_replication for now
			 */
			if (!pgactive_skip_ddl_replication)
				elog(ERROR, "unrecognized node type: %d", (int) nodeTag(parsetree));
			break;
	}

	/* now lock other nodes in the pgactive flock against ddl */
	affects_only_nonpermanent = statement_affects_only_nonpermanent(parsetree);

	/*
	 * replace pgactive_skip_ddl_locking by pgactive_skip_ddl_replication for
	 * now
	 */
	if (!pgactive_skip_ddl_replication && !affects_only_nonpermanent
		&& lock_type != pgactive_LOCK_NOLOCK)
		pgactive_acquire_ddl_lock(lock_type);

	/*
	 * Many top level DDL statements trigger subsequent actions that also
	 * invoke ProcessUtility_hook. We don't want to explicitly replicate these
	 * since running the original statement on the destination will trigger
	 * them to run there too. So we need nesting protection.
	 *
	 * TODO: Capture DDL here, allowing for issues with multi-statements
	 * (including those that mix DDL and DML, and those with transaction
	 * control statements).
	 */
	if (!affects_only_nonpermanent && !pgactive_skip_ddl_replication &&
		!pgactive_in_extension && !in_pgactive_replicate_ddl_command &&
		pgactive_ddl_nestlevel == 0)
	{
		if (context != PROCESS_UTILITY_TOPLEVEL)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("DDL command attempted inside function or multi-statement string"),
					 errdetail("pgactive does not support transparent DDL replication for "
							   "multi-statement strings or function bodies containing DDL "
							   "commands. Problem statement has tag [%s] in SQL string: %s",
							   GetCommandTagName(CreateCommandTag(parsetree)), queryString),
					 errhint("Use pgactive.pgactive_replicate_ddl_command(...) instead.")));

		Assert(pgactive_ddl_nestlevel >= 0);

		pgactive_capture_ddl(parsetree, queryString, context, params, dest, CreateCommandTag(parsetree));

		elog(DEBUG3, "DDLREP: Entering level %d DDL block, toplevel command is %s",
			 pgactive_ddl_nestlevel, queryString);
		incremented_nestlevel = true;
		pgactive_ddl_nestlevel++;
	}
	else
		elog(DEBUG3, "DDLREP: At ddl level %d ignoring non-persistent cmd %s",
			 pgactive_ddl_nestlevel, queryString);

done:
	switch (nodeTag(parsetree))
	{
		case T_TruncateStmt:
			pgactive_start_truncate();
			break;

			/*
			 * To avoid replicating commands inside create/alter/drop
			 * extension, we have to set global state that reentrant calls to
			 * ProcessUtility_hook will see so they can skip the command -
			 * pgactive_in_extension. We also need to know to unset it when
			 * this outer invocation of ProcessUtility_hook ends.
			 */
		case T_DropStmt:
			if (((DropStmt *) parsetree)->removeType != OBJECT_EXTENSION)
				break;
			/* FALLTHROUGH */
		case T_CreateExtensionStmt:
		case T_AlterExtensionStmt:
		case T_AlterExtensionContentsStmt:
			if (!pgactive_in_extension)
			{
				pgactive_in_extension = true;
				entered_extension = true;
			}

			/*
			 * When we are here with pgactive_in_extension true, it means that
			 * we entered create/alter/drop extension previously, but the
			 * extension script file is having one or more of alter extension
			 * ... drop function/drop extension statements. However, postgres
			 * fails with "ERROR: nested CREATE EXTENSION is not supported" or
			 * "ERROR: nested ALTER EXTENSION is not supported" if create
			 * extension or just the alter extension respectively is specified
			 * in an extension script file. We don't do anything fancy here
			 * for pgactive, other than ensuring the alter extension ... drop
			 * function/ drop extension statements within extension script
			 * file aren't replicated, which will be taken care by the flag
			 * pgactive_in_extension that's set to true previously.
			 */

			break;
		default:
			break;
	}

	PG_TRY();
	{
		if (next_ProcessUtility_hook)
			next_ProcessUtility_hook(pstmt, queryString,
#if PG_VERSION_NUM >= 140000
									 readOnlyTree,
									 context, params, queryEnv,
									 dest, qc);
		else
			standard_ProcessUtility(pstmt, queryString,
									readOnlyTree,
									context, params, queryEnv,
									dest, qc);
#elif PG_VERSION_NUM >= 130000
									 context, params, queryEnv,
									 dest, qc);
		else
			standard_ProcessUtility(pstmt, queryString,
									context, params, queryEnv,
									dest, qc);
#else
									 context, params, queryEnv,
									 dest, completionTag);
		else
			standard_ProcessUtility(pstmt, queryString,
									context, params, queryEnv,
									dest, completionTag);

#endif
	}
	PG_CATCH();
	{
		/*
		 * We don't have to do any truncate cleanup here. The next
		 * pgactive_start_truncate() will deal with it.
		 *
		 * We do have to handle nest level unrolling.
		 */
		if (incremented_nestlevel)
		{
			pgactive_ddl_nestlevel--;
			Assert(pgactive_ddl_nestlevel >= 0);
			elog(DEBUG3, "DDLREP: Exiting level %d in exception ",
				 pgactive_ddl_nestlevel);
		}

		/* Error was during extension creation */
		if (entered_extension)
		{
			Assert(pgactive_in_extension);
			pgactive_in_extension = false;
		}

		PG_RE_THROW();
	}
	PG_END_TRY();

	if (nodeTag(parsetree) == T_TruncateStmt)
		pgactive_finish_truncate();

	if (entered_extension)
	{
		Assert(pgactive_in_extension);
		pgactive_in_extension = false;
	}

	if (incremented_nestlevel)
	{
		pgactive_ddl_nestlevel--;
		Assert(pgactive_ddl_nestlevel >= 0);
		elog(DEBUG3, "DDLREP: Exiting level %d block normally",
			 pgactive_ddl_nestlevel);
	}
	Assert(pgactive_ddl_nestlevel >= 0);
}

static void
pgactive_ClientAuthentication_hook(Port *port, int status)
{
	if (MyProcPort->database_name != NULL
		&& strcmp(MyProcPort->database_name, pgactive_SUPERVISOR_DBNAME) == 0)
	{

		/*
		 * No commands may be executed under the supervisor database.
		 *
		 * This check won't catch execution attempts by bgworkers, but as
		 * currently database_name isn't set for those. They'd better just
		 * know better.  It's relatively harmless to run things in the
		 * supervisor database anyway.
		 *
		 * Make it a warning because of #154. Tools like vacuumdb -a like to
		 * connect to all DBs.
		 */
		ereport(WARNING,
				(errcode(ERRCODE_RESERVED_NAME),
				 errmsg("pgactive extension reserves the database "
						pgactive_SUPERVISOR_DBNAME " for its own use"),
				 errhint("Use a different database.")));
	}

	if (next_ClientAuthentication_hook)
		next_ClientAuthentication_hook(port, status);
}


/* Module load */
void
init_pgactive_commandfilter(void)
{
	next_ProcessUtility_hook = ProcessUtility_hook;
	ProcessUtility_hook = pgactive_commandfilter;

	next_ClientAuthentication_hook = ClientAuthentication_hook;
	ClientAuthentication_hook = pgactive_ClientAuthentication_hook;
}
