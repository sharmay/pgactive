/*-------------------------------------------------------------------------
 *
 * pgactive_monitoring.c
 * 		support for monitoring and progress tracking
 *
 * Copyright (c) 2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  pgactive_monitoring.c
 *
 *-------------------------------------------------------------------------
 *
 * This is shamelessly copied from pglogical_monitoring.c
 */
#include "postgres.h"

#include "fmgr.h"
#include "miscadmin.h"

#include "replication/slot.h"

#include "utils/pg_lsn.h"

#include "storage/proc.h"

#include "pgstat.h"

#include "pgactive.h"

PG_FUNCTION_INFO_V1(pgactive_wait_for_slots_confirmed_flush_lsn);

/*
 * Wait for the confirmed_flush_lsn of the specified slot, or all logical slots
 * if none given, to pass the supplied value. If no position is supplied the
 * write position is used.
 *
 * No timeout is offered, use a statement_timeout.
 */
Datum
pgactive_wait_for_slots_confirmed_flush_lsn(PG_FUNCTION_ARGS)
{
	XLogRecPtr	target_lsn;
	Name		slot_name;
	int			i;

	if (PG_ARGISNULL(0))
		slot_name = NULL;
	else
		slot_name = PG_GETARG_NAME(0);

	if (PG_ARGISNULL(1))
		target_lsn = GetXLogWriteRecPtr();
	else
		target_lsn = PG_GETARG_LSN(1);

	elog(DEBUG1, "waiting for %s to pass confirmed_flush position %X/%X",
		 slot_name == NULL ? "all local slots" : NameStr(*slot_name),
		 LSN_FORMAT_ARGS(target_lsn));

	do
	{
		XLogRecPtr	oldest_confirmed_lsn = InvalidXLogRecPtr;
		int			oldest_slot_pos = -1;

		LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
		for (i = 0; i < max_replication_slots; i++)
		{
			ReplicationSlot *s = &ReplicationSlotCtl->replication_slots[i];

			if (!s->in_use)
				continue;

			if (slot_name != NULL && strncmp(NameStr(*slot_name), NameStr(s->data.name), NAMEDATALEN) != 0)
				continue;

			if (oldest_confirmed_lsn == InvalidXLogRecPtr
				|| (s->data.confirmed_flush != InvalidXLogRecPtr && s->data.confirmed_flush < oldest_confirmed_lsn))
			{
				oldest_confirmed_lsn = s->data.confirmed_flush;
				oldest_slot_pos = i;
			}
		}

		if (oldest_slot_pos >= 0)
			elog(DEBUG2, "oldest confirmed lsn is %X/%X on slot '%s', %u bytes left until %X/%X",
				 LSN_FORMAT_ARGS(oldest_confirmed_lsn),
				 NameStr(ReplicationSlotCtl->replication_slots[oldest_slot_pos].data.name),
				 (uint32) (target_lsn - oldest_confirmed_lsn),
				 LSN_FORMAT_ARGS(target_lsn));

		LWLockRelease(ReplicationSlotControlLock);

		if (oldest_confirmed_lsn >= target_lsn)
			break;

		(void) pgactiveWaitLatch(&MyProc->procLatch,
								 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
								 1000L, PG_WAIT_EXTENSION);
		ResetLatch(&MyProc->procLatch);
		CHECK_FOR_INTERRUPTS();
	} while (1);

	PG_RETURN_VOID();
}
