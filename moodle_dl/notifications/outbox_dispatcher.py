"""
Unified transactional-outbox dispatcher used by both CLI and GUI.

Delivery semantics are at-least-once:

* File-state writes and outbox rows are committed in the same SQLite
  transaction (see StateRecorder), so a change either exists in both the
  files table and every channel's outbox, or in neither.
* For every active channel the dispatcher claims due rows with a committed
  lease, renders the change batch, applies message-size and rate sharding,
  sends the shards and acknowledges the rows only after every shard was
  accepted by the channel.
* A crash after "sent" but before "acknowledged" leaves a leased row; once
  the lease expires the same row is claimed again (by this process, a later
  CLI reentry or the GUI) and redelivered. The idempotency key is stable
  across attempts, so duplicate deliveries are observable and, where the
  channel supports it, dedupable.
* Repeated failures get exponential backoff and, after max_attempts, become
  a queryable 'dead' terminal state. HTTP 429 style rate limits reschedule
  at the server-indicated time without consuming an attempt.
* A GUI cancel releases an un-sent claim immediately (no attempt, no
  backoff); a cancel mid-batch is equivalent to a crash: the lease is
  released and the whole batch is redelivered later.
"""

import hashlib
import logging
import random
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

from moodle_dl.config import ConfigHelper
from moodle_dl.database import StateRecorder
from moodle_dl.notifications import get_active_notify_services
from moodle_dl.notifications.notification_service import (
    NotificationRateLimitError,
    NotificationService,
)


@dataclass
class OutboxDispatchResult:
    # Per-service counters: claimed/acknowledged/retried/dead/rate_limited/
    # released(cancelled)/obsolete. Totals are derived from the counters.
    by_service: Dict[str, Dict[str, int]] = field(default_factory=dict)

    def _bump(self, service: str, counter: str, amount: int = 1) -> None:
        self.by_service.setdefault(
            service,
            {'claimed': 0, 'acknowledged': 0, 'retried': 0, 'dead': 0, 'rate_limited': 0, 'released': 0, 'obsolete': 0},
        )
        self.by_service[service][counter] = self.by_service[service].get(counter, 0) + amount

    @property
    def claimed_total(self) -> int:
        return sum(c.get('claimed', 0) for c in self.by_service.values())

    @property
    def acknowledged_total(self) -> int:
        return sum(c.get('acknowledged', 0) for c in self.by_service.values())

    @property
    def retried_total(self) -> int:
        return sum(c.get('retried', 0) for c in self.by_service.values())

    @property
    def dead_total(self) -> int:
        return sum(c.get('dead', 0) for c in self.by_service.values())

    @property
    def released_total(self) -> int:
        return sum(c.get('released', 0) for c in self.by_service.values())

    def summary(self) -> str:
        parts = []
        for service, counters in sorted(self.by_service.items()):
            part = f'{service}: ' + ', '.join(
                f'{name}={counters[name]}'
                for name in ('claimed', 'acknowledged', 'retried', 'dead', 'released')
                if counters.get(name, 0)
            )
            if part != f'{service}: ':
                parts.append(part)
        return '; '.join(parts) if parts else 'no outbox activity'


class NotificationDispatcher:
    # A lease must comfortably outlast a full claim-render-send cycle; if
    # the owner crashes, another process may take over only after expiry.
    DEFAULT_LEASE_SECONDS = 300
    CLAIM_LIMIT = 50

    # Exponential backoff: 30s, 60s, 120s, ... capped at 1h.
    BACKOFF_BASE_SECONDS = 30
    BACKOFF_CAP_SECONDS = 3600

    DEFAULT_RETENTION_DAYS = 7

    def __init__(
        self,
        config: ConfigHelper,
        database: StateRecorder,
        services: Optional[List[NotificationService]] = None,
        lease_seconds: int = DEFAULT_LEASE_SECONDS,
        claim_limit: int = CLAIM_LIMIT,
        clock: Callable[[], float] = time.time,
        sleeper: Callable[[float], None] = time.sleep,
    ):
        self.config = config
        self.database = database
        self.lease_seconds = lease_seconds
        self.claim_limit = claim_limit
        self.clock = clock
        self.sleeper = sleeper
        self._services = services

    def _get_services(self) -> List[NotificationService]:
        if self._services is None:
            self._services = get_active_notify_services(self.config)
        return self._services

    def dispatch(self, is_cancelled: Optional[Callable[[], bool]] = None) -> OutboxDispatchResult:
        """
        Drains due outbox rows for every active channel. Safe to re-enter:
        rows parked in backoff are left untouched and crashed leases are
        taken over only once they expire.
        """
        result = OutboxDispatchResult()
        if is_cancelled is None:
            is_cancelled = lambda: False  # noqa: E731

        self._prune_history()

        for service in self._get_services():
            if is_cancelled():
                break
            try:
                self._drain_service(service, result, is_cancelled)
            except Exception as error:  # pylint: disable=broad-except
                # A channel must never prevent delivery to the other ones.
                logging.error(
                    'Outbox dispatcher: unexpected failure on channel %s: %s',
                    service.service_key,
                    error,
                )
                logging.debug('Outbox dispatcher failure details', exc_info=True)

        self._report_inactive_pending_channels()
        return result

    def has_pending_work(self) -> bool:
        # True while any row is parked in backoff, held by a (possibly dead)
        # lease or sitting in the dead-letter state.
        stats = self.database.outbox_stats()
        for channel in stats.values():
            if channel.get('pending', 0) or channel.get('leased', 0) or channel.get('dead', 0):
                return True
        return False

    def _prune_history(self) -> None:
        retention_days = self.config.get_property_or('outbox_retention_days', self.DEFAULT_RETENTION_DAYS)
        try:
            retention_days = int(retention_days)
        except (TypeError, ValueError):
            retention_days = self.DEFAULT_RETENTION_DAYS

        if retention_days <= 0:
            return

        cutoff = self.clock() - retention_days * 24 * 60 * 60
        deleted = self.database.prune_outbox(cutoff)
        if deleted:
            logging.debug('Outbox history cleanup removed %d acknowledged row(s).', deleted)

    def _report_inactive_pending_channels(self) -> None:
        active_keys = {service.service_key for service in self._get_services()}
        for channel in self.database.pending_outbox_services():
            if channel not in active_keys:
                logging.warning(
                    'Outbox rows for channel "%s" are pending, but the channel is not configured/active. '
                    'They stay untouched until it is reconfigured (or use --requeue-dead-notifications for dead rows).',
                    channel,
                )

    def _drain_service(
        self,
        service: NotificationService,
        result: OutboxDispatchResult,
        is_cancelled: Callable[[], bool],
    ) -> None:
        while True:
            if is_cancelled():
                return

            now = self.clock()
            rows = self.database.claim_outbox(
                service.service_key, now, lease_seconds=self.lease_seconds, limit=self.claim_limit
            )
            if not rows:
                return

            result._bump(service.service_key, 'claimed', len(rows))
            should_continue = self._deliver_batch(service, rows, result, is_cancelled)
            if not should_continue:
                # Failure/cancel: backoff/lease/release is already persisted;
                # stop hammering this channel in this run.
                return

    def _deliver_batch(
        self,
        service: NotificationService,
        rows: List[Dict],
        result: OutboxDispatchResult,
        is_cancelled: Callable[[], bool],
    ) -> bool:
        ids = [row['outbox_id'] for row in rows]
        key = service.service_key
        idempotency_key = self._batch_idempotency_key(rows)

        # Cancellation before any work: hand the claim straight back without
        # consuming an attempt.
        if is_cancelled():
            self.database.release_outbox(ids)
            result._bump(key, 'released', len(ids))
            logging.info('Outbox delivery on %s cancelled before send, released %d row(s).', key, len(ids))
            return False

        logging.info(
            'Outbox delivery on %s: claimed %d event(s), idempotency_key=%s',
            key,
            len(ids),
            idempotency_key,
        )

        if key == 'console':
            return self._deliver_console_batch(service, rows, result, is_cancelled)

        try:
            changes = self.database.changes_to_notify(file_ids=[row['file_id'] for row in rows])

            present_file_ids = {f.file_id for course in changes for f in course.files}
            obsolete_ids = [row['outbox_id'] for row in rows if row['file_id'] not in present_file_ids]
            deliver_ids = [row['outbox_id'] for row in rows if row['file_id'] in present_file_ids]

            if obsolete_ids:
                # The files rows were removed from the offline database; the
                # events no longer have anything to render and are closed.
                self.database.ack_outbox(obsolete_ids, self.clock())
                result._bump(key, 'obsolete', len(obsolete_ids))

            if not deliver_ids:
                return True

            items = service.render_changes_messages(changes)
            if not items:
                self.database.ack_outbox(deliver_ids, self.clock())
                result._bump(key, 'acknowledged', len(deliver_ids))
                return True

            shards = self._shard_items(service, items)
            self.database.mark_outbox_parts(deliver_ids, len(shards))

            for index, shard in enumerate(shards):
                if index > 0:
                    if service.min_send_interval > 0:
                        self.sleeper(service.min_send_interval)
                    if is_cancelled():
                        # Same semantics as a crash mid-batch: release now so
                        # a later run redelivers the whole batch immediately.
                        self.database.release_outbox(deliver_ids)
                        result._bump(key, 'released', len(deliver_ids))
                        logging.info(
                            'Outbox delivery on %s cancelled after %d/%d shard(s), released %d row(s).',
                            key,
                            index,
                            len(shards),
                            len(deliver_ids),
                        )
                        return False

                service.send_messages([shard], idempotency_key=idempotency_key)
                self.database.update_outbox_progress(deliver_ids, index + 1)

            self.database.ack_outbox(deliver_ids, self.clock())
            result._bump(key, 'acknowledged', len(deliver_ids))
            logging.info(
                'Outbox delivery on %s: acknowledged %d row(s) after %d shard(s).',
                key,
                len(deliver_ids),
                len(shards),
            )
            return True

        except NotificationRateLimitError as error:
            retry_after = error.retry_after or NotificationRateLimitError.DEFAULT_RETRY_AFTER
            not_before = self.clock() + float(retry_after)
            self.database.fail_outbox(ids, self.clock(), not_before, f'rate limited: {error}', rate_limited=True)
            result._bump(key, 'rate_limited', len(ids))
            logging.warning(
                'Outbox delivery on %s rate limited; %d row(s) rescheduled for ~%.0fs.',
                key,
                len(ids),
                retry_after,
            )
            return False

        except Exception as error:  # pylint: disable=broad-except
            next_attempt = max(row['attempts'] for row in rows) + 1
            backoff = self.backoff_for_attempt(next_attempt)
            not_before = self.clock() + backoff
            max_attempts = max(row['max_attempts'] for row in rows)
            self.database.fail_outbox(ids, self.clock(), not_before, f'{type(error).__name__}: {error}')

            if next_attempt >= max_attempts:
                result._bump(key, 'dead', len(ids))
                logging.error(
                    'Outbox delivery on %s failed permanently after %d attempt(s); %d row(s) moved to dead-letter: %s',
                    key,
                    next_attempt,
                    len(ids),
                    error,
                )
            else:
                result._bump(key, 'retried', len(ids))
                logging.warning(
                    'Outbox delivery on %s failed (attempt %d/%d); %d row(s) will retry in ~%.0fs: %s',
                    key,
                    next_attempt,
                    max_attempts,
                    len(ids),
                    backoff,
                    error,
                )
            logging.debug('Outbox delivery failure details', exc_info=True)
            return False

    def _deliver_console_batch(
        self,
        service: NotificationService,
        rows: List[Dict],
        result: OutboxDispatchResult,
        is_cancelled: Callable[[], bool],
    ) -> bool:
        # The console channel just prints; it participates in the outbox so
        # its visible output has the same claim/ack lifecycle and crashes are
        # not silently swallowed.
        ids = [row['outbox_id'] for row in rows]
        if is_cancelled():
            self.database.release_outbox(ids)
            result._bump('console', 'released', len(ids))
            return False

        try:
            changes = self.database.changes_to_notify(file_ids=[row['file_id'] for row in rows])
            service.notify_about_changes_in_moodle(changes, idempotency_key=self._batch_idempotency_key(rows))
            self.database.ack_outbox(ids, self.clock())
            result._bump('console', 'acknowledged', len(ids))
            return True
        except Exception as error:  # pylint: disable=broad-except
            backoff = self.backoff_for_attempt(max(row['attempts'] for row in rows) + 1)
            self.database.fail_outbox(ids, self.clock(), self.clock() + backoff, f'{type(error).__name__}: {error}')
            result._bump('console', 'retried', len(ids))
            logging.warning('Outbox console output failed; rows will retry: %s', error)
            return False

    @staticmethod
    def _batch_idempotency_key(rows: List[Dict]) -> str:
        # Deterministic over the claimed (service, per-event key) set, so a
        # redelivery after a crash carries exactly the same key.
        material = '|'.join(f"{row['service']}:{row['idempotency_key']}" for row in rows)
        return 'batch-' + hashlib.sha256(material.encode('utf-8')).hexdigest()[:32]

    @classmethod
    def backoff_for_attempt(cls, attempt: int) -> float:
        if attempt <= 1:
            delay = cls.BACKOFF_BASE_SECONDS
        else:
            delay = min(cls.BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)), cls.BACKOFF_CAP_SECONDS)
        # Small jitter keeps several retried channels from synchronizing.
        return delay * random.uniform(0.8, 1.0)

    def _shard_items(self, service: NotificationService, items: List) -> List:
        # Size sharding for plain-text messages and dict items carrying a
        # 'message' text field (ntfy). Channel-specific sharding (Discord
        # embed chunks) is already performed by the service's renderer.
        limit = getattr(service, 'message_size_limit', 0)
        if not limit:
            return list(items)

        shards = []
        for item in items:
            if isinstance(item, str):
                shards.extend(self._split_text(item, limit))
            elif isinstance(item, dict) and isinstance(item.get('message'), str):
                for piece in self._split_text(item['message'], limit):
                    clone = dict(item)
                    clone['message'] = piece
                    shards.append(clone)
            else:
                shards.append(item)
        return shards

    @staticmethod
    def _split_text(text: str, limit: int) -> List[str]:
        if len(text) <= limit:
            return [text]

        pieces = []
        remaining = text
        while len(remaining) > limit:
            window = remaining[:limit]
            split_at = window.rfind('\n')
            if split_at <= 0:
                split_at = window.rfind(' ')
            if split_at <= 0:
                # Hard split for overlong lines without boundaries.
                split_at = limit
            pieces.append(remaining[:split_at])
            remaining = remaining[split_at:].lstrip('\n') if split_at < limit else remaining[split_at:]
        if remaining:
            pieces.append(remaining)
        return pieces
