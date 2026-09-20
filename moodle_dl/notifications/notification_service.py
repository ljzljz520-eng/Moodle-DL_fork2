import logging
import time
from abc import ABCMeta, abstractmethod
from typing import List, Optional

from moodle_dl.config import ConfigHelper
from moodle_dl.downloader.task import Task
from moodle_dl.types import Course


class NotificationDeliveryError(Exception):
    """A channel could not be reached or rejected a delivered message.

    The outbox dispatcher treats this as a retriable failure with
    exponential backoff.
    """


class NotificationRateLimitError(NotificationDeliveryError):
    """The channel answered with a rate limit (e.g. HTTP 429).

    Such failures do not consume an outbox attempt; the row is rescheduled
    to not_before = now + retry_after (or a default wait when the server
    does not provide one).
    """

    DEFAULT_RETRY_AFTER = 60

    def __init__(self, message: str, retry_after: Optional[float] = None):
        super().__init__(message)
        self.retry_after = retry_after


class NotificationService(metaclass=ABCMeta):
    "Common class for a notification service"

    # Stable key used as the channel part of the outbox identity
    # (event + file version + service). Must never change between releases.
    service_key = 'notification'

    # Hard per-message size limit in characters. 0 disables size sharding
    # (the channel either has no limit or its formatter already splits).
    message_size_limit = 0

    # Minimum seconds between two messages sent to this channel. The
    # dispatcher paces shards accordingly to stay under service rate limits.
    min_send_interval = 0.0

    def __init__(self, config: ConfigHelper):
        self.config = config

    def _is_configured(self) -> bool:
        # The console service is always active; remote services override this.
        return True

    def is_active(self) -> bool:
        # Whether outbox rows should currently be produced/claimed for this
        # channel. Remote channels are active only when configured.
        return self._is_configured()

    def render_changes_messages(self, changes: List[Course]) -> List:
        """
        Renders a claimed change batch into a list of opaque, sendable
        message items (strings, dicts or channel specific payloads).
        Rendering is separated from sending so the dispatcher can own the
        claim -> shard -> send -> acknowledge lifecycle.
        """
        raise NotImplementedError

    def send_messages(self, messages: List, idempotency_key: Optional[str] = None) -> None:
        """
        Sends the rendered message items one by one, pacing them with
        min_send_interval. The idempotency_key is stable across retries of
        the same outbox batch and is logged for at-least-once observability.
        """
        if not messages:
            return
        logging.debug(
            'Outbox delivery on %s: sending %d part(s), idempotency_key=%s',
            self.service_key,
            len(messages),
            idempotency_key,
        )
        for index, item in enumerate(messages):
            if index > 0 and self.min_send_interval > 0:
                time.sleep(self.min_send_interval)
            self._send_message_item(item, idempotency_key)

    def _send_message_item(self, item, idempotency_key: Optional[str] = None) -> None:
        raise NotImplementedError

    @abstractmethod
    def notify_about_changes_in_moodle(self, changes: List[Course], idempotency_key: Optional[str] = None) -> None:
        """
        Sends out a Notification to inform about detected changes for the
        Moodle-Account. The caller shouldn't care about if the sending was
        successful.
        @param changes: The detected changes per course.
        @param idempotency_key: Stable key of the outbox batch (if any).
        """
        pass

    @abstractmethod
    def notify_about_error(self, error_description: str) -> None:
        """
        Sends out a Notification to inform about an error encountered during
        the execution of the program.
        The caller shouldn't care about if the sending was successful.
        @param error_description: The error text.
        """
        pass

    @abstractmethod
    def notify_about_failed_downloads(self, failed_downloads: List[Task]) -> None:
        """
        Sends out a Notification to inform about failed downloads encountered during
        the execution of the program.
        The caller shouldn't care about if the sending was successful.
        @param failed_downloads: A list of failed Tasks.
        """
        pass
