import logging
from typing import List, Optional

import moodle_dl.notifications.ntfy.ntfy_formatter as NF
from moodle_dl.downloader.task import Task
from moodle_dl.notifications.notification_service import NotificationService
from moodle_dl.notifications.ntfy.ntfy_shooter import NtfyShooter
from moodle_dl.types import Course


class NtfyService(NotificationService):
    service_key = 'ntfy'

    # ntfy rejects messages larger than 4096 bytes by default.
    message_size_limit = 4096
    min_send_interval = 0.2

    def _is_configured(self) -> bool:
        # Checks if the sending of ntfy messages has been configured.
        try:
            self.config.get_property("ntfy")
            return True
        except ValueError:
            logging.debug("ntfy-Notifications not configured, skipping.")
            return False

    def _build_shooter(self) -> NtfyShooter:
        ntfy_cfg = self.config.get_property("ntfy")
        return NtfyShooter(ntfy_cfg["topic"], ntfy_cfg.get("server"))

    def _send_messages(self, messages: List[dict]):
        """
        Sends an message
        """
        if not self._is_configured() or messages is None or len(messages) == 0:
            return

        logging.info("Sending Notification via ntfy...")
        self.send_messages(messages)

    def render_changes_messages(self, changes: List[Course]) -> List[dict]:
        return NF.create_full_moodle_diff_messages(changes)

    def _send_message_item(self, item: dict, idempotency_key: Optional[str] = None) -> None:
        self._build_shooter().send(**item)

    def notify_about_changes_in_moodle(self, changes: List[Course], idempotency_key: Optional[str] = None) -> None:
        """
        Sends out a notification about the downloaded changes.
        @param changes: A list of changed courses with changed files.
        @param idempotency_key: Stable outbox batch key, reused on retries.
        """
        if not self._is_configured():
            return

        logging.info("Sending Notification via ntfy...")
        self.send_messages(self.render_changes_messages(changes), idempotency_key)

    def notify_about_error(self, error_description: str):
        """
        Sends out an error message if configured to do so.
        @param error_description: The error object.
        """
        pass

    def notify_about_failed_downloads(self, failed_downloads: List[Task]):
        """
        Sends out an message about failed download if configured to send out error messages.
        @param failed_downloads: A list of failed Tasks.
        """
        pass
