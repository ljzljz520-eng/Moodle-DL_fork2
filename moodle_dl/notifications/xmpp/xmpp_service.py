import logging
from typing import List, Optional

from moodle_dl.downloader.task import Task
from moodle_dl.notifications.notification_service import NotificationService
from moodle_dl.notifications.xmpp.xmpp_formater import XmppFormater as XF
from moodle_dl.notifications.xmpp.xmpp_shooter import XmppShooter
from moodle_dl.types import Course


class XmppService(NotificationService):
    service_key = 'xmpp'

    # Keep individual stanzas comfortably below common server limits.
    message_size_limit = 4000
    # Avoid flooding an XMPP session with a burst of stanzas.
    min_send_interval = 0.2

    def __init__(self, config):
        super().__init__(config)
        self._shooter_instance = None

    def _is_configured(self) -> bool:
        # Checks if the sending of XMPP messages has been configured.
        try:
            self.config.get_property('xmpp')
            return True
        except ValueError:
            logging.debug('XMPP-Notifications not configured, skipping.')
            return False

    def _build_shooter(self) -> XmppShooter:
        # Reuse one connection (and its auth) for a whole batch.
        if self._shooter_instance is None:
            xmpp_cfg = self.config.get_property('xmpp')
            self._shooter_instance = XmppShooter(xmpp_cfg['sender'], xmpp_cfg['password'], xmpp_cfg['target'])
        return self._shooter_instance

    def _send_messages(self, messages: List[str]):
        """
        Sends an message
        """
        if not self._is_configured() or messages is None or len(messages) == 0:
            return

        logging.info('Sending Notification via XMPP...')
        self.send_messages(messages)

    def render_changes_messages(self, changes: List[Course]) -> List[str]:
        return XF.create_full_moodle_diff_messages(changes)

    def _send_message_item(self, item: str, idempotency_key: Optional[str] = None) -> None:
        self._build_shooter().send(item)

    def notify_about_changes_in_moodle(self, changes: List[Course], idempotency_key: Optional[str] = None) -> None:
        """
        Sends out a notification about the downloaded changes.
        @param changes: A list of changed courses with changed files.
        @param idempotency_key: Stable outbox batch key, reused on retries.
        """
        if not self._is_configured():
            return

        logging.info('Sending Notification via XMPP...')
        self.send_messages(self.render_changes_messages(changes), idempotency_key)

    def notify_about_error(self, error_description: str):
        """
        Sends out an error message if configured to do so.
        @param error_description: The error object.
        """
        if not self._is_configured():
            return

        xmpp_cfg = self.config.get_property('xmpp')

        if not xmpp_cfg.get('send_error_msg', True):
            return
        messages = XF.create_full_error_messages(error_description)

        self._send_messages(messages)

    def notify_about_failed_downloads(self, failed_downloads: List[Task]):
        """
        Sends out an message about failed download if configured to send out error messages.
        @param failed_downloads: A list of failed Tasks.
        """
        if not self._is_configured():
            return

        xmpp_cfg = self.config.get_property('xmpp')

        if not xmpp_cfg.get('send_error_msg', True):
            return
        messages = XF.create_full_failed_downloads_messages(failed_downloads)

        self._send_messages(messages)
