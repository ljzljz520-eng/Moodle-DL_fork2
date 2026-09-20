import logging
from typing import List, Optional

from moodle_dl.downloader.task import Task
from moodle_dl.notifications.discord.discord_formatter import DiscordFormatter as DF
from moodle_dl.notifications.discord.discord_shooter import DiscordShooter
from moodle_dl.notifications.notification_service import NotificationService
from moodle_dl.types import Course


class DiscordService(NotificationService):
    service_key = 'discord'

    # Discord accepts at most 10 embeds per webhook request.
    embeds_per_request = 10
    # Pace webhook requests to stay clear of per-webhook rate limits.
    min_send_interval = 1.0

    def _is_configured(self) -> bool:
        # Checks if the sending of Discord messages has been configured.
        try:
            self.config.get_property('discord')
            return True
        except ValueError:
            logging.debug('Discord webhook notifications not configured, skipping.')
            return False

    def _build_shooter(self) -> DiscordShooter:
        discord_cfg = self.config.get_property('discord')
        return DiscordShooter(discord_cfg['webhook_urls'])

    def render_changes_messages(self, changes: List[Course]) -> List[List[dict]]:
        return self.shard_messages(DF.create_full_moodle_diff_messages(changes, self.config.get_moodle_URL().url_base))

    def shard_messages(self, items: List[dict]) -> List[List[dict]]:
        # Groups the flat embed list into webhook-sized chunks.
        chunks = []
        for index in range(0, len(items), self.embeds_per_request):
            chunks.append(items[index : index + self.embeds_per_request])
        return chunks

    def _send_message_item(self, item: List[dict], idempotency_key: Optional[str] = None) -> None:
        self._build_shooter().send(item)

    def _send_embeds(self, embeds: List[List[dict]]):
        """
        Sends a Discord webhook notification
        """
        if not self._is_configured():
            return

        logging.info('Sending Notification via Discord webhooks...')
        self.send_messages(embeds)

    def notify_about_changes_in_moodle(self, changes: List[Course], idempotency_key: Optional[str] = None) -> None:
        """
        Sends out a notification about the downloaded changes.
        @param changes: A list of changed courses with changed files.
        @param idempotency_key: Stable outbox batch key, reused on retries.
        """
        if not self._is_configured():
            return

        self._send_embeds(self.render_changes_messages(changes))

    def notify_about_error(self, error_description: str) -> None:
        # Not yet implemented
        pass

    def notify_about_failed_downloads(self, failed_downloads: List[Task]) -> None:
        # Not yet implemented
        pass
