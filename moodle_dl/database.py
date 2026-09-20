import hashlib
import json
import logging
import os
import socket
import sqlite3
import time
import uuid
from sqlite3 import Error
from typing import Dict, List, Optional

from moodle_dl.config import ConfigHelper
from moodle_dl.types import Course, File, MoodleDlOpts
from moodle_dl.utils import PathTools as PT


class StateRecorder:
    """
    Saves the state and provides utilities to detect changes in the current
    state against the previous.

    Every file-state change (new / modified / moved / deleted) is committed
    together with one outbox row per configured notification service in the
    same SQLite transaction. The outbox is the transactional bridge between
    file-state commits and channel delivery; it carries a per-service
    idempotency key, a payload digest, attempt/backoff bookkeeping, a
    crash-recoverable lease and the acknowledged state.
    """

    # Outbox delivery lifecycle states
    OUTBOX_PENDING = 'pending'
    OUTBOX_LEASED = 'leased'
    OUTBOX_ACKNOWLEDGED = 'acknowledged'
    OUTBOX_DEAD = 'dead'

    OUTBOX_ACTIVE_STATUSES = (OUTBOX_PENDING, OUTBOX_LEASED, OUTBOX_DEAD)
    OUTBOX_RETRYABLE_STATUSES = (OUTBOX_PENDING, OUTBOX_LEASED)

    DEFAULT_MAX_ATTEMPTS = 8
    DEFAULT_LEASE_SECONDS = 300

    SQL_CREATE_OUTBOX_TABLE = """ CREATE TABLE IF NOT EXISTS outbox (
            outbox_id integer PRIMARY KEY AUTOINCREMENT,
            batch_id text NOT NULL,
            service text NOT NULL,
            event_type text NOT NULL,
            file_id integer NOT NULL,
            course_id integer NOT NULL,
            idempotency_key text NOT NULL,
            payload text NOT NULL,
            payload_digest text NOT NULL,
            status text NOT NULL DEFAULT 'pending',
            attempts integer DEFAULT 0 NOT NULL,
            max_attempts integer DEFAULT 8 NOT NULL,
            not_before real DEFAULT 0 NOT NULL,
            lease_expires_at real,
            leased_by text,
            last_error text,
            part_index integer DEFAULT 0 NOT NULL,
            part_total integer DEFAULT 0 NOT NULL,
            created_at real NOT NULL,
            acknowledged_at real,
            dead_at real,
            UNIQUE (service, idempotency_key)
            );
            """

    SQL_CREATE_OUTBOX_DISPATCH_INDEX = """
            CREATE INDEX IF NOT EXISTS idx_outbox_dispatch
            ON outbox (service, status, not_before, lease_expires_at);
            """

    SQL_CREATE_OUTBOX_FILE_INDEX = """
            CREATE INDEX IF NOT EXISTS idx_outbox_file
            ON outbox (file_id);
            """

    SQL_CREATE_OUTBOX_ACK_INDEX = """
            CREATE INDEX IF NOT EXISTS idx_outbox_acknowledged
            ON outbox (acknowledged_at);
            """

    SQL_ENQUEUE_OUTBOX = """
            INSERT OR IGNORE INTO outbox
            (batch_id, service, event_type, file_id, course_id,
             idempotency_key, payload, payload_digest, status, created_at)
            VALUES
            (:batch_id, :service, :event_type, :file_id, :course_id,
             :idempotency_key, :payload, :payload_digest, 'pending', :created_at);
            """

    def __init__(self, config: ConfigHelper, opts: MoodleDlOpts):
        """
        Initiates the database.
        If no database exists yet, a new one is created.
        @param opts: Moodle-dl options
        """
        self.opts = opts
        self.config = config
        self.db_file = PT.make_path(config.get_misc_files_path(), 'moodle_state.db')

        # One batch id per process instance, grouping every outbox row
        # produced by this run (observability across CLI reentry / GUI runs).
        self.batch_id = uuid.uuid4().hex

        # Lazily resolved list of active channel keys (see notifications
        # package). Cached for the lifetime of this recorder.
        self._service_keys = None

        try:
            conn = self._connect()
            conn.row_factory = sqlite3.Row

            c = conn.cursor()

            sql_create_index_table = """ CREATE TABLE IF NOT EXISTS files (
            course_id integer NOT NULL,
            course_fullname integer NOT NULL,
            module_id integer NOT NULL,
            section_name text NOT NULL,
            module_name text NOT NULL,
            content_filepath text NOT NULL,
            content_filename text NOT NULL,
            content_fileurl text NOT NULL,
            content_filesize integer NOT NULL,
            content_timemodified integer NOT NULL,
            module_modname text NOT NULL,
            content_type text NOT NULL,
            content_isexternalfile text NOT NULL,
            saved_to text NOT NULL,
            time_stamp integer NOT NULL,
            modified integer DEFAULT 0 NOT NULL,
            deleted integer DEFAULT 0 NOT NULL,
            notified integer DEFAULT 0 NOT NULL
            );
            """

            # Create two indices for a faster search.
            sql_create_index = """
            CREATE INDEX IF NOT EXISTS idx_module_id
            ON files (module_id);
            """

            sql_create_index2 = """
            CREATE INDEX IF NOT EXISTS idx_course_id
            ON files (course_id);
            """

            c.execute(sql_create_index_table)
            c.execute(sql_create_index)
            c.execute(sql_create_index2)

            conn.commit()

            current_version = c.execute('pragma user_version').fetchone()[0]

            # Update Table
            if current_version == 0:
                # Add Hash Column
                sql_create_hash_column = """ALTER TABLE files
                ADD COLUMN hash text NULL;
                """
                c.execute(sql_create_hash_column)
                c.execute("PRAGMA user_version = 1;")
                current_version = 1
                conn.commit()

            if current_version == 1:
                # Add moved Column
                sql_create_moved_column = """ALTER TABLE files
                ADD COLUMN moved integer DEFAULT 0 NOT NULL;
                """
                c.execute(sql_create_moved_column)

                c.execute('PRAGMA user_version = 2;')
                current_version = 2
                conn.commit()

            if current_version == 2:
                # Modified gets a new meaning
                sql_remove_modified_entries = """UPDATE files
                    SET modified = 0
                    WHERE modified = 1;
                """
                c.execute(sql_remove_modified_entries)

                c.execute('PRAGMA user_version = 3;')
                current_version = 3

                conn.commit()

            if current_version == 3:
                # Add file_id Column
                sql_create_new_files_table_1 = """
                ALTER TABLE files
                RENAME TO old_files;
                """

                sql_create_new_files_table_2 = """
                CREATE TABLE IF NOT EXISTS files (
                file_id INTEGER PRIMARY KEY AUTOINCREMENT,
                course_id integer NOT NULL,
                course_fullname integer NOT NULL,
                module_id integer NOT NULL,
                section_name text NOT NULL,
                module_name text NOT NULL,
                content_filepath text NOT NULL,
                content_filename text NOT NULL,
                content_fileurl text NOT NULL,
                content_filesize integer NOT NULL,
                content_timemodified integer NOT NULL,
                module_modname text NOT NULL,
                content_type text NOT NULL,
                content_isexternalfile text NOT NULL,
                saved_to text NOT NULL,
                hash text NULL,
                time_stamp integer NOT NULL,
                old_file_id integer NULL,
                modified integer DEFAULT 0 NOT NULL,
                moved integer DEFAULT 0 NOT NULL,
                deleted integer DEFAULT 0 NOT NULL,
                notified integer DEFAULT 0 NOT NULL
                );"""

                sql_create_new_files_table_3 = """
                INSERT INTO files
                (course_id, course_fullname, module_id, section_name,
                 module_name, content_filepath, content_filename,
                 content_fileurl, content_filesize, content_timemodified,
                 module_modname, content_type, content_isexternalfile,
                 saved_to, time_stamp, modified, deleted, notified, hash,
                 moved)
                SELECT * FROM old_files
                """

                sql_create_new_files_table_4 = """
                DROP TABLE old_files;
                """
                c.execute(sql_create_new_files_table_1)
                c.execute(sql_create_new_files_table_2)
                c.execute(sql_create_new_files_table_3)
                c.execute(sql_create_new_files_table_4)

                c.execute('PRAGMA user_version = 4;')
                current_version = 4

                conn.commit()

            if current_version == 4:
                # Add section_id Column
                sql_create_section_id_column = """ALTER TABLE files
                ADD COLUMN section_id integer DEFAULT 0 NOT NULL;
                """
                c.execute(sql_create_section_id_column)

                c.execute('PRAGMA user_version = 5;')
                current_version = 5
                conn.commit()

            if current_version == 5:
                # Transactional outbox: one row per (file version change
                # event, configured channel). File-state writes and outbox
                # enqueue happen in the same transaction; the dispatcher
                # claims rows per channel, sends and acknowledges only after
                # a successful delivery.
                c.execute('PRAGMA journal_mode=WAL;')
                c.execute(self.SQL_CREATE_OUTBOX_TABLE)
                c.execute(self.SQL_CREATE_OUTBOX_DISPATCH_INDEX)
                c.execute(self.SQL_CREATE_OUTBOX_FILE_INDEX)
                c.execute(self.SQL_CREATE_OUTBOX_ACK_INDEX)

                # Migration compatibility: rows that were still pending on
                # the legacy files.notified flag become pending outbox rows
                # for every currently active channel. Already notified rows
                # are treated as acknowledged history and are not enqueued.
                self._backfill_legacy_notifications(c)

                c.execute('PRAGMA user_version = 6;')
                current_version = 6
                conn.commit()

            conn.commit()
            logging.debug('Database Version: %s', str(current_version))

            conn.close()

        except Error as error:
            raise RuntimeError(f'Could not create database! Error: {error}') from error

    def _connect(self) -> sqlite3.Connection:
        # Opens a database connection. busy_timeout lets concurrent writers
        # (e.g. a GUI and a CLI run at the same time) wait for the outbox
        # write lock instead of failing immediately.
        conn = sqlite3.connect(self.db_file, timeout=30)
        conn.execute('PRAGMA busy_timeout=30000')
        return conn

    def _active_service_keys(self) -> List[str]:
        if self._service_keys is None:
            try:
                from moodle_dl.notifications import get_active_service_keys

                self._service_keys = get_active_service_keys(self.config)
            except Exception:  # pylint: disable=broad-except
                self._service_keys = None

        if not self._service_keys:
            # The console channel exists without any configuration and must
            # always receive changes, even if no remote channel is set up.
            self._service_keys = ['console']
        return self._service_keys

    @staticmethod
    def _event_type_of_file(file: File) -> str:
        if file.deleted:
            return 'deleted'
        if file.moved:
            return 'moved'
        if file.modified:
            return 'modified'
        return 'new'

    @staticmethod
    def _build_outbox_payload(file: File, course_id: int, event_type: str, file_id: int):
        # Canonical descriptor of one file-version event. The digest lets the
        # dispatcher observe whether a retried delivery still represents the
        # exact same version of the event. Rendering happens at send time
        # from the files table, so only identity/version fields are stored.
        descriptor = {
            'event': event_type,
            'file_id': file_id,
            'course_id': course_id,
            'module_id': file.module_id,
            'section_id': file.section_id,
            'section_name': file.section_name,
            'module_name': file.module_name,
            'content_filepath': file.content_filepath,
            'content_filename': file.content_filename,
            'content_fileurl': file.content_fileurl,
            'content_filesize': file.content_filesize,
            'content_timemodified': file.content_timemodified,
            'module_modname': file.module_modname,
            'content_type': file.content_type,
            'hash': file.hash,
            'old_file_id': file.old_file_id,
            'time_stamp': file.time_stamp,
            'saved_to': file.saved_to,
        }
        payload = json.dumps(descriptor, sort_keys=True, separators=(',', ':'))
        payload_digest = hashlib.sha256(payload.encode('utf-8')).hexdigest()
        return payload, payload_digest

    def _enqueue_outbox(
        self,
        cursor: sqlite3.Cursor,
        file: File,
        course_id: int,
        event_type: str,
        file_id: Optional[int],
        created_at: float,
    ):
        # MUST run on the same cursor/transaction as the corresponding
        # files-table write, so file state and notification outbox are
        # always committed atomically.
        if file_id is None:
            return

        payload, payload_digest = self._build_outbox_payload(file, course_id, event_type, file_id)
        idempotency_key = f'{event_type}:{file_id}'

        for service in self._active_service_keys():
            cursor.execute(
                self.SQL_ENQUEUE_OUTBOX,
                {
                    'batch_id': self.batch_id,
                    'service': service,
                    'event_type': event_type,
                    'file_id': file_id,
                    'course_id': course_id,
                    'idempotency_key': idempotency_key,
                    'payload': payload,
                    'payload_digest': payload_digest,
                    'created_at': created_at,
                },
            )

    def _backfill_legacy_notifications(self, cursor: sqlite3.Cursor):
        # Migrates rows with notified = 0 from pre-outbox databases.
        legacy_rows = cursor.execute('SELECT * FROM files WHERE notified = 0').fetchall()
        if not legacy_rows:
            return

        created_at = time.time()
        for legacy_row in legacy_rows:
            legacy_file = File.fromRow(legacy_row)
            event_type = self._event_type_of_file(legacy_file)
            self._enqueue_outbox(
                cursor,
                legacy_file,
                legacy_row['course_id'],
                event_type,
                legacy_file.file_id,
                created_at,
            )

    @staticmethod
    def files_have_same_type(file1: File, file2: File) -> bool:
        # Returns True if the files have the same type attributes

        if file1.content_type == file2.content_type and file1.module_modname == file2.module_modname:
            return True

        elif (
            file1.content_type == 'description-url'
            and file1.content_type == file2.content_type
            and (
                file1.module_modname.startswith(file2.module_modname)
                or file2.module_modname.startswith(file1.module_modname)
            )
        ):
            # stop redownloading old description urls. Sorry the  module_modname structure has changed
            return True

        return False

    @classmethod
    def files_have_same_path(cls, file1: File, file2: File) -> bool:
        # Returns True if the files have the same path attributes

        if (
            file1.module_id == file2.module_id
            and file1.section_name == file2.section_name
            and file1.content_filepath == file2.content_filepath
            and file1.content_filename == file2.content_filename
            and cls.files_have_same_type(file1, file2)
            and (file1.content_type != 'description' or file1.module_name == file2.module_name)
        ):
            return True
        return False

    @staticmethod
    def files_are_diffrent(file1: File, file2: File) -> bool:
        # Returns True if these files differ from each other

        # Not sure if this would be a good idea
        #  or file1.module_name != file2.module_name)
        if file1.content_filesize != file2.content_filesize or (
            file1.content_fileurl != file2.content_fileurl and file1.content_timemodified != file2.content_timemodified
        ):
            return True
        if (
            file1.content_type in ('description', 'html')
            and file1.content_type == file2.content_type
            and (file1.hash != file2.hash or file1.content_timemodified != file2.content_timemodified)
        ):
            return True

        if (
            file1.content_type == 'description-url'
            and file1.content_type == file2.content_type
            and file1.content_fileurl != file2.content_fileurl
            # One consideration: or file1.section_name != file2.section_name)
            # But useless if description-links in the course must be unique anyway
        ):
            return True
        return False

    @staticmethod
    def files_are_moveable(file1: File, file2: File) -> bool:
        # Descriptions are not not movable at all
        if file1.content_type == 'description' or file2.content_type == 'description':
            return False
        # HTMLs with no hash are not moveable
        if (file1.content_type == 'html' and file1.hash is None) or (
            file2.content_type == 'html' and file2.hash is None
        ):
            return False
        return True

    @classmethod
    def file_was_moved(cls, file1: File, file2: File) -> bool:
        # Returns True if the file was moved to an other path

        if (
            not cls.files_are_diffrent(file1, file2)
            and cls.files_have_same_type(file1, file2)
            and not cls.files_have_same_path(file1, file2)
            and cls.files_are_moveable(file1, file2)
        ):
            return True
        return False

    @staticmethod
    def ignore_deleted(file: File):
        # Returns true if the deleted file should be ignored.
        if file.module_modname.endswith(('forum', 'calendar')):
            return True

        return False

    def get_stored_files(self) -> List[Course]:
        # get all stored files (that are not yet deleted)
        conn = self._connect()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        stored_courses = []

        cursor.execute(
            """SELECT course_id, course_fullname
            FROM files WHERE deleted = 0 AND modified = 0 AND moved = 0
            GROUP BY course_id;"""
        )

        curse_rows = cursor.fetchall()

        for course_row in curse_rows:
            course = Course(course_row['course_id'], course_row['course_fullname'])

            cursor.execute(
                """SELECT *
                FROM files
                WHERE deleted = 0
                AND modified = 0
                AND moved = 0
                AND course_id = ?;""",
                (course.id,),
            )

            file_rows = cursor.fetchall()

            course.files = []

            for file_row in file_rows:
                notify_file = File.fromRow(file_row)
                course.files.append(notify_file)

            stored_courses.append(course)

        conn.close()
        return stored_courses

    def get_old_files(self) -> List[Course]:
        # get all stored files (that are not yet deleted)
        conn = self._connect()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        stored_courses = []

        cursor.execute(
            """SELECT DISTINCT course_id, course_fullname
            FROM files WHERE old_file_id IS NOT NULL"""
        )

        course_rows = cursor.fetchall()
        for course_row in course_rows:
            course = Course(course_row['course_id'], course_row['course_fullname'])

            cursor.execute(
                """SELECT *
                FROM files
                WHERE course_id = ?
                AND old_file_id IS NOT NULL""",
                (course.id,),
            )

            updated_files = cursor.fetchall()

            course.files = []

            for updated_file in updated_files:
                cursor.execute(
                    """SELECT *
                    FROM files
                    WHERE file_id = ?""",
                    (updated_file['old_file_id'],),
                )

                old_file = cursor.fetchone()

                notify_file = File.fromRow(old_file)
                course.files.append(notify_file)

            stored_courses.append(course)

        conn.close()
        return stored_courses

    def get_modified_files(self, stored_courses: List[Course], current_courses: List[Course]) -> List[Course]:
        # returns courses with modified and deleted files
        changed_courses = []

        for stored_course in stored_courses:
            same_course_in_current = None

            for current_course in current_courses:
                if current_course.id == stored_course.id:
                    same_course_in_current = current_course
                    break

            if same_course_in_current is None:
                # stroed_course does not exist anymore!

                # maybe it would be better
                # to not notify about this changes?
                for stored_file in stored_course.files:
                    stored_file.deleted = True
                    stored_file.notified = False
                changed_courses.append(stored_course)
                # skip the next checks!
                continue

            # there is the same course in the current set
            # so try to find removed files, that are still exist in storage
            # also find modified files
            changed_course = Course(stored_course.id, stored_course.fullname)
            for stored_file in stored_course.files:
                matching_file = None

                for current_file in same_course_in_current.files:
                    # Try to find a matching file with same path
                    if self.files_have_same_path(current_file, stored_file):
                        matching_file = current_file
                        # file does still exist
                        break

                if matching_file is not None:
                    # An matching file was found
                    # Test for modification
                    if self.files_are_diffrent(matching_file, stored_file):
                        # file is modified
                        matching_file.modified = True
                        matching_file.old_file = stored_file
                        changed_course.files.append(matching_file)

                    continue

                # No matching file was found --> file was deleted or moved
                # check for moved files

                for current_file in same_course_in_current.files:
                    # Try to find a matching file that was moved
                    if self.file_was_moved(current_file, stored_file):
                        matching_file = current_file
                        # file does still exist
                        break

                if matching_file is None and not self.ignore_deleted(stored_file):
                    # No matching file was found --> file was deleted
                    stored_file.deleted = True
                    stored_file.notified = False
                    changed_course.files.append(stored_file)

                elif matching_file is not None:
                    matching_file.moved = True
                    matching_file.old_file = stored_file
                    changed_course.files.append(matching_file)

            if len(changed_course.files) > 0:
                changed_courses.append(changed_course)

        return changed_courses

    def get_new_files(
        self, changed_courses: List[Course], stored_courses: List[Course], current_courses: List[Course]
    ) -> List[Course]:
        # check for new files
        for current_course in current_courses:
            # check if that file does not exist in stored

            same_course_in_stored = None

            for stored_course in stored_courses:
                if stored_course.id == current_course.id:
                    same_course_in_stored = stored_course
                    break

            if same_course_in_stored is None:
                # current_course is not saved yet

                changed_courses.append(current_course)
                # skip the next checks!
                continue

            changed_course = Course(current_course.id, current_course.fullname)
            for current_file in current_course.files:
                matching_file = None

                for stored_file in same_course_in_stored.files:
                    # Try to find a matching file
                    has_same_path = self.files_have_same_path(current_file, stored_file)
                    was_moved = self.file_was_moved(current_file, stored_file)
                    if has_same_path or was_moved:
                        matching_file = current_file
                        break

                if matching_file is None:
                    # current_file is a new file
                    changed_course.files.append(current_file)

            if len(changed_course.files) > 0:
                matched_changed_course = None
                for ch_course in changed_courses:
                    if ch_course.id == changed_course.id:
                        matched_changed_course = ch_course
                        break
                if matched_changed_course is None:
                    changed_courses.append(changed_course)
                else:
                    matched_changed_course.files += changed_course.files
        return changed_courses

    def changes_of_new_version(self, current_courses: List[Course]) -> List[Course]:
        # all changes are stored inside changed_courses,
        # as a list of changed courses
        changed_courses = []

        # this is kind of bad code ... maybe someone can fix it

        # we need to check if there are files stored that
        # are no longer exists on Moodle => deleted
        # And if there are files that are already existing
        # check if they are modified => modified

        # later check for new files

        # first get all stored files (that are not yet deleted)
        stored_courses = self.get_stored_files()

        changed_courses = self.get_modified_files(stored_courses, current_courses)
        # ----------------------------------------------------------

        # check for new files
        changed_courses = self.get_new_files(changed_courses, stored_courses, current_courses)

        return changed_courses

    def get_last_timestamp_per_mod_module(self) -> Dict[str, Dict[int, int]]:
        """
        Returns a dict per mod of timestamps per course module id
        Like:
        {
            "forum": {
                345: 12345623466,
                346: 12345623531,
            }
        }
        """

        conn = self._connect()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        mod_forum_dict = {}
        mod_calendar_dict = {}

        cursor.execute(
            """SELECT module_id, max(content_timemodified) as content_timemodified
            FROM files WHERE module_modname = 'forum' AND content_type = 'description'
            GROUP BY module_id;"""
        )

        curse_rows = cursor.fetchall()

        for course_row in curse_rows:
            mod_forum_dict[course_row['module_id']] = course_row['content_timemodified']

        cursor.execute(
            """SELECT module_id, max(content_timemodified) as content_timemodified
            FROM files WHERE module_modname = 'calendar' AND content_type = 'html'
            GROUP BY module_id;"""
        )

        course_row = cursor.fetchone()
        if course_row is not None:
            mod_calendar_dict[course_row['module_id']] = course_row['content_timemodified']

        conn.close()

        return {'forum': mod_forum_dict, 'calendar': mod_calendar_dict}

    def changes_to_notify(self, file_ids: Optional[List[int]] = None) -> List[Course]:
        # Rebuilds the change set (with new/old version references) for
        # rendering. When file_ids is given, only the file-version rows
        # claimed by the dispatcher for one channel are reconstructed; this
        # is what keeps per-channel rendering independent of other channels'
        # backoff/lease state.
        changed_courses = []

        conn = self._connect()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        id_filter = ''
        filter_params = []
        if file_ids is not None:
            if len(file_ids) == 0:
                conn.close()
                return []
            placeholders = ','.join('?' for _ in file_ids)
            id_filter = f'AND file_id IN ({placeholders})'
            filter_params = list(file_ids)

        cursor.execute(
            f"""SELECT course_id, course_fullname
            FROM files WHERE notified = 0 {id_filter}
            GROUP BY course_id
            ORDER BY course_id;""",
            filter_params,
        )

        curse_rows = cursor.fetchall()

        for course_row in curse_rows:
            course = Course(course_row['course_id'], course_row['course_fullname'])

            cursor.execute(
                f"""SELECT *
                FROM files WHERE notified = 0 AND course_id = ? {id_filter}
                ORDER BY file_id;""",
                [course.id] + filter_params,
            )

            file_rows = cursor.fetchall()

            course.files = []

            for file_row in file_rows:
                notify_file = File.fromRow(file_row)
                if notify_file.modified or notify_file.moved:
                    # add reference to the new version of the file

                    cursor.execute(
                        """SELECT *
                        FROM files
                        WHERE old_file_id = ?;""",
                        (notify_file.file_id,),
                    )

                    file_row = cursor.fetchone()
                    if file_row is not None:
                        notify_file.new_file = File.fromRow(file_row)

                course.files.append(notify_file)

            changed_courses.append(course)

        conn.close()
        return changed_courses

    def notified(self, courses: List[Course]):
        # Legacy compatibility: pre-outbox callers could mark files as
        # notified directly. New code goes through the outbox (ack_outbox),
        # which maintains files.notified as an aggregate across channels.

        conn = self._connect()
        cursor = conn.cursor()

        for course in courses:
            course_id = course.id

            for file in course.files:
                data = {'course_id': course_id}
                data.update(file.getMap())

                cursor.execute(
                    """UPDATE files
                    SET notified = 1
                    WHERE file_id = :file_id;
                    """,
                    data,
                )

        conn.commit()
        conn.close()

    def save_file(self, file: File, course_id: int, course_fullname: str):
        if file.deleted:
            self.delete_file(file, course_id, course_fullname)
        elif file.modified:
            self.modifie_file(file, course_id, course_fullname)
        elif file.moved:
            self.move_file(file, course_id, course_fullname)
        else:
            self.new_file(file, course_id, course_fullname)

    def new_file(self, file: File, course_id: int, course_fullname: str):
        # saves a file to index and atomically enqueues its notification

        conn = self._connect()
        cursor = conn.cursor()

        data = {'course_id': course_id, 'course_fullname': course_fullname}
        data.update(file.getMap())

        data.update({'modified': 0, 'deleted': 0, 'moved': 0, 'notified': 0})

        cursor.execute(File.INSERT, data)
        new_file_id = cursor.lastrowid
        file.file_id = new_file_id

        self._enqueue_outbox(cursor, file, course_id, 'new', new_file_id, time.time())

        conn.commit()
        conn.close()

    def batch_delete_files(self, courses: List[Course]):
        conn = self._connect()
        cursor = conn.cursor()

        created_at = time.time()
        for course in courses:
            for file in course.files:
                if file.deleted:
                    data = {'course_id': course.id, 'course_fullname': course.fullname}
                    data.update(file.getMap())

                    cursor.execute(
                        """UPDATE files
                        SET notified = 0, deleted = 1, time_stamp = :time_stamp
                        WHERE file_id = :file_id;
                        """,
                        data,
                    )

                    # Delete events are enqueued in the same transaction as
                    # the files-row update.
                    self._enqueue_outbox(cursor, file, course.id, 'deleted', file.file_id, created_at)

        conn.commit()
        conn.close()

    def batch_delete_files_from_db(self, files: List[File]):
        conn = self._connect()
        cursor = conn.cursor()

        for file in files:
            cursor.execute(
                """UPDATE files
                SET old_file_id = NULL
                WHERE old_file_id = ?
                """,
                (file.file_id,),
            )

            data = {}
            data.update(file.getMap())

            cursor.execute(
                """DELETE FROM files
                WHERE file_id = :file_id
                """,
                data,
            )

        conn.commit()
        conn.close()

    def delete_file(self, file: File, course_id: int, course_fullname: str):
        conn = self._connect()
        cursor = conn.cursor()

        data = {'course_id': course_id, 'course_fullname': course_fullname}
        data.update(file.getMap())

        cursor.execute(
            """UPDATE files
            SET notified = 0, deleted = 1, time_stamp = :time_stamp
            WHERE file_id = :file_id;
            """,
            data,
        )

        self._enqueue_outbox(cursor, file, course_id, 'deleted', file.file_id, time.time())

        conn.commit()
        conn.close()

    def move_file(self, file: File, course_id: int, course_fullname: str):
        conn = self._connect()
        cursor = conn.cursor()

        created_at = time.time()
        data_new = {'course_id': course_id, 'course_fullname': course_fullname}
        data_new.update(file.getMap())

        if file.old_file is not None:
            # insert a new file, but it is already notified because the same file already exists as moved
            data_new.update(
                {'old_file_id': file.old_file.file_id, 'modified': 0, 'moved': 0, 'deleted': 0, 'notified': 1}
            )
            cursor.execute(File.INSERT, data_new)
            file.file_id = cursor.lastrowid

            data_old = {'course_id': course_id, 'course_fullname': course_fullname}
            data_old.update(file.old_file.getMap())

            cursor.execute(
                """UPDATE files
            SET notified = 0, moved = 1
            WHERE file_id = :file_id;
            """,
                data_old,
            )

            # The move event belongs to the old file version; the new
            # version row is the rendering reference (new_file) only.
            self._enqueue_outbox(cursor, file.old_file, course_id, 'moved', file.old_file.file_id, created_at)
        else:
            # this should never happen, but the old file is not saved in the
            # file descriptor, so we need to inform about the new file notified = 0
            data_new.update({'modified': 0, 'deleted': 0, 'moved': 0, 'notified': 0})
            cursor.execute(File.INSERT, data_new)
            new_file_id = cursor.lastrowid
            file.file_id = new_file_id

            self._enqueue_outbox(cursor, file, course_id, 'moved', new_file_id, created_at)

        conn.commit()
        conn.close()

    def modifie_file(self, file: File, course_id: int, course_fullname: str):
        conn = self._connect()
        cursor = conn.cursor()

        created_at = time.time()
        data_new = {'course_id': course_id, 'course_fullname': course_fullname}
        data_new.update(file.getMap())

        if file.old_file is not None:
            # insert a new file,
            # but it is already notified because the same file already exists
            # as modified
            data_new.update(
                {'old_file_id': file.old_file.file_id, 'modified': 0, 'moved': 0, 'deleted': 0, 'notified': 1}
            )
            cursor.execute(File.INSERT, data_new)
            file.file_id = cursor.lastrowid

            data_old = {'course_id': course_id, 'course_fullname': course_fullname}
            data_old.update(file.old_file.getMap())

            cursor.execute(
                """UPDATE files
            SET notified = 0, modified = 1,
            saved_to = :saved_to
            WHERE file_id = :file_id;
            """,
                data_old,
            )

            # The modification event belongs to the old file version; the
            # new version row is the rendering reference (new_file) only.
            self._enqueue_outbox(cursor, file.old_file, course_id, 'modified', file.old_file.file_id, created_at)
        else:
            # this should never happen, but the old file is not saved in the
            # file descriptor, so we need to inform about the new file
            # notified = 0

            data_new.update({'modified': 0, 'deleted': 0, 'moved': 0, 'notified': 0})
            cursor.execute(File.INSERT, data_new)
            new_file_id = cursor.lastrowid
            file.file_id = new_file_id

            self._enqueue_outbox(cursor, file, course_id, 'modified', new_file_id, created_at)

        conn.commit()
        conn.close()

    # ------------------------------------------------------------------
    # Outbox lifecycle
    # ------------------------------------------------------------------

    @staticmethod
    def _outbox_worker_id() -> str:
        return f'{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:12]}'

    def claim_outbox(
        self,
        service: str,
        now: float,
        lease_seconds: int = DEFAULT_LEASE_SECONDS,
        limit: int = 50,
    ) -> List[Dict]:
        """
        Atomically claims due rows for one channel.

        Due rows are pending rows whose backoff has elapsed and leased rows
        whose committed lease has expired (owner crashed). The lease is
        committed before the network send, so a crash after a successful
        send but before the ack makes the row reclaimable by this run, a
        later CLI reentry or the GUI - at-least-once delivery.
        """
        conn = self._connect()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        worker_id = self._outbox_worker_id()
        lease_expires_at = now + lease_seconds

        cursor.execute(
            """
            UPDATE outbox
            SET status = 'leased',
                leased_by = :worker_id,
                lease_expires_at = :lease_expires_at,
                last_error = NULL,
                part_index = 0
            WHERE outbox_id IN (
                SELECT outbox_id
                FROM outbox
                WHERE service = :service
                  AND status IN ('pending', 'leased')
                  AND not_before <= :now
                  AND (lease_expires_at IS NULL OR lease_expires_at <= :now)
                ORDER BY created_at ASC, outbox_id ASC
                LIMIT :limit
            );
            """,
            {
                'service': service,
                'now': now,
                'worker_id': worker_id,
                'lease_expires_at': lease_expires_at,
                'limit': limit,
            },
        )

        rows = cursor.execute(
            """SELECT * FROM outbox
            WHERE leased_by = :worker_id AND status = 'leased'
            ORDER BY created_at ASC, outbox_id ASC;""",
            {'worker_id': worker_id},
        ).fetchall()

        conn.commit()
        conn.close()

        return [dict(row) for row in rows]

    def mark_outbox_parts(self, outbox_ids: List[int], part_total: int):
        # Persists the shard count for a claimed batch, so a crash can be
        # observed as "leased with N planned shards, K sent".
        if not outbox_ids:
            return
        conn = self._connect()
        cursor = conn.cursor()
        placeholders = ','.join('?' for _ in outbox_ids)
        cursor.execute(
            f"""UPDATE outbox
            SET part_total = ?, part_index = 0
            WHERE outbox_id IN ({placeholders}) AND status = 'leased';""",
            [part_total] + list(outbox_ids),
        )
        conn.commit()
        conn.close()

    def update_outbox_progress(self, outbox_ids: List[int], part_index: int):
        if not outbox_ids:
            return
        conn = self._connect()
        cursor = conn.cursor()
        placeholders = ','.join('?' for _ in outbox_ids)
        cursor.execute(
            f"""UPDATE outbox
            SET part_index = ?
            WHERE outbox_id IN ({placeholders}) AND status = 'leased';""",
            [part_index] + list(outbox_ids),
        )
        conn.commit()
        conn.close()

    def ack_outbox(self, outbox_ids: List[int], now: float):
        """
        Confirms successful delivery. Acknowledging is only allowed after a
        successful send. files.notified is maintained for legacy readers as
        an aggregate: it flips to 1 only when no pending / leased / dead
        outbox row remains for the file version.
        """
        if not outbox_ids:
            return
        conn = self._connect()
        cursor = conn.cursor()
        ids = list(outbox_ids)
        id_names = ','.join(f':id{index}' for index in range(len(ids)))
        params = {'now': now}
        for index, outbox_id in enumerate(ids):
            params[f'id{index}'] = outbox_id

        cursor.execute(
            f"""UPDATE outbox
            SET status = 'acknowledged',
                acknowledged_at = :now,
                lease_expires_at = NULL,
                leased_by = NULL,
                last_error = NULL,
                part_index = part_total
            WHERE outbox_id IN ({id_names});""",
            params,
        )

        cursor.execute(
            f"""UPDATE files
            SET notified = 1
            WHERE notified = 0
              AND file_id IN (
                  SELECT DISTINCT file_id FROM outbox WHERE outbox_id IN ({id_names})
              )
              AND NOT EXISTS (
                  SELECT 1 FROM outbox AS o
                  WHERE o.file_id = files.file_id
                    AND o.status IN ('pending', 'leased', 'dead')
              );""",
            params,
        )

        conn.commit()
        conn.close()

    def fail_outbox(
        self,
        outbox_ids: List[int],
        now: float,
        not_before: float,
        error: str,
        rate_limited: bool = False,
    ):
        """
        Records a failed delivery attempt.

        Normal failures increment attempts and apply exponential backoff;
        rows that reach max_attempts become a queryable 'dead' terminal
        state. Rate-limit (HTTP 429 style) failures do not consume an
        attempt, they reschedule at the server-indicated time.
        """
        if not outbox_ids:
            return
        conn = self._connect()
        cursor = conn.cursor()
        ids = list(outbox_ids)
        id_names = ','.join(f':id{index}' for index in range(len(ids)))

        params = {
            'rate_limited': 1 if rate_limited else 0,
            'now': now,
            'not_before': not_before,
            'error': error[:2000],
        }
        for index, outbox_id in enumerate(ids):
            params[f'id{index}'] = outbox_id

        cursor.execute(
            f"""UPDATE outbox
            SET attempts = CASE WHEN :rate_limited = 1 THEN attempts ELSE attempts + 1 END,
                status = CASE
                    WHEN :rate_limited = 1 OR attempts + 1 < max_attempts THEN 'pending'
                    ELSE 'dead'
                END,
                dead_at = CASE
                    WHEN :rate_limited = 0 AND attempts + 1 >= max_attempts THEN :now
                    ELSE dead_at
                END,
                not_before = :not_before,
                lease_expires_at = NULL,
                leased_by = NULL,
                last_error = :error,
                part_index = 0
            WHERE outbox_id IN ({id_names});""",
            params,
        )

        conn.commit()
        conn.close()

    def release_outbox(self, outbox_ids: List[int]):
        """
        Releases a claim back to pending without consuming an attempt and
        without backoff. Used when a GUI cancel happens before/while the
        batch is sent; another run picks the rows up immediately.
        """
        if not outbox_ids:
            return
        conn = self._connect()
        cursor = conn.cursor()
        placeholders = ','.join('?' for _ in outbox_ids)
        cursor.execute(
            f"""UPDATE outbox
            SET status = 'pending',
                lease_expires_at = NULL,
                leased_by = NULL,
                part_index = 0
            WHERE outbox_id IN ({placeholders}) AND status = 'leased';""",
            list(outbox_ids),
        )
        conn.commit()
        conn.close()

    def prune_outbox(self, older_than_timestamp: Optional[float] = None) -> int:
        # Historical cleanup: acknowledged rows past the retention point are
        # deleted. Dead rows are intentionally kept - they are the
        # queryable terminal state for poison messages and must be requeued
        # explicitly.
        conn = self._connect()
        cursor = conn.cursor()
        if older_than_timestamp is None:
            older_than_timestamp = time.time() - 7 * 24 * 60 * 60
        cursor.execute(
            """DELETE FROM outbox
            WHERE status = 'acknowledged' AND acknowledged_at < :cutoff;""",
            {'cutoff': older_than_timestamp},
        )
        deleted = cursor.rowcount
        conn.commit()
        conn.close()
        return deleted

    def requeue_dead_outbox(self, service: Optional[str] = None) -> int:
        # CLI reentry / manual recovery: make dead rows eligible again.
        conn = self._connect()
        cursor = conn.cursor()
        if service is None:
            cursor.execute(
                """UPDATE outbox
                SET status = 'pending',
                    attempts = 0,
                    dead_at = NULL,
                    not_before = 0,
                    lease_expires_at = NULL,
                    leased_by = NULL,
                    last_error = NULL,
                    part_index = 0
                WHERE status = 'dead';"""
            )
        else:
            cursor.execute(
                """UPDATE outbox
                SET status = 'pending',
                    attempts = 0,
                    dead_at = NULL,
                    not_before = 0,
                    lease_expires_at = NULL,
                    leased_by = NULL,
                    last_error = NULL,
                    part_index = 0
                WHERE status = 'dead' AND service = :service;""",
                {'service': service},
            )
        changed = cursor.rowcount
        conn.commit()
        conn.close()
        return changed

    def outbox_stats(self) -> Dict[str, Dict[str, int]]:
        # Observability: counts per service and lifecycle status.
        conn = self._connect()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        rows = cursor.execute(
            """SELECT service, status, COUNT(*) AS amount
            FROM outbox GROUP BY service, status;"""
        ).fetchall()
        conn.close()

        result = {}
        for row in rows:
            result.setdefault(row['service'], {})[row['status']] = row['amount']
        return result

    def pending_outbox_services(self) -> List[str]:
        # Returns channel keys that still own retryable rows. Used to report
        # rows that wait for channels currently not configured/active.
        conn = self._connect()
        cursor = conn.cursor()
        rows = cursor.execute(
            """SELECT DISTINCT service FROM outbox
            WHERE status IN ('pending', 'leased');"""
        ).fetchall()
        conn.close()
        return [row[0] for row in rows]
