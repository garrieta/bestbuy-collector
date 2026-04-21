"""Dropbox App-folder client for the pipeline.

Wraps the `dropbox` SDK with upload/download/exists helpers and
transparently handles upload sessions for files > 150 MiB.
"""

from __future__ import annotations

from collections.abc import Iterator

import dropbox
from dropbox.exceptions import ApiError
from dropbox.files import FileMetadata, UploadSessionCursor, CommitInfo, WriteMode


UPLOAD_CHUNK = 8 * 1024 * 1024              # 8 MiB
UPLOAD_SESSION_THRESHOLD = 150 * 1024 * 1024  # Dropbox's cutoff for single-shot upload


class DropboxClient:
    def __init__(self, app_key: str, app_secret: str, refresh_token: str) -> None:
        self._dbx = dropbox.Dropbox(
            oauth2_refresh_token=refresh_token,
            app_key=app_key,
            app_secret=app_secret,
            user_agent="ai-garp-bbpipeline/0.1",
        )

    def upload_bytes(self, path: str, data: bytes) -> None:
        if len(data) < UPLOAD_SESSION_THRESHOLD:
            self._dbx.files_upload(data, path, mode=WriteMode.overwrite)
            return
        # Chunked upload session for large payloads
        start = self._dbx.files_upload_session_start(data[:UPLOAD_CHUNK])
        cursor = UploadSessionCursor(session_id=start.session_id, offset=UPLOAD_CHUNK)
        pos = UPLOAD_CHUNK
        while pos + UPLOAD_CHUNK < len(data):
            self._dbx.files_upload_session_append_v2(
                data[pos:pos + UPLOAD_CHUNK], cursor
            )
            pos += UPLOAD_CHUNK
            cursor.offset = pos
        self._dbx.files_upload_session_finish(
            data[pos:], cursor, CommitInfo(path=path, mode=WriteMode.overwrite)
        )

    def download_bytes(self, path: str) -> bytes | None:
        try:
            _, resp = self._dbx.files_download(path)
            return resp.content
        except ApiError as e:
            if "not_found" in str(e):
                return None
            raise

    def exists(self, path: str) -> bool:
        try:
            self._dbx.files_get_metadata(path)
            return True
        except ApiError as e:
            if "not_found" in str(e):
                return False
            raise

    def list_files(self, folder: str, recursive: bool = True) -> Iterator[str]:
        result = self._dbx.files_list_folder(folder, recursive=recursive)
        while True:
            for entry in result.entries:
                if isinstance(entry, FileMetadata):
                    yield entry.path_display
            if not result.has_more:
                return
            result = self._dbx.files_list_folder_continue(result.cursor)

    def delete(self, path: str) -> None:
        self._dbx.files_delete_v2(path)
