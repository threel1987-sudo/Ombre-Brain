from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
from datetime import datetime, timezone
from typing import Any

from utils import strip_wikilinks


SECTION_ALIASES = {
    "moment": "moment",
    "memory": "moment",
    "fact": "fact",
    "facts": "fact",
    "original": "original",
    "raw": "original",
    "quote": "original",
    "quotes": "original",
    "context": "context",
    "background": "context",
    "feeling": "feeling",
    "feel": "feeling",
    "reflection": "feeling",
    "followup": "followup",
    "follow-up": "followup",
    "next": "followup",
    "todo": "followup",
    "affect_anchor": "affect_anchor",
    "affect anchor": "affect_anchor",
    "favorite_reason": "favorite_reason",
    "favorite reason": "favorite_reason",
    "\u7247\u6bb5": "moment",
    "\u8bb0\u5fc6\u7247\u6bb5": "moment",
    "\u4e8b\u5b9e": "fact",
    "\u5bf9\u8bdd\u4e8b\u5b9e": "fact",
    "\u539f\u6587": "original",
    "\u5bf9\u8bdd\u539f\u6587": "original",
    "\u5f15\u7528": "original",
    "\u4e0a\u4e0b\u6587": "context",
    "\u80cc\u666f": "context",
    "\u8bed\u5883": "context",
    "\u611f\u53d7": "feeling",
    "\u60c5\u7eea": "feeling",
    "\u53cd\u601d": "feeling",
    "\u540e\u7eed": "followup",
    "\u5f85\u529e": "followup",
    "\u559c\u6b22\u5b83\u7684\u539f\u56e0": "favorite_reason",
    "\u559c\u6b22\u7684\u539f\u56e0": "favorite_reason",
}

HEADING_RE = re.compile(r"^(#{2,6})\s+(.+?)\s*$")


class MemoryMomentStore:
    """SQLite index of bucket body/comment moments."""

    def __init__(self, config: dict):
        config = config or {}
        state_dir = config.get("state_dir") or os.path.join(
            os.path.dirname(os.path.abspath(config.get("buckets_dir", "buckets"))),
            "state",
        )
        self.db_path = os.path.join(state_dir, "memory_moments.sqlite")
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        conn = self._connect()
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS memory_moments (
                moment_id TEXT PRIMARY KEY,
                bucket_id TEXT NOT NULL,
                section TEXT NOT NULL,
                text TEXT NOT NULL,
                ordinal INTEGER NOT NULL,
                source TEXT NOT NULL,
                source_id TEXT NOT NULL,
                text_hash TEXT NOT NULL,
                metadata_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_memory_moments_bucket ON memory_moments(bucket_id, ordinal)"
        )
        conn.commit()
        conn.close()

    def upsert_bucket(self, bucket: dict) -> list[dict]:
        moments = parse_bucket_moments(bucket)
        bucket_id = _bucket_id(bucket)
        conn = self._connect()
        self._replace_bucket(conn, bucket_id, moments)
        conn.commit()
        conn.close()
        return [dict(moment) for moment in moments]

    def bulk_upsert(self, buckets: list[dict]) -> dict:
        conn = self._connect()
        indexed_buckets = 0
        indexed_moments = 0
        for bucket in buckets:
            bucket_id = _bucket_id(bucket)
            moments = parse_bucket_moments(bucket)
            self._replace_bucket(conn, bucket_id, moments)
            indexed_buckets += 1
            indexed_moments += len(moments)
        conn.commit()
        conn.close()
        return {"buckets": indexed_buckets, "moments": indexed_moments}

    def list_for_bucket(self, bucket_id: str, limit: int = 100) -> list[dict]:
        bucket_id = str(bucket_id or "").strip()
        if not bucket_id:
            return []
        conn = self._connect()
        rows = conn.execute(
            """
            SELECT * FROM memory_moments
            WHERE bucket_id = ?
            ORDER BY ordinal ASC
            LIMIT ?
            """,
            (bucket_id, max(1, int(limit))),
        ).fetchall()
        conn.close()
        return [self._row_to_moment(row) for row in rows]

    def sample(self, limit: int = 20) -> list[dict]:
        conn = self._connect()
        rows = conn.execute(
            """
            SELECT * FROM memory_moments
            ORDER BY updated_at DESC, bucket_id ASC, ordinal ASC
            LIMIT ?
            """,
            (max(1, int(limit)),),
        ).fetchall()
        conn.close()
        return [self._row_to_moment(row) for row in rows]

    def stats(self) -> dict:
        conn = self._connect()
        row = conn.execute(
            """
            SELECT
                COUNT(*) AS moment_count,
                COUNT(DISTINCT bucket_id) AS bucket_count
            FROM memory_moments
            """
        ).fetchone()
        conn.close()
        return {
            "buckets": int(row["bucket_count"] or 0),
            "moments": int(row["moment_count"] or 0),
        }

    def _replace_bucket(self, conn: sqlite3.Connection, bucket_id: str, moments: list[dict]) -> None:
        conn.execute("DELETE FROM memory_moments WHERE bucket_id = ?", (bucket_id,))
        for moment in moments:
            conn.execute(
                """
                INSERT INTO memory_moments
                (moment_id, bucket_id, section, text, ordinal, source, source_id,
                 text_hash, metadata_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    moment["moment_id"],
                    moment["bucket_id"],
                    moment["section"],
                    moment["text"],
                    moment["ordinal"],
                    moment["source"],
                    moment["source_id"],
                    moment["text_hash"],
                    moment["metadata_json"],
                    moment["created_at"],
                    moment["updated_at"],
                ),
            )

    def _row_to_moment(self, row: sqlite3.Row) -> dict:
        moment = dict(row)
        try:
            metadata = json.loads(moment.get("metadata_json") or "{}")
        except json.JSONDecodeError:
            metadata = {}
        moment["metadata"] = metadata if isinstance(metadata, dict) else {}
        return moment


def parse_bucket_moments(bucket: dict) -> list[dict]:
    if not isinstance(bucket, dict):
        raise ValueError("bucket must be a dict")

    bucket_id = _bucket_id(bucket)
    meta = bucket.get("metadata") if isinstance(bucket.get("metadata"), dict) else {}
    base_meta = _bucket_metadata(meta, bucket)
    updated_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    moments: list[dict] = []
    ordinal = 0

    content = _clean_text(bucket.get("content", ""))
    if content:
        structured = _content_moments(bucket_id, content, base_meta, updated_at)
        if structured:
            for moment in structured:
                moment["ordinal"] = ordinal
                moment["moment_id"] = _moment_id(bucket_id, moment["source"], moment["section"], ordinal, moment["source_id"])
                moments.append(moment)
                ordinal += 1
        else:
            moments.append(
                _make_moment(
                    bucket_id=bucket_id,
                    section="body",
                    text=content,
                    ordinal=ordinal,
                    source="content",
                    source_id="body",
                    metadata=base_meta,
                    created_at=str(meta.get("created") or meta.get("updated_at") or ""),
                    updated_at=updated_at,
                )
            )
            ordinal += 1

    comments = meta.get("comments", [])
    if isinstance(comments, list):
        for index, comment in enumerate(comments):
            if not isinstance(comment, dict):
                continue
            text = _clean_text(comment.get("content", ""))
            if not text:
                continue
            source_id = str(comment.get("id") or f"comment-{index}")
            metadata = _clean_metadata(
                {
                    **base_meta,
                    "comment_id": source_id,
                    "comment_author": comment.get("author"),
                    "comment_kind": comment.get("kind"),
                    "comment_source": comment.get("source"),
                    "comment_valence": comment.get("valence"),
                    "comment_arousal": comment.get("arousal"),
                }
            )
            moments.append(
                _make_moment(
                    bucket_id=bucket_id,
                    section="comment",
                    text=text,
                    ordinal=ordinal,
                    source="comment",
                    source_id=source_id,
                    metadata=metadata,
                    created_at=str(comment.get("created") or meta.get("updated_at") or ""),
                    updated_at=updated_at,
                )
            )
            ordinal += 1

    return moments


def _content_moments(bucket_id: str, content: str, base_meta: dict, updated_at: str) -> list[dict]:
    blocks = _split_markdown_blocks(content)
    if not any(_canonical_section(block["heading"]) for block in blocks if block["heading"]):
        return []

    moments: list[dict] = []
    ordinal = 0
    for block_index, block in enumerate(blocks):
        heading = block["heading"]
        text = block["text"].strip()
        section = _canonical_section(heading) if heading else "body"
        if not section:
            section = "body"
            text = f"{block['heading_line']}\n{text}".strip()
        if not text:
            continue
        moments.append(
            _make_moment(
                bucket_id=bucket_id,
                section=section,
                text=text,
                ordinal=ordinal,
                source="content",
                source_id=f"{section}-{block_index}",
                metadata=base_meta,
                created_at=str(base_meta.get("bucket_created") or ""),
                updated_at=updated_at,
            )
        )
        ordinal += 1
    return moments


def _split_markdown_blocks(content: str) -> list[dict]:
    lines = content.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    blocks = []
    current = {"heading": "", "heading_line": "", "lines": []}
    for line in lines:
        match = HEADING_RE.match(line)
        if match:
            if current["heading"] or any(str(item).strip() for item in current["lines"]):
                blocks.append(
                    {
                        "heading": current["heading"],
                        "heading_line": current["heading_line"],
                        "text": "\n".join(current["lines"]).strip(),
                    }
                )
            current = {"heading": match.group(2), "heading_line": line, "lines": []}
        else:
            current["lines"].append(line)
    if current["heading"] or any(str(item).strip() for item in current["lines"]):
        blocks.append(
            {
                "heading": current["heading"],
                "heading_line": current["heading_line"],
                "text": "\n".join(current["lines"]).strip(),
            }
        )
    return blocks


def _canonical_section(heading: str) -> str:
    raw = _clean_text(heading).lower()
    if not raw:
        return ""
    cleaned = re.sub(r"^[\d.\-\s\u3001]+", "", raw)
    cleaned = re.split(r"[:\uff1a(/|\s]", cleaned, maxsplit=1)[0].strip()
    return SECTION_ALIASES.get(raw) or SECTION_ALIASES.get(cleaned) or ""


def _make_moment(
    *,
    bucket_id: str,
    section: str,
    text: str,
    ordinal: int,
    source: str,
    source_id: str,
    metadata: dict,
    created_at: str,
    updated_at: str,
) -> dict:
    text = _clean_text(text)
    metadata = _clean_metadata(metadata)
    moment_id = _moment_id(bucket_id, source, section, ordinal, source_id)
    metadata_json = json.dumps(metadata, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return {
        "moment_id": moment_id,
        "bucket_id": bucket_id,
        "section": section,
        "text": text,
        "ordinal": int(ordinal),
        "source": source,
        "source_id": str(source_id or ""),
        "text_hash": _sha1(text),
        "metadata": metadata,
        "metadata_json": metadata_json,
        "created_at": str(created_at or ""),
        "updated_at": updated_at,
    }


def _bucket_metadata(meta: dict, bucket: dict) -> dict:
    tags = _list_text(meta.get("tags"))
    content = str(bucket.get("content") or "")
    favorite_tags = [
        tag
        for tag in tags
        if tag == "haven_favorite" or tag.startswith("flavor_")
    ]
    return _clean_metadata(
        {
            "bucket_name": meta.get("name") or bucket.get("name") or "",
            "bucket_type": meta.get("type"),
            "bucket_tags": tags,
            "bucket_domain": _list_text(meta.get("domain")),
            "bucket_importance": meta.get("importance"),
            "bucket_valence": meta.get("valence"),
            "bucket_arousal": meta.get("arousal"),
            "bucket_anchor": meta.get("anchor"),
            "bucket_pinned": meta.get("pinned"),
            "bucket_protected": meta.get("protected"),
            "bucket_favorite": bool(favorite_tags),
            "bucket_favorite_tags": favorite_tags,
            "bucket_has_affect_anchor": "### affect_anchor" in content,
            "bucket_created": meta.get("created"),
            "bucket_updated_at": meta.get("updated_at"),
        }
    )


def _clean_metadata(metadata: dict) -> dict:
    cleaned = {}
    for key, value in (metadata or {}).items():
        if value is None:
            continue
        if isinstance(value, (str, int, float, bool)):
            cleaned[key] = value
        elif isinstance(value, (list, tuple)):
            cleaned[key] = [item for item in value if isinstance(item, (str, int, float, bool))]
        else:
            cleaned[key] = str(value)
    return cleaned


def _bucket_id(bucket: dict) -> str:
    meta = bucket.get("metadata") if isinstance(bucket.get("metadata"), dict) else {}
    bucket_id = str(bucket.get("id") or meta.get("id") or "").strip()
    if not bucket_id:
        raise ValueError("bucket id is required")
    return bucket_id


def _list_text(value: Any) -> list[str]:
    if isinstance(value, (list, tuple, set)):
        return [str(item) for item in value if str(item).strip()]
    if value:
        return [str(value)]
    return []


def _clean_text(value: Any) -> str:
    return strip_wikilinks(str(value or "")).strip()


def _moment_id(bucket_id: str, source: str, section: str, ordinal: int, source_id: str) -> str:
    digest = _sha1(f"{bucket_id}|{source}|{section}|{ordinal}|{source_id}")[:16]
    return f"{bucket_id}:{digest}"


def _sha1(text: str) -> str:
    return hashlib.sha1(str(text).encode("utf-8")).hexdigest()
