#!/usr/bin/env python3
"""
Cleanup duplicate onboarding bot messages in operator chat history.

Safety rules:
- Only affects ChatMessage rows with direction='bot' and source='bot_system'.
- Only affects rows whose text matches workspace onboarding templates
  (prestart/start/after_phone) after whitespace normalization.
- For each conversation + normalized text group keeps the newest row.
- Deletes related OutboxMessage rows linked by chat_message_id.
"""

from __future__ import annotations

import argparse
import re
from collections import defaultdict

from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.manager_bridge import (
    DEFAULT_TEMPLATES,
    TEMPLATE_AFTER_PHONE,
    TEMPLATE_PRESTART,
    TEMPLATE_START,
    get_template_text,
)
from app.models import BotSettings, ChatMessage, OutboxMessage


def _normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip()).strip().lower()


def _workspace_template_map(db: Session, *, workspace_id: int) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for key in (TEMPLATE_PRESTART, TEMPLATE_START, TEMPLATE_AFTER_PHONE):
        raw = str(get_template_text(db, key, workspace_id=workspace_id) or "").strip()
        if raw:
            normalized = _normalize_text(raw)
            if normalized:
                mapping[normalized] = raw
    for raw in DEFAULT_TEMPLATES.values():
        normalized = _normalize_text(raw)
        if normalized and normalized not in mapping:
            mapping[normalized] = raw
    return mapping


def cleanup_workspace(db: Session, *, workspace_id: int, dry_run: bool) -> tuple[int, int]:
    template_map = _workspace_template_map(db, workspace_id=workspace_id)
    if not template_map:
        return 0, 0

    rows = (
        db.query(ChatMessage)
        .filter(
            ChatMessage.workspace_id == int(workspace_id),
            ChatMessage.direction == "bot",
            ChatMessage.source == "bot_system",
        )
        .order_by(ChatMessage.id.desc())
        .all()
    )

    grouped: dict[tuple[int, str], list[ChatMessage]] = defaultdict(list)
    for row in rows:
        normalized = _normalize_text(str(getattr(row, "text", "") or ""))
        if normalized in template_map:
            grouped[(int(row.conversation_id or 0), normalized)].append(row)

    removed_messages = 0
    removed_outbox = 0
    for _, group_rows in grouped.items():
        if len(group_rows) <= 1:
            continue
        # rows are already desc by id -> first is newest, keep it
        for row in group_rows[1:]:
            outbox_deleted = (
                db.query(OutboxMessage)
                .filter(
                    OutboxMessage.workspace_id == int(workspace_id),
                    OutboxMessage.chat_message_id == int(row.id),
                )
                .delete(synchronize_session=False)
            )
            removed_outbox += int(outbox_deleted or 0)
            removed_messages += 1
            if not dry_run:
                db.delete(row)

    if not dry_run and (removed_messages or removed_outbox):
        db.commit()
    elif dry_run:
        db.rollback()
    return removed_messages, removed_outbox


def main() -> None:
    parser = argparse.ArgumentParser(description="Cleanup duplicate onboarding bot messages.")
    parser.add_argument("--workspace-id", type=int, default=0, help="Workspace id (0 = all workspaces)")
    parser.add_argument("--dry-run", action="store_true", help="Only print what would be deleted")
    args = parser.parse_args()

    with SessionLocal() as db:
        if int(args.workspace_id or 0) > 0:
            workspace_ids = [int(args.workspace_id)]
        else:
            workspace_ids = sorted(
                {
                    int(row[0] or 0)
                    for row in db.query(BotSettings.workspace_id).all()
                    if int(row[0] or 0) > 0
                }
            )

        total_messages = 0
        total_outbox = 0
        for workspace_id in workspace_ids:
            removed_messages, removed_outbox = cleanup_workspace(
                db,
                workspace_id=workspace_id,
                dry_run=bool(args.dry_run),
            )
            if removed_messages or removed_outbox:
                print(
                    f"workspace={workspace_id} removed_messages={removed_messages} removed_outbox={removed_outbox}"
                )
            total_messages += int(removed_messages)
            total_outbox += int(removed_outbox)

        print(
            f"done dry_run={bool(args.dry_run)} total_removed_messages={total_messages} total_removed_outbox={total_outbox}"
        )


if __name__ == "__main__":
    main()

