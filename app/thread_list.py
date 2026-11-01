from __future__ import annotations

import base64
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.manager_bridge import ChatThreadItem, _to_moscow_chat_label
from app.models import (
    ChatFolder,
    ChatMessage,
    Conversation,
    ConversationFolderLink,
    ConversationMeta,
    ConversationPin,
    CustomerProfile,
)
from app.services import DEFAULT_WORKSPACE_ID

THREAD_PAGE_SIZE = 50
THREAD_SEARCH_SOFT_CAP = 200


@dataclass
class ThreadSortRow:
    conversation_id: int
    is_pinned: bool
    pin_order: int | None
    is_unread: bool
    last_activity_id: int
    folder_ids: list[int]


@dataclass
class ThreadUnreadAggregates:
    unread_total_count: int
    unread_without_folder_count: int
    folder_unread_counts: dict[int, int]


@dataclass
class ThreadListPage:
    pinned: list[ChatThreadItem]
    items: list[ChatThreadItem]
    next_cursor: str | None
    has_more: bool
    matching_count: int
    list_signature: str
    aggregates: ThreadUnreadAggregates
    all_matching_ids: list[int]


def encode_thread_cursor(
    *,
    is_unread: bool,
    last_activity_id: int,
    conversation_id: int,
) -> str:
    payload = {
        "u": 1 if is_unread else 0,
        "a": int(last_activity_id or 0),
        "c": int(conversation_id or 0),
    }
    raw = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii")


def decode_thread_cursor(raw_cursor: str | None) -> tuple[bool, int, int] | None:
    text = str(raw_cursor or "").strip()
    if not text:
        return None
    try:
        payload = json.loads(base64.urlsafe_b64decode(text.encode("ascii")).decode("utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    try:
        is_unread = bool(int(payload.get("u") or 0))
        last_activity_id = int(payload.get("a") or 0)
        conversation_id = int(payload.get("c") or 0)
    except (TypeError, ValueError):
        return None
    if conversation_id <= 0:
        return None
    return is_unread, last_activity_id, conversation_id


def _unpinned_sort_key(row: ThreadSortRow) -> tuple[int, int, int]:
    return (
        0 if row.is_unread else 1,
        -int(row.last_activity_id or 0),
        -int(row.conversation_id or 0),
    )


def _display_sort_key(row: ThreadSortRow) -> tuple[int, int, int, int, int]:
    return (
        0 if row.is_pinned else 1,
        int(row.pin_order or 10**9) if row.is_pinned else 10**9,
        0 if row.is_unread else 1,
        -int(row.last_activity_id or 0),
        -int(row.conversation_id or 0),
    )


def _signature_from_sort_rows(rows: Iterable[ThreadSortRow]) -> str:
    ordered = sorted(rows, key=_display_sort_key)
    signature_source = "|".join(
        (
            f"{int(row.conversation_id)}:"
            f"{int(row.last_activity_id)}:"
            f"{1 if row.is_unread else 0}:"
            f"{1 if row.is_pinned else 0}:"
            f"{int(row.pin_order or 0)}"
        )
        for row in ordered
    )
    return hashlib.sha256(signature_source.encode("utf-8")).hexdigest()[:16]


def _aggregates_from_sort_rows(rows: Iterable[ThreadSortRow]) -> ThreadUnreadAggregates:
    folder_unread_counts: dict[int, int] = {}
    unread_total = 0
    unread_without_folder = 0
    for row in rows:
        if not row.is_unread:
            continue
        unread_total += 1
        folder_ids = [int(fid) for fid in (row.folder_ids or []) if int(fid or 0) > 0]
        if not folder_ids:
            unread_without_folder += 1
        for folder_id in set(folder_ids):
            folder_unread_counts[folder_id] = int(folder_unread_counts.get(folder_id, 0)) + 1
    return ThreadUnreadAggregates(
        unread_total_count=int(unread_total),
        unread_without_folder_count=int(unread_without_folder),
        folder_unread_counts=folder_unread_counts,
    )


def _folder_links_map(
    db: Session,
    *,
    workspace_id: int,
    conversation_ids: list[int],
    conversations: list[Conversation] | None = None,
) -> dict[int, list[int]]:
    folder_links_map: dict[int, list[int]] = {}
    if conversation_ids:
        for conv_id, folder_id in (
            db.query(ConversationFolderLink.conversation_id, ConversationFolderLink.folder_id)
            .filter(
                ConversationFolderLink.workspace_id == workspace_id,
                ConversationFolderLink.conversation_id.in_(conversation_ids),
            )
            .all()
        ):
            conv_key = int(conv_id or 0)
            folder_key = int(folder_id or 0)
            if conv_key <= 0 or folder_key <= 0:
                continue
            links = folder_links_map.setdefault(conv_key, [])
            if folder_key not in links:
                links.append(folder_key)
    for conv in conversations or []:
        links = folder_links_map.setdefault(int(conv.id), [])
        if conv.folder_id and int(conv.folder_id) not in links:
            links.append(int(conv.folder_id))
    return folder_links_map


def _sorted_folder_ids(
    db: Session,
    *,
    workspace_id: int,
    folder_links_map: dict[int, list[int]],
) -> dict[int, list[int]]:
    all_folder_ids = {
        int(folder_id)
        for folder_ids in folder_links_map.values()
        for folder_id in folder_ids
        if int(folder_id) > 0
    }
    folder_meta: dict[int, tuple[int, str]] = {}
    if all_folder_ids:
        for folder_id, sort_order, name in (
            db.query(ChatFolder.id, ChatFolder.sort_order, ChatFolder.name)
            .filter(
                ChatFolder.workspace_id == workspace_id,
                ChatFolder.id.in_(sorted(all_folder_ids)),
            )
            .all()
        ):
            folder_meta[int(folder_id)] = (int(sort_order or 0), str(name or ""))
    normalized: dict[int, list[int]] = {}
    for conv_id, folder_ids in folder_links_map.items():
        ordered = sorted(folder_ids, key=lambda fid: folder_meta.get(int(fid), (10**9, "")))
        normalized[int(conv_id)] = ordered
    return normalized


def load_thread_sort_rows(
    db: Session,
    *,
    workspace_id: int = DEFAULT_WORKSPACE_ID,
    service_user_id: int = 0,
) -> list[ThreadSortRow]:
    conversations = (
        db.query(Conversation)
        .filter(Conversation.workspace_id == workspace_id)
        .order_by(Conversation.id.desc())
        .all()
    )
    conversation_ids = [int(item.id) for item in conversations]
    if not conversation_ids:
        return []

    folder_links_map = _sorted_folder_ids(
        db,
        workspace_id=workspace_id,
        folder_links_map=_folder_links_map(
            db,
            workspace_id=workspace_id,
            conversation_ids=conversation_ids,
            conversations=conversations,
        ),
    )

    pin_map: dict[int, int] = {}
    user_key = int(service_user_id or 0)
    if user_key >= 0:
        for conv_id, sort_order in (
            db.query(ConversationPin.conversation_id, ConversationPin.sort_order)
            .filter(
                ConversationPin.workspace_id == workspace_id,
                ConversationPin.service_user_id == user_key,
                ConversationPin.conversation_id.in_(conversation_ids),
            )
            .all()
        ):
            conv_key = int(conv_id or 0)
            if conv_key <= 0:
                continue
            pin_map[conv_key] = int(sort_order or 0)

    meta_unread: dict[int, bool] = {}
    for conv_id, is_unread, manual_unread in (
        db.query(
            ConversationMeta.conversation_id,
            ConversationMeta.is_unread,
            ConversationMeta.manual_unread_mark,
        )
        .filter(
            ConversationMeta.workspace_id == workspace_id,
            ConversationMeta.conversation_id.in_(conversation_ids),
        )
        .all()
    ):
        conv_key = int(conv_id or 0)
        if conv_key <= 0:
            continue
        meta_unread[conv_key] = bool(is_unread) or bool(manual_unread)

    last_activity: dict[int, int] = {}
    for max_id, conv_id in (
        db.query(func.max(ChatMessage.id), ChatMessage.conversation_id)
        .filter(
            ChatMessage.workspace_id == workspace_id,
            ChatMessage.conversation_id.in_(conversation_ids),
        )
        .group_by(ChatMessage.conversation_id)
        .all()
    ):
        conv_key = int(conv_id or 0)
        if conv_key <= 0:
            continue
        last_activity[conv_key] = int(max_id or 0)

    rows: list[ThreadSortRow] = []
    for conv in conversations:
        conv_key = int(conv.id or 0)
        if conv_key <= 0:
            continue
        # Match legacy: missing meta => unread=True.
        is_unread = bool(meta_unread[conv_key]) if conv_key in meta_unread else True
        rows.append(
            ThreadSortRow(
                conversation_id=conv_key,
                is_pinned=(conv_key in pin_map),
                pin_order=(pin_map.get(conv_key) if conv_key in pin_map else None),
                is_unread=is_unread,
                last_activity_id=int(last_activity.get(conv_key) or conv_key),
                folder_ids=list(folder_links_map.get(conv_key, [])),
            )
        )
    return rows


def filter_sort_rows_by_folder(
    rows: list[ThreadSortRow],
    folder_id: int | None,
) -> list[ThreadSortRow]:
    if folder_id is None:
        return list(rows)
    if int(folder_id) > 0:
        target = int(folder_id)
        return [row for row in rows if target in [int(v) for v in (row.folder_ids or [])]]
    return [row for row in rows if not [int(v) for v in (row.folder_ids or []) if int(v or 0) > 0]]


def build_chat_thread_items_for_ids(
    db: Session,
    *,
    conversation_ids: list[int],
    workspace_id: int = DEFAULT_WORKSPACE_ID,
    service_user_id: int = 0,
) -> list[ChatThreadItem]:
    wanted_ids = [int(cid) for cid in conversation_ids if int(cid or 0) > 0]
    if not wanted_ids:
        return []
    wanted_set = set(wanted_ids)
    conversations = (
        db.query(Conversation)
        .filter(
            Conversation.workspace_id == workspace_id,
            Conversation.id.in_(wanted_ids),
        )
        .all()
    )
    conversations_by_id = {int(item.id): item for item in conversations if int(item.id or 0) > 0}
    ordered_conversations = [
        conversations_by_id[cid] for cid in wanted_ids if cid in conversations_by_id
    ]
    if not ordered_conversations:
        return []

    folder_links_map = _sorted_folder_ids(
        db,
        workspace_id=workspace_id,
        folder_links_map=_folder_links_map(
            db,
            workspace_id=workspace_id,
            conversation_ids=wanted_ids,
            conversations=ordered_conversations,
        ),
    )
    all_folder_ids = {
        int(folder_id)
        for folder_ids in folder_links_map.values()
        for folder_id in folder_ids
        if int(folder_id) > 0
    }
    folder_meta: dict[int, tuple[int, str]] = {}
    if all_folder_ids:
        for folder_id, sort_order, name in (
            db.query(ChatFolder.id, ChatFolder.sort_order, ChatFolder.name)
            .filter(
                ChatFolder.workspace_id == workspace_id,
                ChatFolder.id.in_(sorted(all_folder_ids)),
            )
            .all()
        ):
            folder_meta[int(folder_id)] = (int(sort_order or 0), str(name or ""))

    pin_map: dict[int, int] = {}
    user_key = int(service_user_id or 0)
    if user_key >= 0:
        for conv_id, sort_order in (
            db.query(ConversationPin.conversation_id, ConversationPin.sort_order)
            .filter(
                ConversationPin.workspace_id == workspace_id,
                ConversationPin.service_user_id == user_key,
                ConversationPin.conversation_id.in_(wanted_ids),
            )
            .all()
        ):
            conv_key = int(conv_id or 0)
            if conv_key <= 0:
                continue
            pin_map[conv_key] = int(sort_order or 0)

    meta_by_conversation_id: dict[int, ConversationMeta] = {
        int(row.conversation_id): row
        for row in (
            db.query(ConversationMeta)
            .filter(
                ConversationMeta.workspace_id == workspace_id,
                ConversationMeta.conversation_id.in_(wanted_ids),
            )
            .all()
        )
        if int(row.conversation_id or 0) > 0
    }

    customer_ids = list(
        {
            str(conv.customer_account_id or "").strip()
            for conv in ordered_conversations
            if str(conv.customer_account_id or "").strip()
        }
    )
    profile_by_customer_id: dict[str, CustomerProfile] = {}
    if customer_ids:
        for row in (
            db.query(CustomerProfile)
            .filter(
                CustomerProfile.workspace_id == workspace_id,
                CustomerProfile.customer_account_id.in_(customer_ids),
            )
            .order_by(CustomerProfile.id.asc())
            .all()
        ):
            key = str(row.customer_account_id or "").strip()
            if not key or key in profile_by_customer_id:
                continue
            profile_by_customer_id[key] = row

    last_msg_by_conversation_id: dict[int, ChatMessage] = {}
    last_msg_ids = (
        db.query(func.max(ChatMessage.id), ChatMessage.conversation_id)
        .filter(
            ChatMessage.workspace_id == workspace_id,
            ChatMessage.conversation_id.in_(wanted_ids),
        )
        .group_by(ChatMessage.conversation_id)
        .all()
    )
    latest_chat_message_ids = [int(row[0]) for row in last_msg_ids if row and int(row[0] or 0) > 0]
    if latest_chat_message_ids:
        for row in db.query(ChatMessage).filter(ChatMessage.id.in_(latest_chat_message_ids)).all():
            conv_key = int(row.conversation_id or 0)
            if conv_key > 0:
                last_msg_by_conversation_id[conv_key] = row

    failed_conversation_ids = {
        int(row[0] or 0)
        for row in (
            db.query(ChatMessage.conversation_id)
            .filter(
                ChatMessage.workspace_id == workspace_id,
                ChatMessage.conversation_id.in_(wanted_ids),
                ChatMessage.direction == "bot",
                ChatMessage.delivery_state == "failed",
            )
            .distinct()
            .all()
        )
        if row and int(row[0] or 0) > 0
    }

    items_by_id: dict[int, ChatThreadItem] = {}
    for conv in ordered_conversations:
        conv_key = int(conv.id or 0)
        if conv_key not in wanted_set:
            continue
        meta = meta_by_conversation_id.get(conv_key)
        profile = profile_by_customer_id.get(str(conv.customer_account_id or "").strip())
        last_msg = last_msg_by_conversation_id.get(conv_key)
        name = (profile.first_name if profile and profile.first_name else "Покупатель").strip()
        username = (profile.username if profile and profile.username else "").strip()
        label = f"{name}{(' @' + username) if username else ''}"
        preview = ""
        last_message_created_at: datetime | None = None
        last_message_time_label = "—"
        last_message_is_max_incoming = False
        if last_msg:
            preview = (last_msg.text or "").strip()
            if not preview and last_msg.image_url:
                preview = "[изображение]"
            created_raw = getattr(last_msg, "created_at", None)
            if isinstance(created_raw, datetime):
                last_message_created_at = created_raw
                last_message_time_label = _to_moscow_chat_label(created_raw)
            last_message_is_max_incoming = (
                str(getattr(last_msg, "direction", "") or "").strip().lower() == "customer"
                and str(getattr(last_msg, "source", "") or "").strip().lower() == "customer"
            )
        status = meta.status if meta else "new"
        is_unread = (bool(meta.is_unread) or bool(meta.manual_unread_mark)) if meta else True
        is_blocked = bool(meta.is_blocked) if meta else False
        phone_verified = bool(meta.phone_verified) if meta else False
        ticket_no = meta.ticket_no if meta else None
        last_activity_id = last_msg.id if last_msg else conv.id
        has_delivery_errors = conv_key in failed_conversation_ids
        conv_folder_ids = list(folder_links_map.get(conv_key, []))
        conv_folder_names = [
            folder_meta[int(folder_id)][1]
            for folder_id in conv_folder_ids
            if int(folder_id) in folder_meta and folder_meta[int(folder_id)][1]
        ]
        primary_folder_id = int(conv_folder_ids[0]) if conv_folder_ids else None
        primary_folder_name = conv_folder_names[0] if conv_folder_names else ""
        items_by_id[conv_key] = ChatThreadItem(
            conversation_id=conv.id,
            chat_id=conv.chat_id,
            customer_account_id=str(conv.customer_account_id or ""),
            ticket_no=ticket_no,
            customer_label=label,
            status=status,
            phone_verified=phone_verified,
            last_message_preview=preview,
            last_message_created_at=last_message_created_at,
            last_message_time_label=last_message_time_label,
            last_message_is_max_incoming=last_message_is_max_incoming,
            has_delivery_errors=has_delivery_errors,
            is_unread=is_unread,
            is_blocked=is_blocked,
            last_activity_id=last_activity_id,
            folder_id=primary_folder_id,
            folder_name=primary_folder_name,
            folder_ids=conv_folder_ids,
            folder_names=conv_folder_names,
            is_pinned=(conv_key in pin_map),
            pin_order=(pin_map.get(conv_key) if conv_key in pin_map else None),
        )
    return [items_by_id[cid] for cid in wanted_ids if cid in items_by_id]


def load_chat_thread_by_id(
    db: Session,
    *,
    conversation_id: int,
    workspace_id: int = DEFAULT_WORKSPACE_ID,
    service_user_id: int = 0,
) -> ChatThreadItem | None:
    conv_key = int(conversation_id or 0)
    if conv_key <= 0:
        return None
    items = build_chat_thread_items_for_ids(
        db,
        conversation_ids=[conv_key],
        workspace_id=workspace_id,
        service_user_id=service_user_id,
    )
    return items[0] if items else None


def _thread_item_matches_query(
    item: ChatThreadItem,
    needle: str,
    *,
    phone_number: str = "",
) -> bool:
    if not needle:
        return True
    searchable_ticket_label = (
        f"[T-{int(item.ticket_no)}] {item.customer_label}"
        if item.ticket_no is not None and int(item.ticket_no or 0) > 0
        else item.customer_label
    )
    searchable_ticket_code = (
        f"T-{int(item.ticket_no)}"
        if item.ticket_no is not None and int(item.ticket_no or 0) > 0
        else ""
    )
    searchable = " ".join(
        [
            str(item.chat_id or ""),
            str(item.customer_account_id or ""),
            str(item.customer_label or ""),
            searchable_ticket_label,
            searchable_ticket_code,
            str(item.last_message_preview or ""),
            str(item.status or ""),
            str(item.ticket_no or ""),
            str(phone_number or ""),
        ]
    ).lower()
    return needle in searchable


def _paginate_unpinned_rows(
    rows: list[ThreadSortRow],
    *,
    cursor: str | None,
    limit: int,
) -> tuple[list[ThreadSortRow], list[ThreadSortRow], str | None, bool, bool]:
    """
    Returns: pinned_rows, page_rows, next_cursor, has_more, cursor_valid.
    cursor_valid is False only when a non-empty cursor was provided but could not be decoded.
    """
    pinned = sorted(
        [row for row in rows if row.is_pinned],
        key=lambda row: (
            int(row.pin_order or 10**9),
            0 if row.is_unread else 1,
            -int(row.last_activity_id or 0),
            -int(row.conversation_id or 0),
        ),
    )
    unpinned = sorted(
        [row for row in rows if not row.is_pinned],
        key=_unpinned_sort_key,
    )
    raw_cursor = str(cursor or "").strip()
    if raw_cursor:
        decoded = decode_thread_cursor(raw_cursor)
        if decoded is None:
            return pinned, [], None, False, False
        cursor_unread, cursor_activity, cursor_id = decoded
        cursor_key = (
            0 if cursor_unread else 1,
            -int(cursor_activity or 0),
            -int(cursor_id or 0),
        )
        unpinned = [row for row in unpinned if _unpinned_sort_key(row) > cursor_key]
    page_limit = max(1, int(limit or THREAD_PAGE_SIZE))
    page_rows = unpinned[:page_limit]
    has_more = len(unpinned) > page_limit
    next_cursor = None
    if has_more and page_rows:
        last = page_rows[-1]
        next_cursor = encode_thread_cursor(
            is_unread=bool(last.is_unread),
            last_activity_id=int(last.last_activity_id or 0),
            conversation_id=int(last.conversation_id or 0),
        )
    return pinned, page_rows, next_cursor, has_more, True


def load_chat_threads_page(
    db: Session,
    *,
    query: str = "",
    folder_id: int | None = None,
    cursor: str | None = None,
    limit: int = THREAD_PAGE_SIZE,
    workspace_id: int = DEFAULT_WORKSPACE_ID,
    service_user_id: int = 0,
) -> ThreadListPage:
    needle = str(query or "").strip().lower()
    page_limit = max(1, min(int(limit or THREAD_PAGE_SIZE), THREAD_SEARCH_SOFT_CAP))

    if needle:
        # Search path keeps legacy field parity: hydrate candidates then filter.
        # Cap hydrate set via soft cap after filter+pagination.
        sort_rows = load_thread_sort_rows(
            db,
            workspace_id=workspace_id,
            service_user_id=service_user_id,
        )
        aggregates = _aggregates_from_sort_rows(sort_rows)
        # Hydrate in activity order batches until soft cap matches, then paginate.
        ordered_ids = [row.conversation_id for row in sorted(sort_rows, key=_display_sort_key)]
        phone_by_customer: dict[str, str] = {}
        for customer_id, phone in (
            db.query(CustomerProfile.customer_account_id, CustomerProfile.phone_number)
            .filter(CustomerProfile.workspace_id == workspace_id)
            .all()
        ):
            key = str(customer_id or "").strip()
            if not key or key in phone_by_customer:
                continue
            phone_by_customer[key] = str(phone or "")
        matched_items: list[ChatThreadItem] = []
        batch_size = 200
        for offset in range(0, len(ordered_ids), batch_size):
            batch_ids = ordered_ids[offset : offset + batch_size]
            batch_items = build_chat_thread_items_for_ids(
                db,
                conversation_ids=batch_ids,
                workspace_id=workspace_id,
                service_user_id=service_user_id,
            )
            for item in batch_items:
                phone_value = phone_by_customer.get(str(item.customer_account_id or "").strip(), "")
                if _thread_item_matches_query(item, needle, phone_number=phone_value):
                    matched_items.append(item)
        # Full scan completed. Recompute aggregates for search matches only.
        matched_ids = {int(item.conversation_id) for item in matched_items}
        search_sort_rows = [row for row in sort_rows if int(row.conversation_id) in matched_ids]
        # Refresh unread/pin/activity from hydrated items for signature accuracy.
        item_by_id = {int(item.conversation_id): item for item in matched_items}
        refreshed_rows: list[ThreadSortRow] = []
        for row in search_sort_rows:
            item = item_by_id.get(int(row.conversation_id))
            if item is None:
                continue
            refreshed_rows.append(
                ThreadSortRow(
                    conversation_id=int(item.conversation_id),
                    is_pinned=bool(item.is_pinned),
                    pin_order=item.pin_order,
                    is_unread=bool(item.is_unread),
                    last_activity_id=int(item.last_activity_id or 0),
                    folder_ids=list(item.folder_ids or []),
                )
            )
        aggregates = _aggregates_from_sort_rows(refreshed_rows)
        filtered_rows = filter_sort_rows_by_folder(refreshed_rows, folder_id)
        list_signature = _signature_from_sort_rows(filtered_rows)
        pinned_rows, page_rows, next_cursor, has_more, cursor_valid = _paginate_unpinned_rows(
            filtered_rows,
            cursor=cursor,
            limit=page_limit,
        )
        include_pinned = not str(cursor or "").strip()
        if not cursor_valid:
            return ThreadListPage(
                pinned=[],
                items=[],
                next_cursor=None,
                has_more=False,
                matching_count=len(filtered_rows),
                list_signature=list_signature,
                aggregates=aggregates,
                all_matching_ids=[
                    row.conversation_id for row in sorted(filtered_rows, key=_display_sort_key)
                ],
            )
        # First page includes pinned; cursor pages return only unpinned.
        if include_pinned:
            pinned_ids = [row.conversation_id for row in pinned_rows]
            page_ids = [row.conversation_id for row in page_rows]
            pinned_items = [item_by_id[cid] for cid in pinned_ids if cid in item_by_id]
            page_items = [item_by_id[cid] for cid in page_ids if cid in item_by_id]
        else:
            pinned_items = []
            page_ids = [row.conversation_id for row in page_rows]
            page_items = [item_by_id[cid] for cid in page_ids if cid in item_by_id]
        return ThreadListPage(
            pinned=pinned_items,
            items=page_items,
            next_cursor=next_cursor,
            has_more=has_more,
            matching_count=len(filtered_rows),
            list_signature=list_signature,
            aggregates=aggregates,
            all_matching_ids=[row.conversation_id for row in sorted(filtered_rows, key=_display_sort_key)],
        )

    sort_rows = load_thread_sort_rows(
        db,
        workspace_id=workspace_id,
        service_user_id=service_user_id,
    )
    aggregates = _aggregates_from_sort_rows(sort_rows)
    filtered_rows = filter_sort_rows_by_folder(sort_rows, folder_id)
    list_signature = _signature_from_sort_rows(filtered_rows)
    pinned_rows, page_rows, next_cursor, has_more, cursor_valid = _paginate_unpinned_rows(
        filtered_rows,
        cursor=cursor,
        limit=page_limit,
    )
    include_pinned = not str(cursor or "").strip()
    if not cursor_valid:
        return ThreadListPage(
            pinned=[],
            items=[],
            next_cursor=None,
            has_more=False,
            matching_count=len(filtered_rows),
            list_signature=list_signature,
            aggregates=aggregates,
            all_matching_ids=[
                row.conversation_id for row in sorted(filtered_rows, key=_display_sort_key)
            ],
        )
    if include_pinned:
        hydrate_ids = [row.conversation_id for row in pinned_rows] + [
            row.conversation_id for row in page_rows
        ]
    else:
        hydrate_ids = [row.conversation_id for row in page_rows]
    hydrated = build_chat_thread_items_for_ids(
        db,
        conversation_ids=hydrate_ids,
        workspace_id=workspace_id,
        service_user_id=service_user_id,
    )
    by_id = {int(item.conversation_id): item for item in hydrated}
    if include_pinned:
        pinned_items = [by_id[row.conversation_id] for row in pinned_rows if row.conversation_id in by_id]
    else:
        pinned_items = []
    page_items = [by_id[row.conversation_id] for row in page_rows if row.conversation_id in by_id]
    return ThreadListPage(
        pinned=pinned_items,
        items=page_items,
        next_cursor=next_cursor,
        has_more=has_more,
        matching_count=len(filtered_rows),
        list_signature=list_signature,
        aggregates=aggregates,
        all_matching_ids=[row.conversation_id for row in sorted(filtered_rows, key=_display_sort_key)],
    )


def load_unread_aggregates(
    db: Session,
    *,
    workspace_id: int = DEFAULT_WORKSPACE_ID,
    service_user_id: int = 0,
) -> ThreadUnreadAggregates:
    rows = load_thread_sort_rows(
        db,
        workspace_id=workspace_id,
        service_user_id=service_user_id,
    )
    return _aggregates_from_sort_rows(rows)
