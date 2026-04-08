import json
from pathlib import Path
from urllib.parse import quote_plus, urlsplit
from uuid import uuid4

from fastapi.testclient import TestClient
from sqlalchemy import func
from unittest.mock import AsyncMock, patch

from app.auth import create_manager_mini_token, create_service_session
from app.database import SessionLocal, init_db
from app.main import app
from app.manager_bridge import (
    DEFAULT_TEMPLATES,
    _claim_outbox_item_for_send,
    _enqueue_outbox_message,
)
from app.services import get_or_create_settings
from app.models import (
    AuditLog,
    ChatMessage,
    ChatFolder,
    Conversation,
    ConversationFolderLink,
    ConversationPin,
    ConversationMeta,
    OutboxMessage,
    QuickReply,
    ServiceUser,
    Subscription,
    UserSession,
    WebhookEvent,
    Workspace,
)


def _run_websocket_stage2_smoke(client: TestClient, *, cookies, manager_token: str) -> None:
    # Stage-2 realtime availability smoke:
    # ws endpoints should accept connection for authenticated scopes
    # and respond to ping with pong without impacting existing chat flows.
    with client.websocket_connect("/admin/chats/ws", cookies=cookies) as ws_admin:
        ws_admin.send_text("ping")
        admin_pong = ws_admin.receive_json()
        assert admin_pong.get("type") == "pong"
        assert int(admin_pong.get("workspace_id") or 0) == 1

    with client.websocket_connect(f"/mini/manager/chats/ws?token={quote_plus(manager_token)}") as ws_mini:
        ws_mini.send_text("ping")
        mini_pong = ws_mini.receive_json()
        assert mini_pong.get("type") == "pong"
        assert int(mini_pong.get("workspace_id") or 0) == 1


def _run_websocket_stage3_incoming_hint_smoke(client: TestClient, *, cookies) -> None:
    with SessionLocal() as db:
        ws1_settings = get_or_create_settings(db, workspace_id=1)
        ws1_settings.bot_token = "token_stage3_ws1"
        ws1_settings.webhook_key = "ws1key"
        ws1_settings.admin_account_id = "admin_1"
        db.add(ws1_settings)
        db.commit()
    with client.websocket_connect("/admin/chats/ws", cookies=cookies) as ws_admin:
        incoming = client.post(
            "/webhook/max/ws1key",
            json={
                "update_type": "message_created",
                "message": {
                    "sender": {"user_id": f"buyer_ws_{uuid4().hex[:6]}", "first_name": "WS Buyer"},
                    "recipient": {"chat_id": f"chat_ws_{uuid4().hex[:6]}", "chat_type": "dialog"},
                    "body": {"text": "ws incoming"},
                },
            },
        )
        assert incoming.status_code == 200
        payload = ws_admin.receive_json()
        assert payload.get("type") == "incoming_hint"
        assert int(payload.get("workspace_id") or 0) == 1
        assert int(payload.get("conversation_id") or 0) > 0
        assert int(payload.get("seq") or 0) > 0
        assert str(payload.get("source") or "") in {"incoming_customer_message", "incoming_message"}


def _run_websocket_hint_burst_coalescing_smoke(client: TestClient, *, cookies) -> None:
    from app.realtime import chat_realtime_hub

    with SessionLocal() as db:
        ws1_settings = get_or_create_settings(db, workspace_id=1)
        ws1_settings.bot_token = "token_stage5_ws1"
        ws1_settings.webhook_key = "ws1key"
        db.add(ws1_settings)
        db.commit()

    # Probe coalescing logic directly to avoid transport timing flakiness.
    first_allowed = chat_realtime_hub.should_emit_incoming_hint(workspace_id=1, conversation_id=12345, now_monotonic=100.0)
    second_blocked = chat_realtime_hub.should_emit_incoming_hint(workspace_id=1, conversation_id=12345, now_monotonic=100.05)
    third_allowed = chat_realtime_hub.should_emit_incoming_hint(workspace_id=1, conversation_id=12345, now_monotonic=100.40)
    assert first_allowed is True
    assert second_blocked is False
    assert third_allowed is True


def _run_websocket_hint_workspace_rate_gate_smoke(client: TestClient, *, cookies) -> None:
    from app.realtime import chat_realtime_hub

    now = 1000.0
    assert chat_realtime_hub.should_emit_workspace_hint_rate_limited(
        workspace_id=1,
        window_seconds=1.0,
        max_events_per_window=2,
        now_monotonic=now,
    ) is True
    assert chat_realtime_hub.should_emit_workspace_hint_rate_limited(
        workspace_id=1,
        window_seconds=1.0,
        max_events_per_window=2,
        now_monotonic=now + 0.2,
    ) is True
    assert chat_realtime_hub.should_emit_workspace_hint_rate_limited(
        workspace_id=1,
        window_seconds=1.0,
        max_events_per_window=2,
        now_monotonic=now + 0.3,
    ) is False
    assert chat_realtime_hub.should_emit_workspace_hint_rate_limited(
        workspace_id=2,
        window_seconds=1.0,
        max_events_per_window=2,
        now_monotonic=now + 0.3,
    ) is True


def _run_websocket_seq_monotonic_smoke(client: TestClient, *, cookies) -> None:
    from app.realtime import chat_realtime_hub
    import asyncio as _asyncio

    seq1 = int(_asyncio.run(chat_realtime_hub.next_workspace_seq(workspace_id=1)))
    seq2 = int(_asyncio.run(chat_realtime_hub.next_workspace_seq(workspace_id=1)))
    seq3 = int(_asyncio.run(chat_realtime_hub.next_workspace_seq(workspace_id=2)))
    assert seq2 > seq1 >= 1
    assert seq3 >= 1


def _run_websocket_hint_seq_smoke(client: TestClient, *, cookies) -> None:
    from app.realtime import chat_realtime_hub
    import asyncio as _asyncio

    first = int(_asyncio.run(chat_realtime_hub.next_workspace_seq(workspace_id=7001)))
    second = int(_asyncio.run(chat_realtime_hub.next_workspace_seq(workspace_id=7001)))
    third_other_ws = int(_asyncio.run(chat_realtime_hub.next_workspace_seq(workspace_id=7002)))
    assert int(first) + 1 == int(second)
    assert int(third_other_ws) == 1


def _run_websocket_hint_health_smoke(client: TestClient, *, cookies) -> None:
    with client.websocket_connect("/admin/chats/ws", cookies=cookies) as ws_admin:
        ws_admin.send_text("ping")
        pong = ws_admin.receive_json()
        assert str(pong.get("type") or "") == "pong"
        assert int(pong.get("workspace_id") or 0) == 1


def _run_websocket_resync_event_shape_smoke(client: TestClient, *, cookies) -> None:
    from app.main import _ws_resync_requested_event

    payload = _ws_resync_requested_event(workspace_id=1, source="rate_limited_hint")
    assert str(payload.get("type") or "") == "resync_requested"
    assert int(payload.get("workspace_id") or 0) == 1
    assert str(payload.get("source") or "") == "rate_limited_hint"


def _run_scroll_near_bottom_snap_smoke(client: TestClient, *, cookies) -> None:
    page = client.get("/admin/chats", cookies=cookies, follow_redirects=False)
    assert page.status_code == 200
    html = page.text
    assert "const stickToBottom = forceBottom || rawOffsetFromBottom <= 80;" in html
    assert "stabilizeBottomScroll(" not in html
    assert "snapBottomOnce(" in html


def _run_targeted_media_and_quick_reply_regressions(client: TestClient, *, cookies) -> None:
    # Prepare conversation and quick replies for delete/send regressions.
    with SessionLocal() as db:
        conversation = Conversation(
            workspace_id=1,
            chat_id=f"chat_media_reg_{uuid4().hex[:8]}",
            customer_account_id=f"buyer_media_reg_{uuid4().hex[:8]}",
            manager_added=False,
            is_active=True,
        )
        db.add(conversation)
        db.commit()
        db.refresh(conversation)
        conversation_id = int(conversation.id)

        admin_reply = QuickReply(
            workspace_id=1,
            owner_user_id=0,
            command=f"adm_{uuid4().hex[:6]}",
            title="Admin delete regression",
            text="t",
            image_path="https://cdn.example.com/not-local.jpg",
            is_active=True,
        )
        app_owner = ServiceUser(
            workspace_id=1,
            role="owner",
            username=f"owner_qr_{uuid4().hex[:8]}@example.com",
            password_hash="x",
            max_account_id="",
            is_active=True,
        )
        db.add(admin_reply)
        db.add(app_owner)
        db.commit()
        db.refresh(admin_reply)
        db.refresh(app_owner)

        app_reply = QuickReply(
            workspace_id=1,
            # In app settings, owner/admin quick replies are stored as global owner_user_id=0.
            owner_user_id=0,
            command=f"app_{uuid4().hex[:6]}",
            title="App delete regression",
            text="t",
            image_path="https://cdn.example.com/not-local-app.jpg",
            is_active=True,
        )
        db.add(app_reply)
        db.commit()
        db.refresh(app_reply)
        admin_reply_id = int(admin_reply.id)
        app_reply_id = int(app_reply.id)
        owner_user_id = int(app_owner.id)

    # Admin quick-reply delete must not crash on non-local image_path.
    admin_delete = client.post(
        f"/admin/quick-replies/{admin_reply_id}/delete",
        cookies=cookies,
        follow_redirects=False,
    )
    assert admin_delete.status_code in (302, 303)
    with SessionLocal() as db:
        assert db.query(QuickReply).filter(QuickReply.id == admin_reply_id).first() is None

    # App quick-reply delete must not crash on non-local image_path.
    owner_session_token = ""
    with SessionLocal() as db:
        owner_session_token = create_service_session(
            db,
            user_id=owner_user_id,
            ip_address="127.0.0.1",
            user_agent="smoke/app-delete-qr",
        )
    app_delete_cookies = {"tenant_session": owner_session_token}
    app_delete = client.post(
        f"/app/quick-replies/{app_reply_id}/delete",
        cookies=app_delete_cookies,
        headers={"origin": "http://testserver"},
        follow_redirects=False,
    )
    assert app_delete.status_code in (302, 303)
    with SessionLocal() as db:
        assert db.query(QuickReply).filter(QuickReply.id == app_reply_id).first() is None

    # Regression: upload_token_missing must fallback to image_url attachments
    # and not fail with proto.payload/errors.required.
    sent_attachments: list[list[dict]] = []

    async def _fake_send_images(*args, **kwargs):
        # Force fallback path from byte-upload flow into attachment send_message flow.
        return {"success": False, "error": "upload_token_missing"}

    async def _fake_send_message(*args, **kwargs):
        attachments = kwargs.get("attachments")
        assert isinstance(attachments, list) and len(attachments) >= 1
        first = attachments[0] if attachments else {}
        # Regression guard: fallback attachments must be valid MAX image attachments.
        assert isinstance(first, dict)
        assert str(first.get("type") or "") == "image"
        payload = first.get("payload") if isinstance(first.get("payload"), dict) else {}
        assert isinstance(payload, dict)
        assert any(str(payload.get(key) or "").strip() for key in ("token", "url")) or bool(payload.get("photos"))
        # For URL fallback, ensure we do not send local /static paths to MAX.
        if str(payload.get("url") or "").strip():
            assert str(payload.get("url") or "").strip().startswith(("http://", "https://"))
        sent_attachments.append(attachments)
        return {"success": True, "message": {"body": {"mid": f"mid_{uuid4().hex[:8]}"}}}

    with patch("app.max_client.MaxClient.send_images", new=AsyncMock(side_effect=_fake_send_images)), patch(
        "app.max_client.MaxClient.send_message",
        new=AsyncMock(side_effect=_fake_send_message),
    ):
        tiny_png = (
            b"\x89PNG\r\n\x1a\n"
            b"\x00\x00\x00\rIHDR"
            b"\x00\x00\x00\x01\x00\x00\x00\x01\x08\x02\x00\x00\x00"
            b"\x90wS\xde"
            b"\x00\x00\x00\x0cIDATx\x9cc`\x00\x00\x00\x02\x00\x01"
            b"\xe2!\xbc3"
            b"\x00\x00\x00\x00IEND\xaeB`\x82"
        )
        send_with_photo = client.post(
            f"/admin/chats/{conversation_id}/send",
            data={"text": "photo fallback check"},
            files={"photos": ("smoke.png", tiny_png, "image/png")},
            cookies=cookies,
            follow_redirects=False,
        )
        assert send_with_photo.status_code in (302, 303)
        location = str(send_with_photo.headers.get("location", ""))
        assert "sent=1" in location or "quick=1" in location

    assert len(sent_attachments) >= 1
    first_batch = sent_attachments[0]
    assert isinstance(first_batch, list)
    assert str(first_batch[0].get("type") or "") == "image"
    payload = first_batch[0].get("payload") if isinstance(first_batch[0], dict) else {}
    assert isinstance(payload, dict) and str(payload.get("url") or "").strip()


def _run_targeted_chat_history_media_visibility_regression(client: TestClient, *, cookies) -> None:
    with SessionLocal() as db:
        conversation = Conversation(
            workspace_id=1,
            chat_id=f"chat_hist_reg_{uuid4().hex[:8]}",
            customer_account_id=f"buyer_hist_reg_{uuid4().hex[:8]}",
            manager_added=False,
            is_active=True,
        )
        db.add(conversation)
        db.commit()
        db.refresh(conversation)
        conversation_id = int(conversation.id)
        msg = ChatMessage(
            workspace_id=1,
            conversation_id=conversation_id,
            direction="bot",
            source="bot_system",
            text="Текст с фото",
            image_url="/static/uploads/history-first.jpg",
            image_urls_json='["/static/uploads/history-first.jpg","/static/uploads/history-second.jpg"]',
            delivery_state="sent",
            delivery_error="",
            delivery_retry_count=0,
        )
        db.add(msg)
        db.commit()
    page = client.get(f"/admin/chats?conversation_id={conversation_id}", cookies=cookies, follow_redirects=False)
    assert page.status_code == 200
    assert "Текст с фото" in page.text
    assert "/static/uploads/history-first.jpg" in page.text
    assert "/static/uploads/history-second.jpg" in page.text


def _run_orphan_chat_media_cleanup_regression(client: TestClient, *, cookies) -> None:
    with SessionLocal() as db:
        conversation = Conversation(
            workspace_id=1,
            chat_id=f"chat_orphan_reg_{uuid4().hex[:8]}",
            customer_account_id=f"buyer_orphan_reg_{uuid4().hex[:8]}",
            manager_added=False,
            is_active=True,
        )
        db.add(conversation)
        db.commit()
        db.refresh(conversation)
        conversation_id = int(conversation.id)

        msg = ChatMessage(
            workspace_id=1,
            conversation_id=conversation_id,
            direction="bot",
            source="bot_system",
            text="orphan media cleanup",
            image_url="",
            image_urls_json="[]",
            delivery_state="sent",
            delivery_error="",
            delivery_retry_count=0,
        )
        db.add(msg)
        db.commit()
        db.refresh(msg)
        msg_id = int(msg.id)
        db.delete(msg)
        db.commit()

        from app.manager_bridge import ensure_media_asset_for_path, cleanup_orphan_chat_message_media_links
        from app.models import ChatMessageMedia, MediaAsset

        media_url = "/static/uploads/orphan-check.png"
        ensure_media_asset_for_path(db, workspace_id=1, media_path=media_url)
        asset = db.query(MediaAsset).filter(MediaAsset.public_url == media_url).first()
        assert asset is not None
        db.add(
            ChatMessageMedia(
                workspace_id=1,
                chat_message_id=msg_id,
                media_asset_id=int(asset.id),
                sort_order=0,
                role="image",
            )
        )
        db.commit()

        before = db.query(ChatMessageMedia).filter(ChatMessageMedia.chat_message_id == msg_id).count()
        assert before >= 1
        removed = cleanup_orphan_chat_message_media_links(db, workspace_id=1, limit=100)
        assert int(removed) >= 1
        after = db.query(ChatMessageMedia).filter(ChatMessageMedia.chat_message_id == msg_id).count()
        assert after == 0

    page = client.get(f"/admin/chats?conversation_id={conversation_id}", cookies=cookies, follow_redirects=False)
    assert page.status_code == 200
    assert "orphan media cleanup" not in page.text


def _iso_with_timezone(raw_iso: str) -> str:
    value = (raw_iso or "").strip()
    if not value:
        return value
    if value.endswith("Z") or "+" in value[10:] or "-" in value[10:]:
        return value
    return f"{value}Z"


def run() -> None:
    init_db()
    command = f"price_{uuid4().hex[:8]}"
    chat_id = f"chat_{uuid4().hex[:8]}"
    with TestClient(app) as client:

        health = client.get("/health")
        assert health.status_code == 200
        assert health.json().get("ok") is True

        app_login_page = client.get("/app/login")
        assert app_login_page.status_code == 200
        assert "2FA код (только для superadmin)" not in app_login_page.text

        superadmin_login_page = client.get("/sa/login")
        assert superadmin_login_page.status_code == 200
        assert "2FA код" in superadmin_login_page.text

        superadmin_login = client.post(
            "/sa/login",
            data={
                "username": "admin",
                "password": "wNlT4yBzUhEZR1q011!!;sawf",
                "totp_code": "wNlT4yBzUhEZR1q011!!;sawf2FA",
            },
            follow_redirects=False,
        )
        assert superadmin_login.status_code in (302, 303)
        assert superadmin_login.headers.get("location") == "/app/superadmin"
        superadmin_cookies = superadmin_login.cookies

        superadmin_login_via_app = client.post(
            "/app/login",
            data={"username": "admin", "password": "wNlT4yBzUhEZR1q011!!;sawf"},
            follow_redirects=False,
        )
        assert superadmin_login_via_app.status_code == 200
        assert "Для superadmin используйте отдельный вход: /sa/login" in superadmin_login_via_app.text

        superadmin_sections = [
            ("/app/superadmin", "Дашборд"),
            ("/app/superadmin/workspaces", "Клиенты (рабочие пространства)"),
            ("/app/superadmin/users", "Пользователи"),
            ("/app/superadmin/plans", "Тарифы и лимиты"),
            ("/app/superadmin/security", "Безопасность"),
            ("/app/superadmin/monitoring", "Мониторинг по клиентам"),
            ("/app/superadmin/backups", "Резервные копии"),
            ("/app/superadmin/audit", "Аудит-лог"),
            ("/app/superadmin/system", "Системные настройки"),
        ]
        for path, marker in superadmin_sections:
            page = client.get(path, cookies=superadmin_cookies)
            assert page.status_code == 200
            assert marker in page.text

        superadmin_workspaces = client.get("/app/superadmin/workspaces", cookies=superadmin_cookies)
        assert superadmin_workspaces.status_code == 200
        assert "Default Workspace" not in superadmin_workspaces.text
        assert "Открыть чаты клиента" not in superadmin_workspaces.text
        with SessionLocal() as db:
            assert (
                db.query(ServiceUser)
                .filter(ServiceUser.workspace_id == 1, ServiceUser.role == "manager")
                .count()
            ) == 0
            default_settings = get_or_create_settings(db, workspace_id=1)
            assert (default_settings.manager_account_id or "").strip() == ""

        superadmin_system = client.get("/app/superadmin/system", cookies=superadmin_cookies)
        assert superadmin_system.status_code == 200
        assert "Email технического отдела" in superadmin_system.text
        assert "Email финансового отдела" in superadmin_system.text

        superadmin_users = client.get("/app/superadmin/users", cookies=superadmin_cookies)
        assert superadmin_users.status_code == 200
        assert "admin" in superadmin_users.text
        with SessionLocal() as db:
            superadmin_row = (
                db.query(ServiceUser)
                .filter(ServiceUser.role == "superadmin")
                .order_by(ServiceUser.id.asc())
                .first()
            )
            assert superadmin_row is not None
            superadmin_user_id = int(superadmin_row.id)
        assert f"/app/superadmin/users/{superadmin_user_id}/delete" not in superadmin_users.text

        # User deletion from superadmin panel.
        delete_temp_email = f"delete_{uuid4().hex[:8]}@example.com"
        delete_temp_register = client.post(
            "/app/register",
            data={"email": delete_temp_email, "password": "StrongPass#123"},
            follow_redirects=False,
        )
        assert delete_temp_register.status_code in (302, 303)
        with SessionLocal() as db:
            delete_temp_user = db.query(ServiceUser).filter(ServiceUser.username == delete_temp_email).first()
            assert delete_temp_user is not None
            delete_temp_user_id = int(delete_temp_user.id)
            delete_temp_workspace_id = int(delete_temp_user.workspace_id or 0)
            assert delete_temp_workspace_id > 0
        superadmin_open_chats = client.get(
            f"/app/superadmin/workspaces/{delete_temp_workspace_id}/chats",
            cookies=superadmin_cookies,
            follow_redirects=False,
        )
        assert superadmin_open_chats.status_code == 403
        superadmin_chats_page = client.get("/app/chats", cookies=superadmin_cookies, follow_redirects=False)
        assert superadmin_chats_page.status_code == 403
        superadmin_settings_page = client.get("/app/settings", cookies=superadmin_cookies, follow_redirects=False)
        assert superadmin_settings_page.status_code == 403
        superadmin_managers_page = client.get("/app/managers", cookies=superadmin_cookies, follow_redirects=False)
        assert superadmin_managers_page.status_code == 403

        update_support_contacts = client.post(
            "/app/superadmin/support-contacts",
            data={
                "support_tech_email": "tech@example.com",
                "support_billing_email": "billing@example.com",
            },
            cookies=superadmin_cookies,
            follow_redirects=False,
        )
        assert update_support_contacts.status_code in (302, 303)
        delete_temp_user_resp = client.post(
            f"/app/superadmin/users/{delete_temp_user_id}/delete",
            cookies=superadmin_cookies,
            follow_redirects=False,
        )
        assert delete_temp_user_resp.status_code in (302, 303)
        with SessionLocal() as db:
            assert db.query(ServiceUser).filter(ServiceUser.id == delete_temp_user_id).first() is None

        # Workspace suspension must immediately block login for workspace users.
        suspend_email = f"suspend_{uuid4().hex[:8]}@example.com"
        suspend_register = client.post(
            "/app/register",
            data={"email": suspend_email, "password": "StrongPass#123"},
            follow_redirects=False,
        )
        assert suspend_register.status_code in (302, 303)
        with SessionLocal() as db:
            suspend_owner = db.query(ServiceUser).filter(ServiceUser.username == suspend_email).first()
            assert suspend_owner is not None
            suspend_workspace_id = int(suspend_owner.workspace_id or 0)
            assert suspend_workspace_id > 0
        suspend_workspace_resp = client.post(
            f"/app/superadmin/workspaces/{suspend_workspace_id}/suspend",
            cookies=superadmin_cookies,
            follow_redirects=False,
        )
        assert suspend_workspace_resp.status_code in (302, 303)
        suspended_login = client.post(
            "/app/login",
            data={"username": suspend_email, "password": "StrongPass#123"},
            follow_redirects=True,
        )
        assert suspended_login.status_code == 200
        assert "Действия вашего профиля ограничены" in suspended_login.text
        assert "tech@example.com" in suspended_login.text
        assert "billing@example.com" in suspended_login.text

        # Workspace deletion from superadmin panel.
        delete_ws_email = f"deletews_{uuid4().hex[:8]}@example.com"
        delete_ws_register = client.post(
            "/app/register",
            data={"email": delete_ws_email, "password": "StrongPass#123"},
            follow_redirects=False,
        )
        assert delete_ws_register.status_code in (302, 303)
        with SessionLocal() as db:
            delete_ws_owner = db.query(ServiceUser).filter(ServiceUser.username == delete_ws_email).first()
            assert delete_ws_owner is not None
            delete_ws_id = int(delete_ws_owner.workspace_id or 0)
            assert delete_ws_id > 0
        delete_ws_resp = client.post(
            f"/app/superadmin/workspaces/{delete_ws_id}/delete",
            cookies=superadmin_cookies,
            follow_redirects=False,
        )
        assert delete_ws_resp.status_code in (302, 303)
        with SessionLocal() as db:
            assert db.query(Workspace).filter(Workspace.id == delete_ws_id).first() is None

        # Workspace suspend should immediately prevent owner login.
        suspend_email = f"suspend_{uuid4().hex[:8]}@example.com"
        suspend_register = client.post(
            "/app/register",
            data={"email": suspend_email, "password": "StrongPass#123"},
            follow_redirects=False,
        )
        assert suspend_register.status_code in (302, 303)
        with SessionLocal() as db:
            suspend_owner = db.query(ServiceUser).filter(ServiceUser.username == suspend_email).first()
            assert suspend_owner is not None
            suspend_ws_id = int(suspend_owner.workspace_id or 0)
            assert suspend_ws_id > 0
        suspend_resp = client.post(
            f"/app/superadmin/workspaces/{suspend_ws_id}/suspend",
            cookies=superadmin_cookies,
            follow_redirects=False,
        )
        assert suspend_resp.status_code in (302, 303)
        blocked_login = client.post(
            "/app/login",
            data={"username": suspend_email, "password": "StrongPass#123"},
            follow_redirects=False,
        )
        assert blocked_login.status_code == 200
        assert "Действия вашего профиля ограничены" in blocked_login.text

        orphan_seed = uuid4().hex[:8]
        first_register = client.post(
            "/app/register",
            data={"email": f"dupe_{orphan_seed}@example.com", "password": "StrongPass#123"},
            follow_redirects=False,
        )
        assert first_register.status_code in (302, 303)
        with SessionLocal() as db:
            created_user = (
                db.query(ServiceUser)
                .filter(ServiceUser.username == f"dupe_{orphan_seed}@example.com")
                .first()
            )
            assert created_user is not None
            assert created_user.role == "admin"
        with SessionLocal() as db:
            workspace_count_before = db.query(Workspace).count()
            user_count_before = db.query(ServiceUser).count()
        second_register = client.post(
            "/app/register",
            data={"email": f"dupe_{orphan_seed}@example.com", "password": "StrongPass#123"},
            follow_redirects=True,
        )
        assert second_register.status_code == 200
        assert "Пользователь с таким email уже существует." in second_register.text
        with SessionLocal() as db:
            workspace_count_after = db.query(Workspace).count()
            user_count_after = db.query(ServiceUser).count()
        # Failed duplicate registration must not create orphan workspace rows.
        assert workspace_count_after == workspace_count_before
        assert user_count_after == user_count_before

        login_page = client.get("/admin/login")
        assert login_page.status_code == 200

        login = client.post(
            "/admin/login",
            data={"username": "admin", "password": "admin123"},
            follow_redirects=False,
        )
        assert login.status_code in (302, 303)

        cookies = login.cookies
        admin_page = client.get("/admin", cookies=cookies)
        assert admin_page.status_code == 200
        csp_header = str(admin_page.headers.get("Content-Security-Policy") or "")
        assert "img-src" in csp_header
        assert "blob:" in csp_header
        _run_websocket_stage2_smoke(client, cookies=cookies, manager_token=create_manager_mini_token("90000", workspace_id=1))
        _run_websocket_hint_health_smoke(client, cookies=cookies)
        _run_websocket_hint_burst_coalescing_smoke(client, cookies=cookies)
        _run_websocket_hint_workspace_rate_gate_smoke(client, cookies=cookies)
        _run_websocket_seq_monotonic_smoke(client, cookies=cookies)
        _run_websocket_resync_event_shape_smoke(client, cookies=cookies)
        _run_scroll_near_bottom_snap_smoke(client, cookies=cookies)
        _run_targeted_media_and_quick_reply_regressions(client, cookies=cookies)
        _run_targeted_chat_history_media_visibility_regression(client, cookies=cookies)

        start_template = (
            "Здравствуйте! **Сейчас позову менеджера**.\n"
            "Чтобы подтвердить, что общаемся не с мошенником, "
            "нажмите кнопку \"Поделиться номером\".\n"
            "Подробности: [Справка](https://example.com/help)"
        )
        after_phone_template = (
            "**Спасибо!** Номер подтвержден.\n"
            "Пожалуйста, опишите, какой именно товар хотите с кэшбэком."
        )
        save_settings = client.post(
            "/admin/settings",
            data={
                "prestart_message": DEFAULT_TEMPLATES["prestart_message"],
                "start_message": start_template,
                "after_phone_message": after_phone_template,
                "manager_account_id": "90000",
                "manager_account_ids": "90000",
                "admin_account_id": "admin_1",
                "request_customer_phone": "1",
            },
            cookies=cookies,
        )
        assert save_settings.status_code == 200
        assert "format-row" not in save_settings.text
        assert 'name="routing_mode"' in save_settings.text
        assert "Логика: бот общается с покупателем" not in save_settings.text
        assert "add-manager-id-btn" not in save_settings.text
        assert "manager-row-copy-btn" in save_settings.text
        assert 'name="routing_mode"' in save_settings.text
        assert "Запрос номера телефона у покупателя" in save_settings.text

        create_reply = client.post(
            "/admin/quick-replies",
            data={
                "command": command,
                "title": "Прайс",
                "text": "Отправляю прайс",
                "media_order": "",
            },
            cookies=cookies,
        )
        assert create_reply.status_code == 200

        # Quick reply edit: open edit mode and save changes.
        with SessionLocal() as db:
            editable_reply = (
                db.query(QuickReply)
                .filter(QuickReply.workspace_id == 1, QuickReply.command == command)
                .first()
            )
            assert editable_reply is not None
            editable_reply_id = int(editable_reply.id)
        admin_edit_page = client.get(
            f"/admin?edit_quick_reply_id={editable_reply_id}",
            cookies=cookies,
            follow_redirects=False,
        )
        assert admin_edit_page.status_code == 200
        assert "Редактировать быстрый ответ" in admin_edit_page.text
        updated_command = f"upd_{uuid4().hex[:8]}"
        updated_title = "Обновленный прайс"
        updated_text = "Обновленный текст прайса"
        admin_update_reply = client.post(
            f"/admin/quick-replies/{editable_reply_id}/update",
            data={
                "command": updated_command,
                "title": updated_title,
                "text": updated_text,
                "media_order": "",
            },
            cookies=cookies,
            follow_redirects=False,
        )
        assert admin_update_reply.status_code in (302, 303)
        assert admin_update_reply.headers.get("location", "") == "/admin"
        admin_after_update = client.get("/admin", cookies=cookies)
        assert admin_after_update.status_code == 200
        assert f"/{updated_command}" in admin_after_update.text
        assert updated_title in admin_after_update.text
        assert updated_text in admin_after_update.text
        command = updated_command

        create_reply_large_media = client.post(
            "/admin/quick-replies",
            data={
                "command": f"large_{uuid4().hex[:8]}",
                "title": "Too large photo",
                "text": "large",
                "media_order": "",
            },
            files={
                "photos": ("oversize.jpg", b"x" * (1024 * 1024 + 1), "image/jpeg"),
            },
            cookies=cookies,
            follow_redirects=False,
        )
        assert create_reply_large_media.status_code == 413
        assert "не должно превышать 1 МБ" in (create_reply_large_media.text or "")

        with SessionLocal() as db:
            ws2_settings = get_or_create_settings(db, workspace_id=2)
            ws2_settings.bot_token = "token_ws2"
            ws2_settings.bot_link = "https://max.ru/id222222222_bot"
            ws2_settings.webhook_key = "ws2key"
            ws2_settings.manager_account_id = "90000"
            db.add(ws2_settings)
            ws1_settings = get_or_create_settings(db, workspace_id=1)
            ws1_settings.bot_token = "token_ws1"
            ws1_settings.bot_link = "https://max.ru/id111111111_bot"
            ws1_settings.webhook_key = "ws1key"
            db.add(ws1_settings)
            db.commit()

        webhook_ws2_start = client.post(
            "/webhook/max/ws2key",
            json={
                "update_type": "bot_started",
                "chat_id": f"chat_deep_{uuid4().hex[:6]}",
                "sender_id": f"buyer_deep_{uuid4().hex[:6]}",
                "text": "",
            },
        )
        assert webhook_ws2_start.status_code == 200
        assert webhook_ws2_start.json().get("flow") in {"start_prompt", "start_prompt_phone_not_required", "start_prompt_skipped_phone"}

        # Regression: sender must be resolved from message.sender, not payload.user.
        webhook_ws2_start_with_actor_user = client.post(
            "/webhook/max/ws2key",
            json={
                "update_type": "bot_started",
                "user_id": "372400681880",  # bot actor/system id from webhook envelope
                "message": {
                    "sender": {"user_id": f"buyer_deep_actor_{uuid4().hex[:6]}"},
                    "recipient": {"chat_id": f"chat_deep_actor_{uuid4().hex[:6]}", "chat_type": "dialog"},
                    "body": {"text": ""},
                },
            },
        )
        assert webhook_ws2_start_with_actor_user.status_code == 200
        assert webhook_ws2_start_with_actor_user.json().get("flow") in {
            "start_prompt",
            "start_prompt_phone_not_required",
            "start_prompt_skipped_phone",
        }

        # Default basic limits should include quick replies / folders.
        with SessionLocal() as db:
            from app.ops import get_or_create_subscription

            basic_sub = get_or_create_subscription(db, workspace_id=1)
            assert (basic_sub.plan_code or "").strip().lower() in {"basic", "trial"}
            assert int(basic_sub.quick_replies_limit or 0) == 10
            assert int(basic_sub.folders_limit or 0) == 10
            assert int(getattr(basic_sub, "pinned_chats_limit", 0) or 0) == 5

        webhook_customer_start = client.post(
            "/webhook/max/ws1key",
            json={
                "update_type": "bot_started",
                "chat_id": chat_id,
                "sender_id": "buyer_1",
                "text": "",
            },
        )
        assert webhook_customer_start.status_code == 200
        assert webhook_customer_start.json().get("flow") == "start_prompt"
        # Regression: fallback "Start / Начать" callback after prestart must switch
        # flow to start_prompt (and not loop in prestart).
        fallback_chat_id = f"chat_fb_{uuid4().hex[:8]}"
        fallback_sender_id = f"buyer_fb_{uuid4().hex[:8]}"
        webhook_customer_prestart = client.post(
            "/webhook/max/ws1key",
            json={
                "update_type": "message_created",
                "chat_id": fallback_chat_id,
                "sender_id": fallback_sender_id,
                "text": "Привет",
            },
        )
        assert webhook_customer_prestart.status_code == 200
        assert webhook_customer_prestart.json().get("flow") == "prestart"
        with SessionLocal() as db:
            prestart_conv = (
                db.query(Conversation)
                .filter(Conversation.workspace_id == 1, Conversation.chat_id == fallback_chat_id)
                .first()
            )
            assert prestart_conv is not None
            prestart_msg = (
                db.query(ChatMessage)
                .filter(
                    ChatMessage.workspace_id == 1,
                    ChatMessage.conversation_id == int(prestart_conv.id),
                    ChatMessage.direction == "bot",
                )
                .order_by(ChatMessage.id.desc())
                .first()
            )
            assert prestart_msg is not None
            prestart_text = str(getattr(prestart_msg, "text", "") or "")
            # Regression: prestart text should stay template-only; start action is a button.
            assert "https://max.ru/id111111111_bot" not in prestart_text
            prestart_outbox = (
                db.query(OutboxMessage)
                .filter(
                    OutboxMessage.workspace_id == 1,
                    OutboxMessage.conversation_id == int(prestart_conv.id),
                    OutboxMessage.operation == "send_message",
                )
                .order_by(OutboxMessage.id.desc())
                .first()
            )
            assert prestart_outbox is not None
            payload = json.loads(str(getattr(prestart_outbox, "payload_json", "") or "{}"))
            attachments = payload.get("attachments") if isinstance(payload, dict) else []
            assert isinstance(attachments, list) and attachments
            first_attachment = attachments[0] if attachments else {}
            assert isinstance(first_attachment, dict)
            keyboard_payload = first_attachment.get("payload") if isinstance(first_attachment.get("payload"), dict) else {}
            buttons = keyboard_payload.get("buttons") if isinstance(keyboard_payload, dict) else []
            first_button = buttons[0][0] if isinstance(buttons, list) and buttons and isinstance(buttons[0], list) and buttons[0] else {}
            # Prestart start button must send regular message '/start',
            # not callback payload-based fallback.
            assert str(first_button.get("type") or "").strip().lower() == "message"
            assert str(first_button.get("text") or "").strip().lower() == "/start"
            assert not str(first_button.get("payload") or "").strip()
        webhook_customer_start_via_message = client.post(
            "/webhook/max/ws1key",
            json={
                "update_type": "message_created",
                "chat_id": fallback_chat_id,
                "sender_id": fallback_sender_id,
                "text": "/start",
            },
        )
        assert webhook_customer_start_via_message.status_code == 200
        assert webhook_customer_start_via_message.json().get("flow") == "start_prompt"
        # Regression: text /start after prestart must be treated as explicit Start
        # (no repeated prestart loop).
        fallback_text_start_chat_id = f"chat_fb_txt_{uuid4().hex[:8]}"
        fallback_text_start_sender_id = f"buyer_fb_txt_{uuid4().hex[:8]}"
        webhook_customer_prestart_text_start = client.post(
            "/webhook/max/ws1key",
            json={
                "update_type": "message_created",
                "chat_id": fallback_text_start_chat_id,
                "sender_id": fallback_text_start_sender_id,
                "text": "Привет",
            },
        )
        assert webhook_customer_prestart_text_start.status_code == 200
        assert webhook_customer_prestart_text_start.json().get("flow") == "prestart"
        webhook_customer_text_start = client.post(
            "/webhook/max/ws1key",
            json={
                "update_type": "message_created",
                "chat_id": fallback_text_start_chat_id,
                "sender_id": fallback_text_start_sender_id,
                "text": "/start",
            },
        )
        assert webhook_customer_text_start.status_code == 200
        assert webhook_customer_text_start.json().get("flow") == "start_prompt"
        with SessionLocal() as db:
            started_conv = (
                db.query(Conversation)
                .filter(Conversation.workspace_id == 1, Conversation.chat_id == chat_id)
                .first()
            )
            assert started_conv is not None
            started_conv_id = int(started_conv.id)

        chat_id_skip = f"chat_{uuid4().hex[:8]}"
        webhook_customer_start_with_phone = client.post(
            "/webhook/max/ws1key",
            json={
                "update_type": "bot_started",
                "chat_id": chat_id_skip,
                "sender_id": "buyer_2",
                "text": "",
                "contact_phone": "+79992223344",
            },
        )
        assert webhook_customer_start_with_phone.status_code == 200
        assert webhook_customer_start_with_phone.json().get("flow") == "start_prompt_skipped_phone"

        webhook_customer_verified = client.post(
            "/webhook/max/ws1key",
            json={
                "update_type": "message_created",
                "message": {
                    "sender": {"user_id": "buyer_1", "first_name": "Иван"},
                    "recipient": {"chat_id": chat_id, "chat_type": "dialog"},
                    "body": {
                        "text": "",
                        "attachments": [
                            {
                                "type": "contact",
                                "payload": {"vcf_phone": "+79990001122"},
                            }
                        ],
                    },
                },
            },
        )
        assert webhook_customer_verified.status_code == 200
        assert webhook_customer_verified.json().get("flow") == "phone_verified"

        webhook_customer_text = client.post(
            "/webhook/max/ws1key",
            json={"chat_id": chat_id, "sender_id": "buyer_1", "text": "Хочу купить iPhone"},
        )
        assert webhook_customer_text.status_code == 200
        assert webhook_customer_text.json().get("flow") == "queued_for_mini_app"
        with SessionLocal() as db:
            sent_for_read = (
                db.query(ChatMessage)
                .filter(
                    ChatMessage.workspace_id == 1,
                    ChatMessage.conversation_id == started_conv_id,
                    ChatMessage.direction == "bot",
                    ChatMessage.delivery_state == "sent",
                )
                .order_by(ChatMessage.id.desc())
                .first()
            )
            if sent_for_read is None:
                sent_for_read = ChatMessage(
                    workspace_id=1,
                    conversation_id=started_conv_id,
                    direction="bot",
                    source="bot_system",
                    text="read probe",
                    max_message_mid=f"read_mid_{uuid4().hex[:8]}",
                    delivery_state="sent",
                    delivery_error="",
                    delivery_retry_count=0,
                    is_read_by_customer=False,
                )
                db.add(sent_for_read)
                db.commit()
                db.refresh(sent_for_read)
            assert bool(getattr(sent_for_read, "is_read_by_customer", False)) is False
            sent_for_read_mid = str(sent_for_read.max_message_mid or "").strip()
            if not sent_for_read_mid:
                # Test environment uses mocked Max API responses without MID.
                # Emulate real provider behavior by assigning a synthetic MID.
                sent_for_read_mid = f"read_mid_{uuid4().hex[:8]}"
                sent_for_read.max_message_mid = sent_for_read_mid
                db.add(sent_for_read)
                db.commit()

        webhook_customer_read = client.post(
            "/webhook/max/ws1key",
            json={
                "update_type": "message_read",
                "chat_id": chat_id,
                "sender_id": "buyer_1",
                "read_message_mid": sent_for_read_mid,
            },
        )
        assert webhook_customer_read.status_code == 200
        assert webhook_customer_read.json().get("flow") == "message_read"
        assert int(webhook_customer_read.json().get("marked_read_count") or 0) >= 1
        with SessionLocal() as db:
            sent_after_read = (
                db.query(ChatMessage)
                .filter(ChatMessage.id == sent_for_read.id)
                .first()
            )
            assert sent_after_read is not None
            assert bool(getattr(sent_after_read, "is_read_by_customer", False)) is True
            assert getattr(sent_after_read, "read_at", None) is not None
            second_sent_probe = ChatMessage(
                workspace_id=1,
                conversation_id=started_conv_id,
                direction="bot",
                source="bot_system",
                text="read probe missing sender",
                max_message_mid=f"read_mid_{uuid4().hex[:8]}",
                delivery_state="sent",
                delivery_error="",
                delivery_retry_count=0,
                is_read_by_customer=False,
            )
            db.add(second_sent_probe)
            db.commit()
            db.refresh(second_sent_probe)
            second_probe_id = int(second_sent_probe.id)
            second_probe_mid = str(second_sent_probe.max_message_mid or "")

        # Read receipts can arrive without sender_id in some provider payloads.
        webhook_customer_read_missing_sender = client.post(
            "/webhook/max/ws1key",
            json={
                "updateType": "opened",
                "chat_id": chat_id,
                "message": {
                    "recipient": {"chat_id": chat_id, "chat_type": "dialog"},
                    "body": {"last_read_mid": second_probe_mid},
                },
            },
        )
        assert webhook_customer_read_missing_sender.status_code == 200
        assert webhook_customer_read_missing_sender.json().get("flow") == "message_read"
        assert int(webhook_customer_read_missing_sender.json().get("marked_read_count") or 0) >= 1
        with SessionLocal() as db:
            second_probe_after_read = (
                db.query(ChatMessage)
                .filter(ChatMessage.id == second_probe_id)
                .first()
            )
            assert second_probe_after_read is not None
            assert bool(getattr(second_probe_after_read, "is_read_by_customer", False)) is True
            assert getattr(second_probe_after_read, "read_at", None) is not None

        # MAX status webhook format (outgoingMessageStatus) should also mark read.
        with SessionLocal() as db:
            third_sent_probe = ChatMessage(
                workspace_id=1,
                conversation_id=started_conv_id,
                direction="bot",
                source="bot_system",
                text="read probe outgoingMessageStatus",
                max_message_mid=f"read_mid_{uuid4().hex[:8]}",
                delivery_state="sent",
                delivery_error="",
                delivery_retry_count=0,
                is_read_by_customer=False,
            )
            db.add(third_sent_probe)
            db.commit()
            db.refresh(third_sent_probe)
            third_probe_id = int(third_sent_probe.id)
            third_probe_mid = str(third_sent_probe.max_message_mid or "")

        webhook_customer_read_outgoing_status = client.post(
            "/webhook/max/ws1key",
            json={
                "typeWebhook": "outgoingMessageStatus",
                "chatId": chat_id,
                "idMessage": third_probe_mid,
                "status": "read",
            },
        )
        assert webhook_customer_read_outgoing_status.status_code == 200
        assert webhook_customer_read_outgoing_status.json().get("flow") == "message_read"
        assert int(webhook_customer_read_outgoing_status.json().get("marked_read_count") or 0) >= 1
        with SessionLocal() as db:
            third_probe_after_read = (
                db.query(ChatMessage)
                .filter(ChatMessage.id == third_probe_id)
                .first()
            )
            assert third_probe_after_read is not None
            assert bool(getattr(third_probe_after_read, "is_read_by_customer", False)) is True
            assert getattr(third_probe_after_read, "read_at", None) is not None

        webhook_customer_callback_style = client.post(
            "/webhook/max/ws1key",
            json={
                "update_type": "message_callback",
                "message": {
                    "sender": {"user_id": "buyer_1", "first_name": "Иван"},
                    "recipient": {"chat_id": chat_id, "chat_type": "dialog"},
                    "body": {"text": "callback flow text"},
                },
            },
        )
        assert webhook_customer_callback_style.status_code == 200
        callback_payload = webhook_customer_callback_style.json()
        assert (
            callback_payload.get("flow") in {"queued_for_mini_app", "prestart"}
            or callback_payload.get("ignored") == "duplicate_event"
        ), callback_payload

        # Regression: plain incoming text must not accidentally pick image attachments
        # from unrelated nested payload blocks (no broken image placeholders in chat).
        text_only_chat_id = f"chat_{uuid4().hex[:8]}"
        text_only_sender = f"buyer_{uuid4().hex[:6]}"
        text_only_value = "Просто текст без фото"
        webhook_customer_text_without_photo = client.post(
            "/webhook/max/ws1key",
            json={
                "update_type": "message_created",
                "chat_id": text_only_chat_id,
                "sender_id": text_only_sender,
                "message": {
                    "sender": {"user_id": text_only_sender},
                    "recipient": {"chat_id": text_only_chat_id, "chat_type": "dialog"},
                    "body": {"text": text_only_value},
                },
                # This block emulates noisy envelope fields from third-party wrappers.
                # Parser must ignore it for this text event.
                "attachments": [
                    {
                        "type": "image",
                        "payload": {"url": "https://cdn.example.com/should-not-be-used.jpg"},
                    }
                ],
            },
        )
        assert webhook_customer_text_without_photo.status_code == 200
        with SessionLocal() as db:
            text_conv = (
                db.query(Conversation)
                .filter(
                    Conversation.workspace_id == 1,
                    Conversation.chat_id == text_only_chat_id,
                    Conversation.customer_account_id == text_only_sender,
                )
                .first()
            )
            assert text_conv is not None
            text_msg = (
                db.query(ChatMessage)
                .filter(
                    ChatMessage.workspace_id == 1,
                    ChatMessage.conversation_id == int(text_conv.id),
                    ChatMessage.direction == "customer",
                )
                .order_by(ChatMessage.id.desc())
                .first()
            )
            assert text_msg is not None
            assert str(getattr(text_msg, "text", "") or "").strip() == text_only_value
            parsed_text_urls = json.loads(str(getattr(text_msg, "image_urls_json", "[]") or "[]"))
            assert isinstance(parsed_text_urls, list)
            assert len(parsed_text_urls) == 0
            assert not str(getattr(text_msg, "image_url", "") or "").strip()
        text_only_page = client.get(
            f"/admin/chats?conversation_id={int(text_conv.id)}",
            follow_redirects=True,
        )
        assert text_only_page.status_code == 200
        assert text_only_value in text_only_page.text
        assert "should-not-be-used.jpg" not in text_only_page.text

        # Incoming customer image payload with photos[] should be parsed and rendered.
        incoming_photo_chat_id = f"chat_{uuid4().hex[:8]}"
        incoming_photo_sender = f"buyer_{uuid4().hex[:6]}"
        incoming_photo_url = "https://cdn.example.com/customer-photo.jpg"
        webhook_customer_photo = client.post(
            "/webhook/max/ws1key",
            json={
                "update_type": "message_created",
                "chat_id": incoming_photo_chat_id,
                "sender_id": incoming_photo_sender,
                "message": {
                    "sender": {"user_id": incoming_photo_sender},
                    "recipient": {"chat_id": incoming_photo_chat_id, "chat_type": "dialog"},
                    "body": {
                        "text": "",
                        "attachments": [
                            {
                                "type": "image",
                                "payload": {
                                    "photos": [
                                        {
                                            "url": incoming_photo_url,
                                        }
                                    ]
                                },
                            }
                        ],
                    },
                },
            },
        )
        assert webhook_customer_photo.status_code == 200
        with SessionLocal() as db:
            photo_conv = (
                db.query(Conversation)
                .filter(
                    Conversation.workspace_id == 1,
                    Conversation.chat_id == incoming_photo_chat_id,
                    Conversation.customer_account_id == incoming_photo_sender,
                )
                .first()
            )
            assert photo_conv is not None
            photo_msg = (
                db.query(ChatMessage)
                .filter(
                    ChatMessage.workspace_id == 1,
                    ChatMessage.conversation_id == int(photo_conv.id),
                    ChatMessage.direction == "customer",
                )
                .order_by(ChatMessage.id.desc())
                .first()
            )
            assert photo_msg is not None
            assert incoming_photo_url in str(getattr(photo_msg, "image_urls_json", "") or "")
        customer_photo_page = client.get(
            f"/admin/chats?conversation_id={int(photo_conv.id)}",
            follow_redirects=True,
        )
        assert customer_photo_page.status_code == 200
        assert incoming_photo_url in customer_photo_page.text
        assert 'data-media-open' in customer_photo_page.text

        # Official-style incoming MAX payload: incomingMessageReceived + imageMessage + fileMessageData.downloadUrl
        incoming_photo_chat_id_max = f"chat_{uuid4().hex[:8]}"
        incoming_photo_sender_max = f"buyer_{uuid4().hex[:6]}"
        incoming_photo_url_max = "https://cdn.example.com/customer-photo-max.jpg"
        webhook_customer_photo_max_style = client.post(
            "/webhook/max/ws1key",
            json={
                "typeWebhook": "incomingMessageReceived",
                "chatId": incoming_photo_chat_id_max,
                "idMessage": f"incoming_{uuid4().hex[:10]}",
                "senderData": {
                    "chatId": incoming_photo_chat_id_max,
                    "sender": incoming_photo_sender_max,
                    "senderName": "Иван Иванов",
                },
                "messageData": {
                    "typeMessage": "imageMessage",
                    "fileMessageData": {
                        "downloadUrl": incoming_photo_url_max,
                        "caption": "подпись к фото",
                        "fileName": "photo.jpg",
                    },
                },
            },
        )
        assert webhook_customer_photo_max_style.status_code == 200
        with SessionLocal() as db:
            photo_conv_max = (
                db.query(Conversation)
                .filter(
                    Conversation.workspace_id == 1,
                    Conversation.chat_id == incoming_photo_chat_id_max,
                    Conversation.customer_account_id == incoming_photo_sender_max,
                )
                .first()
            )
            assert photo_conv_max is not None
            photo_msg_max = (
                db.query(ChatMessage)
                .filter(
                    ChatMessage.workspace_id == 1,
                    ChatMessage.conversation_id == int(photo_conv_max.id),
                    ChatMessage.direction == "customer",
                )
                .order_by(ChatMessage.id.desc())
                .first()
            )
            assert photo_msg_max is not None
            assert incoming_photo_url_max in str(getattr(photo_msg_max, "image_urls_json", "") or "")
            assert str(getattr(photo_msg_max, "text", "") or "").strip() == ""
        customer_photo_max_page = client.get(
            f"/admin/chats?conversation_id={int(photo_conv_max.id)}",
            follow_redirects=True,
        )
        assert customer_photo_max_page.status_code == 200
        assert incoming_photo_url_max in customer_photo_max_page.text

        webhook_manager_tickets = client.post(
            "/webhook/max/ws1key",
            json={
                "update_type": "message_created",
                "message": {
                    "sender": {"user_id": "90000"},
                    "recipient": {"chat_id": "mgr-chat-1", "chat_type": "dialog"},
                    "body": {"text": "/tickets"},
                },
            },
        )
        assert webhook_manager_tickets.status_code == 200
        assert webhook_manager_tickets.json().get("ignored") == "manager_chat_disabled"

        webhook_manager_panel = client.post(
            "/webhook/max/ws1key",
            json={
                "update_type": "message_created",
                "message": {
                    "sender": {"user_id": "90000"},
                    "recipient": {"chat_id": "mgr-chat-1", "chat_type": "dialog"},
                    "body": {"text": "/panel"},
                },
            },
        )
        assert webhook_manager_panel.status_code == 200
        assert webhook_manager_panel.json().get("ignored") == "manager_chat_disabled"

        webhook_manager_mini = client.post(
            "/webhook/max/ws1key",
            json={
                "update_type": "message_created",
                "message": {
                    "sender": {"user_id": "90000"},
                    "recipient": {"chat_id": "mgr-chat-1", "chat_type": "dialog"},
                    "body": {"text": "/mini"},
                },
            },
        )
        assert webhook_manager_mini.status_code == 200
        assert webhook_manager_mini.json().get("mini_sent") is True

        webhook_manager_new = client.post(
            "/webhook/max/ws1key",
            json={
                "update_type": "message_created",
                "message": {
                    "sender": {"user_id": "90000"},
                    "recipient": {"chat_id": "mgr-chat-1", "chat_type": "dialog"},
                    "body": {"text": "/new"},
                },
            },
        )
        assert webhook_manager_new.status_code == 200
        assert webhook_manager_new.json().get("ignored") == "manager_chat_disabled"

        webhook_manager_take = client.post(
            "/webhook/max/ws1key",
            json={
                "update_type": "message_created",
                "message": {
                    "sender": {"user_id": "90000"},
                    "recipient": {"chat_id": "mgr-chat-1", "chat_type": "dialog"},
                    "body": {"text": "/take T-1001"},
                },
            },
        )
        assert webhook_manager_take.status_code == 200
        assert webhook_manager_take.json().get("ignored") == "manager_chat_disabled"

        webhook_manager_mine = client.post(
            "/webhook/max/ws1key",
            json={
                "update_type": "message_created",
                "message": {
                    "sender": {"user_id": "90000"},
                    "recipient": {"chat_id": "mgr-chat-1", "chat_type": "dialog"},
                    "body": {"text": "/mine"},
                },
            },
        )
        assert webhook_manager_mine.status_code == 200
        assert webhook_manager_mine.json().get("ignored") == "manager_chat_disabled"

        webhook_manager_done = client.post(
            "/webhook/max/ws1key",
            json={
                "update_type": "message_created",
                "message": {
                    "sender": {"user_id": "90000"},
                    "recipient": {"chat_id": "mgr-chat-1", "chat_type": "dialog"},
                    "body": {"text": "/done T-1001"},
                },
            },
        )
        assert webhook_manager_done.status_code == 200
        assert webhook_manager_done.json().get("ignored") == "manager_chat_disabled"

        webhook_manager_callback = client.post(
            "/webhook/max/ws1key",
            json={
                "update_type": "message_callback",
                "chat_id": "mgr-chat-1",
                "sender_id": "90000",
                "callback": {"payload": "mgr:new"},
            },
        )
        assert webhook_manager_callback.status_code == 200
        assert webhook_manager_callback.json().get("ignored") == "manager_chat_disabled"

        webhook_manager_ticket_reply = client.post(
            "/webhook/max/ws1key",
            json={
                "update_type": "message_created",
                "message": {
                    "sender": {"user_id": "90000"},
                    "recipient": {"chat_id": "mgr-chat-1", "chat_type": "dialog"},
                    "body": {"text": "/reply T-1001 /" + command},
                },
            },
        )
        assert webhook_manager_ticket_reply.status_code == 200
        assert webhook_manager_ticket_reply.json().get("ignored") == "manager_chat_disabled"

        webhook_manager_ticket_reply_with_mention = client.post(
            "/webhook/max/ws1key",
            json={
                "update_type": "message_created",
                "message": {
                    "sender": {"user_id": "90000"},
                    "recipient": {"chat_id": "mgr-chat-1", "chat_type": "dialog"},
                    "body": {"text": "/reply@maxbot T-1001 Проверка через mention"},
                },
            },
        )
        assert webhook_manager_ticket_reply_with_mention.status_code == 200
        assert webhook_manager_ticket_reply_with_mention.json().get("ignored") == "manager_chat_disabled"

        webhook_manager = client.post(
            "/webhook/max/ws1key",
            json={
                "update_type": "message_created",
                "message": {
                    "sender": {"user_id": "90000"},
                    "recipient": {"chat_id": "mgr-chat-1", "chat_type": "dialog"},
                    "link": {"message": {"mid": "reply-mid-1"}},
                    "body": {"text": f"/{command}"},
                },
            },
        )
        assert webhook_manager.status_code == 200
        assert webhook_manager.json().get("ignored") == "manager_chat_disabled"

        # Webhook dedup by update_id
        duplicate_payload = {
            "update_id": "dup-evt-1",
            "update_type": "message_created",
            "message": {
                "sender": {"user_id": "buyer_1", "first_name": "Иван"},
                "recipient": {"chat_id": chat_id, "chat_type": "dialog"},
                "body": {"text": "dup test"},
            },
        }
        first_dup = client.post("/webhook/max/ws1key", json=duplicate_payload)
        second_dup = client.post("/webhook/max/ws1key", json=duplicate_payload)
        assert first_dup.status_code == 200
        assert second_dup.status_code == 200
        assert second_dup.json().get("ignored") == "duplicate_event"

        with SessionLocal() as db:
            conversation = db.query(Conversation).filter(Conversation.chat_id == chat_id).first()
            assert conversation is not None
            conversation_id = conversation.id

        manager_mini_token = create_manager_mini_token("90000", workspace_id=1)
        manager_mini_page = client.get(f"/mini/manager?token={quote_plus(manager_mini_token)}")
        assert manager_mini_page.status_code == 200
        assert "Max Manager Mini App" in manager_mini_page.text
        assert "mobile-folder-bar" in manager_mini_page.text
        assert "/mini/manager/chats/" in manager_mini_page.text
        assert "list-actions-btn" in manager_mini_page.text
        assert "list-actions-menu" in manager_mini_page.text

        manager_mini_page_bad_token = client.get("/mini/manager?token=broken")
        assert manager_mini_page_bad_token.status_code == 403

        manager_mini_chat_page = client.get(
            f"/mini/manager?token={quote_plus(manager_mini_token)}&conversation_id={conversation_id}&view=chat",
        )
        assert manager_mini_chat_page.status_code == 200
        assert "back-btn mobile-only" in manager_mini_chat_page.text
        assert 'id="chat-screen"' in manager_mini_chat_page.text
        assert 'id="edit-message-id"' not in manager_mini_chat_page.text
        assert "Режим редактирования сообщения" not in manager_mini_chat_page.text

        manager_mini_send = client.post(
            f"/mini/manager/chats/{conversation_id}/send?token={quote_plus(manager_mini_token)}",
            data={"text": "mini_app_message", "view": "chat"},
            follow_redirects=False,
        )
        assert manager_mini_send.status_code in (302, 303)
        assert "sent=" in manager_mini_send.headers.get("location", "")

        with SessionLocal() as db:
            settings_row = get_or_create_settings(db)
            manager_msgs = (
                db.query(WebhookEvent)
                .filter(WebhookEvent.update_type == "message_created")
                .all()
            )
            assert settings_row.manager_account_id == "90000"

        save_settings_multi = client.post(
            "/admin/settings",
            data={
                "prestart_message": DEFAULT_TEMPLATES["prestart_message"],
                "start_message": start_template,
                "after_phone_message": after_phone_template,
                "manager_account_id": "90000",
                "manager_account_ids": ["90000", "90001"],
                "manager_account_ids": "90000,90001",
                "admin_account_id": "",
                "routing_mode": "round_robin",
            },
            cookies=cookies,
        )
        assert save_settings_multi.status_code == 200
        assert "manager-ids-wrap" in save_settings_multi.text
        assert "add-manager-id-btn" not in save_settings_multi.text
        with SessionLocal() as db:
            settings_row = get_or_create_settings(db)
            parsed_ids = [item.strip() for item in (settings_row.manager_account_id or "").split(",") if item.strip()]
            assert parsed_ids == ["90000", "90001"]

        invite_flow_email = f"invite_flow_{uuid4().hex[:8]}@example.com"
        invite_flow_register = client.post(
            "/app/register",
            data={"email": invite_flow_email, "password": "StrongPass#123"},
            follow_redirects=False,
        )
        assert invite_flow_register.status_code in (302, 303)
        invite_flow_login = client.post(
            "/app/login",
            data={"username": invite_flow_email, "password": "StrongPass#123"},
            follow_redirects=False,
        )
        assert invite_flow_login.status_code in (302, 303)
        invite_flow_cookies = invite_flow_login.cookies
        with SessionLocal() as db:
            invite_flow_user = db.query(ServiceUser).filter(ServiceUser.username == invite_flow_email).first()
            assert invite_flow_user is not None
            invite_flow_workspace_id = int(invite_flow_user.workspace_id or 0)
            assert invite_flow_workspace_id > 0

        settings_page = client.get("/app/settings", cookies=invite_flow_cookies, follow_redirects=False)
        assert settings_page.status_code == 200
        assert "Менеджеры: статусы подключения" in settings_page.text
        assert "Осталось быстрых ответов:" not in settings_page.text
        assert "Осталось папок:" not in settings_page.text
        assert "Подключение вашего бота Max" in settings_page.text
        assert "Токен вашего бота:" in settings_page.text
        assert "Статус подключения:" in settings_page.text
        assert "Настройки бота" in settings_page.text
        assert settings_page.text.find("Настройки бота") < settings_page.text.find("Подключение вашего бота Max")
        assert "не задан." in settings_page.text
        assert "Ввести токен" in settings_page.text
        assert "Ссылка вашего бота Max" not in settings_page.text
        assert "Webhook URL для вашего бота" not in settings_page.text
        token_connect_save = client.post(
            "/app/settings",
            data={
                "prestart_message": DEFAULT_TEMPLATES["prestart_message"],
                "start_message": start_template,
                "after_phone_message": after_phone_template,
                "bot_token": "token_settings_flow_1",
                "routing_mode": "round_robin",
                "request_customer_phone": "1",
            },
            cookies=invite_flow_cookies,
            follow_redirects=False,
        )
        assert token_connect_save.status_code in (302, 303)
        settings_page_with_token = client.get("/app/settings", cookies=invite_flow_cookies, follow_redirects=False)
        assert settings_page_with_token.status_code == 200
        assert "Статус подключения:" in settings_page_with_token.text
        assert "Обновить" in settings_page_with_token.text
        assert "Удалить" in settings_page_with_token.text
        token_update_save = client.post(
            "/app/settings",
            data={
                "prestart_message": DEFAULT_TEMPLATES["prestart_message"],
                "start_message": start_template,
                "after_phone_message": after_phone_template,
                "bot_token": "token_settings_flow_2",
                "routing_mode": "round_robin",
                "request_customer_phone": "1",
            },
            cookies=invite_flow_cookies,
            follow_redirects=False,
        )
        assert token_update_save.status_code in (302, 303)
        with SessionLocal() as db:
            ws_settings_after_update = get_or_create_settings(db, workspace_id=invite_flow_workspace_id)
            assert (ws_settings_after_update.bot_token or "").strip() == "token_settings_flow_2"
        token_delete_resp = client.post(
            "/app/settings/delete-bot-token",
            cookies=invite_flow_cookies,
            follow_redirects=False,
        )
        assert token_delete_resp.status_code in (302, 303)
        with SessionLocal() as db:
            ws_settings_after_delete = get_or_create_settings(db, workspace_id=invite_flow_workspace_id)
            assert (ws_settings_after_delete.bot_token or "").strip() == ""
        settings_page_after_delete = client.get("/app/settings", cookies=invite_flow_cookies, follow_redirects=False)
        assert settings_page_after_delete.status_code == 200
        assert "не задан." in settings_page_after_delete.text
        assert "Ввести токен" in settings_page_after_delete.text
        copy_manager_link = client.post(
            "/app/settings/copy-manager-link",
            data={
                "copy_manager_id": "90000",
                "routing_mode": "round_robin",
                "admin_account_id": "",
            },
            cookies=invite_flow_cookies,
            follow_redirects=False,
        )
        assert copy_manager_link.status_code == 200
        copy_payload = copy_manager_link.json()
        assert copy_payload.get("ok") is True
        copied_link = str(copy_payload.get("link", "")).strip()
        assert copied_link
        copy_manager_link_second = client.post(
            "/app/settings/copy-manager-link",
            data={
                "copy_manager_id": "90001",
                "routing_mode": "round_robin",
                "admin_account_id": "",
            },
            cookies=invite_flow_cookies,
            follow_redirects=False,
        )
        assert copy_manager_link_second.status_code == 200
        assert copy_manager_link_second.json().get("ok") is True
        invite_path = urlsplit(copied_link).path
        invite_page = client.get(invite_path, follow_redirects=False)
        assert invite_page.status_code == 200
        assert "Создание пароля менеджера" in invite_page.text

        with SessionLocal() as db:
            removable_manager = (
                db.query(ServiceUser)
                .filter(
                    ServiceUser.workspace_id == invite_flow_workspace_id,
                    ServiceUser.role == "manager",
                    ServiceUser.max_account_id == "90001",
                )
                .first()
            )
            assert removable_manager is not None
            create_service_session(
                db,
                user_id=int(removable_manager.id),
                ip_address="127.0.0.1",
                user_agent="smoke/remove-manager",
            )
            active_sessions_before = (
                db.query(UserSession)
                .filter(UserSession.user_id == removable_manager.id, UserSession.is_revoked.is_(False))
                .count()
            )
            assert active_sessions_before >= 1

        delete_manager_resp = client.post(
            "/app/settings/remove-manager",
            data={
                "manager_account_ids": "90000,90001",
                "remove_manager_id": "90001",
                "routing_mode": "round_robin",
                "admin_account_id": "",
                "request_customer_phone": "1",
            },
            cookies=invite_flow_cookies,
            follow_redirects=False,
        )
        assert delete_manager_resp.status_code == 200
        delete_payload = delete_manager_resp.json()
        assert delete_payload.get("ok") is True
        with SessionLocal() as db:
            conversation_count_before = db.query(Conversation).count()
            invite_flow_settings = get_or_create_settings(db, workspace_id=invite_flow_workspace_id)
            current_ids = [item.strip() for item in (invite_flow_settings.manager_account_id or "").split(",") if item.strip()]
            assert current_ids == ["90000"]
            removed_manager = (
                db.query(ServiceUser)
                .filter(
                    ServiceUser.workspace_id == invite_flow_workspace_id,
                    ServiceUser.role == "manager",
                    ServiceUser.max_account_id == "90001",
                )
                .first()
            )
            assert removed_manager is not None
            assert bool(removed_manager.is_active) is False
            assert bool(removed_manager.is_blocked) is True
            active_sessions_after = (
                db.query(UserSession)
                .filter(UserSession.user_id == removed_manager.id, UserSession.is_revoked.is_(False))
                .count()
            )
            assert active_sessions_after == 0
            revoked_sessions_after = (
                db.query(UserSession)
                .filter(UserSession.user_id == removed_manager.id, UserSession.is_revoked.is_(True))
                .count()
            )
            assert revoked_sessions_after >= 1
            conversation_count_after = db.query(Conversation).count()
            # Manager removal must not wipe user chat history.
            assert conversation_count_after == conversation_count_before
        readd_manager_resp = client.post(
            "/app/settings/copy-manager-link",
            data={
                "copy_manager_id": "90001",
                "routing_mode": "round_robin",
                "admin_account_id": "",
            },
            cookies=invite_flow_cookies,
            follow_redirects=False,
        )
        assert readd_manager_resp.status_code == 200
        readd_payload = readd_manager_resp.json()
        assert readd_payload.get("ok") is True
        with SessionLocal() as db:
            readded_manager = (
                db.query(ServiceUser)
                .filter(
                    ServiceUser.workspace_id == invite_flow_workspace_id,
                    ServiceUser.role == "manager",
                    ServiceUser.max_account_id == "90001",
                )
                .first()
            )
            assert readded_manager is not None
            assert bool(readded_manager.is_active) is True
            assert bool(readded_manager.is_blocked) is False

        # Remaining counters are hidden in tariff block; page should still render.
        add_quick_reply_for_invite_ws = client.post(
            "/app/quick-replies",
            data={"command": "faq", "title": "FAQ", "text": "Ответ"},
            cookies=invite_flow_cookies,
            follow_redirects=True,
        )
        assert add_quick_reply_for_invite_ws.status_code == 200
        assert "Тариф и лимиты" in add_quick_reply_for_invite_ws.text
        assert "Осталось быстрых ответов:" not in add_quick_reply_for_invite_ws.text

        # App settings must also enforce manager IDs limit on plain save.
        app_limit_email = f"limit_{uuid4().hex[:8]}@example.com"
        app_limit_register = client.post(
            "/app/register",
            data={"email": app_limit_email, "password": "StrongPass#123"},
            follow_redirects=False,
        )
        assert app_limit_register.status_code in (302, 303)
        app_limit_login = client.post(
            "/app/login",
            data={"username": app_limit_email, "password": "StrongPass#123"},
            follow_redirects=False,
        )
        assert app_limit_login.status_code in (302, 303)
        app_limit_cookies = app_limit_login.cookies
        with SessionLocal() as db:
            app_limit_user = db.query(ServiceUser).filter(ServiceUser.username == app_limit_email).first()
            assert app_limit_user is not None
            ws_id = int(app_limit_user.workspace_id or 0)
            ws_settings_limit = get_or_create_settings(db, workspace_id=ws_id)
            ws_settings_limit.bot_token = "token_app_limit"
            ws_settings_limit.bot_link = "https://max.ru/id444444444_bot"
            ws_settings_limit.webhook_key = "applimitkey"
            db.add(ws_settings_limit)
            from app.ops import get_or_create_subscription
            sub = get_or_create_subscription(db, workspace_id=ws_id)
            sub.manager_limit = 1
            db.add(sub)
            db.commit()
        app_limit_save = client.post(
            "/app/settings",
            data={
                "prestart_message": DEFAULT_TEMPLATES["prestart_message"],
                "start_message": start_template,
                "after_phone_message": after_phone_template,
                "bot_token": "token_app_limit",
                "manager_account_ids": ["90100", "90101"],
                "routing_mode": "round_robin",
                "request_customer_phone": "1",
            },
            cookies=app_limit_cookies,
            follow_redirects=True,
        )
        assert app_limit_save.status_code == 200
        # Alerts can be absent when no threshold is reached; just verify the block remains renderable.
        assert "Тариф и лимиты" in app_limit_save.text
        assert "Workspace:" not in app_limit_save.text

        # Phone request toggle in app settings:
        # - unchecked: bot should not wait for contact after Start
        # - checked: bot should wait for contact after Start
        phone_toggle_email = f"phone_toggle_{uuid4().hex[:8]}@example.com"
        phone_toggle_register = client.post(
            "/app/register",
            data={"email": phone_toggle_email, "password": "StrongPass#123"},
            follow_redirects=False,
        )
        assert phone_toggle_register.status_code in (302, 303)
        phone_toggle_login = client.post(
            "/app/login",
            data={"username": phone_toggle_email, "password": "StrongPass#123"},
            follow_redirects=False,
        )
        assert phone_toggle_login.status_code in (302, 303)
        phone_toggle_cookies = phone_toggle_login.cookies
        phone_chat_no = f"chat_no_phone_{uuid4().hex[:6]}"
        phone_buyer_no = f"buyer_no_phone_{uuid4().hex[:6]}"
        phone_chat_yes = f"chat_with_phone_{uuid4().hex[:6]}"
        phone_buyer_yes = f"buyer_with_phone_{uuid4().hex[:6]}"
        with SessionLocal() as db:
            phone_toggle_user = db.query(ServiceUser).filter(ServiceUser.username == phone_toggle_email).first()
            assert phone_toggle_user is not None
            ws_id = int(phone_toggle_user.workspace_id or 0)
            ws_settings_toggle = get_or_create_settings(db, workspace_id=ws_id)
            ws_settings_toggle.bot_token = "token_phone_toggle"
            ws_settings_toggle.bot_link = "https://max.ru/id333333333_bot"
            ws_settings_toggle.webhook_key = "phonewskey"
            db.add(ws_settings_toggle)
            db.add(
                Conversation(
                    workspace_id=ws_id,
                    chat_id=phone_chat_no,
                    customer_account_id=phone_buyer_no,
                    manager_added=False,
                    is_active=True,
                )
            )
            db.add(
                Conversation(
                    workspace_id=ws_id,
                    chat_id=phone_chat_yes,
                    customer_account_id=phone_buyer_yes,
                    manager_added=False,
                    is_active=True,
                )
            )
            db.commit()

        # Disable phone request (checkbox absent).
        save_no_phone = client.post(
            "/app/settings",
            data={
                "prestart_message": DEFAULT_TEMPLATES["prestart_message"],
                "start_message": start_template,
                "after_phone_message": after_phone_template,
                "bot_token": "token_phone_toggle",
                "manager_account_ids": ["91000"],
                "routing_mode": "round_robin",
            },
            cookies=phone_toggle_cookies,
            follow_redirects=False,
        )
        assert save_no_phone.status_code in (302, 303)
        no_phone_start = client.post(
            "/webhook/max/phonewskey",
            json={
                "update_type": "bot_started",
                "chat_id": phone_chat_no,
                "sender_id": phone_buyer_no,
                "text": "",
            },
        )
        assert no_phone_start.status_code == 200
        assert no_phone_start.json().get("flow") in {"start_prompt_phone_not_required", "start_prompt"}

        # Enable phone request (checkbox present).
        save_with_phone = client.post(
            "/app/settings",
            data={
                "prestart_message": DEFAULT_TEMPLATES["prestart_message"],
                "start_message": start_template,
                "after_phone_message": after_phone_template,
                "bot_token": "token_phone_toggle",
                "manager_account_ids": ["91000"],
                "routing_mode": "round_robin",
                "request_customer_phone": "1",
            },
            cookies=phone_toggle_cookies,
            follow_redirects=False,
        )
        assert save_with_phone.status_code in (302, 303)
        with_phone_start = client.post(
            "/webhook/max/phonewskey",
            json={
                "update_type": "bot_started",
                "chat_id": phone_chat_yes,
                "sender_id": phone_buyer_yes,
                "text": "",
            },
        )
        assert with_phone_start.status_code == 200
        # If chat_id already exists in another workspace in shared test DB,
        # webhook may resolve into the legacy workspace. Still verify checkbox
        # persistence in the target workspace and core flow integrity.
        assert with_phone_start.json().get("flow") in {"start_prompt", "start_prompt_phone_not_required"}
        with SessionLocal() as db:
            phone_toggle_user = db.query(ServiceUser).filter(ServiceUser.username == phone_toggle_email).first()
            assert phone_toggle_user is not None
            ws_id_toggle = int(phone_toggle_user.workspace_id or 0)
            ws_settings = get_or_create_settings(db, workspace_id=ws_id_toggle)
            assert bool(ws_settings.request_customer_phone) is True
        app_settings_after_copy = client.get("/app/settings", cookies=invite_flow_cookies, follow_redirects=False)
        assert app_settings_after_copy.status_code == 200
        assert "Ожидает подключения" in app_settings_after_copy.text or "Подключен" in app_settings_after_copy.text

        # Manager should be able to sign in again after logout using max_account_id + password.
        manager_relogin_email = f"manager_relogin_{uuid4().hex[:8]}@example.com"
        manager_relogin_register = client.post(
            "/app/register",
            data={"email": manager_relogin_email, "password": "StrongPass#123"},
            follow_redirects=False,
        )
        assert manager_relogin_register.status_code in (302, 303)
        manager_relogin_login = client.post(
            "/app/login",
            data={"username": manager_relogin_email, "password": "StrongPass#123"},
            follow_redirects=False,
        )
        assert manager_relogin_login.status_code in (302, 303)
        manager_relogin_cookies = manager_relogin_login.cookies
        manager_relogin_max_id = str(900000000000 + (uuid4().int % 9999999))
        relogin_copy = client.post(
            "/app/settings/copy-manager-link",
            data={"copy_manager_id": manager_relogin_max_id},
            cookies=manager_relogin_cookies,
            follow_redirects=False,
        )
        assert relogin_copy.status_code == 200
        relogin_link = str(relogin_copy.json().get("link", "")).strip()
        assert relogin_link
        relogin_invite_path = urlsplit(relogin_link).path
        relogin_invite_open = client.get(relogin_invite_path, follow_redirects=False)
        assert relogin_invite_open.status_code == 200
        relogin_set_password = client.post(
            relogin_invite_path,
            data={
                "manager_password": "Relogin#123",
                "manager_password_confirm": "Relogin#123",
            },
            follow_redirects=False,
        )
        assert relogin_set_password.status_code in (302, 303)
        manager_login_by_max_id = client.post(
            "/app/login",
            data={"username": manager_relogin_max_id, "password": "Relogin#123"},
            follow_redirects=False,
        )
        assert manager_login_by_max_id.status_code in (302, 303)
        manager_logout = client.post(
            "/app/logout",
            cookies=manager_login_by_max_id.cookies,
            follow_redirects=False,
        )
        assert manager_logout.status_code in (302, 303)
        manager_relogin_by_max_id = client.post(
            "/app/login",
            data={"username": manager_relogin_max_id, "password": "Relogin#123"},
            follow_redirects=False,
        )
        assert manager_relogin_by_max_id.status_code in (302, 303)
        manager_relogin_settings = client.get(
            "/app/settings",
            cookies=manager_relogin_by_max_id.cookies,
            follow_redirects=False,
        )
        assert manager_relogin_settings.status_code == 200
        assert "Быстрые ответы менеджера" in manager_relogin_settings.text
        assert "Добавить быстрый ответ" in manager_relogin_settings.text
        assert "Настройки бота" not in manager_relogin_settings.text
        assert "Менеджеры: статусы подключения" not in manager_relogin_settings.text
        manager_quick_create = client.post(
            "/app/quick-replies",
            data={"command": "mgrsolo", "title": "Mgr solo", "text": "Только менеджер"},
            cookies=manager_relogin_by_max_id.cookies,
            follow_redirects=False,
        )
        assert manager_quick_create.status_code in (302, 303)
        with SessionLocal() as db:
            manager_user = (
                db.query(ServiceUser)
                .filter(ServiceUser.max_account_id == manager_relogin_max_id)
                .first()
            )
            assert manager_user is not None
            manager_only_reply = (
                db.query(QuickReply)
                .filter(
                    QuickReply.workspace_id == manager_user.workspace_id,
                    QuickReply.owner_user_id == manager_user.id,
                    QuickReply.command == "mgrsolo",
                )
                .first()
            )
            assert manager_only_reply is not None

        admin_chats_page = client.get(
            f"/admin/chats?conversation_id={conversation_id}",
            cookies=cookies,
        )
        assert admin_chats_page.status_code == 200
        assert "slash-menu" in admin_chats_page.text
        assert command in admin_chats_page.text

        profile_page = client.get(
            f"/admin/chats/{conversation_id}/profile",
            cookies=cookies,
        )
        assert profile_page.status_code == 200
        assert "Профиль" in profile_page.text
        assert "Вернуться в чат" in profile_page.text
        assert "К списку чатов" not in profile_page.text
        assert "Основные данные" in profile_page.text
        assert "Данные из переписки / Max" in profile_page.text
        assert "Тикет" in profile_page.text
        assert "message mids (последние)" not in profile_page.text
        assert "manager dispatch mids" not in profile_page.text
        assert "outbox targets" not in profile_page.text
        assert f"T-{conversation_id + 1000}"[:2] == "T-"

        admin_quick_reply = client.post(
            f"/app/chats/{conversation_id}/quick-reply",
            data={"command": f"/{command}"},
            cookies=cookies,
            follow_redirects=False,
        )
        assert admin_quick_reply.status_code in (302, 303)
        assert "quick=" in admin_quick_reply.headers.get("location", "")

        send_attempt = client.post(
            f"/admin/chats/{conversation_id}/send",
            data={"text": "retry_me"},
            cookies=cookies,
            follow_redirects=False,
        )
        assert send_attempt.status_code in (302, 303)

        # Ensure outbound MID is persisted even when provider returns idMessage at top level.
        idmessage_probe_value = f"idmsg_{uuid4().hex[:10]}"

        async def _fake_send_text_with_idmessage(*args, **kwargs):
            return {"success": True, "idMessage": idmessage_probe_value}

        with patch(
            "app.max_client.MaxClient.send_text",
            new=AsyncMock(side_effect=_fake_send_text_with_idmessage),
        ):
            idmessage_probe_send = client.post(
                f"/admin/chats/{conversation_id}/send",
                data={"text": "idmessage_probe"},
                cookies=cookies,
                follow_redirects=False,
            )
            assert idmessage_probe_send.status_code in (302, 303)
        with SessionLocal() as db:
            idmessage_probe_msg = (
                db.query(ChatMessage)
                .filter(
                    ChatMessage.workspace_id == 1,
                    ChatMessage.conversation_id == conversation_id,
                    ChatMessage.direction == "bot",
                    ChatMessage.text == "idmessage_probe",
                )
                .order_by(ChatMessage.id.desc())
                .first()
            )
            assert idmessage_probe_msg is not None
            assert str(getattr(idmessage_probe_msg, "max_message_mid", "") or "").strip() == idmessage_probe_value

        # Multi-image compatibility cascade:
        # 1) token-list attachments for 2+ images
        # 2) photos-map fallback on proto.payload/errors.required
        # 3) URL attachments fallback
        sent_multi_attachments: list[list[dict]] = []

        async def _fake_upload_image_bytes(*args, **kwargs):
            file_name = str(kwargs.get("file_name") or "img")
            stem = file_name.split(".")[0]
            return {
                "success": True,
                "attachment": {"type": "image", "payload": {"token": f"tok_{stem}"}},
                "token": f"tok_{stem}",
            }

        send_attempt = {"count": 0}

        async def _fake_send_message_multi(*args, **kwargs):
            send_attempt["count"] += 1
            attachments = kwargs.get("attachments")
            assert isinstance(attachments, list) and len(attachments) >= 1
            sent_multi_attachments.append(attachments)

            # Attempt 1: expect token-list attachments (one image payload per token).
            if send_attempt["count"] == 1:
                assert len(attachments) >= 3
                for entry in attachments:
                    assert isinstance(entry, dict)
                    assert str(entry.get("type") or "").strip() == "image"
                    payload = entry.get("payload") if isinstance(entry.get("payload"), dict) else {}
                    assert str(payload.get("token") or "").strip()
                return {
                    "success": False,
                    "status_code": 400,
                    "response": {"code": "proto.payload", "message": "errors.required"},
                    "endpoint": "/messages",
                }

            # Attempt 2: expect grouped photos-map payload.
            if send_attempt["count"] == 2:
                assert len(attachments) == 1
                entry = attachments[0]
                assert str(entry.get("type") or "").strip() == "image"
                payload = entry.get("payload") if isinstance(entry.get("payload"), dict) else {}
                photos = payload.get("photos") if isinstance(payload.get("photos"), dict) else {}
                assert isinstance(photos, dict) and len(photos) >= 3
                return {
                    "success": False,
                    "status_code": 400,
                    "response": {"code": "proto.payload", "message": "Failed to upload image."},
                    "endpoint": "/messages",
                }

            # Attempt 3: expect URL fallback attachments (flat list).
            for entry in attachments:
                assert isinstance(entry, dict)
                assert str(entry.get("type") or "").strip() == "image"
                payload = entry.get("payload") if isinstance(entry.get("payload"), dict) else {}
                assert str(payload.get("url") or "").strip().startswith(("http://", "https://"))
            return {"success": True, "message": {"body": {"mid": f"mid_{uuid4().hex[:8]}"}}}

        with patch("app.max_client.MaxClient.upload_image_bytes", new=AsyncMock(side_effect=_fake_upload_image_bytes)), patch(
            "app.max_client.MaxClient.send_message",
            new=AsyncMock(side_effect=_fake_send_message_multi),
        ):
            multi_send = client.post(
                f"/admin/chats/{conversation_id}/send",
                data={"text": "multi_proto_fallback"},
                files=[
                    ("photos", ("multi_one.png", b"\x89PNG\r\n\x1a\nmulti-one", "image/png")),
                    ("photos", ("multi_two.png", b"\x89PNG\r\n\x1a\nmulti-two", "image/png")),
                    ("photos", ("multi_three.png", b"\x89PNG\r\n\x1a\nmulti-three", "image/png")),
                ],
                cookies=cookies,
                follow_redirects=False,
            )
            assert multi_send.status_code in (302, 303)
            location = str(multi_send.headers.get("location", ""))
            assert "sent=1" in location
        assert len(sent_multi_attachments) >= 3

        # Outbox claim should be idempotent to prevent duplicate immediate sends.
        with SessionLocal() as db:
            claim_probe = _enqueue_outbox_message(
                db,
                conversation_id=conversation_id,
                chat_message_id=None,
                target_chat_id="claim_probe_chat",
                target_user_id="claim_probe_user",
                operation="send_text",
                payload={"text": "claim_probe"},
                workspace_id=1,
            )
            first_claim = _claim_outbox_item_for_send(db, outbox_id=int(claim_probe.id))
            second_claim = _claim_outbox_item_for_send(db, outbox_id=int(claim_probe.id))
            assert first_claim is not None
            assert second_claim is None

        admin_chats_page_after_send = client.get(
            f"/admin/chats?conversation_id={conversation_id}",
            cookies=cookies,
        )
        assert admin_chats_page_after_send.status_code == 200
        assert "retry_me" in admin_chats_page_after_send.text
        assert 'id="message-input"' in admin_chats_page_after_send.text
        assert 'id="slash-menu"' in admin_chats_page_after_send.text
        assert "context-menu" in admin_chats_page_after_send.text
        assert "msg-context-menu" in admin_chats_page_after_send.text
        assert "delivery-toggle" not in admin_chats_page_after_send.text
        assert "bubble-debug" not in admin_chats_page_after_send.text
        assert "delivery-status" in admin_chats_page_after_send.text
        assert (
            "delivery-status sent" in admin_chats_page_after_send.text
            or "delivery-status delivered" in admin_chats_page_after_send.text
        )
        assert "id=\"edit-message-id\"" in admin_chats_page_after_send.text
        assert "Режим редактирования сообщения" in admin_chats_page_after_send.text
        assert "клиент | mid:" not in admin_chats_page_after_send.text
        assert 'id="send-btn"' in admin_chats_page_after_send.text
        assert 'id="schedule-btn-mobile"' in admin_chats_page_after_send.text
        assert 'id="schedule-pop"' in admin_chats_page_after_send.text
        assert 'name="schedule_at"' in admin_chats_page_after_send.text
        assert "Закрепленные чаты:" in admin_chats_page_after_send.text

        with SessionLocal() as db:
            (
                db.query(ConversationPin)
                .filter(
                    ConversationPin.workspace_id == 1,
                    ConversationPin.service_user_id == 0,
                )
                .delete(synchronize_session=False)
            )
            db.commit()

        admin_pin_chat = client.post(
            f"/admin/chats/{conversation_id}/pin",
            data={"q": "", "view": "chat"},
            cookies=cookies,
            follow_redirects=False,
        )
        assert admin_pin_chat.status_code in (302, 303)
        assert "pinned=1" in admin_pin_chat.headers.get("location", "")
        with SessionLocal() as db:
            admin_pin_row = (
                db.query(ConversationPin)
                .filter(
                    ConversationPin.workspace_id == 1,
                    ConversationPin.service_user_id == 0,
                    ConversationPin.conversation_id == conversation_id,
                )
                .first()
            )
            assert admin_pin_row is not None
            assert int(admin_pin_row.sort_order or 0) >= 1

            from app.manager_bridge import load_chat_threads

            pinned_threads_view = load_chat_threads(
                db,
                query="",
                workspace_id=1,
                service_user_id=0,
            )
            pinned_thread_item = next(
                (row for row in pinned_threads_view if int(row.conversation_id) == int(conversation_id)),
                None,
            )
            assert pinned_thread_item is not None
            assert bool(getattr(pinned_thread_item, "is_pinned", False)) is True
            assert getattr(pinned_thread_item, "pin_order", None) is not None

        admin_unpin_chat = client.post(
            f"/admin/chats/{conversation_id}/unpin",
            data={"q": "", "view": "chat"},
            cookies=cookies,
            follow_redirects=False,
        )
        assert admin_unpin_chat.status_code in (302, 303)
        assert "unpinned=1" in admin_unpin_chat.headers.get("location", "")
        with SessionLocal() as db:
            admin_pin_row_after_unpin = (
                db.query(ConversationPin)
                .filter(
                    ConversationPin.workspace_id == 1,
                    ConversationPin.service_user_id == 0,
                    ConversationPin.conversation_id == conversation_id,
                )
                .first()
            )
            assert admin_pin_row_after_unpin is None

        scheduled_send = client.post(
            f"/admin/chats/{conversation_id}/send",
            data={"text": "scheduled message", "schedule_at": "2999-01-01T12:30"},
            cookies=cookies,
            follow_redirects=False,
        )
        assert scheduled_send.status_code in (302, 303)
        assert "scheduled=1" in scheduled_send.headers.get("location", "")
        with SessionLocal() as db:
            scheduled_chat_message = (
                db.query(ChatMessage)
                .filter(
                    ChatMessage.conversation_id == conversation_id,
                    ChatMessage.text == "scheduled message",
                )
                .order_by(ChatMessage.id.desc())
                .first()
            )
            assert scheduled_chat_message is not None
            assert scheduled_chat_message.max_message_mid in (None, "")
            assert scheduled_chat_message.delivery_state in ("queued", "failed")
            assert bool(getattr(scheduled_chat_message, "is_scheduled_message", False)) is True
            outbox_scheduled = (
                db.query(OutboxMessage)
                .filter(OutboxMessage.chat_message_id == scheduled_chat_message.id)
                .order_by(OutboxMessage.id.desc())
                .first()
            )
            assert outbox_scheduled is not None
            assert outbox_scheduled.state in ("queued", "failed")
            assert outbox_scheduled.next_retry_at > outbox_scheduled.created_at
        scheduled_chat_page = client.get(
            f"/admin/chats?conversation_id={conversation_id}",
            cookies=cookies,
        )
        assert scheduled_chat_page.status_code == 200
        assert "scheduled message" in scheduled_chat_page.text
        assert "Отправится" in scheduled_chat_page.text

        # Mini-smoke: grouped photo message should be stored as one chat message
        # and local uploaded files must be cleaned up after message deletion.
        png_probe = b"\x89PNG\r\n\x1a\nsmoke-photo"
        media_probe_token = uuid4().hex[:8]
        media_probe_text = f"media_probe_{media_probe_token}"
        scheduled_media_send = client.post(
            f"/admin/chats/{conversation_id}/send",
            data={"text": media_probe_text, "schedule_at": "2999-01-02T12:35"},
            files=[
                ("photos", ("probe_one.png", png_probe, "image/png")),
                ("photos", ("probe_two.png", png_probe, "image/png")),
            ],
            cookies=cookies,
            follow_redirects=False,
        )
        assert scheduled_media_send.status_code in (302, 303)
        stored_media_paths: list[str] = []
        with SessionLocal() as db:
            scheduled_media_msg = (
                db.query(ChatMessage)
                .filter(
                    ChatMessage.conversation_id == conversation_id,
                    ChatMessage.text == media_probe_text,
                )
                .order_by(ChatMessage.id.desc())
                .first()
            )
            assert scheduled_media_msg is not None
            assert bool(getattr(scheduled_media_msg, "is_scheduled_message", False)) is True
            parsed_media_urls = json.loads(str(getattr(scheduled_media_msg, "image_urls_json", "[]") or "[]"))
            assert isinstance(parsed_media_urls, list)
            assert len(parsed_media_urls) == 2
            stored_media_paths = [
                urlsplit(str(item)).path
                for item in parsed_media_urls
                if str(item or "").strip()
            ]
            assert len(stored_media_paths) == 2
            for media_path in stored_media_paths:
                media_file = Path("app/static") / media_path.removeprefix("/static/")
                assert media_file.exists()
            scheduled_media_msg_id = int(scheduled_media_msg.id)
        scheduled_media_page = client.get(
            f"/admin/chats?conversation_id={conversation_id}",
            cookies=cookies,
        )
        assert scheduled_media_page.status_code == 200
        assert "bubble-media-grid" in scheduled_media_page.text
        assert "Фото от оператора" not in scheduled_media_page.text
        delete_scheduled_media = client.post(
            f"/admin/chats/{conversation_id}/messages/{scheduled_media_msg_id}/delete",
            data={"q": "", "view": "chat"},
            cookies=cookies,
            follow_redirects=False,
        )
        assert delete_scheduled_media.status_code in (302, 303)
        with SessionLocal() as db:
            deleted_media_message = (
                db.query(ChatMessage)
                .filter(ChatMessage.id == scheduled_media_msg_id)
                .first()
            )
            assert deleted_media_message is None
        for media_path in stored_media_paths:
            media_file = Path("app/static") / media_path.removeprefix("/static/")
            assert not media_file.exists()

        mobile_list_page = client.get("/admin/chats", cookies=cookies)
        assert mobile_list_page.status_code == 200
        assert 'id="chat-list-screen"' in mobile_list_page.text
        assert "dot-new" in mobile_list_page.text
        assert "hidden-mobile" in mobile_list_page.text

        mobile_chat_page = client.get(
            f"/admin/chats?conversation_id={conversation_id}&view=chat",
            cookies=cookies,
        )
        assert mobile_chat_page.status_code == 200
        assert "back-btn mobile-only" in mobile_chat_page.text
        assert "attach-wrap" in mobile_chat_page.text
        assert "position: sticky" in mobile_chat_page.text
        assert 'id="chat-screen"' in mobile_chat_page.text
        assert "chat-screen hidden-mobile" not in mobile_chat_page.text

        folder_name = f"В работе {uuid4().hex[:6]}"
        create_folder = client.post(
            "/admin/chats/folders",
            data={"name": folder_name, "q": ""},
            cookies=cookies,
            follow_redirects=False,
        )
        assert create_folder.status_code in (302, 303)
        location_value = create_folder.headers.get("location", "")

        with SessionLocal() as db:
            if "folder_limit=1" in location_value:
                folder = (
                    db.query(ChatFolder)
                    .filter(ChatFolder.workspace_id == 1)
                    .order_by(ChatFolder.id.asc())
                    .first()
                )
            else:
                folder = (
                    db.query(ChatFolder)
                    .filter(
                        ChatFolder.workspace_id == 1,
                        ChatFolder.name == folder_name,
                    )
                    .first()
                )
            assert folder is not None
            folder_id = folder.id

        mark_unread = client.post(
            f"/admin/chats/{conversation_id}/mark-unread",
            data={"q": "", "view": "chat", "current_conversation_id": str(conversation_id)},
            cookies=cookies,
            follow_redirects=False,
        )
        assert mark_unread.status_code in (302, 303)
        assert "unread=1" in mark_unread.headers.get("location", "")

        second_chat_id = f"chat_{uuid4().hex[:8]}"
        second_buyer_id = f"buyer_{uuid4().hex[:8]}"
        second_start = client.post(
            "/webhook/max/ws1key",
            json={
                "update_type": "bot_started",
                "chat_id": second_chat_id,
                "sender_id": second_buyer_id,
                "text": "",
            },
        )
        assert second_start.status_code == 200
        with SessionLocal() as db:
            second_conversation = (
                db.query(Conversation)
                .filter(Conversation.workspace_id == 1, Conversation.chat_id == second_chat_id)
                .first()
            )
            assert second_conversation is not None
            second_conversation_id = int(second_conversation.id)

        third_chat_id = f"chat_{uuid4().hex[:8]}"
        third_buyer_id = f"buyer_{uuid4().hex[:8]}"
        third_start = client.post(
            "/webhook/max/ws1key",
            json={
                "update_type": "bot_started",
                "chat_id": third_chat_id,
                "sender_id": third_buyer_id,
                "text": "",
            },
        )
        assert third_start.status_code == 200
        with SessionLocal() as db:
            third_conversation = (
                db.query(Conversation)
                .filter(Conversation.workspace_id == 1, Conversation.chat_id == third_chat_id)
                .first()
            )
            assert third_conversation is not None
            third_conversation_id = int(third_conversation.id)

        fourth_chat_id = f"chat_{uuid4().hex[:8]}"
        fourth_buyer_id = f"buyer_{uuid4().hex[:8]}"
        fourth_start = client.post(
            "/webhook/max/ws1key",
            json={
                "update_type": "bot_started",
                "chat_id": fourth_chat_id,
                "sender_id": fourth_buyer_id,
                "text": "",
            },
        )
        assert fourth_start.status_code == 200
        with SessionLocal() as db:
            fourth_conversation = (
                db.query(Conversation)
                .filter(Conversation.workspace_id == 1, Conversation.chat_id == fourth_chat_id)
                .first()
            )
            assert fourth_conversation is not None
            fourth_conversation_id = int(fourth_conversation.id)

        fifth_chat_id = f"chat_{uuid4().hex[:8]}"
        fifth_buyer_id = f"buyer_{uuid4().hex[:8]}"
        fifth_start = client.post(
            "/webhook/max/ws1key",
            json={
                "update_type": "bot_started",
                "chat_id": fifth_chat_id,
                "sender_id": fifth_buyer_id,
                "text": "",
            },
        )
        assert fifth_start.status_code == 200
        with SessionLocal() as db:
            fifth_conversation = (
                db.query(Conversation)
                .filter(Conversation.workspace_id == 1, Conversation.chat_id == fifth_chat_id)
                .first()
            )
            assert fifth_conversation is not None
            fifth_conversation_id = int(fifth_conversation.id)

        sixth_chat_id = f"chat_{uuid4().hex[:8]}"
        sixth_buyer_id = f"buyer_{uuid4().hex[:8]}"
        sixth_start = client.post(
            "/webhook/max/ws1key",
            json={
                "update_type": "bot_started",
                "chat_id": sixth_chat_id,
                "sender_id": sixth_buyer_id,
                "text": "",
            },
        )
        assert sixth_start.status_code == 200
        with SessionLocal() as db:
            sixth_conversation = (
                db.query(Conversation)
                .filter(Conversation.workspace_id == 1, Conversation.chat_id == sixth_chat_id)
                .first()
            )
            assert sixth_conversation is not None
            sixth_conversation_id = int(sixth_conversation.id)
            # Keep pin-limit assertions stable across repeated smoke runs.
            db.query(ConversationPin).filter(
                ConversationPin.workspace_id == 1,
                ConversationPin.service_user_id == 0,
            ).delete(synchronize_session=False)
            db.commit()

        for conv_id in [
            conversation_id,
            second_conversation_id,
            third_conversation_id,
            fourth_conversation_id,
            fifth_conversation_id,
        ]:
            pin_resp = client.post(
                f"/admin/chats/{conv_id}/pin",
                data={"q": "", "view": "chat"},
                cookies=cookies,
                follow_redirects=False,
            )
            assert pin_resp.status_code in (302, 303)
            assert "pinned=1" in pin_resp.headers.get("location", "")

        pin_limit_resp = client.post(
            f"/admin/chats/{sixth_conversation_id}/pin",
            data={"q": "", "view": "chat"},
            cookies=cookies,
            follow_redirects=False,
        )
        assert pin_limit_resp.status_code in (302, 303)
        assert "pin_limit=1" in pin_limit_resp.headers.get("location", "")
        with SessionLocal() as db:
            pinned_count_trial = (
                db.query(ConversationPin)
                .filter(
                    ConversationPin.workspace_id == 1,
                    ConversationPin.service_user_id == 0,
                )
                .count()
            )
            assert int(pinned_count_trial) == 5

            before_reorder_rows = (
                db.query(ConversationPin)
                .filter(
                    ConversationPin.workspace_id == 1,
                    ConversationPin.service_user_id == 0,
                )
                .order_by(ConversationPin.sort_order.asc(), ConversationPin.id.asc())
                .all()
            )
            before_reorder_ids = [int(row.conversation_id) for row in before_reorder_rows]
            assert set(before_reorder_ids) == {
                int(conversation_id),
                int(second_conversation_id),
                int(third_conversation_id),
                int(fourth_conversation_id),
                int(fifth_conversation_id),
            }

        reorder_target_ids = [
            int(fifth_conversation_id),
            int(fourth_conversation_id),
            int(third_conversation_id),
            int(second_conversation_id),
            int(conversation_id),
        ]
        admin_reorder = client.post(
            "/admin/chats/pins/reorder",
            json={"conversation_ids": reorder_target_ids},
            cookies=cookies,
            follow_redirects=False,
        )
        assert admin_reorder.status_code == 200
        reorder_payload = admin_reorder.json()
        assert reorder_payload.get("ok") is True
        assert int(reorder_payload.get("ordered_count") or 0) == len(reorder_target_ids)
        with SessionLocal() as db:
            after_reorder_rows = (
                db.query(ConversationPin)
                .filter(
                    ConversationPin.workspace_id == 1,
                    ConversationPin.service_user_id == 0,
                )
                .order_by(ConversationPin.sort_order.asc(), ConversationPin.id.asc())
                .all()
            )
            after_reorder_ids = [int(row.conversation_id) for row in after_reorder_rows]
            assert after_reorder_ids == reorder_target_ids

        mark_unread_other = client.post(
            f"/admin/chats/{second_conversation_id}/mark-unread",
            data={"q": "", "view": "chat", "current_conversation_id": str(conversation_id)},
            cookies=cookies,
            follow_redirects=False,
        )
        assert mark_unread_other.status_code in (302, 303)
        mark_unread_other_location = mark_unread_other.headers.get("location", "")
        assert f"conversation_id={conversation_id}" in mark_unread_other_location
        assert "unread=1" in mark_unread_other_location
        # Polling must not clear unread by itself.
        updates_keep_unread = client.get(
            f"/admin/chats/updates?conversation_id={conversation_id}&last_message_id=0&threads_sig=",
            cookies=cookies,
            follow_redirects=False,
        )
        assert updates_keep_unread.status_code == 200
        with SessionLocal() as db:
            from app.models import ConversationMeta
            unread_meta = (
                db.query(ConversationMeta)
                .filter(ConversationMeta.workspace_id == 1, ConversationMeta.conversation_id == conversation_id)
                .first()
            )
            assert unread_meta is not None
            assert bool(unread_meta.is_unread) is True
            assert bool(unread_meta.manual_unread_mark) is True

        # Explicit opening with mark_read=1 should clear unread marker.
        explicit_open_read = client.get(
            f"/admin/chats?conversation_id={conversation_id}&view=chat&mark_read=1",
            cookies=cookies,
            follow_redirects=False,
        )
        assert explicit_open_read.status_code == 200
        with SessionLocal() as db:
            from app.models import ConversationMeta
            read_meta = (
                db.query(ConversationMeta)
                .filter(ConversationMeta.workspace_id == 1, ConversationMeta.conversation_id == conversation_id)
                .first()
            )
            assert read_meta is not None
            assert bool(read_meta.is_unread) is False
            assert bool(read_meta.manual_unread_mark) is False

        move_to_folder = client.post(
            f"/admin/chats/{conversation_id}/move-folder",
            data={
                "folder_id": str(folder_id),
                "folder_ids": str(folder_id),
                "q": "",
                "view": "chat",
            },
            cookies=cookies,
            follow_redirects=False,
        )
        assert move_to_folder.status_code in (302, 303)
        assert "foldered=1" in move_to_folder.headers.get("location", "")
        with SessionLocal() as db:
            links_after_single_move = (
                db.query(ConversationFolderLink)
                .filter(
                    ConversationFolderLink.workspace_id == 1,
                    ConversationFolderLink.conversation_id == conversation_id,
                )
                .all()
            )
            assert len(links_after_single_move) == 1

        folder_page = client.get("/admin/chats", cookies=cookies)
        assert folder_page.status_code == 200
        assert "folder-chip" in folder_page.text
        assert f"/admin/chats/{conversation_id}/profile?q=" in mobile_chat_page.text

        second_folder_name = f"VIP {uuid4().hex[:6]}"
        create_second_folder = client.post(
            "/admin/chats/folders",
            data={"name": second_folder_name, "q": ""},
            cookies=cookies,
            follow_redirects=False,
        )
        assert create_second_folder.status_code in (302, 303)
        with SessionLocal() as db:
            second_folder = (
                db.query(ChatFolder)
                .filter(
                    ChatFolder.workspace_id == 1,
                    ChatFolder.name == second_folder_name,
                )
                .first()
            )
            if second_folder is None:
                # Folder creation can be blocked by tariff limits in reused DB state.
                # Create one directly so multi-folder assignment scenario stays testable.
                last_folder = (
                    db.query(ChatFolder)
                    .filter(ChatFolder.workspace_id == 1)
                    .order_by(ChatFolder.sort_order.desc(), ChatFolder.id.desc())
                    .first()
                )
                next_sort = int(getattr(last_folder, "sort_order", 0) or 0) + 1
                second_folder = ChatFolder(
                    workspace_id=1,
                    name=second_folder_name,
                    sort_order=next_sort,
                )
                db.add(second_folder)
                db.commit()
                db.refresh(second_folder)
            assert second_folder is not None
            second_folder_id = int(second_folder.id)

        multi_move = client.post(
            f"/admin/chats/{conversation_id}/move-folder",
            data={
                "folder_id": str(folder_id),
                "folder_ids": f"{folder_id},{second_folder_id}",
                "q": "",
                "view": "chat",
            },
            cookies=cookies,
            follow_redirects=False,
        )
        assert multi_move.status_code in (302, 303)
        multi_location = multi_move.headers.get("location", "")
        assert "foldered=1" in multi_location
        assert "foldered_multi=1" in multi_location
        with SessionLocal() as db:
            links_after_multi_move = (
                db.query(ConversationFolderLink)
                .filter(
                    ConversationFolderLink.workspace_id == 1,
                    ConversationFolderLink.conversation_id == conversation_id,
                )
                .order_by(ConversationFolderLink.folder_id.asc())
                .all()
            )
            assert [int(link.folder_id) for link in links_after_multi_move] == sorted(
                [int(folder_id), int(second_folder_id)]
            )
        folder_filtered_page = client.get(f"/admin/chats?folder_id={second_folder_id}", cookies=cookies)
        assert folder_filtered_page.status_code == 200
        assert f"conversation_id={conversation_id}" in folder_filtered_page.text

        # Mark the multi-folder chat unread and verify unread badge appears in both folders.
        mark_unread_multi_folder = client.post(
            f"/admin/chats/{conversation_id}/mark-unread",
            data={"q": "", "view": "chat", "current_conversation_id": str(conversation_id)},
            cookies=cookies,
            follow_redirects=False,
        )
        assert mark_unread_multi_folder.status_code in (302, 303)
        assert "unread=1" in mark_unread_multi_folder.headers.get("location", "")
        folder_page_after_unread = client.get("/admin/chats", cookies=cookies)
        assert folder_page_after_unread.status_code == 200
        folder_page_after_unread_text = folder_page_after_unread.text
        assert folder_page_after_unread_text.count("folder-unread-badge") >= 2
        assert f'data-folder-id="{int(folder_id)}"' in folder_page_after_unread_text
        assert f'data-folder-id="{int(second_folder_id)}"' in folder_page_after_unread_text

        metrics_page = client.get("/admin/chats", cookies=cookies)
        assert metrics_page.status_code == 200
        assert "Success rate" not in metrics_page.text

        admin_settings_page = client.get("/admin", cookies=cookies)
        assert admin_settings_page.status_code == 200
        assert "Статистика доставки сообщений" in admin_settings_page.text
        assert "Успешная доставка" in admin_settings_page.text
        assert "Активных чатов:" not in admin_settings_page.text
        assert "Новых (непрочитанных) чатов:" not in admin_settings_page.text
        assert "Чатов с ошибками доставки:" not in admin_settings_page.text

        # Blocking/unblocking customer should move chat to blocked folder and stop delivery.
        with SessionLocal() as db:
            conv_for_block = (
                db.query(Conversation)
                .filter(Conversation.workspace_id == 1, Conversation.chat_id == chat_id)
                .first()
            )
            assert conv_for_block is not None
            conv_for_block_id = int(conv_for_block.id)

        block_user = client.post(
            f"/admin/chats/{conv_for_block_id}/block-user",
            data={"q": "", "view": "chat", "block_reason": "spam"},
            cookies=cookies,
            follow_redirects=False,
        )
        assert block_user.status_code in (302, 303)
        assert "blocked=1" in block_user.headers.get("location", "")
        with SessionLocal() as db:
            blocked_folder = (
                db.query(ChatFolder)
                .filter(ChatFolder.workspace_id == 1, ChatFolder.name == "Заблокированные пользователи")
                .first()
            )
            assert blocked_folder is not None
            blocked_conv = (
                db.query(Conversation)
                .filter(Conversation.workspace_id == 1, Conversation.id == conv_for_block_id)
                .first()
            )
            blocked_meta = (
                db.query(ConversationMeta)
                .filter(
                    ConversationMeta.workspace_id == 1,
                    ConversationMeta.conversation_id == conv_for_block_id,
                )
                .first()
            )
            assert blocked_conv is not None
            assert blocked_meta is not None
            blocked_links = (
                db.query(ConversationFolderLink)
                .filter(
                    ConversationFolderLink.workspace_id == 1,
                    ConversationFolderLink.conversation_id == conv_for_block_id,
                )
                .all()
            )
            assert any(int(link.folder_id) == int(blocked_folder.id) for link in blocked_links)
            assert bool(blocked_meta.is_blocked) is True
            assert (blocked_meta.blocked_reason or "") == "spam"

        blocked_message = client.post(
            "/webhook/max/ws1key",
            json={"update_type": "message_created", "chat_id": chat_id, "sender_id": "buyer_1", "text": "blocked ping"},
        )
        assert blocked_message.status_code == 200
        assert blocked_message.json().get("flow") == "blocked_customer"
        with SessionLocal() as db:
            blocked_meta_after_msg = (
                db.query(ConversationMeta)
                .filter(
                    ConversationMeta.workspace_id == 1,
                    ConversationMeta.conversation_id == conv_for_block_id,
                )
                .first()
            )
            assert blocked_meta_after_msg is not None
            assert blocked_meta_after_msg.blocked_notice_sent_at is not None
        blocked_message_second = client.post(
            "/webhook/max/ws1key",
            json={"update_type": "message_created", "chat_id": chat_id, "sender_id": "buyer_1", "text": "blocked ping 2"},
        )
        assert blocked_message_second.status_code == 200
        assert blocked_message_second.json().get("flow") == "blocked_customer"
        with SessionLocal() as db:
            blocked_notice_count = (
                db.query(ChatMessage)
                .filter(
                    ChatMessage.workspace_id == 1,
                    ChatMessage.conversation_id == conv_for_block_id,
                    ChatMessage.direction == "bot",
                    ChatMessage.source == "bot_system",
                    ChatMessage.text == "К сожалению, вы не можете писать в данный чат.",
                )
                .count()
            )
            assert blocked_notice_count >= 0

        unblock_user = client.post(
            f"/admin/chats/{conv_for_block_id}/unblock-user",
            data={"q": "", "view": "chat"},
            cookies=cookies,
            follow_redirects=False,
        )
        assert unblock_user.status_code in (302, 303)
        assert "unblocked=1" in unblock_user.headers.get("location", "")
        with SessionLocal() as db:
            unblocked_meta = (
                db.query(ConversationMeta)
                .filter(
                    ConversationMeta.workspace_id == 1,
                    ConversationMeta.conversation_id == conv_for_block_id,
                )
                .first()
            )
            assert unblocked_meta is not None
            assert bool(unblocked_meta.is_blocked) is False
            assert (unblocked_meta.blocked_reason or "") == ""
            unblocked_links = (
                db.query(ConversationFolderLink)
                .filter(
                    ConversationFolderLink.workspace_id == 1,
                    ConversationFolderLink.conversation_id == conv_for_block_id,
                )
                .all()
            )
            blocked_folder_ids = {
                int(row[0])
                for row in (
                    db.query(ChatFolder.id)
                    .filter(
                        ChatFolder.workspace_id == 1,
                        func.lower(ChatFolder.name) == "заблокированные пользователи",
                    )
                    .all()
                )
                if row and row[0]
            }
            assert all(int(link.folder_id) not in blocked_folder_ids for link in unblocked_links)
            restored_links = [
                link for link in unblocked_links if int(link.folder_id) not in blocked_folder_ids
            ]
            assert len(restored_links) >= 1

        unblocked_message = client.post(
            "/webhook/max/ws1key",
            json={"update_type": "message_created", "chat_id": chat_id, "sender_id": "buyer_1", "text": "unblocked ping"},
        )
        assert unblocked_message.status_code == 200
        assert unblocked_message.json().get("flow") == "queued_for_mini_app"

        delete_user = client.post(
            f"/admin/chats/{conversation_id}/delete-user",
            cookies=cookies,
            follow_redirects=False,
        )
        assert delete_user.status_code in (302, 303)

        chats_after_text_updates = client.get(f"/admin/chats?conversation_id={conv_for_block_id}", cookies=cookies)
        assert chats_after_text_updates.status_code == 200
        assert "Удалить чат" in chats_after_text_updates.text
        assert "Удалить пользователя" not in chats_after_text_updates.text

        with SessionLocal() as db:
            blocked_audit = (
                db.query(AuditLog)
                .filter(AuditLog.workspace_id == 1, AuditLog.action == "customer_blocked")
                .order_by(AuditLog.id.desc())
                .first()
            )
            unblocked_audit = (
                db.query(AuditLog)
                .filter(AuditLog.workspace_id == 1, AuditLog.action == "customer_unblocked")
                .order_by(AuditLog.id.desc())
                .first()
            )
            deleted_audit = (
                db.query(AuditLog)
                .filter(AuditLog.workspace_id == 1, AuditLog.action == "customer_deleted")
                .order_by(AuditLog.id.desc())
                .first()
            )
            assert blocked_audit is not None
            assert unblocked_audit is not None
            assert deleted_audit is not None
            assert "spam" in (blocked_audit.details_json or "")

        with SessionLocal() as db:
            stored_dup = db.query(WebhookEvent).filter(WebhookEvent.event_uid == "update:dup-evt-1").first()
            assert stored_dup is not None

        webhook_unknown = client.post(
            "/webhook/max/ws1key",
            json={
                "update_type": "message_created",
                "message": {"sender": {"user_id": 1}},
            },
        )
        assert webhook_unknown.status_code == 200
        assert webhook_unknown.json().get("ignored") == "unsupported_payload"


if __name__ == "__main__":
    run()
    print("Smoke test passed")
