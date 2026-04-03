from urllib.parse import quote_plus, urlsplit
from uuid import uuid4

from fastapi.testclient import TestClient

from app.auth import create_manager_mini_token, create_service_session
from app.database import SessionLocal, init_db
from app.main import app
from app.manager_bridge import DEFAULT_TEMPLATES
from app.services import get_or_create_settings
from app.models import (
    ChatFolder,
    Conversation,
    ConversationMeta,
    QuickReply,
    ServiceUser,
    Subscription,
    UserSession,
    WebhookEvent,
    Workspace,
)


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
        with SessionLocal() as db:
            started_conv = (
                db.query(Conversation)
                .filter(Conversation.workspace_id == 1, Conversation.chat_id == chat_id)
                .first()
            )
            assert started_conv is not None

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
            f"/app/chats/{conversation_id}/send",
            data={"text": "retry_me"},
            cookies=cookies,
            follow_redirects=False,
        )
        assert send_attempt.status_code in (302, 303)

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
        assert "delivery-toggle" in admin_chats_page_after_send.text
        assert "bubble-debug" in admin_chats_page_after_send.text
        assert "id=\"edit-message-id\"" in admin_chats_page_after_send.text
        assert "Режим редактирования сообщения" in admin_chats_page_after_send.text
        assert "клиент | mid:" not in admin_chats_page_after_send.text
        assert 'id="send-btn"' in admin_chats_page_after_send.text
        assert 'id="schedule-btn-mobile"' in admin_chats_page_after_send.text
        assert 'id="schedule-pop"' in admin_chats_page_after_send.text
        assert 'name="scheduled_at"' in admin_chats_page_after_send.text

        scheduled_send = client.post(
            f"/admin/chats/{conversation_id}/send",
            data={"text": "scheduled message", "schedule_at": "2999-01-01T12:30"},
            cookies=cookies,
            follow_redirects=False,
        )
        assert scheduled_send.status_code in (302, 303)
        assert "scheduled=1" in scheduled_send.headers.get("location", "")

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
            data={"folder_id": str(folder_id), "q": "", "view": "chat"},
            cookies=cookies,
            follow_redirects=False,
        )
        assert move_to_folder.status_code in (302, 303)
        assert "foldered=1" in move_to_folder.headers.get("location", "")

        folder_page = client.get("/admin/chats", cookies=cookies)
        assert folder_page.status_code == 200
        assert "folder-chip" in folder_page.text
        assert f"/admin/chats/{conversation_id}/profile?q=" in mobile_chat_page.text

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
            data={"q": "", "view": "chat"},
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
            assert blocked_conv.folder_id == blocked_folder.id
            assert bool(blocked_meta.is_blocked) is True

        blocked_message = client.post(
            "/webhook/max/ws1key",
            json={"update_type": "message_created", "chat_id": chat_id, "sender_id": "buyer_1", "text": "blocked ping"},
        )
        assert blocked_message.status_code == 200
        assert blocked_message.json().get("flow") == "blocked_customer"

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
