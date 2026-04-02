from urllib.parse import quote_plus
from uuid import uuid4

from fastapi.testclient import TestClient

from app.auth import create_manager_mini_token
from app.database import SessionLocal, init_db
from app.main import app
from app.manager_bridge import DEFAULT_TEMPLATES
from app.services import get_or_create_settings
from app.models import ChatFolder, Conversation, ServiceUser, WebhookEvent, Workspace


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
            ("/app/superadmin/users", "Пользователи и роли"),
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
        assert "Default Workspace" in superadmin_workspaces.text

        superadmin_users = client.get("/app/superadmin/users", cookies=superadmin_cookies)
        assert superadmin_users.status_code == 200
        assert "admin" in superadmin_users.text

        orphan_seed = uuid4().hex[:8]
        first_register = client.post(
            "/app/register",
            data={"email": f"dupe_{orphan_seed}@example.com", "password": "StrongPass#123"},
            follow_redirects=False,
        )
        assert first_register.status_code in (302, 303)
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
                "admin_account_id": "admin_1",
            },
            cookies=cookies,
        )
        assert save_settings.status_code == 200
        assert "format-row" in save_settings.text
        assert "data-format-target=\"start_message\"" in save_settings.text
        assert "data-format-target=\"after_phone_message\"" in save_settings.text

        create_reply = client.post(
            "/admin/quick-replies",
            data={
                "command": command,
                "title": "Прайс",
                "text": "Отправляю прайс",
            },
            cookies=cookies,
        )
        assert create_reply.status_code == 200

        webhook_customer_start = client.post(
            "/webhook/max",
            json={
                "update_type": "bot_started",
                "chat_id": chat_id,
                "sender_id": "buyer_1",
                "text": "",
            },
        )
        assert webhook_customer_start.status_code == 200
        assert webhook_customer_start.json().get("flow") == "start_prompt"
        chats_after_start = client.get("/admin/chats", cookies=cookies)
        assert chats_after_start.status_code == 200
        assert "Если кнопка контакта не отображается" not in chats_after_start.text
        assert start_template.split("\n")[0] in chats_after_start.text

        chat_id_skip = f"chat_{uuid4().hex[:8]}"
        webhook_customer_start_with_phone = client.post(
            "/webhook/max",
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
            "/webhook/max",
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
            "/webhook/max",
            json={"chat_id": chat_id, "sender_id": "buyer_1", "text": "Хочу купить iPhone"},
        )
        assert webhook_customer_text.status_code == 200
        assert webhook_customer_text.json().get("flow") == "queued_for_mini_app"

        webhook_customer_callback_style = client.post(
            "/webhook/max",
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
            "/webhook/max",
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
            "/webhook/max",
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
            "/webhook/max",
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
            "/webhook/max",
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
            "/webhook/max",
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
            "/webhook/max",
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
            "/webhook/max",
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
            "/webhook/max",
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
            "/webhook/max",
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
            "/webhook/max",
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
            "/webhook/max",
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
        first_dup = client.post("/webhook/max", json=duplicate_payload)
        second_dup = client.post("/webhook/max", json=duplicate_payload)
        assert first_dup.status_code == 200
        assert second_dup.status_code == 200
        assert second_dup.json().get("ignored") == "duplicate_event"

        with SessionLocal() as db:
            conversation = db.query(Conversation).filter(Conversation.chat_id == chat_id).first()
            assert conversation is not None
            conversation_id = conversation.id

        manager_mini_token = create_manager_mini_token("90000")
        manager_mini_page = client.get(f"/mini/manager?token={quote_plus(manager_mini_token)}")
        assert manager_mini_page.status_code == 200
        assert "Max Manager Mini App" in manager_mini_page.text
        assert "mobile-folder-bar" in manager_mini_page.text
        assert "/mini/manager/chats/" in manager_mini_page.text

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
        assert "sent=1" in manager_mini_send.headers.get("location", "")

        with SessionLocal() as db:
            settings_row = get_or_create_settings(db)
            manager_msgs = (
                db.query(WebhookEvent)
                .filter(WebhookEvent.update_type == "message_created")
                .all()
            )
            assert settings_row.manager_account_id == "90000"

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
        assert "Профиль покупателя" in profile_page.text
        assert "Вернуться в чат" in profile_page.text
        assert "К списку чатов" not in profile_page.text
        assert "Основные данные" in profile_page.text
        assert "Данные из переписки / Max" in profile_page.text
        assert "Тикет" in profile_page.text
        assert f"T-{conversation_id + 1000}"[:2] == "T-"

        admin_quick_reply = client.post(
            f"/admin/chats/{conversation_id}/quick-reply",
            data={"command": f"/{command}"},
            cookies=cookies,
            follow_redirects=False,
        )
        assert admin_quick_reply.status_code in (302, 303)
        assert "quick=1" in admin_quick_reply.headers.get("location", "")

        send_attempt = client.post(
            f"/admin/chats/{conversation_id}/send",
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
        assert "доставлено" in admin_chats_page_after_send.text
        assert 'id="message-input"' in admin_chats_page_after_send.text
        assert 'id="slash-menu"' in admin_chats_page_after_send.text
        assert "context-menu" in admin_chats_page_after_send.text
        assert "msg-context-menu" in admin_chats_page_after_send.text
        assert "delivery-toggle" in admin_chats_page_after_send.text
        assert "bubble-debug" in admin_chats_page_after_send.text
        assert "id=\"edit-message-id\"" in admin_chats_page_after_send.text
        assert "Режим редактирования сообщения" in admin_chats_page_after_send.text
        assert "клиент | mid:" not in admin_chats_page_after_send.text

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

        with SessionLocal() as db:
            folder = db.query(ChatFolder).filter(ChatFolder.name == folder_name).first()
            assert folder is not None
            folder_id = folder.id

        mark_unread = client.post(
            f"/admin/chats/{conversation_id}/mark-unread",
            data={"q": "", "view": "chat"},
            cookies=cookies,
            follow_redirects=False,
        )
        assert mark_unread.status_code in (302, 303)
        assert "unread=1" in mark_unread.headers.get("location", "")

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
        assert folder_name in folder_page.text
        assert "folder-chip" in folder_page.text
        assert f"/admin/chats/{conversation_id}/profile?from=chat&q=" in mobile_chat_page.text

        metrics_page = client.get("/admin/chats", cookies=cookies)
        assert metrics_page.status_code == 200
        assert "Success rate" not in metrics_page.text

        admin_settings_page = client.get("/admin", cookies=cookies)
        assert admin_settings_page.status_code == 200
        assert "Статистика доставки сообщений" in admin_settings_page.text
        assert "Успешная доставка" in admin_settings_page.text

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
            "/webhook/max",
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
