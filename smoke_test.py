from uuid import uuid4

from fastapi.testclient import TestClient

from app.database import SessionLocal, init_db
from app.main import app
from app.manager_bridge import DEFAULT_TEMPLATES
from app.models import ChatFolder, Conversation, WebhookEvent


def run() -> None:
    init_db()
    command = f"price_{uuid4().hex[:8]}"
    chat_id = f"chat_{uuid4().hex[:8]}"
    with TestClient(app) as client:

        health = client.get("/health")
        assert health.status_code == 200
        assert health.json().get("ok") is True

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

        save_settings = client.post(
            "/admin/settings",
            data={
                "prestart_message": DEFAULT_TEMPLATES["prestart_message"],
                "start_message": DEFAULT_TEMPLATES["start_message"],
                "after_phone_message": DEFAULT_TEMPLATES["after_phone_message"],
                "manager_account_id": "90000",
                "admin_account_id": "admin_1",
            },
            cookies=cookies,
        )
        assert save_settings.status_code == 200

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
        assert webhook_customer_text.json().get("flow") == "forwarded_to_manager"

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
            callback_payload.get("flow") in {"forwarded_to_manager", "prestart"}
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
        assert webhook_manager_tickets.json().get("tickets_sent") is True

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
        assert webhook_manager_panel.json().get("panel_sent") is True

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
        assert webhook_manager_new.json().get("ok") is True

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
        assert webhook_manager_take.json().get("action") == "take"

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
        assert webhook_manager_mine.json().get("ok") is True

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
        assert webhook_manager_done.json().get("action") == "done"

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
        assert webhook_manager_callback.json().get("ok") is True

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
        assert webhook_manager_ticket_reply.json().get("ticket_reply_sent") is True

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
        assert "ok" in webhook_manager.json()

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
        assert "Success rate" in metrics_page.text

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
