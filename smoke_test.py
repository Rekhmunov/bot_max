from fastapi.testclient import TestClient

from app.database import SessionLocal, init_db
from app.main import app
from app.manager_bridge import DEFAULT_TEMPLATES
from app.models import Conversation, OutboxMessage
from uuid import uuid4


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

        webhook_manager_without_reply = client.post(
            "/webhook/max",
            json={
                "update_type": "message_created",
                "message": {
                    "sender": {"user_id": "90000"},
                    "recipient": {"chat_id": "90000", "chat_type": "dialog"},
                    "body": {"text": f"/{command}"},
                },
            },
        )
        assert webhook_manager_without_reply.status_code == 200
        assert webhook_manager_without_reply.json().get("ignored") == "reply_required"

        webhook_manager = client.post(
            "/webhook/max",
            json={
                "update_type": "message_created",
                "message": {
                    "sender": {"user_id": "90000"},
                    "recipient": {"chat_id": "90000", "chat_type": "dialog"},
                    "link": {"message": {"mid": "reply-mid-1"}},
                    "body": {"text": f"/{command}"},
                },
            },
        )
        assert webhook_manager.status_code == 200
        assert "ok" in webhook_manager.json()

        with SessionLocal() as db:
            conversation = db.query(Conversation).filter(Conversation.chat_id == chat_id).first()
            assert conversation is not None
            conversation_id = conversation.id

        admin_chats_page = client.get(
            f"/admin/chats?conversation_id={conversation_id}",
            cookies=cookies,
        )
        assert admin_chats_page.status_code == 200
        assert "Быстрые ответы" in admin_chats_page.text
        assert f"/{command}" in admin_chats_page.text

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

        failed_chat_message_id = None
        with SessionLocal() as db:
            outbox_row = (
                db.query(OutboxMessage)
                .filter(OutboxMessage.conversation_id == conversation_id)
                .order_by(OutboxMessage.id.desc())
                .first()
            )
            assert outbox_row is not None
            failed_chat_message_id = outbox_row.chat_message_id

        assert failed_chat_message_id is not None
        retry_resp = client.post(
            f"/admin/chats/{conversation_id}/messages/{failed_chat_message_id}/retry",
            cookies=cookies,
            follow_redirects=False,
        )
        assert retry_resp.status_code in (302, 303)
        assert "retried=" in retry_resp.headers.get("location", "")

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
