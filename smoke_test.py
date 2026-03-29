from fastapi.testclient import TestClient

from app.database import init_db
from app.main import app
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
                "greeting_text": "Здравствуйте!",
                "manager_account_id": "manager_1",
                "admin_account_id": "admin_1",
                "manager_added_notice_text": "Менеджер подключен.",
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

        webhook_customer = client.post(
            "/webhook/max",
            json={"chat_id": chat_id, "sender_id": "buyer_1", "text": "Привет"},
        )
        assert webhook_customer.status_code == 200
        assert webhook_customer.json().get("ok") is True

        webhook_customer_official = client.post(
            "/webhook/max",
            json={
                "update_type": "message_created",
                "message": {
                    "sender": {"user_id": 9001},
                    "recipient": {"chat_id": chat_id},
                    "body": {"text": "Здравствуйте"},
                },
            },
        )
        assert webhook_customer_official.status_code == 200
        assert webhook_customer_official.json().get("ok") is True

        webhook_manager = client.post(
            "/webhook/max",
            json={"chat_id": chat_id, "sender_id": "manager_1", "text": f"/{command}"},
        )
        assert webhook_manager.status_code == 200
        assert webhook_manager.json().get("command_sent") is True

        webhook_manager_official = client.post(
            "/webhook/max",
            json={
                "update_type": "message_created",
                "message": {
                    "sender": {"user_id": "manager_1"},
                    "recipient": {"chat_id": chat_id},
                    "body": {"text": f"/{command}"},
                },
            },
        )
        assert webhook_manager_official.status_code == 200
        assert webhook_manager_official.json().get("command_sent") is True


if __name__ == "__main__":
    run()
    print("Smoke test passed")
