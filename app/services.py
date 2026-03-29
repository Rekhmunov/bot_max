from __future__ import annotations

from sqlalchemy.orm import Session

from app.config import settings
from app.max_client import MaxClient
from app.models import BotSettings, Conversation, MessageLog, QuickReply


def get_or_create_settings(db: Session) -> BotSettings:
    bot_settings = db.query(BotSettings).filter(BotSettings.id == 1).first()
    if bot_settings:
        return bot_settings

    bot_settings = BotSettings(id=1)
    db.add(bot_settings)
    db.commit()
    db.refresh(bot_settings)
    return bot_settings


def _get_or_create_conversation(db: Session, chat_id: str, customer_id: str) -> Conversation:
    conversation = db.query(Conversation).filter(Conversation.chat_id == chat_id).first()
    if conversation:
        return conversation

    conversation = Conversation(chat_id=chat_id, customer_account_id=customer_id, manager_added=False)
    db.add(conversation)
    db.commit()
    db.refresh(conversation)
    return conversation


async def process_incoming_customer_message(
    db: Session,
    client: MaxClient,
    customer_id: str,
    chat_id: str,
) -> None:
    bot_settings = get_or_create_settings(db)
    conversation = _get_or_create_conversation(db=db, chat_id=chat_id, customer_id=customer_id)

    message_log = MessageLog(
        conversation_id=conversation.id,
        sender_account_id=customer_id,
        message_text="customer_message",
        message_type="event",
    )
    db.add(message_log)

    if not conversation.manager_added:
        await client.send_text(chat_id=chat_id, text=bot_settings.greeting_text)
        if bot_settings.manager_account_id:
            await client.add_member_to_chat(chat_id=chat_id, account_id=bot_settings.manager_account_id)
            if bot_settings.manager_added_notice_text:
                await client.send_text(chat_id=chat_id, text=bot_settings.manager_added_notice_text)
            conversation.manager_added = True

    db.add(conversation)
    db.commit()


async def handle_manager_command(
    db: Session,
    client: MaxClient,
    chat_id: str,
    command_text: str,
) -> bool:
    command = command_text.strip().lstrip("/").strip().lower()
    if not command:
        return False

    quick_reply = (
        db.query(QuickReply)
        .filter(QuickReply.command == command, QuickReply.is_active.is_(True))
        .first()
    )
    if not quick_reply:
        return False

    if quick_reply.text:
        await client.send_text(chat_id=chat_id, text=quick_reply.text)

    if quick_reply.image_path:
        image_url = f"{settings.public_base_url.rstrip('/')}{quick_reply.image_path}"
        await client.send_photo(chat_id=chat_id, photo_url=image_url)

    return True
