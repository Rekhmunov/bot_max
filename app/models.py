from sqlalchemy import Boolean, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class BotSettings(Base):
    __tablename__ = "bot_settings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, default=1)
    greeting_text: Mapped[str] = mapped_column(
        Text,
        default="Здравствуйте! Сейчас подключу менеджера к диалогу.",
    )
    manager_account_id: Mapped[str] = mapped_column(String(255), default="")
    admin_account_id: Mapped[str] = mapped_column(String(255), default="")
    manager_added_notice_text: Mapped[str] = mapped_column(
        Text,
        default="Менеджер подключен к диалогу.",
    )


class QuickReply(Base):
    __tablename__ = "quick_replies"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    command: Mapped[str] = mapped_column(String(100), unique=True, index=True)
    title: Mapped[str] = mapped_column(String(255))
    text: Mapped[str] = mapped_column(Text, default="")
    image_path: Mapped[str | None] = mapped_column(String(500), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)


class Conversation(Base):
    __tablename__ = "conversations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    chat_id: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    customer_account_id: Mapped[str] = mapped_column(String(255))
    manager_added: Mapped[bool] = mapped_column(Boolean, default=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)

    messages: Mapped[list["MessageLog"]] = relationship(
        back_populates="conversation",
        cascade="all, delete-orphan",
    )


class MessageLog(Base):
    __tablename__ = "message_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    conversation_id: Mapped[int] = mapped_column(ForeignKey("conversations.id"), index=True)
    sender_account_id: Mapped[str] = mapped_column(String(255))
    message_text: Mapped[str] = mapped_column(Text, default="")
    message_type: Mapped[str] = mapped_column(String(50), default="text")

    conversation: Mapped[Conversation] = relationship(back_populates="messages")


class MessageTemplate(Base):
    __tablename__ = "message_templates"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    template_key: Mapped[str] = mapped_column(String(100), unique=True, index=True)
    template_text: Mapped[str] = mapped_column(Text, default="")


class ConversationMeta(Base):
    __tablename__ = "conversation_meta"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    conversation_id: Mapped[int] = mapped_column(ForeignKey("conversations.id"), unique=True, index=True)
    ticket_no: Mapped[int] = mapped_column(Integer, unique=True, index=True)
    status: Mapped[str] = mapped_column(String(50), default="new")
    phone_verified: Mapped[bool] = mapped_column(Boolean, default=False)
    start_prompt_sent: Mapped[bool] = mapped_column(Boolean, default=False)
    phone_number: Mapped[str | None] = mapped_column(String(64), nullable=True)


class CustomerProfile(Base):
    __tablename__ = "customer_profiles"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    customer_account_id: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    first_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    username: Mapped[str | None] = mapped_column(String(255), nullable=True)
    phone_number: Mapped[str] = mapped_column(String(64), default="")
    source_chat_id: Mapped[str] = mapped_column(String(255), default="")


class ManagerDispatch(Base):
    __tablename__ = "manager_dispatches"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    conversation_id: Mapped[int] = mapped_column(ForeignKey("conversations.id"), index=True)
    manager_message_mid: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    dispatch_type: Mapped[str] = mapped_column(String(50), default="customer_to_manager")
