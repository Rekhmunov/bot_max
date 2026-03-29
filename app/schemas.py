from pydantic import BaseModel, Field


class MaxWebhookEvent(BaseModel):
    chat_id: str = Field(..., description="ID чата, где пришло событие")
    sender_id: str = Field(..., description="ID отправителя сообщения")
    text: str = Field(default="", description="Текст сообщения")
