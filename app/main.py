from __future__ import annotations

import asyncio
from contextlib import suppress
from pathlib import Path
from urllib.parse import quote_plus
from uuid import uuid4

from fastapi import Body, Depends, FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware
from sqlalchemy.orm import Session

from app.auth import get_current_admin, require_admin, sign_in_admin
from app.config import settings
from app.database import get_db, init_db
from app.manager_bridge import (
    TEMPLATE_AFTER_PHONE,
    TEMPLATE_PRESTART,
    TEMPLATE_START,
    assign_conversation_to_folder,
    create_chat_folder,
    delete_conversation,
    ensure_default_templates,
    get_chat_metrics,
    get_delivery_metrics,
    get_template_text,
    list_active_quick_replies,
    list_chat_folders,
    load_chat_messages,
    load_chat_threads,
    mark_thread_unread,
    mark_thread_read,
    handle_customer_event,
    handle_manager_message,
    process_outbox_queue,
    remove_chat_message,
    retry_failed_outbox_message,
    set_template_text,
    send_admin_chat_message,
    send_admin_quick_reply,
    update_chat_message_text,
)
from app.max_client import MaxClient
from app.models import (
    ChatMessage,
    ChatFolder,
    Conversation,
    ConversationMeta,
    CustomerProfile,
    ManagerDispatch,
    MessageLog,
    OutboxMessage,
    QuickReply,
    WebhookEvent,
)
from app.schemas import MaxWebhookEvent
from app.services import get_or_create_settings
from fastapi.templating import Jinja2Templates

app = FastAPI(title=settings.app_name)
templates = Jinja2Templates(directory="app/templates")
app.add_middleware(SessionMiddleware, secret_key=settings.secret_key)
webhook_path = settings.webhook_path if settings.webhook_path.startswith("/") else f"/{settings.webhook_path}"

Path("app/static/uploads").mkdir(parents=True, exist_ok=True)
app.mount("/static", StaticFiles(directory="app/static"), name="static")
_outbox_worker_task: asyncio.Task | None = None


async def _outbox_worker_loop() -> None:
    from app.database import SessionLocal

    while True:
        try:
            with SessionLocal() as db:
                await process_outbox_queue(db, limit=settings.outbox_worker_batch_size)
        except Exception:
            # Keep worker alive even if one cycle fails.
            pass
        await asyncio.sleep(max(settings.outbox_poll_interval_seconds, 1))


@app.on_event("startup")
async def startup() -> None:
    global _outbox_worker_task
    init_db()
    from app.database import SessionLocal

    with SessionLocal() as db:
        ensure_default_templates(db)
    if settings.outbox_worker_enabled:
        _outbox_worker_task = asyncio.create_task(_outbox_worker_loop())


@app.on_event("shutdown")
async def shutdown() -> None:
    global _outbox_worker_task
    if _outbox_worker_task is None:
        return
    _outbox_worker_task.cancel()
    with suppress(asyncio.CancelledError):
        await _outbox_worker_task
    _outbox_worker_task = None


@app.get("/", response_class=RedirectResponse)
def index() -> RedirectResponse:
    return RedirectResponse(url="/admin")


@app.get("/health")
def health() -> dict:
    return {"ok": True}


@app.get("/admin/login", response_class=HTMLResponse)
def login_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(request, "login.html", {"request": request, "error": None})


@app.post("/admin/login", response_class=HTMLResponse)
def login_submit(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
) -> HTMLResponse:
    if not sign_in_admin(request, username=username, password=password):
        return templates.TemplateResponse(
            request,
            "login.html",
            {"request": request, "error": "Неверный логин или пароль"},
            status_code=401,
        )
    return RedirectResponse(url="/admin", status_code=302)


@app.post("/admin/logout")
def logout(request: Request) -> RedirectResponse:
    request.session.clear()
    return RedirectResponse(url="/admin/login", status_code=302)


@app.get("/admin", response_class=HTMLResponse)
def admin_page(
    request: Request,
    _admin: str = Depends(require_admin),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    bot_settings = get_or_create_settings(db)
    replies = db.query(QuickReply).order_by(QuickReply.command.asc()).all()
    chat_metrics = get_chat_metrics(db)
    delivery_metrics = get_delivery_metrics(db)
    return templates.TemplateResponse(
        request,
        "admin.html",
        {
            "request": request,
            "settings": bot_settings,
            "template_prestart": get_template_text(db, TEMPLATE_PRESTART),
            "template_start": get_template_text(db, TEMPLATE_START),
            "template_after_phone": get_template_text(db, TEMPLATE_AFTER_PHONE),
            "quick_replies": replies,
            "webhook_path": webhook_path,
            "webhook_url": f"{settings.public_base_url.rstrip('/')}{webhook_path}",
            "chat_metrics": chat_metrics,
            "delivery_metrics": delivery_metrics,
            "message": None,
            "error": None,
        },
    )


@app.post("/admin/settings", response_class=HTMLResponse)
def update_settings(
    request: Request,
    prestart_message: str = Form(...),
    start_message: str = Form(...),
    after_phone_message: str = Form(...),
    manager_account_id: str = Form(...),
    admin_account_id: str = Form(""),
    _admin: str = Depends(get_current_admin),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    if _admin is None:
        return RedirectResponse(url="/admin/login", status_code=302)

    bot_settings = get_or_create_settings(db)
    bot_settings.manager_account_id = manager_account_id.strip()
    bot_settings.admin_account_id = admin_account_id.strip()
    db.add(bot_settings)
    set_template_text(db, TEMPLATE_PRESTART, prestart_message)
    set_template_text(db, TEMPLATE_START, start_message)
    set_template_text(db, TEMPLATE_AFTER_PHONE, after_phone_message)
    db.commit()

    replies = db.query(QuickReply).order_by(QuickReply.command.asc()).all()
    chat_metrics = get_chat_metrics(db)
    delivery_metrics = get_delivery_metrics(db)
    return templates.TemplateResponse(
        request,
        "admin.html",
        {
            "request": request,
            "settings": bot_settings,
            "template_prestart": get_template_text(db, TEMPLATE_PRESTART),
            "template_start": get_template_text(db, TEMPLATE_START),
            "template_after_phone": get_template_text(db, TEMPLATE_AFTER_PHONE),
            "quick_replies": replies,
            "webhook_path": webhook_path,
            "webhook_url": f"{settings.public_base_url.rstrip('/')}{webhook_path}",
            "chat_metrics": chat_metrics,
            "delivery_metrics": delivery_metrics,
            "message": "Настройки сохранены",
            "error": None,
        },
    )


@app.post("/admin/quick-replies", response_class=HTMLResponse)
async def create_quick_reply(
    request: Request,
    command: str = Form(...),
    title: str = Form(...),
    text: str = Form(""),
    photo: UploadFile | None = File(default=None),
    _admin: str = Depends(get_current_admin),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    if _admin is None:
        return RedirectResponse(url="/admin/login", status_code=302)

    normalized = command.strip().lstrip("/")
    normalized = normalized.lower()
    if not normalized:
        raise HTTPException(status_code=400, detail="Команда не может быть пустой")

    if db.query(QuickReply).filter(QuickReply.command == normalized).first():
        bot_settings = get_or_create_settings(db)
        replies = db.query(QuickReply).order_by(QuickReply.command.asc()).all()
        chat_metrics = get_chat_metrics(db)
        delivery_metrics = get_delivery_metrics(db)
        return templates.TemplateResponse(
            request,
            "admin.html",
            {
                "request": request,
                "settings": bot_settings,
                "template_prestart": get_template_text(db, TEMPLATE_PRESTART),
                "template_start": get_template_text(db, TEMPLATE_START),
                "template_after_phone": get_template_text(db, TEMPLATE_AFTER_PHONE),
                "quick_replies": replies,
                "webhook_path": webhook_path,
                "webhook_url": f"{settings.public_base_url.rstrip('/')}{webhook_path}",
                "chat_metrics": chat_metrics,
                "delivery_metrics": delivery_metrics,
                "message": None,
                "error": f"Команда /{normalized} уже существует",
            },
            status_code=400,
        )

    image_path = None
    if photo and photo.filename:
        ext = Path(photo.filename).suffix.lower()
        allowed = {".png", ".jpg", ".jpeg", ".webp", ".gif"}
        if ext not in allowed:
            raise HTTPException(status_code=400, detail="Неподдерживаемый формат фото")
        safe_name = f"{uuid4().hex}{ext}"
        target = Path("app/static/uploads") / safe_name
        content = await photo.read()
        target.write_bytes(content)
        image_path = f"/static/uploads/{safe_name}"

    reply = QuickReply(
        command=normalized,
        title=title.strip(),
        text=text.strip(),
        image_path=image_path,
    )
    db.add(reply)
    db.commit()

    bot_settings = get_or_create_settings(db)
    replies = db.query(QuickReply).order_by(QuickReply.command.asc()).all()
    chat_metrics = get_chat_metrics(db)
    delivery_metrics = get_delivery_metrics(db)
    return templates.TemplateResponse(
        request,
        "admin.html",
        {
            "request": request,
            "settings": bot_settings,
            "template_prestart": get_template_text(db, TEMPLATE_PRESTART),
            "template_start": get_template_text(db, TEMPLATE_START),
            "template_after_phone": get_template_text(db, TEMPLATE_AFTER_PHONE),
            "quick_replies": replies,
            "webhook_path": webhook_path,
            "webhook_url": f"{settings.public_base_url.rstrip('/')}{webhook_path}",
            "chat_metrics": chat_metrics,
            "delivery_metrics": delivery_metrics,
            "message": f"Быстрый ответ /{normalized} добавлен",
            "error": None,
        },
    )


@app.post("/admin/quick-replies/{reply_id}/delete", response_class=RedirectResponse)
def delete_quick_reply(
    reply_id: int,
    _admin: str = Depends(require_admin),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    reply = db.query(QuickReply).filter(QuickReply.id == reply_id).first()
    if reply:
        if reply.image_path:
            relative_static_path = reply.image_path.removeprefix("/static/")
            img_path = Path("app/static") / relative_static_path
            if img_path.exists():
                img_path.unlink()
        db.delete(reply)
        db.commit()
    return RedirectResponse(url="/admin", status_code=302)


@app.get("/admin/chats", response_class=HTMLResponse)
async def admin_chats_page(
    request: Request,
    conversation_id: int | None = None,
    q: str = "",
    quick_query: str = "",
    view: str = "",
    folder_id: int | None = None,
    _admin: str = Depends(require_admin),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    # Opportunistically drain due queue items on every admin page load.
    await process_outbox_queue(db, limit=30)

    sent_flag = request.query_params.get("sent")
    quick_flag = request.query_params.get("quick")
    edited_flag = request.query_params.get("edited")
    deleted_flag = request.query_params.get("deleted")
    retried_flag = request.query_params.get("retried")
    op_message = None
    op_error = None
    if sent_flag == "1":
        op_message = "Сообщение отправлено"
    if sent_flag == "0":
        op_error = "Не удалось отправить сообщение"
    if quick_flag == "1":
        op_message = "Быстрый ответ отправлен"
    if quick_flag == "0":
        op_error = "Не удалось отправить быстрый ответ"
    if edited_flag == "1":
        op_message = "Сообщение изменено"
    if edited_flag == "0":
        op_error = "Не удалось изменить сообщение"
    if deleted_flag == "1":
        op_message = "Сообщение удалено"
    if deleted_flag == "0":
        op_error = "Не удалось удалить сообщение"
    if retried_flag == "1":
        op_message = "Повторная отправка выполнена"
    if retried_flag == "0":
        op_error = "Повторная отправка не удалась"
    if request.query_params.get("unread") == "1":
        op_message = "Чат отмечен непрочитанным"
    if request.query_params.get("unread") == "0":
        op_error = "Не удалось отметить чат непрочитанным"
    if request.query_params.get("foldered") == "1":
        op_message = "Чат перемещен в папку"
    if request.query_params.get("foldered") == "0":
        op_error = "Не удалось переместить чат в папку"

    threads = load_chat_threads(db, query=q)
    if folder_id is not None:
        if folder_id > 0:
            threads = [item for item in threads if item.folder_id == folder_id]
        else:
            threads = [item for item in threads if item.folder_id is None]
    has_explicit_conversation = conversation_id is not None
    active_thread = None
    if conversation_id is not None:
        for item in threads:
            if item.conversation_id == conversation_id:
                active_thread = item
                break
    if active_thread is None and threads and not has_explicit_conversation:
        active_thread = threads[0]
    messages = []
    if active_thread:
        mark_thread_read(db, active_thread.conversation_id)
        messages = load_chat_messages(db, active_thread.conversation_id)
    mobile_chat_view = view.strip().lower() == "chat"

    return templates.TemplateResponse(
        request,
        "admin_chats.html",
        {
            "request": request,
            "threads": threads,
            "active_thread": active_thread,
            "messages": messages,
            "query": q,
            "folder_filter": folder_id,
            "quick_query": quick_query.strip(),
            "message": op_message,
            "error": op_error,
            "quick_replies": list_active_quick_replies(db),
            "removed": request.query_params.get("removed"),
            "mobile_chat_view": mobile_chat_view,
            "chat_metrics": get_chat_metrics(db),
            "admin_quick_options": [
                {"command": item.command, "title": item.title}
                for item in list_active_quick_replies(db)
            ],
            "chat_folders": [
                {"id": folder.id, "name": folder.name}
                for folder in list_chat_folders(db)
            ],
            "removed_message": (
                "Пользователь и чат удалены" if request.query_params.get("removed") == "1"
                else ("Не удалось удалить пользователя" if request.query_params.get("removed") == "0" else None)
            ),
        },
    )


@app.post("/admin/chats/folders", response_class=RedirectResponse)
def admin_chat_create_folder(
    name: str = Form(""),
    conversation_id: int | None = Form(default=None),
    q: str = Form(""),
    view: str = Form(""),
    folder_id: int | None = Form(default=None),
    _admin: str = Depends(require_admin),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    folder_name = name.strip()
    if folder_name:
        created = create_chat_folder(db, folder_name=folder_name)
        if conversation_id is not None:
            assign_conversation_to_folder(
                db,
                conversation_id=conversation_id,
                folder_id=created.id,
            )
    folder_qs = f"&folder_id={folder_id}" if folder_id is not None else ""
    conv_qs = f"&conversation_id={conversation_id}" if conversation_id is not None else ""
    view_qs = f"&view={view}" if view else ""
    return RedirectResponse(
        url=f"/admin/chats?q={q}{folder_qs}{conv_qs}{view_qs}",
        status_code=302,
    )


@app.post("/admin/chats/{conversation_id}/mark-unread", response_class=RedirectResponse)
def admin_chat_mark_unread(
    conversation_id: int,
    q: str = Form(""),
    view: str = Form(""),
    folder_id: int | None = Form(default=None),
    _admin: str = Depends(require_admin),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    ok = mark_thread_unread(db, conversation_id=conversation_id)
    suffix = "1" if ok else "0"
    folder_qs = f"&folder_id={folder_id}" if folder_id is not None else ""
    return RedirectResponse(
        url=f"/admin/chats?conversation_id={conversation_id}&q={q}&view={view}&unread={suffix}{folder_qs}",
        status_code=302,
    )


@app.post("/admin/chats/{conversation_id}/move-folder", response_class=RedirectResponse)
def admin_chat_move_folder(
    conversation_id: int,
    folder_id: int = Form(0),
    q: str = Form(""),
    view: str = Form(""),
    current_folder_id: int | None = Form(default=None),
    _admin: str = Depends(require_admin),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    ok = assign_conversation_to_folder(
        db,
        conversation_id=conversation_id,
        folder_id=(folder_id if folder_id > 0 else None),
    )
    suffix = "1" if ok else "0"
    folder_qs = f"&folder_id={current_folder_id}" if current_folder_id is not None else ""
    return RedirectResponse(
        url=f"/admin/chats?conversation_id={conversation_id}&q={q}&view={view}&foldered={suffix}{folder_qs}",
        status_code=302,
    )


@app.get("/admin/chats/{conversation_id}/profile", response_class=HTMLResponse)
def admin_chat_customer_profile(
    request: Request,
    conversation_id: int,
    q: str = "",
    view: str = "",
    _admin: str = Depends(require_admin),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    conversation = db.query(Conversation).filter(Conversation.id == conversation_id).first()
    if conversation is None:
        raise HTTPException(status_code=404, detail="Чат не найден")

    meta = db.query(ConversationMeta).filter(ConversationMeta.conversation_id == conversation_id).first()
    profile = (
        db.query(CustomerProfile)
        .filter(CustomerProfile.customer_account_id == conversation.customer_account_id)
        .first()
    )

    sender_ids = [
        str(row[0])
        for row in (
            db.query(MessageLog.sender_account_id)
            .filter(MessageLog.conversation_id == conversation_id)
            .distinct()
            .limit(20)
            .all()
        )
        if row and row[0]
    ]
    message_mids = [
        {
            "direction": row.direction,
            "source": row.source,
            "max_mid": row.max_message_mid,
            "link_mid": row.link_mid,
        }
        for row in (
            db.query(ChatMessage)
            .filter(
                ChatMessage.conversation_id == conversation_id,
                (ChatMessage.max_message_mid.is_not(None)) | (ChatMessage.link_mid.is_not(None)),
            )
            .order_by(ChatMessage.id.desc())
            .limit(20)
            .all()
        )
    ]
    manager_dispatch_mids = [
        str(row.manager_message_mid)
        for row in (
            db.query(ManagerDispatch)
            .filter(ManagerDispatch.conversation_id == conversation_id)
            .order_by(ManagerDispatch.id.desc())
            .limit(20)
            .all()
        )
        if row.manager_message_mid
    ]
    outbox_targets = [
        {
            "operation": row.operation,
            "target_chat_id": row.target_chat_id,
            "target_user_id": row.target_user_id,
            "state": row.state,
            "external_message_mid": row.external_message_mid,
        }
        for row in (
            db.query(OutboxMessage)
            .filter(OutboxMessage.conversation_id == conversation_id)
            .order_by(OutboxMessage.id.desc())
            .limit(20)
            .all()
        )
    ]

    safe_q = q.strip()
    safe_view = view.strip().lower()
    back_to_chat_url = f"/admin/chats?conversation_id={conversation_id}&q={safe_q}"
    if view.strip().lower() == "chat":
        back_to_chat_url += "&view=chat"
    back_to_list_url = f"/admin/chats?q={safe_q}"

    basic_rows = [
        {"label": "Тикет", "value": f"T-{meta.ticket_no}" if meta else "—"},
        {
            "label": "Имя",
            "value": (profile.first_name if profile and profile.first_name else "Покупатель"),
        },
        {
            "label": "Username",
            "value": (f"@{profile.username}" if profile and profile.username else "—"),
        },
        {
            "label": "Телефон",
            "value": (
                (meta.phone_number if meta and meta.phone_number else "")
                or (profile.phone_number if profile and profile.phone_number else "")
                or "—"
            ),
        },
        {"label": "Статус", "value": (meta.status if meta else "—")},
        {"label": "Телефон подтвержден", "value": ("да" if meta and meta.phone_verified else "нет")},
    ]
    technical_rows = [
        {"label": "conversation_id", "value": str(conversation.id)},
        {"label": "chat_id", "value": conversation.chat_id or "—"},
        {"label": "customer_account_id", "value": conversation.customer_account_id or "—"},
        {
            "label": "source_chat_id",
            "value": (profile.source_chat_id if profile and profile.source_chat_id else "—"),
        },
        {"label": "manager_owner_id", "value": (meta.manager_owner_id if meta and meta.manager_owner_id else "—")},
        {"label": "sender_ids (последние)", "value": "\n".join(sender_ids) if sender_ids else "—"},
        {
            "label": "message mids (последние)",
            "value": (
                "\n".join(
                    [
                        f"{item['direction']}/{item['source']} max:{item['max_mid'] or '-'} link:{item['link_mid'] or '-'}"
                        for item in message_mids
                    ]
                )
                if message_mids
                else "—"
            ),
        },
        {
            "label": "manager dispatch mids",
            "value": "\n".join(manager_dispatch_mids) if manager_dispatch_mids else "—",
        },
        {
            "label": "outbox targets",
            "value": (
                "\n".join(
                    [
                        f"{item['operation']} chat:{item['target_chat_id'] or '-'} user:{item['target_user_id'] or '-'} [{item['state']}] mid:{item['external_message_mid'] or '-'}"
                        for item in outbox_targets
                    ]
                )
                if outbox_targets
                else "—"
            ),
        },
    ]

    return templates.TemplateResponse(
        request,
        "admin_customer_profile.html",
        {
            "request": request,
            "conversation": conversation,
            "meta": meta,
            "profile": profile,
            "basic_rows": basic_rows,
            "technical_rows": technical_rows,
            "conversation_id": conversation_id,
            "query": safe_q,
            "view": safe_view,
            "back_to_chat_url": back_to_chat_url,
            "back_to_list_url": back_to_list_url,
        },
    )


@app.post("/admin/chats/{conversation_id}/send", response_class=RedirectResponse)
async def admin_chats_send_message(
    conversation_id: int,
    text: str = Form(""),
    edit_message_id: int | None = Form(default=None),
    photo: UploadFile | None = File(default=None),
    q: str = Form(""),
    view: str = Form(""),
    _admin: str = Depends(require_admin),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    text_value = text.strip()
    view_value = view.strip().lower()
    image_path = None
    if photo and photo.filename:
        ext = Path(photo.filename).suffix.lower()
        allowed = {".png", ".jpg", ".jpeg", ".webp", ".gif"}
        if ext not in allowed:
            raise HTTPException(status_code=400, detail="Неподдерживаемый формат фото")
        safe_name = f"{uuid4().hex}{ext}"
        target = Path("app/static/uploads") / safe_name
        content = await photo.read()
        target.write_bytes(content)
        image_path = f"/static/uploads/{safe_name}"

    if edit_message_id is not None:
        updated = None
        if text_value and not image_path:
            updated = await update_chat_message_text(
                db=db,
                chat_message_id=edit_message_id,
                new_text=text_value,
            )
        suffix = "1" if updated else "0"
        redirect_url = f"/admin/chats?conversation_id={conversation_id}&edited={suffix}"
        if q.strip():
            redirect_url += f"&q={quote_plus(q.strip())}"
        if view_value == "chat":
            redirect_url += "&view=chat"
        return RedirectResponse(url=redirect_url, status_code=302)

    if text_value.startswith("/") and not image_path:
        sent_ok = await send_admin_quick_reply(
            db=db,
            conversation_id=conversation_id,
            command_text=text_value,
        )
        suffix = "1" if sent_ok else "0"
        redirect_url = f"/admin/chats?conversation_id={conversation_id}&quick={suffix}"
        if q.strip():
            redirect_url += f"&q={quote_plus(q.strip())}"
        if view_value == "chat":
            redirect_url += "&view=chat"
        return RedirectResponse(url=redirect_url, status_code=302)

    sent_ok = await send_admin_chat_message(
        db=db,
        conversation_id=conversation_id,
        text=text_value,
        image_path=image_path,
    )
    suffix = "1" if sent_ok else "0"
    redirect_url = f"/admin/chats?conversation_id={conversation_id}&sent={suffix}"
    if q.strip():
        redirect_url += f"&q={quote_plus(q.strip())}"
    if view_value == "chat":
        redirect_url += "&view=chat"
    return RedirectResponse(url=redirect_url, status_code=302)


@app.post("/admin/chats/{conversation_id}/quick-reply", response_class=RedirectResponse)
async def admin_chats_send_quick_reply(
    conversation_id: int,
    command: str = Form(""),
    _admin: str = Depends(require_admin),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    sent_ok = await send_admin_quick_reply(
        db=db,
        conversation_id=conversation_id,
        command_text=command,
    )
    suffix = "1" if sent_ok else "0"
    return RedirectResponse(
        url=f"/admin/chats?conversation_id={conversation_id}&quick={suffix}",
        status_code=302,
    )


@app.get("/admin/chats/{conversation_id}/quick-options")
def admin_chats_quick_options(
    conversation_id: int,
    q: str = "",
    _admin: str = Depends(require_admin),
    db: Session = Depends(get_db),
) -> dict:
    conversation = (
        db.query(QuickReply)
        .filter(QuickReply.is_active.is_(True))
        .order_by(QuickReply.command.asc())
        .all()
    )
    query = q.strip().lstrip("/").lower()
    results = []
    for item in conversation:
        if query:
            hay = f"{item.command} {item.title} {item.text}".lower()
            if query not in hay:
                continue
        results.append(
            {
                "command": item.command,
                "title": item.title,
                "text": item.text,
                "has_image": bool(item.image_path),
            }
        )
        if len(results) >= 12:
            break
    return {"conversation_id": conversation_id, "items": results}


@app.post("/admin/chats/{conversation_id}/messages/{chat_message_id}/edit", response_class=RedirectResponse)
async def admin_chats_edit_message(
    conversation_id: int,
    chat_message_id: int,
    text: str = Form(""),
    _admin: str = Depends(require_admin),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    updated = await update_chat_message_text(
        db=db,
        chat_message_id=chat_message_id,
        new_text=text.strip(),
    )
    suffix = "1" if updated else "0"
    return RedirectResponse(
        url=f"/admin/chats?conversation_id={conversation_id}&edited={suffix}",
        status_code=302,
    )


@app.post("/admin/chats/{conversation_id}/messages/{chat_message_id}/delete", response_class=RedirectResponse)
async def admin_chats_delete_message(
    conversation_id: int,
    chat_message_id: int,
    q: str = Form(""),
    view: str = Form(""),
    _admin: str = Depends(require_admin),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    removed = await remove_chat_message(db=db, chat_message_id=chat_message_id)
    suffix = "1" if removed else "0"
    redirect_url = f"/admin/chats?conversation_id={conversation_id}&deleted={suffix}"
    if q.strip():
        redirect_url += f"&q={quote_plus(q.strip())}"
    if view.strip().lower() == "chat":
        redirect_url += "&view=chat"
    return RedirectResponse(url=redirect_url, status_code=302)


@app.post(
    "/admin/chats/{conversation_id}/messages/{chat_message_id}/retry",
    response_class=RedirectResponse,
)
async def admin_chats_retry_message(
    conversation_id: int,
    chat_message_id: int,
    _admin: str = Depends(require_admin),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    retried = await retry_failed_outbox_message(db=db, chat_message_id=chat_message_id)
    suffix = "1" if retried else "0"
    return RedirectResponse(
        url=f"/admin/chats?conversation_id={conversation_id}&retried={suffix}",
        status_code=302,
    )


@app.post("/admin/chats/{conversation_id}/delete-user", response_class=RedirectResponse)
async def admin_chats_delete_conversation(
    conversation_id: int,
    _admin: str = Depends(require_admin),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    deleted = delete_conversation(db, conversation_id=conversation_id)
    suffix = "1" if deleted else "0"
    return RedirectResponse(url=f"/admin/chats?removed={suffix}", status_code=302)


@app.post(webhook_path)
@app.post("/webhook/max")
@app.post("/max-webhook")
@app.post("/max-webhok")
@app.post("/webhok/max")
async def max_webhook(
    payload: dict = Body(...),
    db: Session = Depends(get_db),
) -> dict:
    event = MaxWebhookEvent.from_payload(payload)
    if event is None:
        # Ignore non-message updates or malformed events without failing webhook delivery.
        return {"ok": True, "ignored": "unsupported_payload"}

    # Webhook dedup by stable event UID.
    event_uid = event.event_uid_value()
    if event_uid:
        seen = db.query(WebhookEvent).filter(WebhookEvent.event_uid == event_uid).first()
        if seen:
            return {"ok": True, "ignored": "duplicate_event"}
        db.add(WebhookEvent(event_uid=event_uid, update_type=event.update_type))
        db.commit()

    accepted_update_types = {
        "message_created",
        "message_callback",
        "new_message",
        "bot_started",
        "bot_start",
    }
    if event.update_type and event.update_type not in accepted_update_types:
        return {"ok": True, "ignored": event.update_type}

    settings_db = get_or_create_settings(db)
    max_client = MaxClient()

    if event.sender_id == settings.max_bot_account_id:
        return {"ok": True}

    if settings_db.admin_account_id and event.sender_id == settings_db.admin_account_id:
        return {"ok": True, "ignored": "admin"}

    if event.sender_id == settings_db.manager_account_id:
        return await handle_manager_message(
            db=db,
            client=max_client,
            settings=settings_db,
            event=event,
        )

    return await handle_customer_event(
        db=db,
        client=max_client,
        settings=settings_db,
        event=event,
    )
