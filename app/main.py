from __future__ import annotations

from pathlib import Path
from uuid import uuid4

from fastapi import Depends, FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware
from sqlalchemy.orm import Session

from app.auth import get_current_admin, require_admin, sign_in_admin
from app.config import settings
from app.database import get_db, init_db
from app.max_client import MaxClient
from app.models import QuickReply
from app.schemas import MaxWebhookEvent
from app.services import (
    get_or_create_settings,
    handle_manager_command,
    process_incoming_customer_message,
)
from fastapi.templating import Jinja2Templates

app = FastAPI(title=settings.app_name)
templates = Jinja2Templates(directory="app/templates")
app.add_middleware(SessionMiddleware, secret_key=settings.secret_key)
webhook_path = settings.webhook_path if settings.webhook_path.startswith("/") else f"/{settings.webhook_path}"

Path("app/static/uploads").mkdir(parents=True, exist_ok=True)
app.mount("/static", StaticFiles(directory="app/static"), name="static")


@app.on_event("startup")
def startup() -> None:
    init_db()


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
    return templates.TemplateResponse(
        request,
        "admin.html",
        {
            "request": request,
            "settings": bot_settings,
            "quick_replies": replies,
            "webhook_path": webhook_path,
            "webhook_url": f"{settings.public_base_url.rstrip('/')}{webhook_path}",
            "message": None,
            "error": None,
        },
    )


@app.post("/admin/settings", response_class=HTMLResponse)
def update_settings(
    request: Request,
    greeting_text: str = Form(...),
    manager_account_id: str = Form(...),
    admin_account_id: str = Form(""),
    manager_added_notice_text: str = Form("Менеджер подключен к диалогу."),
    _admin: str = Depends(get_current_admin),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    if _admin is None:
        return RedirectResponse(url="/admin/login", status_code=302)

    bot_settings = get_or_create_settings(db)
    bot_settings.greeting_text = greeting_text.strip()
    bot_settings.manager_account_id = manager_account_id.strip()
    bot_settings.admin_account_id = admin_account_id.strip()
    bot_settings.manager_added_notice_text = manager_added_notice_text.strip()
    db.add(bot_settings)
    db.commit()

    replies = db.query(QuickReply).order_by(QuickReply.command.asc()).all()
    return templates.TemplateResponse(
        request,
        "admin.html",
        {
            "request": request,
            "settings": bot_settings,
            "quick_replies": replies,
            "webhook_path": webhook_path,
            "webhook_url": f"{settings.public_base_url.rstrip('/')}{webhook_path}",
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
        return templates.TemplateResponse(
            request,
            "admin.html",
            {
                "request": request,
                "settings": bot_settings,
                "quick_replies": replies,
                "webhook_path": webhook_path,
                "webhook_url": f"{settings.public_base_url.rstrip('/')}{webhook_path}",
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
    return templates.TemplateResponse(
        request,
        "admin.html",
        {
            "request": request,
            "settings": bot_settings,
            "quick_replies": replies,
            "webhook_path": webhook_path,
            "webhook_url": f"{settings.public_base_url.rstrip('/')}{webhook_path}",
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


@app.post(webhook_path)
async def max_webhook(event: MaxWebhookEvent, db: Session = Depends(get_db)) -> dict:
    settings_db = get_or_create_settings(db)
    max_client = MaxClient()

    if event.sender_id == settings.max_bot_account_id:
        return {"ok": True}

    # Manager commands in shared chat (/command)
    if event.sender_id == settings_db.manager_account_id and event.text.startswith("/"):
        sent = await handle_manager_command(
            db=db,
            client=max_client,
            chat_id=event.chat_id,
            command_text=event.text,
        )
        return {"ok": True, "command_sent": sent}

    # First contact from customer: greet and add manager.
    if event.sender_id != settings_db.manager_account_id:
        if settings_db.admin_account_id and event.sender_id == settings_db.admin_account_id:
            return {"ok": True, "ignored": "admin"}
        await process_incoming_customer_message(
            db=db,
            client=max_client,
            customer_id=event.sender_id,
            chat_id=event.chat_id,
        )

    return {"ok": True}
