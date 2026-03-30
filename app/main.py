from __future__ import annotations

from pathlib import Path
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
    ensure_default_templates,
    get_template_text,
    handle_customer_event,
    handle_manager_message,
    set_template_text,
)
from app.max_client import MaxClient
from app.models import QuickReply
from app.schemas import MaxWebhookEvent
from app.services import get_or_create_settings
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
    from app.database import SessionLocal

    with SessionLocal() as db:
        ensure_default_templates(db)


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
            "template_prestart": get_template_text(db, TEMPLATE_PRESTART),
            "template_start": get_template_text(db, TEMPLATE_START),
            "template_after_phone": get_template_text(db, TEMPLATE_AFTER_PHONE),
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
                "template_prestart": get_template_text(db, TEMPLATE_PRESTART),
                "template_start": get_template_text(db, TEMPLATE_START),
                "template_after_phone": get_template_text(db, TEMPLATE_AFTER_PHONE),
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
            "template_prestart": get_template_text(db, TEMPLATE_PRESTART),
            "template_start": get_template_text(db, TEMPLATE_START),
            "template_after_phone": get_template_text(db, TEMPLATE_AFTER_PHONE),
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

    if event.update_type and event.update_type not in {"message_created", "bot_started"}:
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
