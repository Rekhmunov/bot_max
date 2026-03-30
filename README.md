# bot_max

Бот для мессенджера Max с админ-панелью:

- показывает покупателю настраиваемые сообщения до/после Start и после подтверждения номера;
- просит покупателя подтвердить контакт (anti-fraud) и сохраняет верифицированные номера;
- пересылает сообщения покупателя менеджеру в менеджерский чат (мост), менеджер отвечает reply-сообщениями;
- даёт менеджеру быстрые ответы по командам `/...` в менеджерском чате;
- быстрые ответы создаются в админке, поддерживают текст и фото;
- ограничение доступа к админке по логину/паролю;
- можно игнорировать сообщения аккаунта админа Max (по ID в настройках).

## Стек

- Python + FastAPI
- SQLite (по умолчанию)
- SQLAlchemy
- Jinja2 (встроенная веб-админка)

## Быстрый старт (локально)

1. Создайте и активируйте виртуальное окружение:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

2. Установите зависимости:

```bash
pip install -r requirements.txt
```

3. Создайте `.env` из примера:

```bash
cp .env.example .env
```

4. Запустите приложение:

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

5. Откройте админку:

- `http://localhost:8000/admin/login`

## Основные переменные окружения

- `ADMIN_USERNAME`, `ADMIN_PASSWORD` — доступ к админке
- `MAX_API_BASE_URL` — базовый URL API Max (должен быть `https://platform-api.max.ru`)
- `MAX_BOT_TOKEN` — токен бота Max
- `MAX_BOT_ACCOUNT_ID` — ID аккаунта бота в Max
- `PUBLIC_BASE_URL` — публичный HTTPS URL сервера (важно для фото в быстрых ответах)
- `WEBHOOK_PATH` — путь webhook (по умолчанию `/webhook/max`)

## Как работает webhook

Формат события (JSON), который ожидает endpoint:

```json
{
  "chat_id": "chat_123",
  "sender_id": "user_456",
  "text": "Привет"
}
```

Логика моста (покупатель -> бот -> менеджер):

1. Покупатель пишет боту:
   - бот показывает сообщение до Start (редактируется в админке),
   - после Start отправляет настраиваемое сообщение с запросом контакта.
2. После подтверждения контакта:
   - бот благодарит и просит описать товар (текст настраивается в админке),
   - сохраняет контакт в БД,
   - пересылает диалог менеджеру в менеджерский чат с тикет-меткой.
3. Менеджер отвечает в менеджерском чате:
   - обычным reply-сообщением (бот отправит покупателю),
   - или `/command` в reply для быстрого ответа (текст/фото из админки).

## Развертывание на сервере REG.RU

Ниже рабочая схема: **FastAPI + systemd + Nginx + HTTPS**.

### 1) Подготовка проекта на сервере

```bash
sudo mkdir -p /opt/bot_max
sudo chown -R $USER:$USER /opt/bot_max
cd /opt/bot_max
git clone <your-repo-url> .
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Заполните `.env` (особенно `PUBLIC_BASE_URL`, `MAX_BOT_TOKEN`, `MAX_BOT_ACCOUNT_ID`).

### 2) systemd сервис

Создайте `/etc/systemd/system/bot-max.service`:

```ini
[Unit]
Description=Max Support Bot
After=network.target

[Service]
User=www-data
Group=www-data
WorkingDirectory=/opt/bot_max
EnvironmentFile=/opt/bot_max/.env
ExecStart=/opt/bot_max/.venv/bin/uvicorn app.main:app --host 127.0.0.1 --port 8000
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
```

Права:

```bash
sudo chown -R www-data:www-data /opt/bot_max
sudo systemctl daemon-reload
sudo systemctl enable bot-max
sudo systemctl start bot-max
sudo systemctl status bot-max
```

### 3) Nginx reverse proxy

Пример `/etc/nginx/sites-available/bot-max`:

```nginx
server {
    listen 80;
    server_name your-domain.ru www.your-domain.ru;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

Активируйте конфиг:

```bash
sudo ln -s /etc/nginx/sites-available/bot-max /etc/nginx/sites-enabled/bot-max
sudo nginx -t
sudo systemctl reload nginx
```

### 4) HTTPS (обязательно)

Для webhook нужен HTTPS. Обычно удобно через certbot:

```bash
sudo apt-get update
sudo apt-get install -y certbot python3-certbot-nginx
sudo certbot --nginx -d your-domain.ru -d www.your-domain.ru
```

Проверьте:

- `https://your-domain.ru/health`
- `https://your-domain.ru/admin/login`

### 5) Указать webhook в Max

В кабинете/настройках бота Max укажите:

- `https://your-domain.ru/webhook/max` (или ваш `WEBHOOK_PATH`)

## Примечания

- Сейчас база данных по умолчанию SQLite (`bot.db`), достаточно для MVP.
- Для production можно перейти на PostgreSQL, поменяв `DATABASE_URL`.
- Фото быстрых ответов хранятся локально в `app/static/uploads`.
