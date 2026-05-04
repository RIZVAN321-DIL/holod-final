import os
from zoneinfo import ZoneInfo

TIMEZONE = ZoneInfo("Europe/Moscow")

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_ID = int(os.getenv("ADMIN_ID", 0))
MASTER_PHONE = os.getenv("MASTER_PHONE", "+7 (900) 123-45-67")

# Слоты для установки: 3 слота в день
WORK_SLOTS = ["09:00-12:00", "12:00-15:00", "15:00-18:00"]

# Символ для заблокированной даты
BLOCK_SYMBOL = "❌"