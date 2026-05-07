from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
from datetime import datetime, timedelta
from config import WORK_SLOTS, BLOCK_SYMBOL, ALLOWED_CITIES

def get_month_name(date_obj):
    months = {
        1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
        5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
        9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь"
    }
    return months.get(date_obj.month, "")

def generate_calendar(year, month, blocked_dates):
    first_day = datetime(year, month, 1)
    start_weekday = first_day.weekday()
    if month == 12:
        next_month = datetime(year+1, 1, 1)
    else:
        next_month = datetime(year, month+1, 1)
    days_in_month = (next_month - first_day).days

    keyboard = []
    month_name = get_month_name(first_day)
    nav_buttons = [
        InlineKeyboardButton(text="◀️", callback_data=f"cal_prev_{year}_{month}"),
        InlineKeyboardButton(text=f"{month_name} {year}", callback_data="ignore"),
        InlineKeyboardButton(text="▶️", callback_data=f"cal_next_{year}_{month}")
    ]
    keyboard.append(nav_buttons)

    week_days = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
    keyboard.append([InlineKeyboardButton(text=d, callback_data="ignore") for d in week_days])

    row = []
    for _ in range(start_weekday):
        row.append(InlineKeyboardButton(text=" ", callback_data="ignore"))

    for day in range(1, days_in_month+1):
        date_str = f"{year:04d}-{month:02d}-{day:02d}"
        display = str(day)
        if date_str in blocked_dates:
            display += BLOCK_SYMBOL
        row.append(InlineKeyboardButton(text=display, callback_data=f"date_{date_str}"))
        if len(row) == 7:
            keyboard.append(row)
            row = []
    if row:
        while len(row) < 7:
            row.append(InlineKeyboardButton(text=" ", callback_data="ignore"))
        keyboard.append(row)

    keyboard.append([InlineKeyboardButton(text="◀️ Назад в меню", callback_data="back_to_menu")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def main_menu():
    buttons = [
        [KeyboardButton(text="⚡ Установка кондиционера")],
        [KeyboardButton(text="❄️ Обслуживание")],
        [KeyboardButton(text="📞 Консультация")],
        [KeyboardButton(text="📋 Мои записи")],
        [KeyboardButton(text="📞 Поделиться ботом")]
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)

def admin_menu():
    buttons = [
        [KeyboardButton(text="📋 Все записи"), KeyboardButton(text="📞 Быстрая запись (по звонку)")],
        [KeyboardButton(text="❌ Отменить/Перенести"), KeyboardButton(text="⛔ Выходной")],
        [KeyboardButton(text="🗓 Открыть день"), KeyboardButton(text="📊 Статистика/История")],
        [KeyboardButton(text="📞 Поделиться ботом")]
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)

def service_buttons():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❄️ Установка кондиционера", callback_data="service_установка")]
    ])

def quick_or_manual():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚡ Ближайшее время", callback_data="quick_auto")],
        [InlineKeyboardButton(text="📅 Выбрать вручную", callback_data="manual_date")]
    ])

def admin_quick_or_manual():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚡ Ближайшее время", callback_data="admin_quick_auto")],
        [InlineKeyboardButton(text="📅 Выбрать вручную", callback_data="admin_manual_date")]
    ])

def time_slots_buttons(free_slots):
    keyboard = []
    for slot in free_slots:
        keyboard.append([InlineKeyboardButton(text=slot, callback_data=f"slot_{slot}")])
    keyboard.append([InlineKeyboardButton(text="◀️ Назад к дате", callback_data="back_to_date")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def confirm_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить", callback_data="confirm_yes")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="confirm_no")]
    ])

def admin_confirm_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить", callback_data="confirm_admin_yes")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="confirm_no")]
    ])

def cities_keyboard(page=0, items_per_page=10):
    total = len(ALLOWED_CITIES)
    start = page * items_per_page
    end = min(start + items_per_page, total)
    buttons = []
    for city in ALLOWED_CITIES[start:end]:
        buttons.append([InlineKeyboardButton(text=city, callback_data=f"city_{city}")])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="◀️ Назад", callback_data=f"city_page_{page-1}"))
    if end < total:
        nav.append(InlineKeyboardButton(text="▶️ Вперёд", callback_data=f"city_page_{page+1}"))
    if nav:
        buttons.append(nav)
    buttons.append([InlineKeyboardButton(text="✍️ Ввести вручную", callback_data="manual_city_input")])
    buttons.append([InlineKeyboardButton(text="◀️ Назад в меню", callback_data="back_to_menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def phone_request_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📱 Отправить номер", request_contact=True)],
            [KeyboardButton(text="✍️ Ввести вручную")]
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )

def cancel_order_inline(order_id):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отменить", callback_data=f"client_cancel_{order_id}")]
    ])

def confirm_cancel_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Да, отменить", callback_data="cancel_confirm_yes")],
        [InlineKeyboardButton(text="❌ Нет", callback_data="cancel_confirm_no")]
    ])

def confirm_block_day_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Да, заблокировать день", callback_data="block_day_yes")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="block_day_no")]
    ])

def confirm_unblock_day_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Да, открыть день", callback_data="unblock_day_yes")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="unblock_day_no")]
    ])

def move_bookings_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Перенести", callback_data="move_bookings_yes")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="move_bookings_no")]
    ])

def orders_list_keyboard(orders, page, total_pages):
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    for order in orders:
        order_id, service, date_str, slot, name, phone, user_id = order
        display_date = datetime.strptime(date_str, "%Y-%m-%d").strftime("%d.%m")
        marker = " 📞" if user_id == 0 else ""
        text = f"ID {order_id}: {display_date} {slot} | {name}{marker}"
        kb.inline_keyboard.append([InlineKeyboardButton(text=text, callback_data=f"select_order_{order_id}")])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="◀️ Назад", callback_data=f"orders_page_{page-1}"))
    if page + 1 < total_pages:
        nav.append(InlineKeyboardButton(text="▶️ Вперёд", callback_data=f"orders_page_{page+1}"))
    if nav:
        kb.inline_keyboard.append(nav)
    kb.inline_keyboard.append([InlineKeyboardButton(text="◀️ Назад в меню", callback_data="back_to_menu")])
    return kb

def cancel_or_move_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отменить", callback_data="admin_cancel_only")],
        [InlineKeyboardButton(text="🔄 Перенести", callback_data="admin_move_booking")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_orders_list")]
    ])

def admin_move_choice_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚡ Ближайшее время", callback_data="auto_move")],
        [InlineKeyboardButton(text="📅 Выбрать вручную", callback_data="manual_move")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_orders_list")]
    ])

def confirm_move_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить перенос", callback_data="confirm_move_yes")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="confirm_move_no")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_orders_list")]
    ])

def stats_or_history_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats")],
        [InlineKeyboardButton(text="📜 История отмен", callback_data="admin_history")]
    ])