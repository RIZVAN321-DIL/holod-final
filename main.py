import asyncio
import re
import logging
import sqlite3
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery, ReplyKeyboardRemove
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.filters import Command, StateFilter
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from aiogram.client.default import DefaultBotProperties

from config import BOT_TOKEN, ADMIN_ID, MASTER_PHONE, WORK_SLOTS, TIMEZONE, BLOCK_SYMBOL, ALLOWED_CITIES
from database import (
    init_db, is_slot_free, book_slot, get_active_order_count,
    get_user_orders, get_all_future_orders, get_orders_for_today,
    get_orders_for_tomorrow, get_order_by_id, cancel_order,
    get_orders_for_reminder_24h, get_orders_for_reminder_2h,
    mark_reminder_sent, is_user_banned, ban_user, unban_user,
    block_day, unblock_day, is_day_blocked, get_cancelled_orders,
    save_cancelled_order, update_order_slot
)
from keyboards import (
    main_menu, admin_menu, service_buttons, quick_or_manual, admin_quick_or_manual,
    generate_calendar, time_slots_buttons, confirm_keyboard, admin_confirm_keyboard,
    cities_keyboard, phone_request_keyboard,
    cancel_order_inline, confirm_cancel_keyboard,
    confirm_block_day_keyboard, confirm_unblock_day_keyboard,
    move_bookings_keyboard, cancel_or_move_keyboard, confirm_move_keyboard,
    stats_or_history_keyboard, orders_list_keyboard, admin_move_choice_keyboard
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()

# ---------- ХЕЛПЕРЫ ----------
def now_moscow():
    return datetime.now(TIMEZONE)

def format_date(date_str: str) -> str:
    return datetime.strptime(date_str, "%Y-%m-%d").strftime("%d.%m")

def validate_phone(phone_raw: str) -> bool:
    digits = re.sub(r'\D', '', phone_raw)
    return len(digits) >= 10 and digits.startswith(('7', '8'))

def get_blocked_dates():
    now = now_moscow()
    blocked = []
    for i in range(90):
        check_date = (now + timedelta(days=i)).strftime("%Y-%m-%d")
        if datetime.strptime(check_date, "%Y-%m-%d").date() < now.date():
            blocked.append(check_date)
            continue
        free_count = sum(1 for slot in WORK_SLOTS if is_slot_free(check_date, slot))
        if free_count == 0 or is_day_blocked(check_date):
            blocked.append(check_date)
    return blocked

# ---------- ПОИСК СВОБОДНОГО СЛОТА ДЛЯ АВТОМАТИЧЕСКОГО ПЕРЕНОСА ----------
async def find_next_free_slot(after_date: str, avoid_date: str = None):
    start_date = datetime.strptime(after_date, "%Y-%m-%d").date()
    if avoid_date:
        avoid = datetime.strptime(avoid_date, "%Y-%m-%d").date()
    else:
        avoid = None
    for i in range(30):
        check_date = start_date + timedelta(days=i)
        if avoid and check_date == avoid:
            continue
        date_str = check_date.strftime("%Y-%m-%d")
        for slot in WORK_SLOTS:
            if is_slot_free(date_str, slot):
                if date_str == now_moscow().strftime("%Y-%m-%d"):
                    slot_start_hour = int(slot.split(":")[0])
                    if slot_start_hour <= now_moscow().hour:
                        continue
                return date_str, slot
    return None, None

# ---------- ПЕРЕНОС ЗАПИСИ (ОБНОВЛЕНИЕ В БД) ----------
async def move_booking(order_id, new_date, new_slot):
    order = get_order_by_id(order_id)
    if not order or order[9] != 'active':
        return False, None
    old_date, old_slot = order[3], order[4]
    if not is_slot_free(new_date, new_slot):
        return False, None
    conn = sqlite3.connect("cond.db")
    cur = conn.cursor()
    cur.execute("DELETE FROM bookings WHERE date=? AND slot=?", (old_date, old_slot))
    cur.execute("INSERT INTO bookings (date, slot) VALUES (?, ?)", (new_date, new_slot))
    cur.execute("UPDATE orders SET date=?, slot=? WHERE id=?", (new_date, new_slot, order_id))
    conn.commit()
    conn.close()
    return True, (old_date, old_slot)

# ---------- FSM ----------
class BookingState(StatesGroup):
    service = State()
    date = State()
    slot = State()
    name = State()
    phone = State()
    city = State()
    address = State()
    ready_to_book = State()

class QuickState(StatesGroup):
    service = State()
    date = State()
    slot = State()
    name = State()
    phone = State()
    city = State()
    address = State()
    ready = State()

class AdminCancelMoveState(StatesGroup):
    choosing_order = State()
    choosing_action = State()
    confirm_auto_move = State()
    waiting_for_new_date = State()
    waiting_for_new_slot = State()
    confirm_new_booking = State()

class AdminBlockDayState(StatesGroup):
    waiting_for_date = State()
    wait_for_move_choice = State()

class AdminUnblockDayState(StatesGroup):
    waiting_for_date = State()
    confirm = State()

# ---------- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ДЛЯ СПИСКА ЗАКАЗОВ ----------
async def send_orders_page(message_or_callback, state, page=0):
    orders = get_all_future_orders()
    if not orders:
        await message_or_callback.edit_text("📭 <b>Нет будущих записей.</b>")
        await state.clear()
        return
    total = len(orders)
    total_pages = (total + 4) // 5
    page_orders = orders[page*5 : (page+1)*5]
    orders_for_kb = []
    for o in page_orders:
        order_id, user_id, service, date_str, slot, name, phone, city, address = o
        orders_for_kb.append((order_id, service, date_str, slot, name, phone, user_id))
    kb = orders_list_keyboard(orders_for_kb, page, total_pages)
    await message_or_callback.edit_text("📋 <b>Выберите запись для отмены или переноса:</b>", reply_markup=kb)
    await state.update_data(current_page=page, orders=orders)

# ---------- КОМАНДА /cancel ----------
@dp.message(Command("cancel"))
async def cancel_cmd(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("❌ Действие отменено.")
    if message.from_user.id == ADMIN_ID:
        await message.answer("Меню администратора", reply_markup=admin_menu())
    else:
        await message.answer("Главное меню", reply_markup=main_menu())

# ---------- НАЗАД ----------
@dp.callback_query(F.data == "back_to_menu")
async def back_to_menu(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    user_id = callback.from_user.id
    try:
        await callback.message.delete()
    except:
        pass
    text = "❄️ Кондиционер-сервис. Запись на установку."
    if user_id == ADMIN_ID:
        await callback.message.answer(text, reply_markup=admin_menu())
    else:
        await callback.message.answer(text, reply_markup=main_menu())
    await callback.answer()

@dp.callback_query(F.data == "back_to_date")
async def back_to_date(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if "service" not in data:
        await callback.answer("Ошибка, начните заново", show_alert=True)
        await callback.message.delete()
        await callback.message.answer("Главное меню", reply_markup=main_menu())
        await state.clear()
        return
    await callback.message.delete()
    now = now_moscow()
    year = data.get("calendar_year", now.year)
    month = data.get("calendar_month", now.month)
    blocked_dates = get_blocked_dates()
    await callback.message.answer("📅 Выберите дату:", reply_markup=generate_calendar(year, month, blocked_dates))
    await state.update_data(blocked_dates=blocked_dates, calendar_year=year, calendar_month=month)
    await state.set_state(BookingState.date)
    await callback.answer()

@dp.callback_query(F.data == "ignore")
async def ignore_callback(callback: CallbackQuery):
    await callback.answer()

# ---------- КАЛЕНДАРЬ: ПЕРЕКЛЮЧЕНИЕ МЕСЯЦЕВ ----------
@dp.callback_query(F.data.startswith("cal_prev_"))
async def calendar_prev(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    parts = callback.data.split("_")
    if len(parts) != 4:
        logger.error(f"Неверный формат callback: {callback.data}")
        return
    try:
        year = int(parts[2])
        month = int(parts[3])
    except:
        logger.error(f"Ошибка преобразования year/month: {parts}")
        return
    month -= 1
    if month < 1:
        month = 12
        year -= 1
    data = await state.get_data()
    blocked_dates = data.get("blocked_dates", get_blocked_dates())
    try:
        await callback.message.edit_reply_markup(reply_markup=generate_calendar(year, month, blocked_dates))
    except Exception as e:
        logger.error(f"Ошибка при edit_reply_markup: {e}")
        await callback.message.edit_text("📅 Выберите дату:", reply_markup=generate_calendar(year, month, blocked_dates))
    await state.update_data(calendar_year=year, calendar_month=month, blocked_dates=blocked_dates)

@dp.callback_query(F.data.startswith("cal_next_"))
async def calendar_next(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    parts = callback.data.split("_")
    if len(parts) != 4:
        logger.error(f"Неверный формат callback: {callback.data}")
        return
    try:
        year = int(parts[2])
        month = int(parts[3])
    except:
        logger.error(f"Ошибка преобразования year/month: {parts}")
        return
    month += 1
    if month > 12:
        month = 1
        year += 1
    data = await state.get_data()
    blocked_dates = data.get("blocked_dates", get_blocked_dates())
    try:
        await callback.message.edit_reply_markup(reply_markup=generate_calendar(year, month, blocked_dates))
    except Exception as e:
        logger.error(f"Ошибка при edit_reply_markup: {e}")
        await callback.message.edit_text("📅 Выберите дату:", reply_markup=generate_calendar(year, month, blocked_dates))
    await state.update_data(calendar_year=year, calendar_month=month, blocked_dates=blocked_dates)

# ---------- СТАРТ ----------
@dp.message(Command("start"))
async def start_cmd(message: Message):
    user_id = message.from_user.id
    if is_user_banned(user_id):
        await message.answer("❌ Вы заблокированы.")
        logger.warning(f"Заблокированный пользователь {user_id} попытался начать")
        return
    text = "❄️ Кондиционер-сервис. Запись на установку."
    if user_id == ADMIN_ID:
        await message.answer(text, reply_markup=admin_menu())
    else:
        await message.answer(text, reply_markup=main_menu())
    logger.info(f"Пользователь {user_id} запустил бота")

@dp.message(Command("help"))
async def help_cmd(message: Message):
    user_id = message.from_user.id
    if user_id == ADMIN_ID:
        help_text = (
            "👨‍💼 <b>Помощь для администратора</b>\n\n"
            "📋 <b>Все записи</b> – список будущих заказов\n"
            "📞 <b>Быстрая запись (по звонку)</b> – добавить клиента\n"
            "❌ <b>Отменить/Перенести</b> – выбор записи, затем отмена или перенос (авто или вручную)\n"
            "⛔ <b>Выходной</b> – заблокировать день с переносом записей\n"
            "🗓 <b>Открыть день</b> – снять блокировку\n"
            "📊 <b>Статистика/История</b> – статистика и список отменённых заказов\n"
            "📞 <b>Поделиться</b> – ссылка на бота\n\n"
            "Команды: /ban &lt;id&gt;, /unban &lt;id&gt;, /help, /cancel"
        )
    else:
        help_text = (
            "❄️ <b>Помощь</b>\n\n"
            "⚡ <b>Установка кондиционера</b> – запись через календарь\n"
            "❄️ <b>Обслуживание</b> – контактный телефон\n"
            "📞 <b>Консультация</b> – связаться с мастером\n"
            "📋 <b>Мои записи</b> – посмотреть и отменить активные записи\n"
            "📞 <b>Поделиться</b> – ссылка на бота\n\n"
            "Напоминания придут за 24 и 2 часа до визита."
        )
    await message.answer(help_text)

# ---------- ОБСЛУЖИВАНИЕ И КОНСУЛЬТАЦИЯ ----------
@dp.message(F.text == "❄️ Обслуживание")
async def service_info(message: Message):
    await message.answer(f"📞 <b>Для обслуживания кондиционеров позвоните мастеру:</b>\n{MASTER_PHONE}")

@dp.message(F.text == "📞 Консультация")
async def consultation(message: Message):
    await message.answer(f"📞 <b>Консультация по телефону:</b>\n{MASTER_PHONE}")

# ---------- БАН / РАЗБАН ----------
@dp.message(Command("ban"))
async def ban_cmd(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    parts = message.text.split()
    if len(parts) != 2:
        await message.answer("Используйте: /ban 123456789")
        return
    try:
        uid = int(parts[1])
    except:
        await message.answer("ID должен быть числом")
        return
    ban_user(uid)
    await message.answer(f"✅ <b>Пользователь {uid} забанен.</b>")
    logger.info(f"Админ {ADMIN_ID} забанил пользователя {uid}")

@dp.message(Command("unban"))
async def unban_cmd(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    parts = message.text.split()
    if len(parts) != 2:
        await message.answer("Используйте: /unban 123456789")
        return
    try:
        uid = int(parts[1])
    except:
        await message.answer("ID должен быть числом")
        return
    unban_user(uid)
    await message.answer(f"✅ <b>Пользователь {uid} разбанен.</b>")
    logger.info(f"Админ {ADMIN_ID} разбанил пользователя {uid}")

# ---------- ЗАПИСЬ КЛИЕНТА ----------
@dp.message(F.text == "⚡ Установка кондиционера")
async def client_booking(message: Message, state: FSMContext):
    user_id = message.from_user.id
    if is_user_banned(user_id):
        await message.answer("❌ Вы заблокированы.")
        return
    if get_active_order_count(user_id) >= 1:
        await message.answer("❌ У вас уже есть активная запись. Отмените её, чтобы записаться снова.")
        return
    await state.clear()
    await message.answer("Выберите услугу:", reply_markup=service_buttons())
    await state.set_state(BookingState.service)

@dp.callback_query(BookingState.service, F.data.startswith("service_"))
async def service_chosen(callback: CallbackQuery, state: FSMContext):
    service = "Установка кондиционера"
    await state.update_data(service=service)
    await callback.message.edit_text("Как хотите записаться?", reply_markup=quick_or_manual())
    await state.set_state(BookingState.date)
    await callback.answer()

# ---------- БЛИЖАЙШЕЕ ВРЕМЯ ----------
@dp.callback_query(F.data == "quick_auto")
async def quick_booking(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    data = await state.get_data()
    if not data.get("service"):
        await callback.message.edit_text("❌ Ошибка: не выбрана услуга. Начните заново.", reply_markup=main_menu())
        await state.clear()
        return
    now = now_moscow()
    today = now.date()
    for i in range(14):
        check_date = (today + timedelta(days=i)).strftime("%Y-%m-%d")
        for slot in WORK_SLOTS:
            if not is_slot_free(check_date, slot):
                continue
            if check_date == now.strftime("%Y-%m-%d"):
                slot_start_hour = int(slot.split(":")[0])
                if slot_start_hour <= now.hour:
                    continue
            await state.update_data(date=check_date, slot=slot)
            await callback.message.edit_text("Введите ваше <b>ИМЯ</b>:")
            await state.set_state(BookingState.name)
            logger.info(f"Авто-слот найден: {check_date} {slot}")
            return
    await callback.message.edit_text("❌ Свободных слотов в ближайшие 14 дней не найдено.", reply_markup=quick_or_manual())

# ---------- РУЧНОЙ ВЫБОР ДАТЫ ----------
@dp.callback_query(F.data == "manual_date")
async def manual_date_selection(callback: CallbackQuery, state: FSMContext):
    now = now_moscow()
    year = now.year
    month = now.month
    blocked_dates = get_blocked_dates()
    await state.update_data(calendar_year=year, calendar_month=month, blocked_dates=blocked_dates)
    await callback.message.edit_text("📅 Выберите дату:", reply_markup=generate_calendar(year, month, blocked_dates))
    await state.set_state(BookingState.date)
    await callback.answer()

@dp.callback_query(BookingState.date, F.data.startswith("date_"))
async def date_chosen(callback: CallbackQuery, state: FSMContext):
    date_str = callback.data.split("_")[1]
    now = now_moscow()
    if datetime.strptime(date_str, "%Y-%m-%d").date() < now.date():
        await callback.answer("Эта дата уже прошла. Выберите другую.", show_alert=True)
        return
    free_slots = []
    for slot in WORK_SLOTS:
        if not is_slot_free(date_str, slot):
            continue
        if date_str == now.strftime("%Y-%m-%d"):
            slot_start_hour = int(slot.split(":")[0])
            if slot_start_hour <= now.hour:
                continue
        free_slots.append(slot)
    if not free_slots:
        await callback.answer("На эту дату все часы заняты или день заблокирован.", show_alert=True)
        return
    await state.update_data(date=date_str)
    await callback.message.edit_text(f"📅 <b>{format_date(date_str)}</b>\nВыберите час:", reply_markup=time_slots_buttons(free_slots))
    await state.set_state(BookingState.slot)
    await callback.answer()

@dp.callback_query(BookingState.slot, F.data.startswith("slot_"))
async def slot_chosen(callback: CallbackQuery, state: FSMContext):
    slot = callback.data.split("_", 1)[1]
    data = await state.get_data()
    if not is_slot_free(data['date'], slot):
        await callback.answer("Это время уже занято или день заблокирован", show_alert=True)
        return
    await state.update_data(slot=slot)
    await callback.message.edit_text("Введите ваше <b>ИМЯ</b>:")
    await state.set_state(BookingState.name)
    await callback.answer()

@dp.message(BookingState.name)
async def name_entered(message: Message, state: FSMContext):
    name = message.text.strip()
    if len(name) < 2:
        await message.answer("❌ Введите имя (минимум 2 символа).")
        return
    await state.update_data(name=name)
    await message.answer("📞 <b>Номер телефона</b> (можно отправить автоматически)", reply_markup=phone_request_keyboard())
    await state.set_state(BookingState.phone)

# ---------- ТЕЛЕФОН (АВТО/РУЧНОЙ) ----------
@dp.message(BookingState.phone, F.contact)
async def phone_contact(message: Message, state: FSMContext):
    phone = message.contact.phone_number
    await state.update_data(phone=phone)
    await message.answer("✅ Номер получен.", reply_markup=ReplyKeyboardRemove())
    await message.answer("🏙️ <b>Выберите населённый пункт</b> (листайте список или введите вручную):", reply_markup=cities_keyboard(0))
    await state.set_state(BookingState.city)

@dp.message(BookingState.phone)
async def phone_manual(message: Message, state: FSMContext):
    phone_raw = message.text.strip()
    if not validate_phone(phone_raw):
        await message.answer("❌ Некорректный номер. Введите российский номер (от 10 цифр).")
        return
    await state.update_data(phone=phone_raw)
    await message.answer("✅ Номер сохранён.", reply_markup=ReplyKeyboardRemove())
    await message.answer("🏙️ <b>Выберите населённый пункт</b> (листайте список или введите вручную):", reply_markup=cities_keyboard(0))
    await state.set_state(BookingState.city)

# ---------- ВЫБОР ГОРОДА (ПАГИНАЦИЯ + РУЧНОЙ ВВОД) ----------
@dp.callback_query(BookingState.city, F.data.startswith("city_page_"))
async def city_change_page(callback: CallbackQuery, state: FSMContext):
    page = int(callback.data.split("_")[-1])
    await callback.message.edit_reply_markup(reply_markup=cities_keyboard(page))
    await callback.answer()

@dp.callback_query(BookingState.city, F.data.startswith("city_"))
async def city_chosen(callback: CallbackQuery, state: FSMContext):
    city = callback.data.split("city_")[1]
    await state.update_data(city=city)
    await callback.message.edit_text(f"✅ Выбран населённый пункт: <b>{city}</b>\nТеперь введите <b>АДРЕС</b> (улица, дом):")
    await state.set_state(BookingState.address)
    await callback.answer()

@dp.callback_query(BookingState.city, F.data == "manual_city_input")
async def manual_city_input(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text("✍️ Введите <b>населённый пункт</b> (город, село, посёлок) вручную:")
    await state.set_state(BookingState.city)
    await callback.answer()

@dp.message(BookingState.city)
async def city_manual(message: Message, state: FSMContext):
    city = message.text.strip()
    if len(city) < 2:
        await message.answer("❌ Введите населённый пункт (минимум 2 символа).")
        return
    await state.update_data(city=city)
    await message.answer(f"✅ Выбран населённый пункт: <b>{city}</b>\nТеперь введите <b>АДРЕС</b> (улица, дом):")
    await state.set_state(BookingState.address)

# ---------- АДРЕС ----------
@dp.message(BookingState.address)
async def address_entered(message: Message, state: FSMContext):
    address = message.text.strip()
    if len(address) < 3:
        await message.answer("❌ Введите адрес (минимум 3 символа).")
        return
    await state.update_data(address=address)
    await confirm_booking_stage(message, state)

async def confirm_booking_stage(msg, state):
    data = await state.get_data()
    text = (
        f"✅ <b>Проверьте данные:</b>\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f"🔧 <b>Услуга:</b> {data['service']}\n"
        f"📅 <b>Дата:</b> {format_date(data['date'])}\n"
        f"⏰ <b>Время:</b> {data['slot']}\n"
        f"👤 <b>Имя:</b> {data['name']}\n"
        f"📞 <b>Телефон:</b> {data['phone']}\n"
        f"🏙️ <b>Населённый пункт:</b> {data['city']}\n"
        f"📍 <b>Адрес:</b> {data['address']}\n"
        f"━━━━━━━━━━━━━━━━━\n\n"
        f"Подтверждаете запись?"
    )
    await msg.answer(text, reply_markup=confirm_keyboard())
    await state.set_state(BookingState.ready_to_book)

@dp.callback_query(F.data == "confirm_yes", StateFilter(BookingState.ready_to_book))
async def confirm_booking(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    user_id = callback.from_user.id
    if not is_slot_free(data['date'], data['slot']):
        await callback.message.edit_text("❌ Этот слот только что заняли. Начните заново.")
        await state.clear()
        await callback.answer()
        return

    success = book_slot(data['date'], data['slot'], user_id, data['service'],
                        data['name'], data['phone'], data['city'], data['address'])
    if not success:
        await callback.message.edit_text("❌ Ошибка при записи. Попробуйте позже.")
        await state.clear()
        await callback.answer()
        return

    date_display = format_date(data['date'])
    await callback.message.edit_text(
        f"✅ <b>Вы записаны на установку кондиционера!</b>\n"
        f"📅 {date_display} {data['slot']}\n"
        f"👤 {data['name']}\n"
        f"📍 {data['city']}, {data['address']}\n\n"
        f"Напомним за 24 часа и за 2 часа."
    )

    admin_text = (
        f"<b>❗️ НОВАЯ ЗАПИСЬ НА УСТАНОВКУ</b>\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f"📅 <b>Дата:</b> {data['date']}\n"
        f"⏰ <b>Время:</b> {data['slot']}\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f"👤 <b>Клиент:</b> {data['name']}\n"
        f"📞 <b>Телефон:</b> {data['phone']}\n"
        f"🏙️ <b>Населённый пункт:</b> {data['city']}\n"
        f"📍 <b>Адрес:</b> {data['address']}\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f"<i>Требуется подтверждение мастера</i>"
    )
    try:
        await bot.send_message(ADMIN_ID, admin_text)
    except Exception as e:
        logger.error(f"Не удалось отправить уведомление админу: {e}")

    await state.clear()
    logger.info(f"Запись подтверждена: пользователь {user_id}, {data['date']} {data['slot']}")
    if user_id == ADMIN_ID:
        await callback.message.answer("Меню", reply_markup=admin_menu())
    else:
        await callback.message.answer("Меню", reply_markup=main_menu())
    await callback.answer()

@dp.callback_query(F.data == "confirm_no", StateFilter(BookingState.ready_to_book))
async def cancel_booking(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text("❌ Запись отменена.")
    await state.clear()
    if callback.from_user.id == ADMIN_ID:
        await callback.message.answer("Меню", reply_markup=admin_menu())
    else:
        await callback.message.answer("Меню", reply_markup=main_menu())
    await callback.answer()

# ---------- МОИ ЗАПИСИ ----------
@dp.message(F.text == "📋 Мои записи")
async def my_orders(message: Message):
    user_id = message.from_user.id
    orders = get_user_orders(user_id)
    if not orders:
        await message.answer("📭 <b>У вас нет активных записей.</b>")
        return
    for order_id, service, date_str, slot, name, phone, city, address in orders:
        date_display = format_date(date_str)
        text = (
            f"┌ <b>ID {order_id}</b>\n"
            f"├ 📅 {date_display} {slot}\n"
            f"├ ❄️ {service}\n"
            f"├ 👤 {name}\n"
            f"├ 📞 {phone}\n"
            f"├ 📍 {city}, {address}\n"
            f"└───────────────"
        )
        slot_start_hour = int(slot.split(":")[0])
        slot_dt = datetime.strptime(f"{date_str} {slot_start_hour:02d}:00", "%Y-%m-%d %H:%M")
        now_no_tz = now_moscow().replace(tzinfo=None)
        if (slot_dt - now_no_tz).total_seconds() > 2 * 3600:
            await message.answer(text, reply_markup=cancel_order_inline(order_id))
        else:
            await message.answer(text + "\n\n⚠️ <b>Отмена недоступна – менее 2 часов.</b>")

@dp.callback_query(F.data.startswith("client_cancel_"))
async def cancel_my_order(callback: CallbackQuery):
    parts = callback.data.split("_")
    if len(parts) != 3:
        await callback.answer("Ошибка формата", show_alert=True)
        await callback.answer()
        return
    try:
        order_id = int(parts[2])
    except ValueError:
        await callback.answer("Неверный ID", show_alert=True)
        await callback.answer()
        return
    user_id = callback.from_user.id
    success, info = cancel_order(order_id, user_id, is_admin=False, move_to_history=True, reason="client_cancel")
    if success:
        await callback.message.edit_text("✅ <b>Ваша запись отменена.</b>")
        try:
            await bot.send_message(ADMIN_ID, f"❌ <b>Клиент отменил запись #{order_id}</b>")
        except Exception as e:
            logger.error(f"Ошибка уведомления админа: {e}")
    else:
        if info == "too_late":
            await callback.answer("❌ Отмена невозможна – менее 2 часов.", show_alert=True)
        elif info == "not_yours":
            await callback.answer("❌ Это не ваша запись.", show_alert=True)
        else:
            await callback.answer("❌ Запись не найдена.", show_alert=True)
    await callback.answer()

# ---------- АДМИН: ВСЕ ЗАПИСИ ----------
@dp.message(F.text == "📋 Все записи")
async def admin_all_orders(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    orders = get_all_future_orders()
    if not orders:
        await message.answer("Нет будущих записей.")
        return
    text = "<b>📋 ВСЕ БУДУЩИЕ ЗАПИСИ:</b>\n\n"
    for o in orders:
        order_id, user_id, service, date_str, slot, name, phone, city, address = o
        date_display = format_date(date_str)
        marker = " 📞 (по телефону)" if user_id == 0 else ""
        text += (
            f"┌ <b>ID {order_id}</b>{marker}\n"
            f"├ 📅 <b>Дата:</b> {date_display} {slot}\n"
            f"├ 👤 <b>Клиент:</b> {name}\n"
            f"├ 📞 <b>Телефон:</b> {phone}\n"
            f"├ 📍 <b>Адрес:</b> {city}, {address}\n"
            f"└───────────────\n"
        )
    await message.answer(text[:4000])

# ---------- АДМИН: БЫСТРАЯ ЗАПИСЬ (ПО ЗВОНКУ) ----------
@dp.message(F.text == "📞 Быстрая запись (по звонку)")
async def admin_booking_phone(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    await state.clear()
    await message.answer("Выберите услугу:", reply_markup=service_buttons())
    await state.set_state(QuickState.service)

@dp.callback_query(QuickState.service, F.data.startswith("service_"))
async def admin_service_chosen(callback: CallbackQuery, state: FSMContext):
    service = "Установка кондиционера"
    await state.update_data(service=service)
    await callback.message.edit_text("Выберите способ записи:", reply_markup=admin_quick_or_manual())
    await state.set_state(QuickState.date)
    await callback.answer()

@dp.callback_query(F.data == "admin_quick_auto")
async def admin_quick_booking(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    data = await state.get_data()
    if not data.get("service"):
        await callback.message.edit_text("Ошибка, начните заново.")
        await state.clear()
        return
    now = now_moscow()
    today = now.date()
    for i in range(14):
        check_date = (today + timedelta(days=i)).strftime("%Y-%m-%d")
        for slot in WORK_SLOTS:
            if not is_slot_free(check_date, slot):
                continue
            if check_date == now.strftime("%Y-%m-%d"):
                slot_start_hour = int(slot.split(":")[0])
                if slot_start_hour <= now.hour:
                    continue
            await state.update_data(date=check_date, slot=slot)
            await callback.message.edit_text("Введите <b>ИМЯ</b> клиента:")
            await state.set_state(QuickState.name)
            return
    await callback.message.edit_text("❌ Нет свободных слотов в ближайшие 14 дней.", reply_markup=admin_quick_or_manual())

@dp.callback_query(F.data == "admin_manual_date")
async def admin_manual_date_selection(callback: CallbackQuery, state: FSMContext):
    now = now_moscow()
    year = now.year
    month = now.month
    blocked_dates = get_blocked_dates()
    await state.update_data(calendar_year=year, calendar_month=month, blocked_dates=blocked_dates)
    await callback.message.edit_text("📅 Выберите дату:", reply_markup=generate_calendar(year, month, blocked_dates))
    await state.set_state(QuickState.date)
    await callback.answer()

@dp.callback_query(QuickState.date, F.data.startswith("date_"))
async def admin_date_chosen(callback: CallbackQuery, state: FSMContext):
    date_str = callback.data.split("_")[1]
    now = now_moscow()
    if datetime.strptime(date_str, "%Y-%m-%d").date() < now.date():
        await callback.answer("Эта дата уже прошла.", show_alert=True)
        return
    free_slots = []
    for slot in WORK_SLOTS:
        if not is_slot_free(date_str, slot):
            continue
        if date_str == now.strftime("%Y-%m-%d"):
            slot_start_hour = int(slot.split(":")[0])
            if slot_start_hour <= now.hour:
                continue
        free_slots.append(slot)
    if not free_slots:
        await callback.answer("Нет свободных часов.", show_alert=True)
        return
    await state.update_data(date=date_str)
    await callback.message.edit_text(f"📅 {format_date(date_str)}\nВыберите час:", reply_markup=time_slots_buttons(free_slots))
    await state.set_state(QuickState.slot)
    await callback.answer()

@dp.callback_query(QuickState.slot, F.data.startswith("slot_"))
async def admin_slot_chosen(callback: CallbackQuery, state: FSMContext):
    slot = callback.data.split("_", 1)[1]
    data = await state.get_data()
    if not is_slot_free(data['date'], slot):
        await callback.answer("Слот занят.", show_alert=True)
        return
    await state.update_data(slot=slot)
    await callback.message.edit_text("Введите <b>ИМЯ</b> клиента:")
    await state.set_state(QuickState.name)
    await callback.answer()

@dp.message(QuickState.name)
async def admin_name_entered(message: Message, state: FSMContext):
    name = message.text.strip()
    if len(name) < 2:
        await message.answer("❌ Имя должно быть не менее 2 символов.")
        return
    await state.update_data(name=name)
    await message.answer("📞 Введите <b>ТЕЛЕФОН</b> клиента (или нажмите кнопку):", reply_markup=phone_request_keyboard())
    await state.set_state(QuickState.phone)

@dp.message(QuickState.phone, F.contact)
async def admin_phone_contact(message: Message, state: FSMContext):
    phone = message.contact.phone_number
    await state.update_data(phone=phone)
    await message.answer("✅ Номер получен.", reply_markup=ReplyKeyboardRemove())
    await message.answer("🏙️ <b>Выберите населённый пункт</b> (листайте список или введите вручную):", reply_markup=cities_keyboard(0))
    await state.set_state(QuickState.city)

@dp.message(QuickState.phone)
async def admin_phone_manual(message: Message, state: FSMContext):
    phone_raw = message.text.strip()
    if not validate_phone(phone_raw):
        await message.answer("❌ Некорректный номер. Введите российский номер (от 10 цифр).")
        return
    await state.update_data(phone=phone_raw)
    await message.answer("✅ Номер сохранён.", reply_markup=ReplyKeyboardRemove())
    await message.answer("🏙️ <b>Выберите населённый пункт</b> (листайте список или введите вручную):", reply_markup=cities_keyboard(0))
    await state.set_state(QuickState.city)

@dp.callback_query(QuickState.city, F.data.startswith("city_page_"))
async def admin_city_change_page(callback: CallbackQuery, state: FSMContext):
    page = int(callback.data.split("_")[-1])
    await callback.message.edit_reply_markup(reply_markup=cities_keyboard(page))
    await callback.answer()

@dp.callback_query(QuickState.city, F.data.startswith("city_"))
async def admin_city_chosen(callback: CallbackQuery, state: FSMContext):
    city = callback.data.split("city_")[1]
    await state.update_data(city=city)
    await callback.message.edit_text(f"✅ Выбран населённый пункт: <b>{city}</b>\nТеперь введите <b>АДРЕС</b> (улица, дом):")
    await state.set_state(QuickState.address)
    await callback.answer()

@dp.callback_query(QuickState.city, F.data == "manual_city_input")
async def admin_manual_city_input(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text("✍️ Введите <b>населённый пункт</b> вручную:")
    await state.set_state(QuickState.city)
    await callback.answer()

@dp.message(QuickState.city)
async def admin_city_manual(message: Message, state: FSMContext):
    city = message.text.strip()
    if len(city) < 2:
        await message.answer("❌ Введите населённый пункт (минимум 2 символа).")
        return
    await state.update_data(city=city)
    await message.answer(f"✅ Выбран населённый пункт: <b>{city}</b>\nТеперь введите <b>АДРЕС</b> (улица, дом):")
    await state.set_state(QuickState.address)

@dp.message(QuickState.address)
async def admin_address_entered(message: Message, state: FSMContext):
    address = message.text.strip()
    if len(address) < 3:
        await message.answer("❌ Адрес должен быть не менее 3 символов.")
        return
    await state.update_data(address=address)
    data = await state.get_data()
    confirm_text = (
        f"✅ <b>Данные клиента:</b>\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f"🔧 <b>Услуга:</b> {data['service']}\n"
        f"📅 <b>Дата:</b> {format_date(data['date'])}\n"
        f"⏰ <b>Время:</b> {data['slot']}\n"
        f"👤 <b>Имя:</b> {data['name']}\n"
        f"📞 <b>Телефон:</b> {data['phone']}\n"
        f"🏙️ <b>Населённый пункт:</b> {data['city']}\n"
        f"📍 <b>Адрес:</b> {data['address']}\n"
        f"━━━━━━━━━━━━━━━━━\n\n"
        f"Сохранить запись?"
    )
    await message.answer(confirm_text, reply_markup=admin_confirm_keyboard())
    await state.set_state(QuickState.ready)

@dp.callback_query(F.data == "confirm_admin_yes", StateFilter(QuickState.ready))
async def admin_save_booking(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not is_slot_free(data['date'], data['slot']):
        await callback.message.edit_text("❌ Слот уже занят.")
        await state.clear()
        return
    success = book_slot(data['date'], data['slot'], 0, data['service'],
                        data['name'], data['phone'], data['city'], data['address'])
    if not success:
        await callback.message.edit_text("❌ Ошибка при сохранении.")
        await state.clear()
        return
    date_display = format_date(data['date'])
    await callback.message.edit_text(f"✅ <b>Запись добавлена (по телефону)</b>\n{data['service']}, {date_display} {data['slot']}\nКлиент: {data['name']}")
    await state.clear()
    await callback.message.answer("Меню", reply_markup=admin_menu())
    logger.info(f"Админ создал телефонную запись: {data['date']} {data['slot']}")
    await callback.answer()

@dp.callback_query(F.data == "confirm_no", StateFilter(QuickState.ready))
async def admin_cancel_booking(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text("❌ Запись отменена.")
    await state.clear()
    await callback.message.answer("Меню", reply_markup=admin_menu())
    await callback.answer()

# ---------- АДМИН: ОТМЕНИТЬ/ПЕРЕНЕСТИ (СПИСОК + ВЫБОР СПОСОБА) ----------
@dp.message(F.text == "❌ Отменить/Перенести")
async def admin_cancel_move_start(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    await state.clear()
    orders = get_all_future_orders()
    if not orders:
        await message.answer("📭 <b>Нет будущих записей.</b>")
        return
    await state.update_data(orders=orders, current_page=0)
    await send_orders_page(message, state, 0)
    await state.set_state(AdminCancelMoveState.choosing_order)

@dp.callback_query(F.data.startswith("orders_page_"))
async def orders_page(callback: CallbackQuery, state: FSMContext):
    page = int(callback.data.split("_")[-1])
    data = await state.get_data()
    orders = data.get("orders")
    if not orders:
        await callback.message.edit_text("❌ Ошибка, список заказов пуст.")
        await state.clear()
        return
    total = len(orders)
    total_pages = (total + 4) // 5
    if page < 0 or page >= total_pages:
        await callback.answer("Нет страницы")
        return
    await send_orders_page(callback, state, page)
    await callback.answer()

@dp.callback_query(F.data.startswith("select_order_"))
async def select_order(callback: CallbackQuery, state: FSMContext):
    order_id = int(callback.data.split("_")[-1])
    order = get_order_by_id(order_id)
    if not order or order[9] != 'active':
        await callback.message.edit_text("❌ Заказ уже не активен.")
        await state.clear()
        await callback.message.answer("Меню", reply_markup=admin_menu())
        return
    await state.update_data(selected_order_id=order_id)
    await callback.message.edit_text(
        f"✅ Выбран заказ №{order_id}\n\n"
        f"<b>Что делаем?</b>",
        reply_markup=cancel_or_move_keyboard()
    )
    await state.set_state(AdminCancelMoveState.choosing_action)

@dp.callback_query(F.data == "back_to_orders_list")
async def back_to_orders_list(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    orders = data.get("orders")
    if not orders:
        await callback.message.edit_text("❌ Ошибка, список заказов пуст.")
        await state.clear()
        return
    page = data.get("current_page", 0)
    await send_orders_page(callback, state, page)
    await state.set_state(AdminCancelMoveState.choosing_order)

@dp.callback_query(F.data == "admin_cancel_only", StateFilter(AdminCancelMoveState.choosing_action))
async def admin_cancel_only(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    order_id = data.get("selected_order_id")
    if not order_id:
        await callback.message.edit_text("❌ Ошибка, заказ не выбран.")
        await state.clear()
        return
    order = get_order_by_id(order_id)
    if not order or order[9] != 'active':
        await callback.message.edit_text("❌ Заказ уже не активен.")
        await state.clear()
        return
    user_id = order[1]
    phone = order[6]
    success, owner_id = cancel_order(order_id, ADMIN_ID, is_admin=True, move_to_history=True, reason="admin_cancel")
    if success:
        await callback.message.edit_text(f"✅ <b>Заказ #{order_id} отменён.</b>")
        if owner_id and owner_id != 0:
            try:
                await bot.send_message(owner_id, "😔 <b>Извините, ваш заказ был отменён мастером.</b>\nМастер свяжется с вами в ближайшее время.")
            except:
                pass
        elif owner_id == 0:
            await callback.message.answer(f"📞 <b>Это телефонная запись. Позвоните клиенту и сообщите об отмене.</b>\nНомер: {phone}")
    else:
        await callback.message.edit_text("❌ Не удалось отменить.")
    await state.clear()
    await callback.message.answer("Меню", reply_markup=admin_menu())
    await callback.answer()

# ---------- ИСПРАВЛЕННЫЕ ОБРАБОТЧИКИ ПЕРЕНОСА (без ошибки can't be edited) ----------
@dp.callback_query(F.data == "admin_move_booking", StateFilter(AdminCancelMoveState.choosing_action))
async def admin_move_booking_choice(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    order_id = data.get("selected_order_id")
    if not order_id:
        await callback.message.edit_text("❌ Ошибка, заказ не выбран.")
        await state.clear()
        return
    order = get_order_by_id(order_id)
    if not order or order[9] != 'active':
        await callback.message.edit_text("❌ Заказ уже не активен.")
        await state.clear()
        return
    await state.update_data(move_order_id=order_id)
    # Удаляем старое сообщение и отправляем новое
    try:
        await callback.message.delete()
    except:
        pass
    await callback.message.answer("🔄 <b>Выберите способ переноса:</b>", reply_markup=admin_move_choice_keyboard())
    await state.set_state(AdminCancelMoveState.choosing_action)
    await callback.answer()

@dp.callback_query(F.data == "auto_move", StateFilter(AdminCancelMoveState.choosing_action))
async def auto_move_selected(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    order_id = data.get("move_order_id")
    if not order_id:
        await callback.message.answer("❌ Ошибка.")
        await state.clear()
        return
    order = get_order_by_id(order_id)
    if not order or order[9] != 'active':
        await callback.message.answer("❌ Заказ уже не активен.")
        await state.clear()
        return
    new_date, new_slot = await find_next_free_slot((datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d"))
    if not new_date:
        await callback.message.answer("❌ Нет свободных слотов для переноса в ближайшие 30 дней.")
        await state.clear()
        return
    await state.update_data(new_date=new_date, new_slot=new_slot)
    try:
        await callback.message.delete()
    except:
        pass
    await callback.message.answer(
        f"🔄 <b>Ближайший свободный слот:</b>\n{format_date(new_date)} {new_slot}\n\n"
        f"Перенести запись #{order_id} на это время?",
        reply_markup=confirm_move_keyboard()
    )
    await state.set_state(AdminCancelMoveState.confirm_auto_move)
    await callback.answer()

@dp.callback_query(F.data == "manual_move", StateFilter(AdminCancelMoveState.choosing_action))
async def manual_move_selected(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    order_id = data.get("move_order_id")
    if not order_id:
        await callback.message.answer("❌ Ошибка.")
        await state.clear()
        return
    order = get_order_by_id(order_id)
    if not order or order[9] != 'active':
        await callback.message.answer("❌ Заказ уже не активен.")
        await state.clear()
        return
    now = now_moscow()
    year = now.year
    month = now.month
    blocked_dates = get_blocked_dates()
    try:
        await callback.message.delete()
    except:
        pass
    await callback.message.answer("📅 Выберите НОВУЮ дату для переноса:", 
                                  reply_markup=generate_calendar(year, month, blocked_dates))
    await state.set_state(AdminCancelMoveState.waiting_for_new_date)
    await callback.answer()

@dp.callback_query(AdminCancelMoveState.waiting_for_new_date, F.data.startswith("date_"))
async def admin_new_date_chosen(callback: CallbackQuery, state: FSMContext):
    date_str = callback.data.split("_")[1]
    now = now_moscow()
    if datetime.strptime(date_str, "%Y-%m-%d").date() < now.date():
        await callback.answer("Эта дата уже прошла. Выберите другую.", show_alert=True)
        return
    free_slots = []
    for slot in WORK_SLOTS:
        if not is_slot_free(date_str, slot):
            continue
        if date_str == now.strftime("%Y-%m-%d"):
            slot_start_hour = int(slot.split(":")[0])
            if slot_start_hour <= now.hour:
                continue
        free_slots.append(slot)
    if not free_slots:
        await callback.answer("На эту дату все часы заняты или день заблокирован.", show_alert=True)
        return
    await state.update_data(move_new_date=date_str)
    try:
        await callback.message.delete()
    except:
        pass
    await callback.message.answer(f"📅 <b>{format_date(date_str)}</b>\nВыберите новое время:",
                                  reply_markup=time_slots_buttons(free_slots))
    await state.set_state(AdminCancelMoveState.waiting_for_new_slot)
    await callback.answer()

@dp.callback_query(AdminCancelMoveState.waiting_for_new_slot, F.data.startswith("slot_"))
async def admin_new_slot_chosen(callback: CallbackQuery, state: FSMContext):
    slot = callback.data.split("_", 1)[1]
    data = await state.get_data()
    new_date = data.get("move_new_date")
    order_id = data.get("move_order_id")
    if not new_date or not order_id:
        await callback.message.answer("❌ Ошибка, данные потеряны. Начните заново.")
        await state.clear()
        return
    if not is_slot_free(new_date, slot):
        await callback.answer("Это время уже занято или день заблокирован", show_alert=True)
        return
    await state.update_data(move_new_slot=slot)
    order = get_order_by_id(order_id)
    if not order or order[9] != 'active':
        await callback.message.answer("❌ Заказ уже не активен.")
        await state.clear()
        return
    old_date = order[3]
    old_slot = order[4]
    client_name = order[5]
    confirm_text = (
        f"🔄 <b>Перенос записи #{order_id}</b>\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f"👤 <b>Клиент:</b> {client_name}\n"
        f"📅 <b>Было:</b> {format_date(old_date)} {old_slot}\n"
        f"📅 <b>Станет:</b> {format_date(new_date)} {slot}\n"
        f"━━━━━━━━━━━━━━━━━\n\n"
        f"Подтверждаете перенос?"
    )
    try:
        await callback.message.delete()
    except:
        pass
    await callback.message.answer(confirm_text, reply_markup=confirm_move_keyboard())
    await state.set_state(AdminCancelMoveState.confirm_new_booking)
    await callback.answer()

@dp.callback_query(F.data == "confirm_move_yes", StateFilter(AdminCancelMoveState.confirm_auto_move, AdminCancelMoveState.confirm_new_booking))
async def admin_confirm_move(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    order_id = data.get("move_order_id") or data.get("selected_order_id")
    new_date = data.get("new_date") or data.get("move_new_date")
    new_slot = data.get("new_slot") or data.get("move_new_slot")
    if not order_id or not new_date or not new_slot:
        await callback.message.answer("❌ Ошибка, данные не найдены.")
        await state.clear()
        return
    order = get_order_by_id(order_id)
    if not order or order[9] != 'active':
        await callback.message.answer("❌ Заказ уже не активен.")
        await state.clear()
        return
    old_date, old_slot = order[3], order[4]
    user_id = order[1]
    phone = order[6]
    save_cancelled_order(order_id, order[1], order[5], order[6], order[7], order[8], order[2], old_date, old_slot, "move")
    success, _ = await move_booking(order_id, new_date, new_slot)
    if success:
        await callback.message.answer(f"✅ <b>Заказ #{order_id} перенесён на {format_date(new_date)} {new_slot}.</b>")
        if user_id != 0:
            try:
                await bot.send_message(user_id,
                    f"🔄 <b>Ваша запись была перенесена мастером</b>\n"
                    f"Было: {format_date(old_date)} {old_slot}\n"
                    f"Стало: {format_date(new_date)} {new_slot}\n\n"
                    f"Если новое время не подходит – отмените запись через бота и запишитесь заново.")
            except:
                pass
        else:
            await callback.message.answer(f"📞 <b>Это телефонная запись. Позвоните клиенту и сообщите о переносе.</b>\nНомер: {phone}")
    else:
        await callback.message.answer("❌ Ошибка при переносе (возможно, слот занят).")
    await state.clear()
    await callback.answer()

@dp.callback_query(F.data == "confirm_move_no", StateFilter(AdminCancelMoveState.confirm_auto_move, AdminCancelMoveState.confirm_new_booking))
async def admin_confirm_move_no(callback: CallbackQuery, state: FSMContext):
    await callback.message.answer("❌ Перенос отменён.")
    await state.clear()
    await callback.answer()

# ---------- АДМИН: ВЫХОДНОЙ ----------
@dp.message(F.text == "⛔ Выходной")
async def admin_block_day_start(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    await state.clear()
    now = now_moscow()
    year = now.year
    month = now.month
    blocked_dates = get_blocked_dates()
    await message.answer("📅 Выберите день для блокировки:", reply_markup=generate_calendar(year, month, blocked_dates))
    await state.set_state(AdminBlockDayState.waiting_for_date)

@dp.callback_query(AdminBlockDayState.waiting_for_date, F.data.startswith("date_"))
async def admin_block_day_date(callback: CallbackQuery, state: FSMContext):
    date_str = callback.data.split("_")[1]
    conn = sqlite3.connect("cond.db")
    cur = conn.cursor()
    cur.execute("SELECT id, user_id, service, slot, client_name, phone, city, address FROM orders WHERE date=? AND status='active'", (date_str,))
    bookings = cur.fetchall()
    conn.close()
    if bookings:
        await state.update_data(bookings_to_move=bookings, block_date=date_str)
        text = f"⚠️ <b>На день {format_date(date_str)} есть {len(bookings)} записей.</b>\n\n"
        for b in bookings:
            text += f"• {b[2]} в {b[3]} (клиент: {b[4]})\n"
        text += "\n<b>Перенести их на ближайшие свободные часы?</b>"
        await callback.message.edit_text(text, reply_markup=move_bookings_keyboard())
        await state.set_state(AdminBlockDayState.wait_for_move_choice)
    else:
        success, msg = block_day(date_str)
        if success:
            await callback.message.edit_text(f"✅ <b>День {format_date(date_str)} полностью заблокирован (выходной).</b>")
            now = now_moscow()
            year = now.year
            month = now.month
            blocked_dates = get_blocked_dates()
            await callback.message.answer("📅 Обновлённый календарь:", reply_markup=generate_calendar(year, month, blocked_dates))
        else:
            await callback.message.edit_text("❌ Ошибка при блокировке.")
        await state.clear()
        await callback.message.answer("Меню", reply_markup=admin_menu())
        await callback.answer()

@dp.callback_query(F.data == "move_bookings_yes", StateFilter(AdminBlockDayState.wait_for_move_choice))
async def move_bookings_confirm(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    data = await state.get_data()
    bookings = data.get("bookings_to_move", [])
    block_date = data.get("block_date")
    if not bookings:
        await callback.message.edit_text("❌ Нет записей для переноса.")
        await state.clear()
        return
    conn = sqlite3.connect("cond.db")
    cur = conn.cursor()
    moved = 0
    errors = 0
    for book in bookings:
        order_id, user_id, service, old_slot, name, phone, city, address = book
        new_date, new_slot = await find_next_free_slot((datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d"), avoid_date=block_date)
        if not new_date:
            errors += 1
            continue
        old_date, old_slot_ = await move_booking(order_id, new_date, new_slot)
        if old_date:
            save_cancelled_order(order_id, user_id, name, phone, city, address, service, old_date, old_slot_, "block_move")
            moved += 1
            if user_id != 0:
                try:
                    await bot.send_message(
                        user_id,
                        f"🔔 <b>Извините, мастер объявил {format_date(block_date)} выходным днём.</b>\n"
                        f"Ваша запись автоматически перенесена на <b>{format_date(new_date)} {new_slot}</b>.\n\n"
                        f"Если новое время вам не подходит, пожалуйста, отмените запись через бота (кнопка «Мои записи») и запишитесь заново.\n"
                        f"Приносим извинения за неудобства."
                    )
                except Exception as e:
                    logger.error(f"Не удалось уведомить клиента {user_id}: {e}")
            else:
                logger.info(f"Телефонная запись #{order_id} перенесена, клиенту нужно позвонить: {phone}")
    conn.commit()
    conn.close()
    success, msg = block_day(block_date)
    if success:
        await callback.message.edit_text(
            f"✅ <b>День {format_date(block_date)} заблокирован.</b>\n"
            f"📦 <b>Перенесено записей:</b> {moved}\n"
            f"⚠️ <b>Ошибок переноса:</b> {errors}\n\n"
            f"📞 <b>Телефонные записи:</b> не забудьте позвонить клиентам, у которых нет Telegram, и сообщить о переносе."
        )
        now = now_moscow()
        year = now.year
        month = now.month
        blocked_dates = get_blocked_dates()
        await callback.message.answer("📅 Обновлённый календарь:", reply_markup=generate_calendar(year, month, blocked_dates))
    else:
        await callback.message.edit_text("❌ Ошибка при блокировке дня после переноса.")
    await state.clear()
    await callback.message.answer("Меню", reply_markup=admin_menu())
    await callback.answer()

@dp.callback_query(F.data == "move_bookings_no", StateFilter(AdminBlockDayState.wait_for_move_choice))
async def move_bookings_cancel(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await callback.message.edit_text("❌ Операция отменена.")
    await state.clear()
    await callback.message.answer("Меню", reply_markup=admin_menu())
    await callback.answer()

# ---------- АДМИН: ОТКРЫТЬ ДЕНЬ ----------
@dp.message(F.text == "🗓 Открыть день")
async def admin_unblock_day_start(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    await state.clear()
    now = now_moscow()
    year = now.year
    month = now.month
    blocked_dates = get_blocked_dates()
    await message.answer("📅 Выберите день для открытия:", reply_markup=generate_calendar(year, month, blocked_dates))
    await state.set_state(AdminUnblockDayState.waiting_for_date)

@dp.callback_query(AdminUnblockDayState.waiting_for_date, F.data.startswith("date_"))
async def admin_unblock_day_date(callback: CallbackQuery, state: FSMContext):
    date_str = callback.data.split("_")[1]
    if not is_day_blocked(date_str):
        await callback.message.edit_text("❌ Этот день не заблокирован.")
        await state.clear()
        await callback.message.answer("Меню", reply_markup=admin_menu())
        return
    await state.update_data(unblock_date=date_str)
    await callback.message.edit_text(f"Вы уверены, что хотите открыть день {format_date(date_str)}?", reply_markup=confirm_unblock_day_keyboard())
    await state.set_state(AdminUnblockDayState.confirm)

@dp.callback_query(F.data == "unblock_day_yes", StateFilter(AdminUnblockDayState.confirm))
async def confirm_unblock_day(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    date_str = data.get('unblock_date')
    if not date_str:
        await callback.message.edit_text("❌ Ошибка, дата не выбрана.")
        await state.clear()
        return
    success, msg = unblock_day(date_str)
    if success:
        await callback.message.edit_text(f"✅ <b>День {format_date(date_str)} открыт.</b>")
        now = now_moscow()
        year = now.year
        month = now.month
        blocked_dates = get_blocked_dates()
        await callback.message.answer("📅 Обновлённый календарь:", reply_markup=generate_calendar(year, month, blocked_dates))
    else:
        await callback.message.edit_text("❌ Ошибка при открытии дня.")
    await state.clear()
    await callback.message.answer("Меню", reply_markup=admin_menu())
    await callback.answer()

@dp.callback_query(F.data == "unblock_day_no", StateFilter(AdminUnblockDayState.confirm))
async def cancel_unblock_day(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text("❌ Операция отменена.")
    await state.clear()
    await callback.message.answer("Меню", reply_markup=admin_menu())
    await callback.answer()

# ---------- АДМИН: СТАТИСТИКА / ИСТОРИЯ ----------
@dp.message(F.text == "📊 Статистика/История")
async def stats_or_history(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    await message.answer("Выберите раздел:", reply_markup=stats_or_history_keyboard())

@dp.callback_query(F.data == "admin_stats")
async def admin_stats(callback: CallbackQuery):
    await callback.answer()
    today_cnt = len(get_orders_for_today())
    tomorrow_cnt = len(get_orders_for_tomorrow())
    future_cnt = len(get_all_future_orders())
    await callback.message.edit_text(f"📊 <b>Статистика:</b>\nСегодня: {today_cnt}\nЗавтра: {tomorrow_cnt}\nВсего будущих: {future_cnt}")
    await callback.message.answer("Меню", reply_markup=admin_menu())
    await callback.answer()

@dp.callback_query(F.data == "admin_history")
async def admin_history(callback: CallbackQuery):
    await callback.answer()
    cancels = get_cancelled_orders()
    if not cancels:
        await callback.message.edit_text("📜 <b>История отмен пуста.</b>")
        await callback.message.answer("Меню", reply_markup=admin_menu())
        return
    text = "<b>📜 ИСТОРИЯ ОТМЕНЁННЫХ ЗАКАЗОВ:</b>\n\n"
    for c in cancels:
        text += (
            f"┌ <b>Заказ #{c[1]}</b> ({c[10]})\n"
            f"├ Дата: {format_date(c[7])} {c[8]}\n"
            f"├ Услуга: {c[6]}\n"
            f"├ Клиент: {c[2]}\n"
            f"├ 📞 {c[3]}\n"
            f"├ 📍 {c[4]}, {c[5]}\n"
            f"└───────────────\n"
        )
    await callback.message.edit_text(text[:4000])
    await callback.message.answer("Меню", reply_markup=admin_menu())
    await callback.answer()

# ---------- ПОДЕЛИТЬСЯ ----------
@dp.message(F.text == "📞 Поделиться ботом")
async def share_bot(message: Message):
    bot_username = (await bot.get_me()).username
    await message.answer(f"📣 <b>Поделитесь ботом с друзьями:</b>\nhttps://t.me/{bot_username}")

# ---------- НАПОМИНАНИЯ ----------
async def send_reminders():
    now = now_moscow().replace(tzinfo=None)
    for o in get_orders_for_reminder_24h():
        order_id, user_id, date_str, slot, phone = o
        try:
            await bot.send_message(user_id, f"🔔 <b>Напоминание!</b>\nЗавтра, {format_date(date_str)} в {slot}, у вас запись на установку кондиционера.")
            mark_reminder_sent(order_id, '24h')
        except Exception as e:
            logger.error(f"Ошибка отправки напоминания 24ч: {e}")
    for o in get_orders_for_reminder_2h():
        order_id, user_id, date_str, slot, phone = o
        slot_start = int(slot.split(":")[0])
        slot_dt = datetime.strptime(f"{date_str} {slot_start:02d}:00", "%Y-%m-%d %H:%M")
        diff_h = (slot_dt - now).total_seconds() / 3600
        if 0 < diff_h <= 2:
            try:
                await bot.send_message(user_id, f"🔔 <b>Скоро запись!</b>\nЧерез ~2 часа, {format_date(date_str)} в {slot}, у вас установка кондиционера.")
                mark_reminder_sent(order_id, '2h')
            except Exception as e:
                logger.error(f"Ошибка отправки напоминания 2ч: {e}")

# ---------- ЗАПУСК ----------
async def main():
    init_db()
    scheduler = AsyncIOScheduler(timezone=str(TIMEZONE))
    scheduler.add_job(send_reminders, 'interval', minutes=5)
    scheduler.start()
    logger.info("Бот запущен")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())