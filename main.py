import asyncio
import re
import logging
import sqlite3
from datetime import datetime, timedelta, date
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.filters import Command, StateFilter
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from config import BOT_TOKEN, ADMIN_ID, MASTER_PHONE, WORK_SLOTS, TIMEZONE, BLOCK_SYMBOL
from database import (
    init_db, is_slot_free, book_slot, get_active_order_count,
    get_user_orders, get_all_future_orders, get_orders_for_today,
    get_orders_for_tomorrow, get_order_by_id, cancel_order,
    get_orders_for_reminder_24h, get_orders_for_reminder_2h,
    mark_reminder_sent, is_user_banned, ban_user, unban_user,
    block_day, unblock_day, is_day_blocked
)
from keyboards import (
    main_menu, admin_menu, service_buttons, quick_or_manual, admin_quick_or_manual,
    generate_calendar, time_slots_buttons, confirm_keyboard, admin_confirm_keyboard,
    cancel_order_inline, confirm_cancel_keyboard,
    confirm_block_day_keyboard, confirm_unblock_day_keyboard, move_bookings_keyboard
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN)
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
    for i in range(90):  # 90 дней
        check_date = (now + timedelta(days=i)).strftime("%Y-%m-%d")
        if datetime.strptime(check_date, "%Y-%m-%d").date() < now.date():
            blocked.append(check_date)
            continue
        free_count = sum(1 for slot in WORK_SLOTS if is_slot_free(check_date, slot))
        if free_count == 0 or is_day_blocked(check_date):
            blocked.append(check_date)
    return blocked

# ---------- ФУНКЦИИ ДЛЯ ПЕРЕНОСА ----------
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

async def move_booking(order_id, new_date, new_slot, conn, cur):
    cur.execute("SELECT date, slot FROM orders WHERE id=?", (order_id,))
    old_date, old_slot = cur.fetchone()
    cur.execute("DELETE FROM bookings WHERE date=? AND slot=?", (old_date, old_slot))
    cur.execute("INSERT INTO bookings (date, slot) VALUES (?, ?)", (new_date, new_slot))
    cur.execute("UPDATE orders SET date=?, slot=? WHERE id=?", (new_date, new_slot, order_id))
    return old_date, old_slot

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

class AdminCancelState(StatesGroup):
    waiting_for_id = State()
    confirm = State()

class AdminBlockDayState(StatesGroup):
    waiting_for_date = State()
    wait_for_move_choice = State()

class AdminUnblockDayState(StatesGroup):
    waiting_for_date = State()
    confirm = State()

# ---------- КНОПКИ «НАЗАД» ----------
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
    logger.info(f"Пользователь {callback.from_user.id} вернулся в меню")

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
            "👨‍💼 **Помощь для администратора**\n\n"
            "📋 **Все записи** – список будущих заказов\n"
            "📞 **Быстрая запись (по звонку)** – добавить клиента (авто или вручную)\n"
            "❌ **Отменить по ID** – отмена заказа по его номеру\n"
            "⛔ **Выходной** – заблокировать все слоты на выбранный день (с переносом записей)\n"
            "🗓 **Открыть день** – снять блокировку\n"
            "📊 **Статистика** – количество записей сегодня, завтра, всего\n"
            "📞 **Поделиться** – ссылка на бота\n\n"
            "Команды: /ban <id>, /unban <id>, /help"
        )
    else:
        help_text = (
            "❄️ **Помощь**\n\n"
            "⚡ **Установка кондиционера** – запись через календарь (обязательно имя, телефон, город, адрес)\n"
            "❄️ **Обслуживание** – контактный телефон\n"
            "📞 **Консультация** – связаться с мастером\n"
            "📋 **Мои записи** – посмотреть и отменить активные записи (если до записи >2 часов)\n"
            "📞 **Поделиться** – отправить ссылку на бота\n\n"
            "Напоминания придут за 24 часа и за 2 часа до визита."
        )
    await message.answer(help_text)

# ---------- ОБСЛУЖИВАНИЕ И КОНСУЛЬТАЦИЯ (ВЫДАЧА НОМЕРА) ----------
@dp.message(F.text == "❄️ Обслуживание")
async def service_info(message: Message):
    await message.answer(f"📞 Для обслуживания кондиционеров позвоните мастеру: {MASTER_PHONE}")

@dp.message(F.text == "📞 Консультация")
async def consultation(message: Message):
    await message.answer(f"📞 Консультация по телефону: {MASTER_PHONE}")

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
    await message.answer(f"✅ Пользователь {uid} забанен.")
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
    await message.answer(f"✅ Пользователь {uid} разбанен.")
    logger.info(f"Админ {ADMIN_ID} разбанил пользователя {uid}")

# ---------- КЛИЕНТ: ЗАПИСАТЬСЯ ----------
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
            await callback.message.edit_text("Введите ваше ИМЯ:")
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
        await callback.answer("На эту дату все часы заняты или день заблокирован, либо время уже прошло", show_alert=True)
        return
    await state.update_data(date=date_str)
    await callback.message.edit_text(f"📅 {format_date(date_str)}\nВыберите час:", reply_markup=time_slots_buttons(free_slots))
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
    await callback.message.edit_text("Введите ваше ИМЯ (обязательно):")
    await state.set_state(BookingState.name)
    await callback.answer()

@dp.message(BookingState.name)
async def name_entered(message: Message, state: FSMContext):
    name = message.text.strip()
    if len(name) < 2:
        await message.answer("❌ Введите имя (минимум 2 символа).")
        return
    await state.update_data(name=name)
    await message.answer("📞 Введите номер телефона (обязательно, российский):")
    await state.set_state(BookingState.phone)

@dp.message(BookingState.phone)
async def phone_entered(message: Message, state: FSMContext):
    phone_raw = message.text.strip()
    if not validate_phone(phone_raw):
        await message.answer("❌ Некорректный номер. Введите российский номер (от 10 цифр).")
        return
    await state.update_data(phone=phone_raw)
    await message.answer("🏙️ Введите ГОРОД или СЕЛО (обязательно):")
    await state.set_state(BookingState.city)

@dp.message(BookingState.city)
async def city_entered(message: Message, state: FSMContext):
    city = message.text.strip()
    if len(city) < 2:
        await message.answer("❌ Введите город или село (минимум 2 символа).")
        return
    await state.update_data(city=city)
    await message.answer("📍 Введите АДРЕС (улица, дом) обязательно:")
    await state.set_state(BookingState.address)

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
        f"✅ Проверьте данные:\n"
        f"Услуга: {data['service']}\n"
        f"Дата: {format_date(data['date'])}\n"
        f"Время: {data['slot']}\n"
        f"Имя: {data['name']}\n"
        f"Телефон: {data['phone']}\n"
        f"Населённый пункт: {data['city']}\n"
        f"Адрес: {data['address']}\n\n"
        f"Подтверждаете запись?"
    )
    await msg.answer(text, reply_markup=confirm_keyboard())
    await state.set_state(BookingState.ready_to_book)

@dp.callback_query(F.data == "confirm_yes", StateFilter(BookingState.ready_to_book))
async def confirm_booking(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    user_id = callback.from_user.id
    if not is_slot_free(data['date'], data['slot']):
        await callback.message.edit_text("❌ Этот час только что заняли или день заблокирован. Начните заново.")
        await state.clear()
        await callback.answer()
        return

    success = book_slot(
        data['date'], data['slot'], user_id, data['service'],
        data['name'], data['phone'], data['city'], data['address']
    )
    if not success:
        await callback.message.edit_text("❌ Ошибка при записи. Попробуйте позже.")
        await state.clear()
        await callback.answer()
        return

    date_display = format_date(data['date'])
    await callback.message.edit_text(
        f"✅ Вы записаны на установку кондиционера на {date_display} {data['slot']}.\n"
        f"Мастер свяжется с вами за день до выезда.\n"
        f"Напомним за 2 часа и за 24 часа."
    )

    admin_text = (
        f"❄️ Новая запись на установку!\n"
        f"Дата: {data['date']} {data['slot']}\n"
        f"Клиент: {data['name']}, тел. {data['phone']}\n"
        f"Адрес: {data['city']}, {data['address']}"
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
        await message.answer("📭 У вас нет активных записей.")
        return
    for order_id, service, date_str, slot, name, phone, city, address in orders:
        date_display = format_date(date_str)
        text = f"🗓 {date_display} {slot}\n❄️ {service}\n👤 {name}\n📍 {city}, {address}"
        slot_start_hour = int(slot.split(":")[0])
        slot_dt = datetime.strptime(f"{date_str} {slot_start_hour:02d}:00", "%Y-%m-%d %H:%M")
        now_no_tz = now_moscow().replace(tzinfo=None)
        if (slot_dt - now_no_tz).total_seconds() > 2 * 3600:
            await message.answer(text, reply_markup=cancel_order_inline(order_id))
        else:
            await message.answer(text + "\n\n⚠️ Отмена недоступна – менее 2 часов.")

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
        await callback.answer("Неверный ID заказа", show_alert=True)
        await callback.answer()
        return
    user_id = callback.from_user.id
    success, info = cancel_order(order_id, user_id, is_admin=False)
    if success:
        await callback.message.edit_text("✅ Ваша запись отменена.")
        try:
            await bot.send_message(ADMIN_ID, f"❌ Клиент отменил запись #{order_id}")
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
    text = "📋 **Все будущие записи:**\n\n"
    for o in orders:
        order_id, user_id, service, date_str, slot, name, phone, city, address = o
        date_display = format_date(date_str)
        client = name if name else (f"user_{user_id}" if user_id else "По телефону")
        text += f"ID {order_id}: {service} | {date_display} {slot} | {client} | тел:{phone} | {city}, {address}\n"
    await message.answer(text[:4000])
    logger.info(f"Админ просмотрел список записей ({len(orders)} шт.)")

# ---------- АДМИН: БЫСТРАЯ ЗАПИСЬ ПО ЗВОНКУ ----------
# (аналогично BARBER_FINAL, но с обязательными полями name, phone, city, address; слоты из WORK_SLOTS)
@dp.message(F.text == "📞 Быстрая запись (по звонку)")
async def admin_booking_phone(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    await state.clear()
    await message.answer("Выберите услугу (только установка):", reply_markup=service_buttons())
    await state.set_state(QuickState.service)

@dp.callback_query(QuickState.service, F.data.startswith("service_"))
async def admin_service_chosen(callback: CallbackQuery, state: FSMContext):
    service = "Установка кондиционера"
    await state.update_data(service=service)
    await callback.message.edit_text("Выберите способ:", reply_markup=admin_quick_or_manual())
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
            await callback.message.edit_text("Введите ИМЯ клиента:")
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
    await callback.message.edit_text("Введите ИМЯ клиента:")
    await state.set_state(QuickState.name)
    await callback.answer()

@dp.message(QuickState.name)
async def admin_name_entered(message: Message, state: FSMContext):
    name = message.text.strip()
    if len(name) < 2:
        await message.answer("❌ Имя должно быть не менее 2 символов.")
        return
    await state.update_data(name=name)
    await message.answer("📞 Введите ТЕЛЕФОН клиента:")
    await state.set_state(QuickState.phone)

@dp.message(QuickState.phone)
async def admin_phone_entered(message: Message, state: FSMContext):
    phone = message.text.strip()
    if not validate_phone(phone):
        await message.answer("❌ Некорректный номер. Введите российский номер.")
        return
    await state.update_data(phone=phone)
    await message.answer("🏙️ Введите ГОРОД/СЕЛО клиента:")
    await state.set_state(QuickState.city)

@dp.message(QuickState.city)
async def admin_city_entered(message: Message, state: FSMContext):
    city = message.text.strip()
    if len(city) < 2:
        await message.answer("❌ Город/село должно быть не менее 2 символов.")
        return
    await state.update_data(city=city)
    await message.answer("📍 Введите АДРЕС клиента:")
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
        f"✅ Данные клиента:\n"
        f"Услуга: {data['service']}\n"
        f"Дата: {format_date(data['date'])}\n"
        f"Время: {data['slot']}\n"
        f"Имя: {data['name']}\n"
        f"Телефон: {data['phone']}\n"
        f"Город/село: {data['city']}\n"
        f"Адрес: {data['address']}\n\n"
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
    await callback.message.edit_text(f"✅ Запись добавлена: {data['service']}, {date_display} {data['slot']}\n(По телефону)")
    await state.clear()
    await callback.message.answer("Меню администратора", reply_markup=admin_menu())
    logger.info(f"Админ создал запись по звонку: {data['date']} {data['slot']}")
    await callback.answer()

@dp.callback_query(F.data == "confirm_no", StateFilter(QuickState.ready))
async def admin_cancel_booking(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text("❌ Запись отменена.")
    await state.clear()
    await callback.message.answer("Меню администратора", reply_markup=admin_menu())
    await callback.answer()

# ---------- АДМИН: ОТМЕНА ПО ID ----------
@dp.message(F.text == "❌ Отменить по ID")
async def admin_cancel_start(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    await message.answer("Введите ID записи для отмены (цифру). ID можно посмотреть в «Все записи».")
    await state.set_state(AdminCancelState.waiting_for_id)

@dp.message(AdminCancelState.waiting_for_id)
async def admin_cancel_id(message: Message, state: FSMContext):
    try:
        order_id = int(message.text.strip())
    except ValueError:
        await message.answer("❌ Введите число")
        return
    order = get_order_by_id(order_id)
    if not order or order[9] != 'active':
        await message.answer("❌ Заказ не найден или уже отменён.")
        await state.clear()
        return
    await state.update_data(order_id=order_id)
    await state.set_state(AdminCancelState.confirm)
    await message.answer(f"Вы уверены, что хотите отменить заказ #{order_id}?", reply_markup=confirm_cancel_keyboard())

@dp.callback_query(F.data == "cancel_confirm_yes")
async def admin_confirm_cancel(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    current_state = await state.get_state()
    if current_state != AdminCancelState.confirm:
        await callback.message.edit_text("❌ Операция не активна. Начните отмену заново.")
        await state.clear()
        return
    data = await state.get_data()
    order_id = data.get('order_id')
    if not order_id:
        await callback.message.edit_text("❌ ID заказа не найден.")
        await state.clear()
        return
    success, owner_id = cancel_order(order_id, ADMIN_ID, is_admin=True)
    if success:
        await callback.message.edit_text(f"✅ Заказ #{order_id} отменён.")
        if owner_id and owner_id != 0:
            try:
                await bot.send_message(owner_id, "😔 Ваш заказ был отменён мастером.")
            except Exception as e:
                logger.error(f"Не удалось уведомить клиента об отмене: {e}")
    else:
        await callback.message.edit_text("❌ Не удалось отменить.")
    await state.clear()
    await callback.message.answer("Меню администратора", reply_markup=admin_menu())
    logger.info(f"Админ отменил заказ #{order_id}")

@dp.callback_query(F.data == "cancel_confirm_no")
async def admin_cancel_no(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await callback.message.edit_text("❌ Отмена заказа отменена.")
    await state.clear()
    await callback.message.answer("Меню администратора", reply_markup=admin_menu())

# ---------- АДМИН: ВЫХОДНОЙ (С ПЕРЕНОСОМ) ----------
@dp.message(F.text == "⛔ Выходной")
async def admin_block_day_start(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    await state.clear()
    now = now_moscow()
    year = now.year
    month = now.month
    blocked_dates = get_blocked_dates()
    await message.answer("📅 Выберите день, который хотите заблокировать:",
                         reply_markup=generate_calendar(year, month, blocked_dates))
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
        text = f"⚠️ На день {format_date(date_str)} есть {len(bookings)} активных записей.\n\n"
        for b in bookings:
            text += f"• {b[2]} в {b[3]} (клиент: {b[4]})\n"
        text += "\nПеренести их на ближайшие свободные часы?"
        await callback.message.edit_text(text, reply_markup=move_bookings_keyboard())
        await state.set_state(AdminBlockDayState.wait_for_move_choice)
    else:
        success, msg = block_day(date_str)
        if success:
            await callback.message.edit_text(f"✅ День {format_date(date_str)} полностью заблокирован (выходной).")
            now = now_moscow()
            year = now.year
            month = now.month
            blocked_dates = get_blocked_dates()
            await callback.message.answer("📅 Обновлённый календарь:", reply_markup=generate_calendar(year, month, blocked_dates))
        else:
            await callback.message.edit_text("❌ Ошибка при блокировке.")
        await state.clear()
        await callback.message.answer("Меню администратора", reply_markup=admin_menu())
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
        old_date, old_slot = await move_booking(order_id, new_date, new_slot, conn, cur)
        try:
            await bot.send_message(
                user_id,
                f"🔔 **Извините, мастер объявил {format_date(block_date)} выходным днём.**\n"
                f"Ваша запись автоматически перенесена:\n"
                f"Было: {format_date(old_date)} {old_slot}\n"
                f"Стало: {format_date(new_date)} {new_slot}\n\n"
                f"Если новое время не подходит – отмените запись через бота."
            )
        except Exception as e:
            logger.error(f"Не удалось уведомить клиента {user_id}: {e}")
        moved += 1
    conn.commit()
    conn.close()
    success, msg = block_day(block_date)
    if success:
        await callback.message.edit_text(f"✅ День {format_date(block_date)} заблокирован.\nПеренесено: {moved}, ошибок: {errors}")
        now = now_moscow()
        year = now.year
        month = now.month
        blocked_dates = get_blocked_dates()
        await callback.message.answer("📅 Обновлённый календарь:", reply_markup=generate_calendar(year, month, blocked_dates))
    else:
        await callback.message.edit_text("❌ Ошибка при блокировке после переноса.")
    await state.clear()
    await callback.message.answer("Меню администратора", reply_markup=admin_menu())
    await callback.answer()

@dp.callback_query(F.data == "move_bookings_no", StateFilter(AdminBlockDayState.wait_for_move_choice))
async def move_bookings_cancel(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await callback.message.edit_text("❌ Операция отменена. День не заблокирован.")
    await state.clear()
    await callback.message.answer("Меню администратора", reply_markup=admin_menu())
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
    await message.answer("📅 Выберите день, который хотите открыть:",
                         reply_markup=generate_calendar(year, month, blocked_dates))
    await state.set_state(AdminUnblockDayState.waiting_for_date)

@dp.callback_query(AdminUnblockDayState.waiting_for_date, F.data.startswith("date_"))
async def admin_unblock_day_date(callback: CallbackQuery, state: FSMContext):
    date_str = callback.data.split("_")[1]
    if not is_day_blocked(date_str):
        await callback.message.edit_text("❌ Этот день не заблокирован.")
        await state.clear()
        await callback.message.answer("Меню администратора", reply_markup=admin_menu())
        return
    await state.update_data(unblock_date=date_str)
    await callback.message.edit_text(f"Вы уверены, что хотите открыть день {format_date(date_str)}?",
                                     reply_markup=confirm_unblock_day_keyboard())
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
        await callback.message.edit_text(f"✅ День {format_date(date_str)} открыт. Слоты доступны для записи.")
        now = now_moscow()
        year = now.year
        month = now.month
        blocked_dates = get_blocked_dates()
        await callback.message.answer("📅 Обновлённый календарь:", reply_markup=generate_calendar(year, month, blocked_dates))
    else:
        await callback.message.edit_text("❌ Ошибка при открытии дня.")
    await state.clear()
    await callback.message.answer("Меню администратора", reply_markup=admin_menu())
    await callback.answer()

@dp.callback_query(F.data == "unblock_day_no", StateFilter(AdminUnblockDayState.confirm))
async def cancel_unblock_day(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text("❌ Операция отменена.")
    await state.clear()
    await callback.message.answer("Меню администратора", reply_markup=admin_menu())
    await callback.answer()

# ---------- СТАТИСТИКА ----------
@dp.message(F.text == "📊 Статистика")
async def stats(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    today_cnt = len(get_orders_for_today())
    tomorrow_cnt = len(get_orders_for_tomorrow())
    future_cnt = len(get_all_future_orders())
    await message.answer(f"📊 Статистика:\nСегодня: {today_cnt}\nЗавтра: {tomorrow_cnt}\nВсего будущих: {future_cnt}")
    logger.info(f"Админ запросил статистику: {today_cnt}/{tomorrow_cnt}/{future_cnt}")

# ---------- ПОДЕЛИТЬСЯ ----------
@dp.message(F.text == "📞 Поделиться ботом")
async def share_bot(message: Message):
    bot_username = (await bot.get_me()).username
    await message.answer(f"📣 Поделитесь ботом с друзьями:\nhttps://t.me/{bot_username}")

# ---------- НАПОМИНАНИЯ ----------
async def send_reminders():
    now = now_moscow().replace(tzinfo=None)
    for o in get_orders_for_reminder_24h():
        order_id, user_id, date_str, slot, phone = o
        try:
            await bot.send_message(user_id, f"🔔 Напоминание: завтра, {format_date(date_str)} в {slot}, у вас запись на установку кондиционера.")
            mark_reminder_sent(order_id, '24h')
            logger.info(f"Отправлено напоминание 24ч пользователю {user_id}")
        except Exception as e:
            logger.error(f"Ошибка отправки напоминания 24ч: {e}")
    for o in get_orders_for_reminder_2h():
        order_id, user_id, date_str, slot, phone = o
        slot_start = int(slot.split(":")[0])
        slot_dt = datetime.strptime(f"{date_str} {slot_start:02d}:00", "%Y-%m-%d %H:%M")
        diff_h = (slot_dt - now).total_seconds() / 3600
        if 0 < diff_h <= 2:
            try:
                await bot.send_message(user_id, f"🔔 Напоминание: через ~2 часа, {format_date(date_str)} в {slot}, у вас запись.")
                mark_reminder_sent(order_id, '2h')
                logger.info(f"Отправлено напоминание 2ч пользователю {user_id}")
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