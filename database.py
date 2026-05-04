import sqlite3
from datetime import datetime, timedelta
import logging
from config import TIMEZONE, WORK_SLOTS

def now_moscow():
    return datetime.now(TIMEZONE)

logger = logging.getLogger(__name__)
DB_NAME = "cond.db"

def init_db():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    # Основная таблица активных заказов
    cur.execute('''
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            service TEXT,
            date TEXT,
            slot TEXT,
            client_name TEXT DEFAULT '',
            phone TEXT DEFAULT '',
            city TEXT DEFAULT '',
            address TEXT DEFAULT '',
            status TEXT DEFAULT 'active',
            reminder_sent24 INTEGER DEFAULT 0,
            reminder_sent2 INTEGER DEFAULT 0
        )
    ''')
    # Таблица занятых слотов
    cur.execute('''
        CREATE TABLE IF NOT EXISTS bookings (
            date TEXT,
            slot TEXT,
            PRIMARY KEY (date, slot)
        )
    ''')
    # Таблица заблокированных слотов (выходной)
    cur.execute('''
        CREATE TABLE IF NOT EXISTS blocked_slots (
            date TEXT,
            slot TEXT,
            PRIMARY KEY (date, slot)
        )
    ''')
    # Чёрный список
    cur.execute('''
        CREATE TABLE IF NOT EXISTS banned_users (
            user_id INTEGER PRIMARY KEY
        )
    ''')
    # История отменённых заказов
    cur.execute('''
        CREATE TABLE IF NOT EXISTS cancelled_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            original_order_id INTEGER,
            user_id INTEGER,
            client_name TEXT,
            phone TEXT,
            city TEXT,
            address TEXT,
            service TEXT,
            original_date TEXT,
            original_slot TEXT,
            cancelled_at TEXT,
            reason TEXT
        )
    ''')
    conn.commit()
    conn.close()
    logger.info("База данных инициализирована")

def is_slot_free(date_str, slot):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM bookings WHERE date=? AND slot=?", (date_str, slot))
    if cur.fetchone():
        conn.close()
        return False
    cur.execute("SELECT 1 FROM blocked_slots WHERE date=? AND slot=?", (date_str, slot))
    blocked = cur.fetchone() is not None
    conn.close()
    return not blocked

def book_slot(date_str, slot, user_id, service, name, phone, city, address):
    conn = sqlite3.connect(DB_NAME)
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM bookings WHERE date=? AND slot=?", (date_str, slot))
        if cur.fetchone():
            return False
        cur.execute("SELECT 1 FROM blocked_slots WHERE date=? AND slot=?", (date_str, slot))
        if cur.fetchone():
            return False
        cur.execute("INSERT INTO bookings (date, slot) VALUES (?, ?)", (date_str, slot))
        cur.execute('''
            INSERT INTO orders (user_id, service, date, slot, client_name, phone, city, address)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (user_id, service, date_str, slot, name, phone, city, address))
        conn.commit()
        logger.info(f"Запись создана: {date_str} {slot}, пользователь {user_id}")
        return True
    except Exception as e:
        conn.rollback()
        logger.error(f"Ошибка при бронировании: {e}")
        return False
    finally:
        conn.close()

def block_day(date_str):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM orders WHERE date=? AND status='active'", (date_str,))
    if cur.fetchone():
        conn.close()
        return False, "has_bookings"
    for slot in WORK_SLOTS:
        cur.execute("INSERT OR IGNORE INTO blocked_slots (date, slot) VALUES (?, ?)", (date_str, slot))
    conn.commit()
    conn.close()
    return True, "ok"

def unblock_day(date_str):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM orders WHERE date=? AND status='active'", (date_str,))
    if cur.fetchone():
        conn.close()
        return False, "has_bookings"
    cur.execute("DELETE FROM blocked_slots WHERE date=?", (date_str,))
    conn.commit()
    conn.close()
    return True, "ok"

def is_day_blocked(date_str):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM blocked_slots WHERE date=?", (date_str,))
    blocked = cur.fetchone() is not None
    conn.close()
    return blocked

def get_active_order_count(user_id):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute('''
        SELECT COUNT(*) FROM orders
        WHERE user_id=? AND status='active' AND date >= date('now', 'localtime')
    ''', (user_id,))
    count = cur.fetchone()[0]
    conn.close()
    return count

def get_user_orders(user_id):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute('''
        SELECT id, service, date, slot, client_name, phone, city, address
        FROM orders
        WHERE user_id=? AND date >= date('now', 'localtime') AND status='active'
        ORDER BY date, slot
    ''', (user_id,))
    orders = cur.fetchall()
    conn.close()
    return orders

def get_all_future_orders():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute('''
        SELECT id, user_id, service, date, slot, client_name, phone, city, address
        FROM orders
        WHERE date >= date('now', 'localtime') AND status='active'
        ORDER BY date, slot
    ''')
    orders = cur.fetchall()
    conn.close()
    return orders

def get_orders_for_today():
    today = now_moscow().strftime("%Y-%m-%d")
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute('''
        SELECT id, client_name, service, slot, phone, city, address
        FROM orders
        WHERE date = ? AND status='active'
        ORDER BY slot
    ''', (today,))
    orders = cur.fetchall()
    conn.close()
    return orders

def get_orders_for_tomorrow():
    tomorrow = (now_moscow() + timedelta(days=1)).strftime("%Y-%m-%d")
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute('''
        SELECT id, client_name, service, slot, phone, city, address
        FROM orders
        WHERE date = ? AND status='active'
        ORDER BY slot
    ''', (tomorrow,))
    orders = cur.fetchall()
    conn.close()
    return orders

def get_order_by_id(order_id):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute('''
        SELECT id, user_id, service, date, slot, client_name, phone, city, address, status
        FROM orders WHERE id=?
    ''', (order_id,))
    order = cur.fetchone()
    conn.close()
    return order

def get_cancelled_orders():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute('''
        SELECT id, original_order_id, client_name, phone, city, address, service,
               original_date, original_slot, cancelled_at, reason
        FROM cancelled_orders
        ORDER BY cancelled_at DESC
    ''')
    rows = cur.fetchall()
    conn.close()
    return rows

def save_cancelled_order(order_id, user_id, name, phone, city, address, service, old_date, old_slot, reason):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute('''
        INSERT INTO cancelled_orders
        (original_order_id, user_id, client_name, phone, city, address, service,
         original_date, original_slot, cancelled_at, reason)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (order_id, user_id, name, phone, city, address, service,
          old_date, old_slot, now_moscow().isoformat(), reason))
    conn.commit()
    conn.close()

def cancel_order(order_id, user_id, is_admin=False, move_to_history=True, reason="cancel"):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("SELECT date, slot, user_id, client_name, phone, city, address, service FROM orders WHERE id=? AND status='active'", (order_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return False, "not_found"
    date_str, slot, owner_id, name, phone, city, address, service = row
    if not is_admin and owner_id != user_id:
        conn.close()
        return False, "not_yours"
    if not is_admin:
        slot_start_hour = int(slot.split(":")[0])
        slot_dt = datetime.strptime(f"{date_str} {slot_start_hour:02d}:00", "%Y-%m-%d %H:%M")
        if (slot_dt - now_moscow().replace(tzinfo=None)).total_seconds() < 2 * 3600:
            conn.close()
            return False, "too_late"
    if move_to_history:
        save_cancelled_order(order_id, owner_id, name, phone, city, address, service, date_str, slot, reason)
    cur.execute("DELETE FROM bookings WHERE date=? AND slot=?", (date_str, slot))
    cur.execute("UPDATE orders SET status='cancelled' WHERE id=?", (order_id,))
    conn.commit()
    conn.close()
    logger.info(f"Запись #{order_id} отменена (админ: {is_admin}), причина: {reason}")
    return True, owner_id

def update_order_slot(order_id, new_date, new_slot):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("DELETE FROM bookings WHERE date=(SELECT date FROM orders WHERE id=?) AND slot=(SELECT slot FROM orders WHERE id=?)", (order_id, order_id))
    cur.execute("INSERT INTO bookings (date, slot) VALUES (?, ?)", (new_date, new_slot))
    cur.execute("UPDATE orders SET date=?, slot=? WHERE id=?", (new_date, new_slot, order_id))
    conn.commit()
    conn.close()

def get_orders_for_reminder_24h():
    tomorrow = (now_moscow() + timedelta(days=1)).strftime("%Y-%m-%d")
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute('''
        SELECT id, user_id, date, slot, phone
        FROM orders
        WHERE date = ? AND status='active' AND reminder_sent24 = 0 AND user_id != 0
    ''', (tomorrow,))
    rows = cur.fetchall()
    conn.close()
    return rows

def get_orders_for_reminder_2h():
    today = now_moscow().strftime("%Y-%m-%d")
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute('''
        SELECT id, user_id, date, slot, phone
        FROM orders
        WHERE date = ? AND status='active' AND reminder_sent2 = 0 AND user_id != 0
    ''', (today,))
    rows = cur.fetchall()
    conn.close()
    return rows

def mark_reminder_sent(order_id, type_):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    if type_ == '24h':
        cur.execute("UPDATE orders SET reminder_sent24 = 1 WHERE id = ?", (order_id,))
    elif type_ == '2h':
        cur.execute("UPDATE orders SET reminder_sent2 = 1 WHERE id = ?", (order_id,))
    conn.commit()
    conn.close()

def is_user_banned(user_id):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM banned_users WHERE user_id=?", (user_id,))
    banned = cur.fetchone() is not None
    conn.close()
    return banned

def ban_user(user_id):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO banned_users (user_id) VALUES (?)", (user_id,))
    conn.commit()
    conn.close()
    logger.info(f"Пользователь {user_id} забанен")

def unban_user(user_id):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("DELETE FROM banned_users WHERE user_id=?", (user_id,))
    conn.commit()
    conn.close()
    logger.info(f"Пользователь {user_id} разбанен")