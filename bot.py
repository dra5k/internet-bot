import os
import re
import shutil
import sqlite3
from datetime import datetime, timedelta

from openpyxl import Workbook
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)


TOKEN = os.getenv("TOKEN")
ALLOWED_USER_IDS = [6697009890, 222222222, 333333333]

DB_FILE = "subscribers.db"
EXCEL_FILE = "subscribers_export.xlsx"
BACKUP_FILE = "subscribers_backup.db"
RESTORE_TEMP_FILE = "restore_uploaded.db"

(
    ADD_NAME,
    ADD_PHONE,
    ADD_USERNAME,
    ADD_IP,
    ADD_CONNECTION,
    ADD_PROFILE,
    ADD_PRICE,
    ADD_NOTES,
    ADD_BALANCE,
    CALC_AUTO_DAYS,
    USER_LINK_USERNAME,
    ADMIN_TICKET_CATEGORY,
    ADMIN_TICKET_TARGET,
    ADMIN_TICKET_DETAILS,
    USER_TICKET_CATEGORY,
    USER_TICKET_DETAILS,
    CLOSE_TICKET_ID,
    RESTORE_WAIT_FILE,
) = range(18)

PROFILE_PRICES = {
    "Super": 22000,
    "Super plus": 25000,
    "Super extra": 35000,
    "Super turbo": 55000,
    "Business ultra": 75000,
    "Free": 0,
}

ADMIN_TICKET_CATEGORIES = [
    "نصب جديد",
    "صيانة",
    "تغيير رمز",
    "قطع / إيقاف",
    "إعادة تشغيل / ريفرش",
    "إعداد / تهيئة",
    "غير ذلك",
]

USER_TICKET_CATEGORIES = [
    "ضعف الإنترنت",
    "انقطاع الخدمة",
    "نسيت الرمز",
    "بطء / تقطيع",
    "طلب اشتراك جديد",
    "نقل اشتراك",
    "طلب صيانة",
    "غير ذلك",
]

main_keyboard = ReplyKeyboardMarkup(
    [
        ["إضافة مشترك", "بحث عن مشترك"],
        ["تسجيل دفعة", "كل المشتركين"],
        ["المنتهية", "قريب ينتهي"],
        ["عرض الرصيد", "سجل المشترك"],
        ["المدينون", "المقدمون"],
        ["💰 مجموع الديون", "كم تصرف"],
        ["فلترة نوع", "فلترة بروفايل"],
        ["إحصائيات", "إيرادات"],
        ["تقرير اليوم", "سجل التعديلات"],
        ["أرشفة مشترك", "المؤرشفون"],
        ["استرجاع مشترك", "حذف نهائي"],
        ["تعديل مشترك", "نسخة احتياطية"],
        ["استرجاع نسخة", "تصدير Excel"],
        ["رفع تكت إداري", "كل التكتات"],
        ["التكتات المفتوحة", "إغلاق تكت"],
        ["إلغاء"],
    ],
    resize_keyboard=True
)

user_keyboard_linked = ReplyKeyboardMarkup(
    [
        ["حسابي", "فتح تكت"],
        ["تكتاتي", "إلغاء"],
    ],
    resize_keyboard=True
)

user_keyboard_unlinked = ReplyKeyboardMarkup(
    [
        ["ربط حسابي", "فتح تكت"],
        ["إلغاء"],
    ],
    resize_keyboard=True
)

edit_keyboard = ReplyKeyboardMarkup(
    [
        ["👤 الاسم", "🧑‍💻 اليوزر"],
        ["📞 الهاتف", "🌐 IP"],
        ["📦 البروفايل", "💰 السعر"],
        ["📡 نوع الربط", "📝 الملاحظات"],
        ["📅 تاريخ البداية", "⏳ تاريخ النهاية"],
        ["💳 الرصيد", "📒 ملاحظة مالية"],
        ["إلغاء"],
    ],
    resize_keyboard=True
)

connection_keyboard = ReplyKeyboardMarkup(
    [
        ["OLT", "Sector"],
        ["إلغاء"],
    ],
    resize_keyboard=True
)

profile_keyboard = ReplyKeyboardMarkup(
    [
        ["Super", "Super plus"],
        ["Super extra", "Super turbo"],
        ["Business ultra", "Free"],
        ["إلغاء"],
    ],
    resize_keyboard=True
)

price_keyboard = ReplyKeyboardMarkup(
    [
        ["47000", "57000"],
        ["70000", "150000"],
        ["قيمة أخرى"],
        ["إلغاء"],
    ],
    resize_keyboard=True
)

admin_ticket_keyboard = ReplyKeyboardMarkup(
    [
        ["نصب جديد", "صيانة"],
        ["تغيير رمز", "قطع / إيقاف"],
        ["إعادة تشغيل / ريفرش", "إعداد / تهيئة"],
        ["غير ذلك"],
        ["إلغاء"],
    ],
    resize_keyboard=True
)

user_ticket_keyboard = ReplyKeyboardMarkup(
    [
        ["ضعف الإنترنت", "انقطاع الخدمة"],
        ["نسيت الرمز", "بطء / تقطيع"],
        ["طلب اشتراك جديد", "نقل اشتراك"],
        ["طلب صيانة", "غير ذلك"],
        ["إلغاء"],
    ],
    resize_keyboard=True
)


def get_conn():
    return sqlite3.connect(DB_FILE)


def ensure_column(cursor, table_name, column_name, column_type):
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [row[1] for row in cursor.fetchall()]
    if column_name not in columns:
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")


def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def today_str():
    return datetime.now().strftime("%Y-%m-%d")


def init_db():
    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS subscribers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            phone TEXT,
            username TEXT UNIQUE NOT NULL,
            connection_type TEXT,
            profile TEXT,
            price REAL,
            notes TEXT
        )
    """)

    ensure_column(cursor, "subscribers", "ip", "TEXT")
    ensure_column(cursor, "subscribers", "start_date", "TEXT")
    ensure_column(cursor, "subscribers", "end_date", "TEXT")
    ensure_column(cursor, "subscribers", "balance", "REAL DEFAULT 0")
    ensure_column(cursor, "subscribers", "archived", "INTEGER DEFAULT 0")
    ensure_column(cursor, "subscribers", "financial_note", "TEXT")
    ensure_column(cursor, "subscribers", "telegram_id", "INTEGER")

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            subscriber_id INTEGER NOT NULL,
            type TEXT NOT NULL,
            amount REAL NOT NULL,
            note TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY (subscriber_id) REFERENCES subscribers(id)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS edit_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            subscriber_id INTEGER,
            subscriber_name TEXT,
            subscriber_username TEXT,
            editor_id INTEGER,
            editor_name TEXT,
            field_name TEXT,
            old_value TEXT,
            new_value TEXT,
            created_at TEXT NOT NULL
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS tickets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticket_number TEXT UNIQUE,
            creator_role TEXT NOT NULL,
            creator_telegram_id INTEGER,
            subscriber_id INTEGER,
            subscriber_name TEXT,
            subscriber_username TEXT,
            category TEXT NOT NULL,
            target_name TEXT,
            details TEXT,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL,
            closed_at TEXT,
            closed_by INTEGER
        )
    """)

    conn.commit()
    conn.close()


def is_admin(update: Update) -> bool:
    user = update.effective_user
    return user is not None and user.id in ALLOWED_USER_IDS


def format_balance(balance):
    if balance is None:
        balance = 0
    if balance < 0:
        return f"عليه دين: {abs(balance):,.0f}"
    if balance > 0:
        return f"له رصيد مقدم: {balance:,.0f}"
    return "الرصيد: 0"


def valid_phone(phone: str) -> bool:
    return phone.isdigit() and len(phone) == 11


def valid_ip(ip: str) -> bool:
    ip_pattern = (
        r"^(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)\."
        r"(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)\."
        r"(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)\."
        r"(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)$"
    )
    return re.match(ip_pattern, ip) is not None


def valid_date(date_text: str) -> bool:
    try:
        datetime.strptime(date_text, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def valid_profile(profile: str) -> bool:
    return profile in PROFILE_PRICES.keys()


def valid_connection_type(conn_type: str) -> bool:
    return conn_type in ["OLT", "Sector"]


def get_linked_subscriber_by_telegram_id(telegram_id: int):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, name, username, profile, price, end_date, balance, notes
        FROM subscribers
        WHERE telegram_id = ? AND (archived IS NULL OR archived = 0)
        LIMIT 1
    """, (telegram_id,))
    row = cursor.fetchone()
    conn.close()
    return row


def get_user_keyboard(update: Update):
    if is_admin(update):
        return main_keyboard

    linked = get_linked_subscriber_by_telegram_id(update.effective_user.id)
    return user_keyboard_linked if linked else user_keyboard_unlinked


def add_edit_log(subscriber_id, subscriber_name, subscriber_username, editor_id, editor_name, field_name, old_value, new_value):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO edit_logs
        (subscriber_id, subscriber_name, subscriber_username, editor_id, editor_name, field_name, old_value, new_value, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        subscriber_id,
        subscriber_name,
        subscriber_username,
        editor_id,
        editor_name,
        str(field_name),
        str(old_value) if old_value is not None else "",
        str(new_value) if new_value is not None else "",
        now_str()
    ))
    conn.commit()
    conn.close()


async def notify_admins(context: ContextTypes.DEFAULT_TYPE, text: str):
    for admin_id in ALLOWED_USER_IDS:
        try:
            await context.bot.send_message(chat_id=admin_id, text=text)
        except Exception:
            pass


def next_ticket_number():
    return f"T-{int(datetime.now().timestamp())}"


def build_daily_report_text():
    conn = get_conn()
    cursor = conn.cursor()

    today = datetime.now().date()
    limit_date = today + timedelta(days=3)

    cursor.execute("""
        SELECT name, username, end_date
        FROM subscribers
        WHERE (archived IS NULL OR archived = 0)
          AND end_date IS NOT NULL AND end_date < ?
        ORDER BY end_date ASC
    """, (today.strftime("%Y-%m-%d"),))
    expired = cursor.fetchall()

    cursor.execute("""
        SELECT name, username, end_date
        FROM subscribers
        WHERE (archived IS NULL OR archived = 0)
          AND end_date IS NOT NULL AND end_date >= ? AND end_date <= ?
        ORDER BY end_date ASC
    """, (today.strftime("%Y-%m-%d"), limit_date.strftime("%Y-%m-%d")))
    near = cursor.fetchall()

    cursor.execute("""
        SELECT COUNT(*)
        FROM subscribers
        WHERE archived IS NULL OR archived = 0
    """)
    total_subscribers = cursor.fetchone()[0]
    conn.close()

    message = (
        f"📊 تقرير اليوم\n"
        f"التاريخ: {today.strftime('%Y-%m-%d')}\n"
        f"إجمالي المشتركين: {total_subscribers}\n\n"
    )

    if expired:
        message += "❌ المنتهية:\n"
        for row in expired:
            message += f"- {row[0]} | {row[1]} | انتهى: {row[2]}\n"
    else:
        message += "❌ لا يوجد مشترك منتهي حاليًا\n"

    message += "\n"

    if near:
        message += "⚠️ قريب ينتهي خلال 3 أيام:\n"
        for row in near:
            message += f"- {row[0]} | {row[1]} | ينتهي: {row[2]}\n"
    else:
        message += "⚠️ لا يوجد مشترك قريب ينتهي خلال 3 أيام\n"

    return message


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    if is_admin(update):
        await update.message.reply_text("أهلاً بك في بوت إدارة البرج", reply_markup=main_keyboard)
    else:
        await update.message.reply_text("أهلاً بك", reply_markup=get_user_keyboard(update))


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await update.message.reply_text("تم الإلغاء.", reply_markup=get_user_keyboard(update))
    return ConversationHandler.END


# =========================
# دخول المشترك بنفسه
# =========================
async def link_account_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await update.message.reply_text("أرسل اسم المستخدم الخاص باشتراكك (PPPoE username):")
    return USER_LINK_USERNAME


async def link_account_username(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if text == "إلغاء":
        return await cancel(update, context)

    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, name, username
        FROM subscribers
        WHERE username = ? AND (archived IS NULL OR archived = 0)
        LIMIT 1
    """, (text,))
    row = cursor.fetchone()

    if not row:
        conn.close()
        await update.message.reply_text("لم يتم العثور على هذا اليوزر.", reply_markup=get_user_keyboard(update))
        return ConversationHandler.END

    cursor.execute("""
        UPDATE subscribers
        SET telegram_id = ?
        WHERE id = ?
    """, (update.effective_user.id, row[0]))
    conn.commit()
    conn.close()

    await update.message.reply_text(
        f"تم ربط حسابك بنجاح ✅\nالاسم: {row[1]}\nاليوزر: {row[2]}",
        reply_markup=get_user_keyboard(update)
    )
    return ConversationHandler.END


async def show_my_account(update: Update, context: ContextTypes.DEFAULT_TYPE):
    row = get_linked_subscriber_by_telegram_id(update.effective_user.id)

    if not row:
        await update.message.reply_text(
            "حسابك غير مربوط بعد.\nاضغط: ربط حسابي",
            reply_markup=get_user_keyboard(update)
        )
        return

    msg = (
        f"👤 حسابي\n\n"
        f"الاسم: {row[1]}\n"
        f"اليوزر: {row[2]}\n"
        f"البروفايل: {row[3]}\n"
        f"السعر: {row[4]}\n"
        f"نهاية الاشتراك: {row[5]}\n"
        f"{format_balance(row[6])}"
    )
    await update.message.reply_text(msg, reply_markup=get_user_keyboard(update))


# =========================
# إضافة مشترك
# =========================
async def add_subscriber_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        await update.message.reply_text("هذا الخيار للإدارة فقط.", reply_markup=get_user_keyboard(update))
        return ConversationHandler.END

    context.user_data.clear()
    await update.message.reply_text("أرسل اسم المشترك:")
    return ADD_NAME


async def add_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if text == "إلغاء":
        return await cancel(update, context)

    if not text:
        await update.message.reply_text("اسم المشترك لا يمكن أن يكون فارغًا.")
        return ADD_NAME

    context.user_data["name"] = text
    await update.message.reply_text("أرسل رقم الهاتف (11 رقم):")
    return ADD_PHONE


async def add_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if text == "إلغاء":
        return await cancel(update, context)

    if not valid_phone(text):
        await update.message.reply_text("رقم الهاتف يجب أن يكون 11 رقم فقط.")
        return ADD_PHONE

    context.user_data["phone"] = text
    await update.message.reply_text("أرسل اسم المستخدم PPPoE:")
    return ADD_USERNAME


async def add_username(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if text == "إلغاء":
        return await cancel(update, context)

    if not text:
        await update.message.reply_text("اسم المستخدم لا يمكن أن يكون فارغًا.")
        return ADD_USERNAME

    context.user_data["username"] = text
    await update.message.reply_text("أرسل IP المشترك بصيغة x.x.x.x:")
    return ADD_IP


async def add_ip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if text == "إلغاء":
        return await cancel(update, context)

    if not valid_ip(text):
        await update.message.reply_text("صيغة الـ IP غير صحيحة. مثال: 192.168.1.1")
        return ADD_IP

    context.user_data["ip"] = text
    await update.message.reply_text("اختر نوع الربط:", reply_markup=connection_keyboard)
    return ADD_CONNECTION


async def add_connection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if text == "إلغاء":
        return await cancel(update, context)

    if not valid_connection_type(text):
        await update.message.reply_text("اختر فقط OLT أو Sector.", reply_markup=connection_keyboard)
        return ADD_CONNECTION

    context.user_data["connection_type"] = text
    await update.message.reply_text("اختر اسم البروفايل:", reply_markup=profile_keyboard)
    return ADD_PROFILE


async def add_profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if text == "إلغاء":
        return await cancel(update, context)

    if not valid_profile(text):
        await update.message.reply_text("اختر البروفايل من الأزرار فقط.", reply_markup=profile_keyboard)
        return ADD_PROFILE

    context.user_data["profile"] = text
    await update.message.reply_text("اختر سعر الاشتراك:", reply_markup=price_keyboard)
    return ADD_PRICE


async def add_price(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if text == "إلغاء":
        return await cancel(update, context)

    allowed_prices = ["47000", "57000", "70000", "150000"]

    if text == "قيمة أخرى":
        context.user_data["waiting_custom_price"] = True
        await update.message.reply_text("أرسل السعر كرقم فقط مثل 25000")
        return ADD_PRICE

    if text in allowed_prices:
        context.user_data["price"] = float(text)
        context.user_data.pop("waiting_custom_price", None)
        await update.message.reply_text("أرسل الملاحظات العامة، أو اكتب: لا يوجد", reply_markup=main_keyboard)
        return ADD_NOTES

    if context.user_data.get("waiting_custom_price"):
        try:
            context.user_data["price"] = float(text)
            context.user_data.pop("waiting_custom_price", None)
            await update.message.reply_text("أرسل الملاحظات العامة، أو اكتب: لا يوجد", reply_markup=main_keyboard)
            return ADD_NOTES
        except ValueError:
            await update.message.reply_text("السعر غير صحيح. أرسل رقم فقط مثل 25000")
            return ADD_PRICE

    await update.message.reply_text("اختر السعر من الأزرار أو اختر قيمة أخرى.", reply_markup=price_keyboard)
    return ADD_PRICE


async def add_notes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if text == "إلغاء":
        return await cancel(update, context)

    context.user_data["notes"] = "" if text == "لا يوجد" else text
    await update.message.reply_text(
        "إذا عليه دين سابق اكتب بالسالب مثل: -15000\n"
        "إذا عنده مبلغ مقدم اكتب بالموجب مثل: 20000\n"
        "إذا لا يوجد اكتب: 0",
        reply_markup=main_keyboard
    )
    return ADD_BALANCE


async def add_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if text == "إلغاء":
        return await cancel(update, context)

    try:
        balance = float(text)
    except ValueError:
        await update.message.reply_text("أدخل رقم صحيح مثل 0 أو -15000 أو 20000")
        return ADD_BALANCE

    start_date = datetime.now().date()
    end_date = start_date + timedelta(days=30)

    try:
        conn = get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO subscribers
            (name, phone, username, ip, connection_type, profile, price, notes, start_date, end_date, balance, archived, financial_note)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
        """, (
            context.user_data["name"],
            context.user_data["phone"],
            context.user_data["username"],
            context.user_data["ip"],
            context.user_data["connection_type"],
            context.user_data["profile"],
            context.user_data["price"],
            context.user_data["notes"],
            start_date.strftime("%Y-%m-%d"),
            end_date.strftime("%Y-%m-%d"),
            balance,
            ""
        ))

        subscriber_id = cursor.lastrowid

        if balance != 0:
            cursor.execute("""
                INSERT INTO transactions (subscriber_id, type, amount, note, created_at)
                VALUES (?, ?, ?, ?, ?)
            """, (
                subscriber_id,
                "initial_balance",
                balance,
                "رصيد ابتدائي عند إنشاء المشترك",
                now_str()
            ))

        conn.commit()
        conn.close()

        await update.message.reply_text(
            f"تم حفظ المشترك بنجاح ✅\n"
            f"ينتهي اشتراكه: {end_date.strftime('%Y-%m-%d')}\n"
            f"{format_balance(balance)}",
            reply_markup=main_keyboard
        )

    except sqlite3.IntegrityError:
        await update.message.reply_text(
            "هذا اليوزر موجود مسبقًا ❌\nجرّب اسم مستخدم آخر.",
            reply_markup=main_keyboard
        )
    except Exception as e:
        await update.message.reply_text(f"حدث خطأ أثناء الحفظ:\n{e}", reply_markup=main_keyboard)

    context.user_data.clear()
    return ConversationHandler.END


# =========================
# تسجيل دفعة / تجديد
# =========================
async def register_payment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        await update.message.reply_text("هذا الخيار للإدارة فقط.", reply_markup=get_user_keyboard(update))
        return

    context.user_data.clear()
    context.user_data["payment_step"] = "lookup"
    await update.message.reply_text("أرسل الاسم أو اليوزر للمشترك:")


async def handle_payment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()

    if text == "إلغاء":
        context.user_data.clear()
        await update.message.reply_text("تم الإلغاء.", reply_markup=get_user_keyboard(update))
        return

    step = context.user_data.get("payment_step")

    if step == "lookup":
        conn = get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, name, username, price, balance
            FROM subscribers
            WHERE (username = ? OR name LIKE ?)
              AND (archived IS NULL OR archived = 0)
            LIMIT 1
        """, (text, f"%{text}%"))
        row = cursor.fetchone()
        conn.close()

        if not row:
            await update.message.reply_text("المشترك غير موجود ❌", reply_markup=main_keyboard)
            context.user_data.clear()
            return

        context.user_data["pay_subscriber_id"] = row[0]
        context.user_data["pay_name"] = row[1]
        context.user_data["pay_username"] = row[2]
        context.user_data["pay_price"] = float(row[3] or 0)
        context.user_data["pay_old_balance"] = float(row[4] or 0)
        context.user_data["payment_step"] = "amount"

        await update.message.reply_text(
            f"تم العثور على المشترك ✅\n"
            f"الاسم: {row[1]}\n"
            f"اليوزر: {row[2]}\n"
            f"سعر الاشتراك: {row[3]}\n"
            f"{format_balance(row[4])}\n\n"
            f"أرسل مبلغ الدفعة:"
        )
        return

    if step == "amount":
        try:
            amount = float(text)
            if amount <= 0:
                await update.message.reply_text("أدخل مبلغًا أكبر من صفر.")
                return
        except ValueError:
            await update.message.reply_text("أدخل رقم صحيح فقط مثل 25000")
            return

        context.user_data["pay_amount"] = amount
        context.user_data["payment_step"] = "months"
        await update.message.reply_text("كم شهر تريد تجديد؟ اكتب 0 إذا كانت دفعة فقط بدون تجديد")
        return

    if step == "months":
        try:
            months = int(text)
            if months < 0:
                await update.message.reply_text("أدخل رقم 0 أو أكبر")
                return
        except ValueError:
            await update.message.reply_text("أدخل رقم صحيح مثل 0 أو 1 أو 2")
            return

        context.user_data["pay_months"] = months
        context.user_data["payment_step"] = "note"
        await update.message.reply_text("أرسل ملاحظة للعملية، أو اكتب: لا يوجد")
        return

    if step == "note":
        note = "" if text == "لا يوجد" else text

        subscriber_id = context.user_data["pay_subscriber_id"]
        username = context.user_data["pay_username"]
        name = context.user_data["pay_name"]
        price = context.user_data["pay_price"]
        old_balance = context.user_data["pay_old_balance"]
        amount_paid = context.user_data["pay_amount"]
        months = context.user_data["pay_months"]

        conn = get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT end_date FROM subscribers WHERE id = ?", (subscriber_id,))
        row = cursor.fetchone()

        today = datetime.now().date()
        current_end = today
        if row and row[0]:
            try:
                current_end = datetime.strptime(row[0], "%Y-%m-%d").date()
            except Exception:
                current_end = today

        new_balance = old_balance + amount_paid
        new_end = current_end

        cursor.execute("""
            INSERT INTO transactions (subscriber_id, type, amount, note, created_at)
            VALUES (?, ?, ?, ?, ?)
        """, (
            subscriber_id,
            "payment",
            amount_paid,
            note if note else "دفعة واردة",
            now_str()
        ))

        if months > 0:
            charge = price * months
            new_balance -= charge

            base_date = current_end if current_end > today else today
            new_end = base_date + timedelta(days=30 * months)

            cursor.execute("""
                INSERT INTO transactions (subscriber_id, type, amount, note, created_at)
                VALUES (?, ?, ?, ?, ?)
            """, (
                subscriber_id,
                "renewal",
                -charge,
                f"تجديد {months} شهر",
                now_str()
            ))

            cursor.execute("""
                UPDATE subscribers
                SET balance = ?, end_date = ?
                WHERE id = ?
            """, (new_balance, new_end.strftime("%Y-%m-%d"), subscriber_id))
        else:
            cursor.execute("""
                UPDATE subscribers
                SET balance = ?
                WHERE id = ?
            """, (new_balance, subscriber_id))

        conn.commit()
        conn.close()

        invoice = (
            f"🧾 وصل دفع\n\n"
            f"الاسم: {name}\n"
            f"اليوزر: {username}\n"
            f"المبلغ المدفوع: {amount_paid:,.0f}\n"
            f"عدد الأشهر: {months}\n"
            f"سعر الاشتراك: {price:,.0f}\n"
            f"التاريخ: {now_str()}\n"
            f"الملاحظة: {note if note else 'لا توجد'}\n"
            f"{format_balance(new_balance)}"
        )

        if months > 0:
            invoice += f"\nالانتهاء الجديد: {new_end.strftime('%Y-%m-%d')}"

        await update.message.reply_text(invoice, reply_markup=main_keyboard)
        context.user_data.clear()


# =========================
# مجموع الديون
# =========================
async def total_debt_only(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT COALESCE(SUM(balance), 0)
        FROM subscribers
        WHERE (archived IS NULL OR archived = 0) AND balance < 0
    """)
    result = cursor.fetchone()
    total_debt = abs(result[0] if result and result[0] else 0)
    conn.close()

    await update.message.reply_text(
        f"💰 مجموع الديون الكلي:\n\n{total_debt:,.0f} دينار",
        reply_markup=get_user_keyboard(update)
    )


# =========================
# كم تصرف
# =========================
async def calc_auto_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        await update.message.reply_text("هذا الخيار للإدارة فقط.", reply_markup=get_user_keyboard(update))
        return ConversationHandler.END

    context.user_data.clear()
    await update.message.reply_text("أدخل عدد الأيام:")
    return CALC_AUTO_DAYS


async def calc_auto_days(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        days = int(update.message.text.strip())
        if days <= 0:
            await update.message.reply_text("أدخل رقم صحيح")
            return CALC_AUTO_DAYS
    except ValueError:
        await update.message.reply_text("أدخل رقم فقط")
        return CALC_AUTO_DAYS

    limit_date = (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d")

    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT profile, COUNT(*)
        FROM subscribers
        WHERE (archived IS NULL OR archived = 0)
          AND end_date IS NOT NULL
          AND end_date <= ?
        GROUP BY profile
    """, (limit_date,))
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        await update.message.reply_text("لا يوجد كروت مطلوبة خلال هذه الفترة ✅", reply_markup=main_keyboard)
        return ConversationHandler.END

    total_cards = 0
    total_money = 0
    msg = f"📊 الكروت المطلوبة خلال {days} يوم:\n\n"

    for profile, count in rows:
        price = PROFILE_PRICES.get(profile, 0)
        money = price * count
        total_cards += count
        total_money += money
        msg += f"{profile}: {count} كارت → {money:,.0f}\n"

    msg += f"\n💰 مجموع الكروت: {total_cards}"
    msg += f"\n💵 المبلغ الكلي: {total_money:,.0f} دينار"

    await update.message.reply_text(msg, reply_markup=main_keyboard)
    return ConversationHandler.END


# =========================
# حذف / أرشفة / استرجاع / حذف نهائي
# =========================
async def delete_subscriber(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        await update.message.reply_text("هذا الخيار للإدارة فقط.", reply_markup=get_user_keyboard(update))
        return

    context.user_data.clear()
    context.user_data["delete_mode"] = "username"
    await update.message.reply_text("أرسل اسم المستخدم (username) للمشترك الذي تريد حذفه:")


async def handle_delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()

    if text == "إلغاء":
        context.user_data.clear()
        await update.message.reply_text("تم الإلغاء.", reply_markup=get_user_keyboard(update))
        return

    if context.user_data.get("delete_mode") == "username":
        context.user_data["delete_username"] = text
        context.user_data["delete_mode"] = "confirm"
        await update.message.reply_text("هل أنت متأكد؟ اكتب: نعم")
        return

    if context.user_data.get("delete_mode") == "confirm":
        if text != "نعم":
            context.user_data.clear()
            await update.message.reply_text("تم الإلغاء.", reply_markup=get_user_keyboard(update))
            return

        username = context.user_data["delete_username"]

        conn = get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT id, name FROM subscribers WHERE username = ?", (username,))
        row = cursor.fetchone()

        if not row:
            conn.close()
            context.user_data.clear()
            await update.message.reply_text("المشترك غير موجود ❌", reply_markup=main_keyboard)
            return

        subscriber_id, subscriber_name = row
        cursor.execute("DELETE FROM transactions WHERE subscriber_id = ?", (subscriber_id,))
        cursor.execute("DELETE FROM subscribers WHERE id = ?", (subscriber_id,))
        conn.commit()
        conn.close()

        context.user_data.clear()
        await update.message.reply_text(f"تم حذف المشترك: {subscriber_name} ✅", reply_markup=main_keyboard)


async def archive_subscriber(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        await update.message.reply_text("هذا الخيار للإدارة فقط.", reply_markup=get_user_keyboard(update))
        return

    context.user_data.clear()
    context.user_data["archive_mode"] = True
    await update.message.reply_text("أرسل الاسم أو اليوزر لأرشفة المشترك:")


async def handle_archive(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if text == "إلغاء":
        context.user_data.clear()
        await update.message.reply_text("تم الإلغاء.", reply_markup=main_keyboard)
        return

    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, name, username
        FROM subscribers
        WHERE username = ? OR name LIKE ?
        LIMIT 1
    """, (text, f"%{text}%"))
    row = cursor.fetchone()

    if not row:
        conn.close()
        context.user_data.clear()
        await update.message.reply_text("المشترك غير موجود ❌", reply_markup=main_keyboard)
        return

    cursor.execute("UPDATE subscribers SET archived = 1 WHERE id = ?", (row[0],))
    conn.commit()
    conn.close()

    context.user_data.clear()
    await update.message.reply_text(
        f"تمت الأرشفة ✅\nالاسم: {row[1]}\nاليوزر: {row[2]}",
        reply_markup=main_keyboard
    )


async def show_archived(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        await update.message.reply_text("هذا الخيار للإدارة فقط.", reply_markup=get_user_keyboard(update))
        return

    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT name, username, end_date, balance
        FROM subscribers
        WHERE archived = 1
        ORDER BY id DESC
    """)
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        await update.message.reply_text("لا يوجد مشتركون مؤرشفون حاليًا.", reply_markup=main_keyboard)
        return

    msg = "المشتركون المؤرشفون:\n\n"
    for row in rows:
        msg += (
            f"الاسم: {row[0]}\n"
            f"اليوزر: {row[1]}\n"
            f"الانتهاء: {row[2]}\n"
            f"{format_balance(row[3])}\n"
            f"--------------------\n"
        )
    await update.message.reply_text(msg, reply_markup=main_keyboard)


async def restore_subscriber_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        await update.message.reply_text("هذا الخيار للإدارة فقط.", reply_markup=get_user_keyboard(update))
        return

    context.user_data.clear()
    context.user_data["restore_mode"] = True
    await update.message.reply_text("أرسل الاسم أو اليوزر للمشترك الذي تريد استرجاعه من الأرشيف:")


async def handle_restore(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()

    if text == "إلغاء":
        context.user_data.clear()
        await update.message.reply_text("تم الإلغاء.", reply_markup=main_keyboard)
        return

    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, name, username
        FROM subscribers
        WHERE archived = 1 AND (username = ? OR name LIKE ?)
        LIMIT 1
    """, (text, f"%{text}%"))
    row = cursor.fetchone()

    if not row:
        conn.close()
        context.user_data.clear()
        await update.message.reply_text("المشترك المؤرشف غير موجود ❌", reply_markup=main_keyboard)
        return

    cursor.execute("UPDATE subscribers SET archived = 0 WHERE id = ?", (row[0],))
    conn.commit()
    conn.close()

    context.user_data.clear()
    await update.message.reply_text(
        f"تم استرجاع المشترك من الأرشيف ✅\nالاسم: {row[1]}\nاليوزر: {row[2]}",
        reply_markup=main_keyboard
    )


async def permanent_delete_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        await update.message.reply_text("هذا الخيار للإدارة فقط.", reply_markup=get_user_keyboard(update))
        return

    context.user_data.clear()
    context.user_data["permanent_delete_mode"] = "username"
    await update.message.reply_text("أرسل اليوزر للمشترك الذي تريد حذفه نهائيًا:")


async def handle_permanent_delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if text == "إلغاء":
        context.user_data.clear()
        await update.message.reply_text("تم الإلغاء.", reply_markup=main_keyboard)
        return

    if context.user_data.get("permanent_delete_mode") == "username":
        context.user_data["permanent_delete_username"] = text
        context.user_data["permanent_delete_mode"] = "confirm"
        await update.message.reply_text("هذا حذف نهائي لا يمكن التراجع عنه.\nاكتب: نعم")
        return

    if context.user_data.get("permanent_delete_mode") == "confirm":
        if text != "نعم":
            context.user_data.clear()
            await update.message.reply_text("تم الإلغاء.", reply_markup=main_keyboard)
            return

        conn = get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT id, name FROM subscribers WHERE username = ?", (context.user_data["permanent_delete_username"],))
        row = cursor.fetchone()

        if not row:
            conn.close()
            context.user_data.clear()
            await update.message.reply_text("المشترك غير موجود ❌", reply_markup=main_keyboard)
            return

        cursor.execute("DELETE FROM transactions WHERE subscriber_id = ?", (row[0],))
        cursor.execute("DELETE FROM subscribers WHERE id = ?", (row[0],))
        conn.commit()
        conn.close()

        context.user_data.clear()
        await update.message.reply_text(f"تم الحذف النهائي للمشترك: {row[1]} ✅", reply_markup=main_keyboard)


# =========================
# تعديل مشترك
# =========================
async def edit_subscriber(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        await update.message.reply_text("هذا الخيار للإدارة فقط.", reply_markup=get_user_keyboard(update))
        return

    context.user_data.clear()
    context.user_data["edit_step"] = "lookup"
    await update.message.reply_text("أرسل الاسم أو اليوزر للمشترك الذي تريد تعديله:")


async def handle_edit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()

    if text == "إلغاء":
        context.user_data.clear()
        await update.message.reply_text("تم الإلغاء.", reply_markup=main_keyboard)
        return

    field_map = {
        "👤 الاسم": "name",
        "🧑‍💻 اليوزر": "username",
        "📞 الهاتف": "phone",
        "🌐 IP": "ip",
        "📦 البروفايل": "profile",
        "💰 السعر": "price",
        "📡 نوع الربط": "connection_type",
        "📝 الملاحظات": "notes",
        "📅 تاريخ البداية": "start_date",
        "⏳ تاريخ النهاية": "end_date",
        "💳 الرصيد": "balance",
        "📒 ملاحظة مالية": "financial_note",
    }

    step = context.user_data.get("edit_step")

    if step == "lookup":
        conn = get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, name, username, phone, ip, profile, price, connection_type, notes, start_date, end_date, balance, financial_note
            FROM subscribers
            WHERE username = ? OR name LIKE ?
            LIMIT 1
        """, (text, f"%{text}%"))
        row = cursor.fetchone()
        conn.close()

        if not row:
            await update.message.reply_text("المشترك غير موجود ❌", reply_markup=main_keyboard)
            context.user_data.clear()
            return

        context.user_data["edit_subscriber_id"] = row[0]
        context.user_data["edit_subscriber_name"] = row[1]
        context.user_data["edit_subscriber_username"] = row[2]
        context.user_data["edit_step"] = "field"

        await update.message.reply_text(
            f"تم العثور على المشترك ✅\n\n"
            f"الاسم: {row[1]}\n"
            f"اليوزر: {row[2]}\n"
            f"الهاتف: {row[3]}\n"
            f"IP: {row[4]}\n"
            f"البروفايل: {row[5]}\n"
            f"السعر: {row[6]}\n"
            f"النوع: {row[7]}\n"
            f"الملاحظات: {row[8] if row[8] else 'لا توجد'}\n"
            f"تاريخ البداية: {row[9]}\n"
            f"تاريخ النهاية: {row[10]}\n"
            f"{format_balance(row[11])}\n"
            f"الملاحظة المالية: {row[12] if row[12] else 'لا توجد'}\n\n"
            f"اختر الحقل الذي تريد تعديله:",
            reply_markup=edit_keyboard
        )
        return

    if step == "field":
        if text not in field_map:
            await update.message.reply_text("اختر الحقل من الأزرار فقط.", reply_markup=edit_keyboard)
            return

        context.user_data["edit_field"] = field_map[text]
        context.user_data["edit_field_label"] = text
        context.user_data["edit_step"] = "value"

        field = field_map[text]
        if field == "connection_type":
            await update.message.reply_text("اختر نوع الربط الجديد:", reply_markup=connection_keyboard)
            return
        if field == "profile":
            await update.message.reply_text("اختر البروفايل الجديد:", reply_markup=profile_keyboard)
            return
        if field == "price":
            await update.message.reply_text("اختر السعر الجديد:", reply_markup=price_keyboard)
            return

        await update.message.reply_text(f"أرسل القيمة الجديدة لـ {text}:")
        return

    if step == "value":
        subscriber_id = context.user_data["edit_subscriber_id"]
        field = context.user_data["edit_field"]
        new_value = text

        if field == "phone" and not valid_phone(new_value):
            await update.message.reply_text("رقم الهاتف يجب أن يكون 11 رقم فقط.")
            return

        if field == "ip" and not valid_ip(new_value):
            await update.message.reply_text("صيغة الـ IP غير صحيحة. مثال: 192.168.1.1")
            return

        if field == "connection_type" and not valid_connection_type(new_value):
            await update.message.reply_text("اختر فقط OLT أو Sector.", reply_markup=connection_keyboard)
            return

        if field == "profile" and not valid_profile(new_value):
            await update.message.reply_text("اختر البروفايل من الأزرار فقط.", reply_markup=profile_keyboard)
            return

        if field == "price":
            allowed_prices = ["47000", "57000", "70000", "150000"]

            if new_value == "قيمة أخرى":
                context.user_data["waiting_custom_edit_price"] = True
                await update.message.reply_text("أرسل السعر الجديد كرقم فقط مثل 25000")
                return

            if new_value in allowed_prices:
                new_value = float(new_value)
                context.user_data.pop("waiting_custom_edit_price", None)
            elif context.user_data.get("waiting_custom_edit_price"):
                try:
                    new_value = float(new_value)
                    context.user_data.pop("waiting_custom_edit_price", None)
                except ValueError:
                    await update.message.reply_text("السعر غير صحيح. أرسل رقم فقط مثل 25000")
                    return
            else:
                await update.message.reply_text("اختر السعر من الأزرار أو اختر قيمة أخرى.", reply_markup=price_keyboard)
                return

        if field == "balance":
            try:
                new_value = float(new_value)
            except ValueError:
                await update.message.reply_text("الرصيد يجب أن يكون رقمًا فقط.")
                return

        if field in ["start_date", "end_date"] and not valid_date(new_value):
            await update.message.reply_text("صيغة التاريخ خطأ ❌ اكتب هكذا: 2026-03-20")
            return

        conn = get_conn()
        cursor = conn.cursor()
        cursor.execute(f"SELECT {field} FROM subscribers WHERE id = ?", (subscriber_id,))
        old_row = cursor.fetchone()
        old_value = old_row[0] if old_row else ""

        try:
            cursor.execute(f"UPDATE subscribers SET {field} = ? WHERE id = ?", (new_value, subscriber_id))

            if field == "balance":
                cursor.execute("""
                    INSERT INTO transactions (subscriber_id, type, amount, note, created_at)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    subscriber_id,
                    "balance_adjustment",
                    float(new_value),
                    "تعديل يدوي للرصيد",
                    now_str()
                ))

            conn.commit()
            conn.close()

            add_edit_log(
                subscriber_id=subscriber_id,
                subscriber_name=context.user_data["edit_subscriber_name"],
                subscriber_username=context.user_data["edit_subscriber_username"],
                editor_id=update.effective_user.id,
                editor_name=update.effective_user.full_name,
                field_name=field,
                old_value=old_value,
                new_value=new_value
            )

            await update.message.reply_text("تم التعديل بنجاح ✅", reply_markup=main_keyboard)

        except sqlite3.IntegrityError:
            conn.close()
            await update.message.reply_text("فشل التعديل ❌ غالبًا اليوزر الجديد موجود مسبقًا.", reply_markup=main_keyboard)

        context.user_data.clear()


# =========================
# سجل التعديلات
# =========================
async def show_edit_logs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        await update.message.reply_text("هذا الخيار للإدارة فقط.", reply_markup=get_user_keyboard(update))
        return

    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT subscriber_name, subscriber_username, editor_name, field_name, old_value, new_value, created_at
        FROM edit_logs
        ORDER BY id DESC
        LIMIT 20
    """)
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        await update.message.reply_text("لا يوجد سجل تعديلات حاليًا.", reply_markup=main_keyboard)
        return

    msg = "🧾 آخر التعديلات:\n\n"
    for row in rows:
        msg += (
            f"المشترك: {row[0]} | {row[1]}\n"
            f"المعدل: {row[2]}\n"
            f"الحقل: {row[3]}\n"
            f"القديم: {row[4]}\n"
            f"الجديد: {row[5]}\n"
            f"الوقت: {row[6]}\n"
            f"--------------------\n"
        )

    await update.message.reply_text(msg, reply_markup=main_keyboard)


# =========================
# البحث والعرض
# =========================
async def search_subscriber(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        await update.message.reply_text("هذا الخيار للإدارة فقط.", reply_markup=get_user_keyboard(update))
        return

    context.user_data.clear()
    context.user_data["waiting_for_search"] = True
    await update.message.reply_text("أرسل الاسم أو اليوزر للبحث:")


async def show_all_subscribers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        await update.message.reply_text("هذا الخيار للإدارة فقط.", reply_markup=get_user_keyboard(update))
        return

    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT name, username, ip, connection_type, profile, price, end_date, balance
        FROM subscribers
        WHERE archived IS NULL OR archived = 0
        ORDER BY id DESC
    """)
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        await update.message.reply_text("لا يوجد مشتركون حاليًا.", reply_markup=main_keyboard)
        return

    parts = []
    message = "قائمة المشتركين:\n\n"

    for i, row in enumerate(rows, start=1):
        block = (
            f"{i}) الاسم: {row[0]}\n"
            f"اليوزر: {row[1]}\n"
            f"IP: {row[2]}\n"
            f"النوع: {row[3]}\n"
            f"البروفايل: {row[4]}\n"
            f"السعر: {row[5]}\n"
            f"الانتهاء: {row[6]}\n"
            f"{format_balance(row[7])}\n"
            f"--------------------\n"
        )
        if len(message) + len(block) > 3500:
            parts.append(message)
            message = block
        else:
            message += block

    if message:
        parts.append(message)

    for part in parts:
        await update.message.reply_text(part, reply_markup=main_keyboard)


async def show_expired(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        await update.message.reply_text("هذا الخيار للإدارة فقط.", reply_markup=get_user_keyboard(update))
        return

    today = today_str()
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT name, username, end_date
        FROM subscribers
        WHERE (archived IS NULL OR archived = 0)
          AND end_date IS NOT NULL AND end_date < ?
        ORDER BY end_date ASC
    """, (today,))
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        await update.message.reply_text("لا يوجد مشترك منتهي حاليًا ✅", reply_markup=main_keyboard)
        return

    message = "المشتركين المنتهية اشتراكاتهم:\n\n"
    for row in rows:
        message += f"الاسم: {row[0]}\nاليوزر: {row[1]}\nانتهى: {row[2]}\n--------------------\n"

    await update.message.reply_text(message, reply_markup=main_keyboard)


async def show_near_expiry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        await update.message.reply_text("هذا الخيار للإدارة فقط.", reply_markup=get_user_keyboard(update))
        return

    today = datetime.now().date()
    limit_date = today + timedelta(days=3)

    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT name, username, end_date
        FROM subscribers
        WHERE (archived IS NULL OR archived = 0)
          AND end_date IS NOT NULL AND end_date >= ? AND end_date <= ?
        ORDER BY end_date ASC
    """, (today.strftime("%Y-%m-%d"), limit_date.strftime("%Y-%m-%d")))
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        await update.message.reply_text("لا يوجد مشترك قريب ينتهي خلال 3 أيام ✅", reply_markup=main_keyboard)
        return

    message = "المشتركين القريب انتهاءهم خلال 3 أيام:\n\n"
    for row in rows:
        message += f"الاسم: {row[0]}\nاليوزر: {row[1]}\nينتهي: {row[2]}\n--------------------\n"

    await update.message.reply_text(message, reply_markup=main_keyboard)


async def show_balance_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        await update.message.reply_text("هذا الخيار للإدارة فقط.", reply_markup=get_user_keyboard(update))
        return

    context.user_data.clear()
    context.user_data["balance_lookup"] = True
    await update.message.reply_text("أرسل الاسم أو اليوزر لعرض الرصيد:")


async def show_history_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        await update.message.reply_text("هذا الخيار للإدارة فقط.", reply_markup=get_user_keyboard(update))
        return

    context.user_data.clear()
    context.user_data["history_lookup"] = True
    await update.message.reply_text("أرسل الاسم أو اليوزر لعرض السجل:")


async def show_debtors(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        await update.message.reply_text("هذا الخيار للإدارة فقط.", reply_markup=get_user_keyboard(update))
        return

    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT name, username, balance
        FROM subscribers
        WHERE (archived IS NULL OR archived = 0) AND balance < 0
        ORDER BY balance ASC
    """)
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        await update.message.reply_text("لا يوجد مدينون حاليًا ✅", reply_markup=main_keyboard)
        return

    message = "المشتركون الذين عليهم ديون:\n\n"
    for row in rows:
        message += f"الاسم: {row[0]}\nاليوزر: {row[1]}\nالدين: {abs(row[2]):,.0f}\n--------------------\n"

    await update.message.reply_text(message, reply_markup=main_keyboard)


async def show_prepaid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        await update.message.reply_text("هذا الخيار للإدارة فقط.", reply_markup=get_user_keyboard(update))
        return

    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT name, username, balance
        FROM subscribers
        WHERE (archived IS NULL OR archived = 0) AND balance > 0
        ORDER BY balance DESC
    """)
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        await update.message.reply_text("لا يوجد مشتركون لديهم مبالغ مقدمة حاليًا ✅", reply_markup=main_keyboard)
        return

    message = "المشتركون الذين لديهم مبالغ مقدمة:\n\n"
    for row in rows:
        message += f"الاسم: {row[0]}\nاليوزر: {row[1]}\nالمقدم: {row[2]:,.0f}\n--------------------\n"

    await update.message.reply_text(message, reply_markup=main_keyboard)


# =========================
# فلترة
# =========================
async def filter_type_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        await update.message.reply_text("هذا الخيار للإدارة فقط.", reply_markup=get_user_keyboard(update))
        return

    context.user_data.clear()
    context.user_data["filter_type"] = True
    await update.message.reply_text("اختر نوع الربط:", reply_markup=connection_keyboard)


async def filter_profile_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        await update.message.reply_text("هذا الخيار للإدارة فقط.", reply_markup=get_user_keyboard(update))
        return

    context.user_data.clear()
    context.user_data["filter_profile"] = True
    await update.message.reply_text("اختر البروفايل:", reply_markup=profile_keyboard)


# =========================
# إحصائيات / إيرادات / تقرير
# =========================
async def show_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        await update.message.reply_text("هذا الخيار للإدارة فقط.", reply_markup=get_user_keyboard(update))
        return

    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM subscribers WHERE archived IS NULL OR archived = 0")
    total = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM subscribers WHERE (archived IS NULL OR archived = 0) AND balance < 0")
    debtors = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM subscribers WHERE (archived IS NULL OR archived = 0) AND balance > 0")
    prepaid = cursor.fetchone()[0]
    cursor.execute("SELECT COALESCE(SUM(balance),0) FROM subscribers WHERE (archived IS NULL OR archived = 0) AND balance < 0")
    total_debt = cursor.fetchone()[0] or 0
    cursor.execute("SELECT COALESCE(SUM(balance),0) FROM subscribers WHERE (archived IS NULL OR archived = 0) AND balance > 0")
    total_prepaid = cursor.fetchone()[0] or 0
    conn.close()

    await update.message.reply_text(
        f"📊 لوحة الإحصائيات\n\n"
        f"👥 المشتركين النشطين: {total}\n"
        f"💸 المدينون: {debtors}\n"
        f"💰 المقدمون: {prepaid}\n\n"
        f"🔻 مجموع الديون: {abs(total_debt):,.0f}\n"
        f"🔺 مجموع المقدم: {total_prepaid:,.0f}",
        reply_markup=main_keyboard
    )


async def show_income(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        await update.message.reply_text("هذا الخيار للإدارة فقط.", reply_markup=get_user_keyboard(update))
        return

    conn = get_conn()
    cursor = conn.cursor()
    today = today_str()
    month = datetime.now().strftime("%Y-%m")

    cursor.execute("""
        SELECT COALESCE(SUM(amount),0) FROM transactions
        WHERE type='payment' AND created_at LIKE ?
    """, (f"{today}%",))
    today_income = cursor.fetchone()[0] or 0

    cursor.execute("""
        SELECT COALESCE(SUM(amount),0) FROM transactions
        WHERE type='payment' AND created_at LIKE ?
    """, (f"{month}%",))
    month_income = cursor.fetchone()[0] or 0
    conn.close()

    await update.message.reply_text(
        f"💵 الإيرادات\n\n"
        f"📅 اليوم: {today_income:,.0f}\n"
        f"📆 هذا الشهر: {month_income:,.0f}",
        reply_markup=main_keyboard
    )


async def send_today_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        await update.message.reply_text("هذا الخيار للإدارة فقط.", reply_markup=get_user_keyboard(update))
        return

    await update.message.reply_text(build_daily_report_text(), reply_markup=main_keyboard)


# =========================
# Backup + Excel + Restore
# =========================
async def send_backup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        await update.message.reply_text("هذا الخيار للإدارة فقط.", reply_markup=get_user_keyboard(update))
        return

    try:
        shutil.copyfile(DB_FILE, BACKUP_FILE)
        with open(BACKUP_FILE, "rb") as f:
            await update.message.reply_document(
                document=f,
                filename="subscribers_backup.db",
                caption="هذه النسخة الاحتياطية لقاعدة البيانات ✅"
            )
        if os.path.exists(BACKUP_FILE):
            os.remove(BACKUP_FILE)
    except Exception as e:
        await update.message.reply_text(f"حدث خطأ أثناء إنشاء النسخة الاحتياطية:\n{e}", reply_markup=main_keyboard)


async def restore_backup_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        await update.message.reply_text("هذا الخيار للإدارة فقط.", reply_markup=get_user_keyboard(update))
        return ConversationHandler.END

    context.user_data.clear()
    await update.message.reply_text("أرسل ملف النسخة الاحتياطية .db الآن", reply_markup=main_keyboard)
    return RESTORE_WAIT_FILE


async def restore_backup_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.document:
        await update.message.reply_text("أرسل ملف قاعدة البيانات فقط.")
        return RESTORE_WAIT_FILE

    doc = update.message.document
    if not doc.file_name.endswith(".db"):
        await update.message.reply_text("يجب أن يكون الملف بصيغة .db")
        return RESTORE_WAIT_FILE

    try:
        file = await doc.get_file()
        await file.download_to_drive(RESTORE_TEMP_FILE)

        if os.path.exists(DB_FILE):
            shutil.copyfile(DB_FILE, DB_FILE + ".before_restore")

        shutil.copyfile(RESTORE_TEMP_FILE, DB_FILE)

        if os.path.exists(RESTORE_TEMP_FILE):
            os.remove(RESTORE_TEMP_FILE)

        await update.message.reply_text("تم استرجاع النسخة الاحتياطية بنجاح ✅", reply_markup=main_keyboard)
        return ConversationHandler.END

    except Exception as e:
        await update.message.reply_text(f"فشل استرجاع النسخة:\n{e}", reply_markup=main_keyboard)
        return ConversationHandler.END


async def export_excel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        await update.message.reply_text("هذا الخيار للإدارة فقط.", reply_markup=get_user_keyboard(update))
        return

    try:
        conn = get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                name,
                phone,
                username,
                ip,
                connection_type,
                profile,
                price,
                notes,
                start_date,
                end_date,
                balance,
                archived,
                financial_note,
                telegram_id
            FROM subscribers
            ORDER BY id DESC
        """)
        rows = cursor.fetchall()
        conn.close()

        wb = Workbook()
        ws = wb.active
        ws.title = "Subscribers"

        headers = [
            "الاسم", "الهاتف", "اليوزر", "IP", "نوع الربط", "البروفايل",
            "السعر", "الملاحظات", "تاريخ البداية", "تاريخ النهاية",
            "الرصيد", "مؤرشف", "ملاحظة مالية", "Telegram ID"
        ]
        ws.append(headers)

        for row in rows:
            ws.append(list(row))

        for col in ws.columns:
            max_length = 0
            col_letter = col[0].column_letter
            for cell in col:
                value = str(cell.value) if cell.value is not None else ""
                if len(value) > max_length:
                    max_length = len(value)
            ws.column_dimensions[col_letter].width = max_length + 2

        wb.save(EXCEL_FILE)

        with open(EXCEL_FILE, "rb") as f:
            await update.message.reply_document(
                document=f,
                filename="subscribers_export.xlsx",
                caption="تم تصدير ملف Excel بنجاح ✅"
            )

        if os.path.exists(EXCEL_FILE):
            os.remove(EXCEL_FILE)

    except Exception as e:
        await update.message.reply_text(f"حدث خطأ أثناء تصدير Excel:\n{e}", reply_markup=main_keyboard)


# =========================
# التكتات
# =========================
async def admin_ticket_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        await update.message.reply_text("هذا الخيار للإدارة فقط.", reply_markup=get_user_keyboard(update))
        return ConversationHandler.END

    context.user_data.clear()
    await update.message.reply_text("اختر نوع التكت الإداري:", reply_markup=admin_ticket_keyboard)
    return ADMIN_TICKET_CATEGORY


async def admin_ticket_category(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if text == "إلغاء":
        return await cancel(update, context)

    if text not in ADMIN_TICKET_CATEGORIES:
        await update.message.reply_text("اختر من الأزرار فقط.", reply_markup=admin_ticket_keyboard)
        return ADMIN_TICKET_CATEGORY

    context.user_data["admin_ticket_category"] = text

    if text in ["صيانة", "تغيير رمز", "قطع / إيقاف", "إعادة تشغيل / ريفرش"]:
        await update.message.reply_text("لمن هذا الإجراء؟ اكتب اسم المشترك أو اليوزر:")
        return ADMIN_TICKET_TARGET

    await update.message.reply_text("اكتب تفاصيل التكت:")
    return ADMIN_TICKET_DETAILS


async def admin_ticket_target(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if text == "إلغاء":
        return await cancel(update, context)

    context.user_data["admin_ticket_target"] = text
    await update.message.reply_text("اكتب تفاصيل التكت:")
    return ADMIN_TICKET_DETAILS


async def admin_ticket_details(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if text == "إلغاء":
        return await cancel(update, context)

    category = context.user_data.get("admin_ticket_category", "")
    target = context.user_data.get("admin_ticket_target", "")
    ticket_number = next_ticket_number()

    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO tickets
        (ticket_number, creator_role, creator_telegram_id, category, target_name, details, status, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        ticket_number,
        "admin",
        update.effective_user.id,
        category,
        target,
        text,
        "مفتوح",
        now_str()
    ))
    conn.commit()
    conn.close()

    await notify_admins(
        context,
        f"🔔 تكت إداري جديد\n\n"
        f"رقم: {ticket_number}\n"
        f"النوع: {category}\n"
        f"الهدف: {target if target else 'لا يوجد'}"
    )

    await update.message.reply_text(
        f"تم إنشاء التكت بنجاح ✅\n"
        f"رقم التكت: {ticket_number}\n"
        f"النوع: {category}\n"
        f"الهدف: {target if target else 'لا يوجد'}\n"
        f"الحالة: مفتوح",
        reply_markup=main_keyboard
    )
    return ConversationHandler.END


async def user_ticket_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await update.message.reply_text("اختر نوع التكت:", reply_markup=user_ticket_keyboard)
    return USER_TICKET_CATEGORY


async def user_ticket_category(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if text == "إلغاء":
        return await cancel(update, context)

    if text not in USER_TICKET_CATEGORIES:
        await update.message.reply_text("اختر من الأزرار فقط.", reply_markup=user_ticket_keyboard)
        return USER_TICKET_CATEGORY

    context.user_data["user_ticket_category"] = text
    await update.message.reply_text("اكتب تفاصيل المشكلة:")
    return USER_TICKET_DETAILS


async def user_ticket_details(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if text == "إلغاء":
        return await cancel(update, context)

    linked = get_linked_subscriber_by_telegram_id(update.effective_user.id)
    subscriber_id = linked[0] if linked else None
    subscriber_name = linked[1] if linked else update.effective_user.full_name
    subscriber_username = linked[2] if linked else ""

    ticket_number = next_ticket_number()
    category = context.user_data.get("user_ticket_category", "")

    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO tickets
        (ticket_number, creator_role, creator_telegram_id, subscriber_id, subscriber_name, subscriber_username, category, details, status, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        ticket_number,
        "user",
        update.effective_user.id,
        subscriber_id,
        subscriber_name,
        subscriber_username,
        category,
        text,
        "مفتوح",
        now_str()
    ))
    conn.commit()
    conn.close()

    await notify_admins(
        context,
        f"🔔 تكت جديد من مشترك\n\n"
        f"رقم: {ticket_number}\n"
        f"النوع: {category}\n"
        f"الاسم: {subscriber_name}\n"
        f"اليوزر: {subscriber_username if subscriber_username else '---'}\n"
        f"التفاصيل: {text}"
    )

    await update.message.reply_text(
        f"تم فتح التكت بنجاح ✅\n"
        f"رقم التكت: {ticket_number}\n"
        f"النوع: {category}\n"
        f"الحالة: مفتوح",
        reply_markup=get_user_keyboard(update)
    )
    return ConversationHandler.END


async def show_all_tickets(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        await update.message.reply_text("هذا الخيار للإدارة فقط.", reply_markup=get_user_keyboard(update))
        return

    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT ticket_number, creator_role, subscriber_name, subscriber_username, category, status, created_at
        FROM tickets
        ORDER BY id DESC
        LIMIT 30
    """)
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        await update.message.reply_text("لا يوجد تكتات حاليًا.", reply_markup=main_keyboard)
        return

    msg = "📋 كل التكتات:\n\n"
    for row in rows:
        msg += (
            f"رقم: {row[0]}\n"
            f"الجهة: {row[1]}\n"
            f"الاسم: {row[2] if row[2] else '---'}\n"
            f"اليوزر: {row[3] if row[3] else '---'}\n"
            f"النوع: {row[4]}\n"
            f"الحالة: {row[5]}\n"
            f"التاريخ: {row[6]}\n"
            f"--------------------\n"
        )
    await update.message.reply_text(msg, reply_markup=main_keyboard)


async def show_open_tickets(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        await update.message.reply_text("هذا الخيار للإدارة فقط.", reply_markup=get_user_keyboard(update))
        return

    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT ticket_number, creator_role, subscriber_name, subscriber_username, category, created_at
        FROM tickets
        WHERE status = 'مفتوح'
        ORDER BY id DESC
        LIMIT 30
    """)
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        await update.message.reply_text("لا يوجد تكتات مفتوحة حاليًا.", reply_markup=main_keyboard)
        return

    msg = "📂 التكتات المفتوحة:\n\n"
    for row in rows:
        msg += (
            f"رقم: {row[0]}\n"
            f"الجهة: {row[1]}\n"
            f"الاسم: {row[2] if row[2] else '---'}\n"
            f"اليوزر: {row[3] if row[3] else '---'}\n"
            f"النوع: {row[4]}\n"
            f"التاريخ: {row[5]}\n"
            f"--------------------\n"
        )
    await update.message.reply_text(msg, reply_markup=main_keyboard)


async def close_ticket_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        await update.message.reply_text("هذا الخيار للإدارة فقط.", reply_markup=get_user_keyboard(update))
        return ConversationHandler.END

    context.user_data.clear()
    await update.message.reply_text("أرسل رقم التكت مثل: T-1234567890")
    return CLOSE_TICKET_ID


async def close_ticket_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if text == "إلغاء":
        return await cancel(update, context)

    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, status
        FROM tickets
        WHERE ticket_number = ?
        LIMIT 1
    """, (text,))
    row = cursor.fetchone()

    if not row:
        conn.close()
        await update.message.reply_text("رقم التكت غير موجود.", reply_markup=main_keyboard)
        return ConversationHandler.END

    if row[1] == "مغلق":
        conn.close()
        await update.message.reply_text("هذا التكت مغلق أصلًا.", reply_markup=main_keyboard)
        return ConversationHandler.END

    cursor.execute("""
        UPDATE tickets
        SET status = 'مغلق', closed_at = ?, closed_by = ?
        WHERE id = ?
    """, (now_str(), update.effective_user.id, row[0]))
    conn.commit()
    conn.close()

    await update.message.reply_text("تم إغلاق التكت بنجاح ✅", reply_markup=main_keyboard)
    return ConversationHandler.END


async def show_my_tickets(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT ticket_number, category, status, created_at
        FROM tickets
        WHERE creator_telegram_id = ?
        ORDER BY id DESC
        LIMIT 20
    """, (update.effective_user.id,))
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        await update.message.reply_text("لا توجد لديك تكتات حاليًا.", reply_markup=get_user_keyboard(update))
        return

    msg = "📋 تكتاتي:\n\n"
    for row in rows:
        msg += (
            f"رقم: {row[0]}\n"
            f"النوع: {row[1]}\n"
            f"الحالة: {row[2]}\n"
            f"التاريخ: {row[3]}\n"
            f"--------------------\n"
        )
    await update.message.reply_text(msg, reply_markup=get_user_keyboard(update))


# =========================
# الرسائل العامة
# =========================
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()

    if context.user_data.get("payment_step"):
        await handle_payment(update, context)
        return

    if context.user_data.get("delete_mode"):
        await handle_delete(update, context)
        return

    if context.user_data.get("archive_mode"):
        await handle_archive(update, context)
        return

    if context.user_data.get("restore_mode"):
        await handle_restore(update, context)
        return

    if context.user_data.get("permanent_delete_mode"):
        await handle_permanent_delete(update, context)
        return

    if context.user_data.get("edit_step"):
        await handle_edit(update, context)
        return

    if context.user_data.get("filter_type"):
        if text == "إلغاء":
            context.user_data.clear()
            await update.message.reply_text("تم الإلغاء.", reply_markup=get_user_keyboard(update))
            return

        if not valid_connection_type(text):
            await update.message.reply_text("اختر فقط OLT أو Sector.", reply_markup=connection_keyboard)
            return

        conn = get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT name, username
            FROM subscribers
            WHERE (archived IS NULL OR archived = 0) AND connection_type = ?
            ORDER BY id DESC
        """, (text,))
        rows = cursor.fetchall()
        conn.close()
        context.user_data.clear()

        if not rows:
            await update.message.reply_text("لا يوجد نتائج", reply_markup=main_keyboard)
            return

        msg = "نتائج الفلترة حسب النوع:\n\n"
        for r in rows:
            msg += f"{r[0]} | {r[1]}\n"
        await update.message.reply_text(msg, reply_markup=main_keyboard)
        return

    if context.user_data.get("filter_profile"):
        if text == "إلغاء":
            context.user_data.clear()
            await update.message.reply_text("تم الإلغاء.", reply_markup=get_user_keyboard(update))
            return

        if not valid_profile(text):
            await update.message.reply_text("اختر البروفايل من الأزرار فقط.", reply_markup=profile_keyboard)
            return

        conn = get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT name, username
            FROM subscribers
            WHERE (archived IS NULL OR archived = 0) AND profile = ?
            ORDER BY id DESC
        """, (text,))
        rows = cursor.fetchall()
        conn.close()
        context.user_data.clear()

        if not rows:
            await update.message.reply_text("لا يوجد نتائج", reply_markup=main_keyboard)
            return

        msg = "نتائج الفلترة حسب البروفايل:\n\n"
        for r in rows:
            msg += f"{r[0]} | {r[1]}\n"
        await update.message.reply_text(msg, reply_markup=main_keyboard)
        return

    if context.user_data.get("waiting_for_search"):
        if text == "إلغاء":
            context.user_data.clear()
            await update.message.reply_text("تم الإلغاء.", reply_markup=get_user_keyboard(update))
            return

        conn = get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, name, username
            FROM subscribers
            WHERE (archived IS NULL OR archived = 0)
              AND (username LIKE ? OR name LIKE ?)
            ORDER BY id DESC
            LIMIT 10
        """, (f"%{text}%", f"%{text}%"))
        matches = cursor.fetchall()

        if not matches:
            conn.close()
            context.user_data.clear()
            await update.message.reply_text("لم يتم العثور على مشترك بهذا الاسم أو اليوزر.", reply_markup=main_keyboard)
            return

        if len(matches) > 1:
            msg = "وجدت أكثر من نتيجة:\n\n"
            for row in matches:
                msg += f"- {row[1]} | {row[2]}\n"
            msg += "\nأرسل اليوزر بالضبط لاختيار المشترك."
            context.user_data["waiting_for_search_exact"] = True
            context.user_data["waiting_for_search"] = False
            conn.close()
            await update.message.reply_text(msg, reply_markup=main_keyboard)
            return

        cursor.execute("""
            SELECT name, phone, username, ip, connection_type, profile, price, notes, start_date, end_date, balance, financial_note
            FROM subscribers
            WHERE id = ?
        """, (matches[0][0],))
        row = cursor.fetchone()
        conn.close()
        context.user_data.clear()

        if row:
            message = (
                f"تم العثور على المشترك ✅\n\n"
                f"الاسم: {row[0]}\n"
                f"الهاتف: {row[1]}\n"
                f"اليوزر: {row[2]}\n"
                f"IP: {row[3]}\n"
                f"نوع الربط: {row[4]}\n"
                f"البروفايل: {row[5]}\n"
                f"السعر: {row[6]}\n"
                f"الملاحظات: {row[7] if row[7] else 'لا توجد'}\n"
                f"بداية الاشتراك: {row[8]}\n"
                f"نهاية الاشتراك: {row[9]}\n"
                f"{format_balance(row[10])}\n"
                f"الملاحظة المالية: {row[11] if row[11] else 'لا توجد'}"
            )
            await update.message.reply_text(message, reply_markup=main_keyboard)
        return

    if context.user_data.get("waiting_for_search_exact"):
        if text == "إلغاء":
            context.user_data.clear()
            await update.message.reply_text("تم الإلغاء.", reply_markup=get_user_keyboard(update))
            return

        conn = get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT name, phone, username, ip, connection_type, profile, price, notes, start_date, end_date, balance, financial_note
            FROM subscribers
            WHERE username = ?
            LIMIT 1
        """, (text,))
        row = cursor.fetchone()
        conn.close()
        context.user_data.clear()

        if row:
            message = (
                f"تم العثور على المشترك ✅\n\n"
                f"الاسم: {row[0]}\n"
                f"الهاتف: {row[1]}\n"
                f"اليوزر: {row[2]}\n"
                f"IP: {row[3]}\n"
                f"نوع الربط: {row[4]}\n"
                f"البروفايل: {row[5]}\n"
                f"السعر: {row[6]}\n"
                f"الملاحظات: {row[7] if row[7] else 'لا توجد'}\n"
                f"بداية الاشتراك: {row[8]}\n"
                f"نهاية الاشتراك: {row[9]}\n"
                f"{format_balance(row[10])}\n"
                f"الملاحظة المالية: {row[11] if row[11] else 'لا توجد'}"
            )
            await update.message.reply_text(message, reply_markup=main_keyboard)
        else:
            await update.message.reply_text("لم يتم العثور على اليوزر.", reply_markup=main_keyboard)
        return

    if context.user_data.get("balance_lookup"):
        if text == "إلغاء":
            context.user_data.clear()
            await update.message.reply_text("تم الإلغاء.", reply_markup=get_user_keyboard(update))
            return

        conn = get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT name, username, balance
            FROM subscribers
            WHERE (username = ? OR name LIKE ?)
            LIMIT 1
        """, (text, f"%{text}%"))
        row = cursor.fetchone()
        conn.close()
        context.user_data.clear()

        if row:
            await update.message.reply_text(
                f"الاسم: {row[0]}\nاليوزر: {row[1]}\n{format_balance(row[2])}",
                reply_markup=main_keyboard
            )
        else:
            await update.message.reply_text("المشترك غير موجود ❌", reply_markup=main_keyboard)
        return

    if context.user_data.get("history_lookup"):
        if text == "إلغاء":
            context.user_data.clear()
            await update.message.reply_text("تم الإلغاء.", reply_markup=get_user_keyboard(update))
            return

        conn = get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, name, username
            FROM subscribers
            WHERE username = ? OR name LIKE ?
            LIMIT 1
        """, (text, f"%{text}%"))
        sub = cursor.fetchone()

        if not sub:
            conn.close()
            context.user_data.clear()
            await update.message.reply_text("المشترك غير موجود ❌", reply_markup=main_keyboard)
            return

        cursor.execute("""
            SELECT type, amount, note, created_at
            FROM transactions
            WHERE subscriber_id = ?
            ORDER BY id DESC
            LIMIT 20
        """, (sub[0],))
        rows = cursor.fetchall()
        conn.close()
        context.user_data.clear()

        if not rows:
            await update.message.reply_text(
                f"لا يوجد سجل عمليات لهذا المشترك.\nالاسم: {sub[1]}\nاليوزر: {sub[2]}",
                reply_markup=main_keyboard
            )
            return

        message = f"سجل المشترك:\nالاسم: {sub[1]}\nاليوزر: {sub[2]}\n\n"
        for row in rows:
            message += (
                f"النوع: {row[0]}\n"
                f"المبلغ: {row[1]:,.0f}\n"
                f"الملاحظة: {row[2] if row[2] else 'لا توجد'}\n"
                f"التاريخ: {row[3]}\n"
                f"--------------------\n"
            )

        await update.message.reply_text(message, reply_markup=main_keyboard)
        return

    if text == "بحث عن مشترك":
        await search_subscriber(update, context)
        return
    if text == "تسجيل دفعة":
        await register_payment(update, context)
        return
    if text == "كل المشتركين":
        await show_all_subscribers(update, context)
        return
    if text == "المنتهية":
        await show_expired(update, context)
        return
    if text == "قريب ينتهي":
        await show_near_expiry(update, context)
        return
    if text == "عرض الرصيد":
        await show_balance_start(update, context)
        return
    if text == "سجل المشترك":
        await show_history_start(update, context)
        return
    if text == "المدينون":
        await show_debtors(update, context)
        return
    if text == "المقدمون":
        await show_prepaid(update, context)
        return
    if text == "💰 مجموع الديون":
        await total_debt_only(update, context)
        return
    if text == "فلترة نوع":
        await filter_type_start(update, context)
        return
    if text == "فلترة بروفايل":
        await filter_profile_start(update, context)
        return
    if text == "إحصائيات":
        await show_stats(update, context)
        return
    if text == "إيرادات":
        await show_income(update, context)
        return
    if text == "تقرير اليوم":
        await send_today_report(update, context)
        return
    if text == "سجل التعديلات":
        await show_edit_logs(update, context)
        return
    if text == "أرشفة مشترك":
        await archive_subscriber(update, context)
        return
    if text == "المؤرشفون":
        await show_archived(update, context)
        return
    if text == "استرجاع مشترك":
        await restore_subscriber_start(update, context)
        return
    if text == "حذف نهائي":
        await permanent_delete_start(update, context)
        return
    if text == "تعديل مشترك":
        await edit_subscriber(update, context)
        return
    if text == "نسخة احتياطية":
        await send_backup(update, context)
        return
    if text == "تصدير Excel":
        await export_excel(update, context)
        return
    if text == "رفع تكت إداري":
        if is_admin(update):
            await admin_ticket_start(update, context)
        else:
            await update.message.reply_text("هذا الخيار للإدارة فقط.", reply_markup=get_user_keyboard(update))
        return
    if text == "كل التكتات":
        await show_all_tickets(update, context)
        return
    if text == "التكتات المفتوحة":
        await show_open_tickets(update, context)
        return
    if text == "إغلاق تكت":
        if is_admin(update):
            await close_ticket_start(update, context)
        else:
            await update.message.reply_text("هذا الخيار للإدارة فقط.", reply_markup=get_user_keyboard(update))
        return
    if text == "ربط حسابي":
        await link_account_start(update, context)
        return
    if text == "حسابي":
        await show_my_account(update, context)
        return
    if text == "فتح تكت":
        await user_ticket_start(update, context)
        return
    if text == "تكتاتي":
        await show_my_tickets(update, context)
        return
    if text == "إلغاء":
        context.user_data.clear()
        await update.message.reply_text("تم الإلغاء.", reply_markup=get_user_keyboard(update))
        return

    await update.message.reply_text("اختر من الأزرار الموجودة أو اكتب /start", reply_markup=get_user_keyboard(update))


def main():
    init_db()

    app = ApplicationBuilder().token(TOKEN).build()

    add_subscriber_handler = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex("^إضافة مشترك$"), add_subscriber_start)],
        states={
            ADD_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_name)],
            ADD_PHONE: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_phone)],
            ADD_USERNAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_username)],
            ADD_IP: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_ip)],
            ADD_CONNECTION: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_connection)],
            ADD_PROFILE: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_profile)],
            ADD_PRICE: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_price)],
            ADD_NOTES: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_notes)],
            ADD_BALANCE: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_balance)],
        },
        fallbacks=[MessageHandler(filters.Regex("^إلغاء$"), cancel)],
    )

    calc_auto_handler = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex("^كم تصرف$"), calc_auto_start)],
        states={
            CALC_AUTO_DAYS: [MessageHandler(filters.TEXT & ~filters.COMMAND, calc_auto_days)],
        },
        fallbacks=[MessageHandler(filters.Regex("^إلغاء$"), cancel)],
    )

    user_link_handler = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex("^ربط حسابي$"), link_account_start)],
        states={
            USER_LINK_USERNAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, link_account_username)],
        },
        fallbacks=[MessageHandler(filters.Regex("^إلغاء$"), cancel)],
    )

    admin_ticket_handler = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex("^رفع تكت إداري$"), admin_ticket_start)],
        states={
            ADMIN_TICKET_CATEGORY: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_ticket_category)],
            ADMIN_TICKET_TARGET: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_ticket_target)],
            ADMIN_TICKET_DETAILS: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_ticket_details)],
        },
        fallbacks=[MessageHandler(filters.Regex("^إلغاء$"), cancel)],
    )

    user_ticket_handler = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex("^فتح تكت$"), user_ticket_start)],
        states={
            USER_TICKET_CATEGORY: [MessageHandler(filters.TEXT & ~filters.COMMAND, user_ticket_category)],
            USER_TICKET_DETAILS: [MessageHandler(filters.TEXT & ~filters.COMMAND, user_ticket_details)],
        },
        fallbacks=[MessageHandler(filters.Regex("^إلغاء$"), cancel)],
    )

    close_ticket_handler = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex("^إغلاق تكت$"), close_ticket_start)],
        states={
            CLOSE_TICKET_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, close_ticket_id)],
        },
        fallbacks=[MessageHandler(filters.Regex("^إلغاء$"), cancel)],
    )

    restore_backup_handler = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex("^استرجاع نسخة$"), restore_backup_start)],
        states={
            RESTORE_WAIT_FILE: [MessageHandler(filters.Document.ALL, restore_backup_file)],
        },
        fallbacks=[MessageHandler(filters.Regex("^إلغاء$"), cancel)],
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(add_subscriber_handler)
    app.add_handler(calc_auto_handler)
    app.add_handler(user_link_handler)
    app.add_handler(admin_ticket_handler)
    app.add_handler(user_ticket_handler)
    app.add_handler(close_ticket_handler)
    app.add_handler(restore_backup_handler)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    print("Bot is running...")
    app.run_polling()


if __name__ == "__main__":
    main()