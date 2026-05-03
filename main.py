import os
import logging
import asyncio
import stripe
import psycopg2
import subprocess
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.utils.exceptions import BotBlocked
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiohttp import web
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# --- НАСТРОЙКИ ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info("Начинаю подключение к базе данных...")
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
GROUP_ID = os.getenv("GROUP_ID")
ADMIN_IDS = [int(id.strip()) for id in os.getenv("ADMIN_IDS", "").split(",") if id.strip()]
stripe.api_key = os.getenv("STRIPE_API_KEY")

if not DATABASE_URL:
    raise ValueError("Критическая ошибка: DATABASE_URL не задан!")

PHOTO_URL_INTRO = "AgACAgIAAxkBAAMPaee4TD_FGuIQ4LProdOdL5XV5EkAAiYRaxulqkBL5YKQtOj0fV4BAAMCAAN5AAM7BA"
PHOTO_URL_RULES = "AgACAgIAAxkBAAMSaee9wO7psIiqhOR3M52AQ_aRwPgAAjgRaxulqkBLRv00tJs-NW8BAAMCAAN5AAM7BA"

bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)
scheduler = AsyncIOScheduler()

# --- СОСТОЯНИЯ FSM ---
class RegistrationStates(StatesGroup):
    intro = State()
    description = State()
    rules = State()
    choice = State()

# --- ФУНКЦИИ БАЗЫ ДАННЫХ ---
def get_db_conn():
    return psycopg2.connect(DATABASE_URL, sslmode='require')

def init_db():
    conn = get_db_conn()
    cur = conn.cursor()
    # Основная таблица пользователей
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            telegram_id BIGINT UNIQUE NOT NULL,
            paid BOOLEAN DEFAULT FALSE,
            expiry_date TIMESTAMP,
            stripe_subscription_id TEXT,
            reminder_sent BOOLEAN DEFAULT FALSE,
            payment_failed BOOLEAN DEFAULT FALSE,
            grace_period_end TIMESTAMP,
            auto_renew BOOLEAN DEFAULT TRUE,
            trial_used BOOLEAN DEFAULT FALSE,
            first_payment_done BOOLEAN DEFAULT FALSE
        );
    """)
    # Таблица для идемпотентности вебхуков Stripe
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stripe_events (
            event_id TEXT PRIMARY KEY,
            processed BOOLEAN DEFAULT TRUE,
            processed_at TIMESTAMP DEFAULT NOW()
        );
    """)
    # Добавляем недостающие колонки (для старых БД)
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS payment_failed BOOLEAN DEFAULT FALSE;")
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS grace_period_end TIMESTAMP;")
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS auto_renew BOOLEAN DEFAULT TRUE;")
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS trial_used BOOLEAN DEFAULT FALSE;")
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS first_payment_done BOOLEAN DEFAULT FALSE;")
    conn.commit()
    cur.close()
    conn.close()
    logging.info("--- БД ИНИЦИАЛИЗИРОВАНА И ПРОВЕРЕНА ---")

# Идемпотентность вебхуков
async def is_event_processed(event_id):
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM stripe_events WHERE event_id = %s", (event_id,))
    exists = cur.fetchone() is not None
    cur.close()
    conn.close()
    return exists

async def mark_event_processed(event_id):
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("INSERT INTO stripe_events (event_id) VALUES (%s) ON CONFLICT DO NOTHING", (event_id,))
    conn.commit()
    cur.close()
    conn.close()

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---
async def generate_invite_link():
    try:
        invite = await bot.create_chat_invite_link(chat_id=int(GROUP_ID), member_limit=1)
        return invite.invite_link
    except Exception as e:
        logging.error(f"Ошибка создания ссылки: {e}")
        return None

def get_tariffs_keyboard(show_trial=True):
    kb = InlineKeyboardMarkup(row_width=1)
    if show_trial:
        kb.add(InlineKeyboardButton("🌟 Пробная неделя", callback_data="sub_trial"))
    kb.add(
        InlineKeyboardButton("💳 1 месяц (50€)", callback_data="sub_1"),
        InlineKeyboardButton("💳 6 месяцев (240€)", callback_data="sub_6"),
        InlineKeyboardButton("💳 12 месяцев (410€)", callback_data="sub_12")
    )
    return kb

async def notify_admins(text: str):
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, f"⚠️ {text}")
        except Exception:
            pass

# --- АВТОМАТИЧЕСКАЯ ПРОВЕРКА ПОДПИСОК (КРОН) ---
async def ban_user_logic(telegram_id, cur):
    try:
        # aiogram 2.x использует kick_chat_member для бана
        await bot.kick_chat_member(chat_id=int(GROUP_ID), user_id=telegram_id)
        cur.execute("""
            UPDATE users 
            SET paid = FALSE, payment_failed = FALSE, grace_period_end = NULL, reminder_sent = FALSE 
            WHERE telegram_id = %s
        """, (telegram_id,))
        await bot.send_message(telegram_id, 
            "⚠️ Ваша подписка истекла. Доступ закрыт.\nВы можете оформить новую подписку в любое время.",
            reply_markup=get_tariffs_keyboard(show_trial=False))
    except Exception as e:
        logging.error(f"Ошибка при бане {telegram_id}: {e}")

async def check_subscriptions_and_reminders():
    logging.info("--- Запуск ежедневной проверки подписок ---")
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT telegram_id, expiry_date, payment_failed, grace_period_end, auto_renew, reminder_sent, trial_used
        FROM users WHERE paid = TRUE
    """)
    users = cur.fetchall()
    now = datetime.utcnow()

    for (telegram_id, expiry, payment_failed, grace_end, auto_renew, reminder_sent, _) in users:
        time_left = expiry - now

        # Истекший доступ
        if time_left.total_seconds() < 0:
            if payment_failed and grace_end and now < grace_end:
                continue  # льготный период ещё действует
            else:
                await ban_user_logic(telegram_id, cur)

        # Напоминание за 48 часов
        elif timedelta(0) < time_left < timedelta(days=2):
            if not reminder_sent:
                text = "⏳ Ваша подписка заканчивается через 48 часов. Продлите доступ, чтобы не потерять связь с клубом."
                await bot.send_message(telegram_id, text, reply_markup=get_tariffs_keyboard(show_trial=False))
                cur.execute("UPDATE users SET reminder_sent = TRUE WHERE telegram_id = %s", (telegram_id,))

    conn.commit()
    cur.close()
    conn.close()

# --- БЭКАП БАЗЫ ДАННЫХ ---
async def send_db_backup():
    filename = f"backup_{datetime.now().strftime('%Y-%m-%d_%H-%M')}.sql"
    db_url = os.getenv("DATABASE_URL")
    try:
        # Добавляем --no-version-check для совместимости версий
        dump_cmd = f"pg_dump '{db_url}' --no-owner --no-privileges --no-version-check > {filename}"
        process = await asyncio.create_subprocess_shell(dump_cmd, shell=True)
        await process.communicate()
        if process.returncode != 0:
            raise Exception("pg_dump failed")

        for admin_id in ADMIN_IDS:
            with open(filename, 'rb') as f:
                await bot.send_document(admin_id, f, caption=f"📦 Бэкап БД от {datetime.now().strftime('%d.%m.%Y %H:%M')}")
        os.remove(filename)
    except Exception as e:
        await notify_admins(f"Ошибка бэкапа: {e}")
        logging.error(f"Ошибка бэкапа: {e}")

# --- ХЕНДЛЕРЫ КОМАНД И КОЛБЭКОВ ---
@dp.message_handler(commands=['start'], state='*')
async def start(message: types.Message, state: FSMContext):
    await state.finish()
    await RegistrationStates.intro.set()
    text = """Привет! 👋
<b>Добро пожаловать в закрытый клуб Натальи Ребковец.</b>

Это место, где тренировки перестают быть борьбой с собой и становятся ресурсом. Мы не выжимаем мышцы до отказа — мы учим тело двигаться естественно, без боли и зажимов.

Здесь нет «быстрых результатов любой ценой». Зато есть система, которая возвращает лёгкость, энергию и радость от движения.

<b>Готовы начать путь к здоровому и сильному телу? Тогда — поехали!</b>"""
    kb = InlineKeyboardMarkup().add(InlineKeyboardButton("➡️ Продолжить", callback_data="to_desc"))
    await bot.send_photo(message.chat.id, PHOTO_URL_INTRO, caption=text, reply_markup=kb, parse_mode="HTML")

@dp.callback_query_handler(text="to_desc", state=RegistrationStates.intro)
async def show_description(callback: types.CallbackQuery, state: FSMContext):
    await RegistrationStates.description.set()
    text = """<b>Внутри клуба вас ждёт:</b>
    
🧠 <b>Библиотека тренировок</b> — 50+ видео (и постоянно пополняется). От осанки и стоп до силы и гибкости.

☀️ <b>Короткие зарядки</b> — 10–15 минут, чтобы проснуться или снять напряжение.

📚 <b>Мини-уроки</b> — как дышать, как ходить, как поднимать сумки без вреда для спины.

🧘 <b>Медитации</b> — для нервной системы, чтобы убрать стресс и вернуть спокойствие.

🎥 <b>Живые эфиры</b> 2–4 раза в месяц — разбираем технику, отвечаем на вопросы.

🩹 <b> Фитнес-аптечка</b> — готовые решения: болит поясница, затекла шея, отеки.

💬 <b>Поддержка 24/7</b> — закрытый чат, где я лично отвечаю."""
    kb = InlineKeyboardMarkup().add(InlineKeyboardButton("➡️ Продолжить", callback_data="to_rules"))
    await bot.send_message(callback.message.chat.id, text, reply_markup=kb, parse_mode="HTML")
    await callback.answer()

@dp.callback_query_handler(text="to_rules", state=RegistrationStates.description)
async def show_rules(callback: types.CallbackQuery, state: FSMContext):
    await RegistrationStates.rules.set()
    text = """Часто спрашивают:

🤔 <i>«Я новичок, справлюсь?»</i>
— Да. Все упражнения имеют упрощённые варианты.

🤔 <i>«У меня болит спина / колено / шея»</i>
— Клуб помогает восстанавливаться. Но если острый период — сначала к врачу.

🤔 <i>«Нет времени»</i>
— У нас есть зарядки на 10 минут. И система, которая встраивается в ваш ритм.

🤔 <i>«Я далеко, в другом часовом поясе»</i>
— Всё онлайн. Доступ из любой точки мира.

Клуб подходит и мужчинам, и женщинам, любому возрасту и уровню подготовки.
Главное — желание чувствовать себя лучше."""
    kb = InlineKeyboardMarkup().add(InlineKeyboardButton("➡️ Продолжить", callback_data="to_choice"))
    await bot.send_message(callback.message.chat.id, text, reply_markup=kb, parse_mode="HTML")
    await callback.answer()

@dp.callback_query_handler(text="to_choice", state=RegistrationStates.rules)
async def show_choice(callback: types.CallbackQuery, state: FSMContext):
    await RegistrationStates.choice.set()
    text = """<b>Выберите свой формат участия</b>

🌟 Пробная неделя — попробуйте формат.
💳 1, 6 или 12 месяцев — выберите ритм, который подходит именно вам.

<i>Нажмите на кнопку ниже, чтобы перейти к оплате.</i>
Буду рада видеть вас в клубе! ❤️"""
    # Определяем, показывать ли пробный период (если пользователь уже paid — не показываем)
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT paid FROM users WHERE telegram_id = %s", (callback.from_user.id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    show_trial = not (row and row[0])
    kb = get_tariffs_keyboard(show_trial=show_trial)
    await bot.send_photo(callback.message.chat.id, PHOTO_URL_RULES, caption=text, reply_markup=kb, parse_mode="HTML")
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('sub_'), state='*')
async def process_payment(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer("⏳ Перенаправляем на оплату...")
    sub_type = callback.data
    user_id = callback.from_user.id

    # Защита от повторного триала
    if sub_type == "sub_trial":
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute("SELECT trial_used FROM users WHERE telegram_id = %s", (user_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if row and row[0] is True:
            await callback.answer("⚠️ Вы уже использовали пробную неделю.", show_alert=True)
            return

    price_map = {
        "sub_trial": "PRICE_TRIAL",
        "sub_1": "PRICE_1M",
        "sub_6": "PRICE_6M",
        "sub_12": "PRICE_12M"
    }
    days_map = {
        "sub_trial": 7,
        "sub_1": 30,
        "sub_6": 180,
        "sub_12": 365
    }
    price_id = os.getenv(price_map[sub_type])
    days = days_map[sub_type]

    if not price_id:
        await callback.answer("Ошибка конфигурации тарифа.", show_alert=True)
        return

    mode = 'payment' if sub_type == "sub_trial" else 'subscription'

    try:
        session = stripe.checkout.Session.create(
            payment_method_types=['card'],
            line_items=[{'price': price_id, 'quantity': 1}],
            mode=mode,
            success_url='https://t.me/Natalia_SoulFit_bot',
            cancel_url='https://t.me/Natalia_SoulFit_bot',
            client_reference_id=str(user_id),
            metadata={'days': str(days)}
        )
        kb = InlineKeyboardMarkup(row_width=1).add(
            InlineKeyboardButton("💳 Перейти к оплате", url=session.url),
            InlineKeyboardButton("🔙 Назад к тарифам", callback_data="back_to_tariffs")
        )
        # --- Универсальное редактирование ---
        if callback.message.photo:
            await callback.message.edit_caption(
                caption="✅ Вы выбрали тариф. Нажмите кнопку для оплаты:",
                reply_markup=kb
            )
        else:
            await callback.message.edit_text(
                text="✅ Вы выбрали тариф. Нажмите кнопку для оплаты:",
                reply_markup=kb
            )
        await state.finish()
    except Exception as e:
        logging.error(f"Stripe ошибка: {e}")
        await callback.answer(
            "Техническая ошибка. Попробуйте позже или напишите @re_tasha",
            show_alert=True
        )
@dp.callback_query_handler(text="back_to_tariffs", state='*')
async def back_to_tariffs(callback: types.CallbackQuery, state: FSMContext):
    await RegistrationStates.choice.set()
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT paid FROM users WHERE telegram_id = %s", (callback.from_user.id,))
    user = cur.fetchone()
    cur.close()
    conn.close()
    show_trial = not (user and user[0])
    kb = get_tariffs_keyboard(show_trial=show_trial)
    text = "Выберите свой формат участия:"
    try:
        await callback.message.edit_caption(caption=text, reply_markup=kb)
    except Exception:
        await callback.message.edit_text(text=text, reply_markup=kb)
    await callback.answer()

@dp.callback_query_handler(text="cancel_subscription", state='*')
async def cancel_subscription(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT stripe_subscription_id FROM users WHERE telegram_id = %s", (user_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row or not row[0]:
        await callback.answer("Активная подписка не найдена.", show_alert=True)
        return

    sub_id = row[0]
    try:
        stripe.Subscription.modify(sub_id, cancel_at_period_end=True)
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute("UPDATE users SET auto_renew = FALSE WHERE telegram_id = %s", (user_id,))
        conn.commit()
        cur.close()
        conn.close()
        await callback.message.edit_text("✅ Автопродление отключено. Ваш доступ сохранится до конца оплаченного периода.")
    except Exception as e:
        logging.error(f"Ошибка отмены подписки {sub_id}: {e}")
        await callback.answer("Ошибка при отмене. Напишите администратору.", show_alert=True)

@dp.message_handler(commands=['profile'], state='*')
async def profile(message: types.Message):
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT paid, expiry_date, stripe_subscription_id FROM users WHERE telegram_id = %s", (message.from_user.id,))
    user = cur.fetchone()
    cur.close()
    conn.close()

    if not user or not user[0]:
        await message.answer("У вас пока нет активной подписки. Нажмите /start, чтобы оформить её.")
    else:
        date_text = user[1].strftime("%d.%m.%Y") if user[1] else "не установлена"
        text = f"✅ Ваша подписка активна.\n📅 Действует до: {date_text}\n\nХотите продлить доступ?"
        kb = InlineKeyboardMarkup(row_width=1)
        kb.add(InlineKeyboardButton("💳 Продлить доступ", callback_data="show_renew_options"))
        if user[2]:
            kb.add(InlineKeyboardButton("❌ Отменить автопродление", callback_data="cancel_subscription"))
        await message.answer(text, reply_markup=kb)

@dp.callback_query_handler(text="show_renew_options", state='*')
async def show_renew_options(callback: types.CallbackQuery):
    kb = get_tariffs_keyboard(show_trial=False)
    await callback.message.edit_text("Выберите тариф для продления доступа:", reply_markup=kb)
    await callback.answer()

@dp.message_handler(commands=['broadcast'], state='*')
async def broadcast(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    text = message.text.replace('/broadcast ', '')
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT telegram_id FROM users")
    users = cur.fetchall()
    success = 0
    blocked = 0
    for (user_id,) in users:
        try:
            await bot.send_message(user_id, text)
            success += 1
        except BotBlocked:
            blocked += 1
        except Exception:
            pass
    cur.close()
    conn.close()
    await message.answer(f"Рассылка завершена. Успешно: {success}, заблокировали: {blocked}.")

@dp.message_handler(commands=['give_access'], state='*')
async def give_access_command(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    args = message.get_args().split()
    if len(args) < 1:
        await message.reply("⚠️ Использование: /give_access <user_id> [дней]")
        return
    target_user_id = args[0]
    days = int(args[1]) if len(args) > 1 else 30
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO users (telegram_id, paid, expiry_date)
            VALUES (%s, TRUE, NOW() + INTERVAL '%s days')
            ON CONFLICT (telegram_id) DO UPDATE 
            SET paid = TRUE, 
                expiry_date = CASE 
                    WHEN users.expiry_date > NOW() THEN users.expiry_date + INTERVAL '%s days'
                    ELSE NOW() + INTERVAL '%s days'
                END,
                payment_failed = FALSE,
                grace_period_end = NULL;
        """, (int(target_user_id), days, days, days))
        conn.commit()
        link = await generate_invite_link()
        try:
            if link:
                await bot.send_message(target_user_id, f"✅ Администратор предоставил вам доступ на {days} дней!\nСсылка: {link}")
            else:
                await bot.send_message(target_user_id, f"✅ Администратор предоставил вам доступ на {days} дней. Добро пожаловать!")
            await message.answer(f"✅ Доступ пользователю {target_user_id} предоставлен.")
        except BotBlocked:
            await message.answer("⚠️ Доступ обновлён, но пользователь заблокировал бота.")
    except Exception as e:
        conn.rollback()
        await message.answer(f"❌ Ошибка: {e}")
    finally:
        cur.close()
        conn.close()

@dp.message_handler(commands=['help'], state='*')
async def help_command(message: types.Message):
    await message.answer("По всем вопросам @re_tasha")

@dp.message_handler(commands=['test_expiry'])
async def test_expiry(message: types.Message):
    if message.from_user.id in ADMIN_IDS:
        await message.answer("Запускаю проверку подписок...")
        await check_subscriptions_and_reminders()
        await message.answer("Проверка завершена.")
    else:
        await message.answer("Нет прав.")

@dp.message_handler(commands=['test_grace'])
async def test_grace(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    args = message.get_args().split()
    if len(args) != 1:
        await message.reply("Использование: /test_grace <user_id>")
        return
    user_id = args[0]
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE users 
            SET payment_failed = TRUE, 
                grace_period_end = NOW() + INTERVAL '1 day'
            WHERE telegram_id = %s
        """, (int(user_id),))
        conn.commit()
        await message.reply(f"✅ Установлен grace period для {user_id} на 24 часа.")
        # Отправим уведомление пользователю
        await bot.send_message(int(user_id), "⚠️ Тестовое: не удалось списать оплату. У вас есть 24 часа для исправления.")
    except Exception as e:
        await message.reply(f"Ошибка: {e}")
    finally:
        cur.close()
        conn.close()

async def stripe_webhook(request):
    payload = await request.read()
    sig_header = request.headers.get('Stripe-Signature')
    try:
        event = stripe.Webhook.construct_event(
            payload, sig_header, os.getenv("STRIPE_WEBHOOK_SECRET")
        )
    except Exception as e:
        logging.error(f"Ошибка подписи вебхука: {e}")
        return web.Response(status=400)

    event_id = event['id']
    if await is_event_processed(event_id):
        return web.Response(status=200)

    # ---------- 1. ОПЛАТА ЧЕРЕЗ CHECKOUT (ПЕРВИЧНАЯ ИЛИ ПРОДЛЕНИЕ) ----------
    if event['type'] == 'checkout.session.completed':
        session = event['data']['object']
        user_id = getattr(session, 'client_reference_id', None)
        if not user_id:
            await mark_event_processed(event_id)
            return web.Response(status=200)

        sub_id = getattr(session, 'subscription', None)
        days_to_add = 0
        metadata_raw = getattr(session, 'metadata', None)
        if metadata_raw is not None:
            try:
                days_to_add = int(metadata_raw['days'])
            except (KeyError, TypeError, ValueError):
                try:
                    days_val = getattr(metadata_raw, 'days', None)
                    if days_val is not None:
                        days_to_add = int(days_val)
                except:
                    pass
        logging.info(f"WEBHOOK DEBUG: user={user_id}, days={days_to_add}, mode={getattr(session, 'mode', '?')}")
        if days_to_add <= 0:
            logging.error(f"Не удалось получить days для {user_id}")
            await mark_event_processed(event_id)
            return web.Response(status=200)

        is_trial = (days_to_add == 7)
        conn = get_db_conn()
        cur = conn.cursor()
        try:
            cur.execute("SELECT paid, expiry_date, first_payment_done FROM users WHERE telegram_id = %s", (int(user_id),))
            row = cur.fetchone()
            now = datetime.utcnow()

            if row and row[0] and row[1] and row[1] > now:
                new_expiry = row[1] + timedelta(days=days_to_add)
            else:
                new_expiry = now + timedelta(days=days_to_add)

            # Нужна ли ссылка? Да, если нет активной подписки (paid=False или expiry_date < now)
            needs_link = (row is None) or (not row[0]) or (row[1] is not None and row[1] < now)
            cur.execute("""
                INSERT INTO users (telegram_id, paid, expiry_date, stripe_subscription_id, auto_renew, trial_used, payment_failed, grace_period_end, first_payment_done)
                VALUES (%s, TRUE, %s, %s, TRUE, %s, FALSE, NULL, FALSE)
                ON CONFLICT (telegram_id) DO UPDATE SET
                    paid = TRUE,
                    expiry_date = EXCLUDED.expiry_date,
                    stripe_subscription_id = COALESCE(EXCLUDED.stripe_subscription_id, users.stripe_subscription_id),
                    trial_used = CASE WHEN EXCLUDED.trial_used = TRUE THEN TRUE ELSE users.trial_used END,
                    payment_failed = FALSE,
                    grace_period_end = NULL,
                    auto_renew = TRUE,
                    reminder_sent = FALSE,
                    first_payment_done = CASE WHEN %s THEN FALSE ELSE COALESCE(users.first_payment_done, FALSE) END
            """, (int(user_id), new_expiry, sub_id, is_trial, needs_link))
            conn.commit()

            if needs_link:
                link = await generate_invite_link()
                msg = f"✅ Оплата прошла успешно! Доступ до {new_expiry.strftime('%d.%m.%Y')}.\nСсылка для вступления: {link}\n\nДобро пожаловать!"
            else:
                msg = f"✅ Ваша подписка продлена до {new_expiry.strftime('%d.%m.%Y')}. Спасибо! ❤️"
            try:
                await bot.send_message(int(user_id), msg)
            except BotBlocked:
                pass  # не беспокоим админа
            try:
                await bot.unban_chat_member(chat_id=int(GROUP_ID), user_id=int(user_id))
            except Exception as e:
                if "administrator" in str(e).lower():
                    logging.warning(f"Не удалось разбанить админа {user_id}: {e}")
                else:
                    logging.error(f"Ошибка разбана {user_id}: {e}")
        except Exception as e:
            conn.rollback()
            logging.error(f"Ошибка checkout: {e}")
        finally:
            cur.close()
            conn.close()

    # ---------- 2. УСПЕШНОЕ АВТОПРОДЛЕНИЕ (invoice.payment_succeeded) ----------
    elif event['type'] == 'invoice.payment_succeeded':
        invoice = event['data']['object']
        sub_id = getattr(invoice, 'subscription', None)
        if not sub_id:
            await mark_event_processed(event_id)
            return web.Response(status=200)
        try:
            subscription = stripe.Subscription.retrieve(sub_id)
            new_expiry = datetime.fromtimestamp(subscription.current_period_end)
            conn = get_db_conn()
            cur = conn.cursor()
            cur.execute("""
                UPDATE users 
                SET expiry_date = %s, 
                    paid = TRUE, 
                    payment_failed = FALSE, 
                    grace_period_end = NULL,
                    reminder_sent = FALSE
                WHERE stripe_subscription_id = %s
            """, (new_expiry, sub_id))
            conn.commit()
            cur.execute("SELECT telegram_id FROM users WHERE stripe_subscription_id = %s", (sub_id,))
            row = cur.fetchone()
            cur.close()
            conn.close()
            if row:
                try:
                    await bot.send_message(row[0], f"✅ Автопродление успешно! Доступ продлён до {new_expiry.strftime('%d.%m.%Y')}. Хорошего дня!")
                except BotBlocked:
                    pass
        except Exception as e:
            logging.error(f"Ошибка invoice.payment_succeeded: {e}")

    # ---------- 3. ОШИБКА ОПЛАТЫ (invoice.payment_failed) – GRACE PERIOD ----------
    elif event['type'] == 'invoice.payment_failed':
        invoice = event['data']['object']
        sub_id = getattr(invoice, 'subscription', None)
        if sub_id:
            conn = get_db_conn()
            cur = conn.cursor()
            cur.execute("""
                UPDATE users 
                SET payment_failed = TRUE, 
                    grace_period_end = NOW() + INTERVAL '1 day' 
                WHERE stripe_subscription_id = %s
            """, (sub_id,))
            conn.commit()
            cur.execute("SELECT telegram_id FROM users WHERE stripe_subscription_id = %s", (sub_id,))
            row = cur.fetchone()
            cur.close()
            conn.close()
            if row:
                try:
                    await bot.send_message(row[0], 
                        "⚠️ Не удалось списать оплату за подписку. У вас есть 24 часа, чтобы пополнить карту или связаться с администратором.\n"
                        "После устранения проблемы доступ восстановится автоматически.")
                except BotBlocked:
                    pass

    # ---------- 4. ПОЛЬЗОВАТЕЛЬ ОТМЕНИЛ ПОДПИСКУ (customer.subscription.deleted) ----------
    elif event['type'] == 'customer.subscription.deleted':
        sub = event['data']['object']
        sub_id = getattr(sub, 'id', None)
        if sub_id:
            conn = get_db_conn()
            cur = conn.cursor()
            cur.execute("""
                UPDATE users 
                SET paid = FALSE, 
                    stripe_subscription_id = NULL 
                WHERE stripe_subscription_id = %s
            """, (sub_id,))
            conn.commit()
            cur.close()
            conn.close()

    # ---------- 5. СЕССИЯ ОПЛАТЫ ИСТЕКЛА ИЛИ НЕ УДАЛАСЬ ----------
    elif event['type'] in ('checkout.session.expired', 'checkout.session.async_payment_failed'):
        session = event['data']['object']
        user_id = getattr(session, 'client_reference_id', None)
        if user_id:
            try:
                await bot.send_message(int(user_id), 
                    "❌ Оплата не прошла или время сессии истекло. Попробуйте снова.")
            except Exception:
                pass

    await mark_event_processed(event_id)
    return web.Response(status=200)
    
# --- ЗАПУСК И ВЕБХУК TELEGRAM ---
async def on_startup(app):
    init_db()
    await bot.delete_webhook()
    secret = os.getenv("WEBHOOK_SECRET")
    domain = os.getenv("YOUR_DOMAIN")
    if not domain:
        logging.error("YOUR_DOMAIN не задан! Вебхук Telegram не установлен.")
    else:
        webhook_url = f"{domain}/webhook"
        if secret:
            webhook_url += f"?token={secret}"
        await bot.set_webhook(webhook_url)
        logging.info(f"Webhook установлен: {webhook_url}")
    scheduler.add_job(check_subscriptions_and_reminders, 'cron', hour=10, minute=0)
    scheduler.add_job(send_db_backup, 'cron', day_of_week='mon', hour=3, minute=0)
    scheduler.start()

async def on_shutdown(app):
    await bot.close()
    logging.info("Бот остановлен.")

if __name__ == "__main__":
    from aiogram.dispatcher.webhook import get_new_configured_app
    app = get_new_configured_app(dispatcher=dp, path='/webhook')
    app.router.add_post('/stripe-payment', stripe_webhook)
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    port = int(os.environ.get("PORT", 8080))
    web.run_app(app, host='0.0.0.0', port=port)
