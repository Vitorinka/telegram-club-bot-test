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
class PromoStates(StatesGroup):
    waiting_for_media = State()   # ждём фото или видео
    waiting_for_text = State()    # ждём текст (можно объединить, но так проще)

class ContactState(StatesGroup):
    waiting_for_message = State()
class ReplyState(StatesGroup):
    waiting_for_reply = State()  # с каким user_id сейчас идёт диалог

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

PHOTO_URL_INTRO = "https://i.imgur.com/TnzeGgh.jpg"
PHOTO_URL_RULES = "https://i.imgur.com/TnzeGgh.jpg"
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
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS registered_at TIMESTAMP DEFAULT NOW();")
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS video_sent BOOLEAN DEFAULT FALSE;")
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS video_sent_at TIMESTAMP;")
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS feedback_sent BOOLEAN DEFAULT FALSE;")
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS feedback_sent_at TIMESTAMP;")
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS feedback_received BOOLEAN DEFAULT FALSE;")
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS reminder_sent BOOLEAN DEFAULT FALSE;")
    conn.commit()
    cur.close()
    conn.close()
    logging.info("--- БД ИНИЦИАЛИЗИРОВАНА И ПРОВЕРЕНА ---")

@dp.message_handler(commands=['menu'], state='*')
async def show_menu(message: types.Message):
    kb = get_main_keyboard()
    await message.answer("🌟 <b>Главное меню</b>\n\nВыберите действие:", reply_markup=kb, parse_mode="HTML")

@dp.message_handler(commands=['promo_trial'], state='*')
async def promo_trial(message: types.Message, state: FSMContext):
    await state.finish()   # <--- сбросить любое зависшее состояние
    logging.info(f"Команда promo_trial от {message.from_user.id}")
    if message.from_user.id not in ADMIN_IDS:
        logging.warning(f"Отказано {message.from_user.id}")
        return
    await PromoStates.waiting_for_media.set()
    await message.reply("📎 Отправьте фото или видео, которое будет в рассылке.\n\n"
                        "Чтобы отменить, отправьте /cancel")

@dp.message_handler(commands=['cancel'], state='*')
async def cancel_handler(message: types.Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state is None:
        await message.reply("Нет активного действия для отмены.")
        return
    
    # Сбрасываем любое состояние
    await state.finish()
    await message.reply("✅ Действие отменено. Можете начать заново.")

from aiogram.types import ReplyKeyboardRemove

@dp.message_handler(commands=['hide'])
async def hide_keyboard(message: types.Message):
    await message.answer("Клавиатура скрыта.", reply_markup=ReplyKeyboardRemove())
    
@dp.message_handler(content_types=['photo', 'video'], state=PromoStates.waiting_for_media)
async def promo_get_media(message: types.Message, state: FSMContext):
    if message.photo:
        file_id = message.photo[-1].file_id
        media_type = 'photo'
    else:
        file_id = message.video.file_id
        media_type = 'video'
    await state.update_data(media_type=media_type, file_id=file_id)
    await PromoStates.waiting_for_text.set()
    await message.reply("✏️ Теперь отправьте текст сообщения.\n\n"
                        "Можно использовать HTML-разметку (<b>жирный</b>, <i>курсив</i>).")

@dp.message_handler(state=PromoStates.waiting_for_text, content_types=types.ContentTypes.TEXT)
async def promo_get_text(message: types.Message, state: FSMContext):
    text = message.html_text
    data = await state.get_data()
    media_type = data['media_type']
    file_id = data['file_id']

    # Подтверждение перед отправкой
    kb = InlineKeyboardMarkup(row_width=2).add(
        InlineKeyboardButton("✅ Да, отправить", callback_data="confirm_promo"),
        InlineKeyboardButton("❌ Отмена", callback_data="cancel_promo")
    )
    await state.update_data(text=text)
    # Отправим админу предпросмотр
    if media_type == 'photo':
        await message.reply_photo(file_id, caption=text + "\n\n---\n<i>Предпросмотр. Отправляем?</i>", reply_markup=kb, parse_mode="HTML")
    else:
        await message.reply_video(file_id, caption=text + "\n\n---\n<i>Предпросмотр. Отправляем?</i>", reply_markup=kb, parse_mode="HTML")

from aiogram.types import ReplyKeyboardMarkup, KeyboardButton

def get_main_keyboard():
    kb = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    kb.add(
        KeyboardButton("🎁 Бесплатный урок")
    )
    kb.add(
        KeyboardButton("💬 Задать вопрос"),
        KeyboardButton("🆘 Правила клуба")
    )
    kb.add(
        KeyboardButton("👤 Профиль и подписка"),
    )
    return kb
    
@dp.message_handler(text="🎁 Бесплатный урок", state='*')
async def free_lesson_button(message: types.Message, state: FSMContext):
    await state.finish()
    user_id = message.from_user.id

    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT video_sent FROM users WHERE telegram_id = %s", (user_id,))
    row = cur.fetchone()
    if row and row[0]:
    # Вместо просто "❌ Вы уже получали бесплатный урок."
    kb = get_tariffs_keyboard(show_trial=True)  # показываем пробную неделю и платные тарифы
    await message.answer(
        "✅ Вы уже получали бесплатный урок.\n\n"
        "Теперь вы можете оформить подписку и получить полный доступ к клубу:",
        reply_markup=kb,
        parse_mode="HTML"
    )
    cur.close()
    conn.close()
    return
    
    VIDEO_FREE_LESSON = "BAACAgIAAxkBAAPSahQr16KLxtDqFbqXnIH_zdI0IeMAAsmiAAJ326lIpX7yBQ88ReY7BA"
    
    # Длинный текст + кнопка
    caption_text = """
<b>Чтобы почувствовать изменения в теле и самочувствии, не нужно усложнять.</b>

Для того чтобы уменьшить напряжение, скованность и дискомфорт в теле, не нужен зал, сложное оборудование и час свободного времени. Иногда достаточно коврика и 15 минут правильного движения.

Именно поэтому я подготовила эту пробную тренировку на осанку – приятную, понятную и эффективную.

<b>Она подойдёт, если вы:</b>
- только начинаете тренироваться
- устали от жёстких нагрузок
- хотите чувствовать тело лучше без перегрузки

<b>После тренировки вы почувствуете:</b>
- больше лёгкости и подвижности
- меньше напряжения в теле
- ощущение, что тело наконец стало хорошо

А если вам понравился такой подход, я подготовила <i>специальное предложение на пробную неделю в онлайн-клубе.</i>

<b>Там вас ждут полноценные тренировки, короткие зарядки, медитации, рецепты и системная работа с телом.</b>
"""
    kb = InlineKeyboardMarkup().add(InlineKeyboardButton("🌟 Начать пробную неделю", callback_data="sub_trial"))
    
    await bot.send_video(message.chat.id, VIDEO_FREE_LESSON, caption=caption_text, reply_markup=kb, parse_mode="HTML")

    cur.execute("""
        UPDATE users 
        SET video_sent = TRUE, video_sent_at = NOW() 
        WHERE telegram_id = %s
    """, (user_id,))
    conn.commit()
    cur.close()
    conn.close()

    # Не отправляем дополнительное сообщение, так как текст уже в caption
    # await message.answer("Приятного просмотра!")

async def check_followup():
    logging.info("--- Запуск проверки автоматических уроков и отзывов ---")
    conn = get_db_conn()
    cur = conn.cursor()
    now = datetime.utcnow()

    VIDEO_ID = "BAACAgIAAxkBAAPSahQr16KLxtDqFbqXnIH_zdI0IeMAAsmiAAJ326lIpX7yBQ88ReY7BA"

        # ---------- 1. Отправка видео-урока (всем, кто зарегистрировался 2 дня назад) ----------
    cur.execute("""
        SELECT telegram_id, paid, trial_used FROM users 
        WHERE registered_at IS NOT NULL 
        AND video_sent = FALSE 
        AND registered_at <= NOW() - INTERVAL '2 days'
    """)
    users_for_video = cur.fetchall()
    for (user_id, paid, trial_used) in users_for_video:
        # Длинный текст + кнопка
        caption_text = """
<b>Чтобы почувствовать изменения в теле и самочувствии, не нужно усложнять.</b>

Для того чтобы уменьшить напряжение, скованность и дискомфорт в теле, не нужен зал, сложное оборудование и час свободного времени. Иногда достаточно коврика и 15 минут правильного движения.

Именно поэтому я подготовила эту пробную тренировку на осанку – приятную, понятную и эффективную.

<b>Она подойдёт, если вы:</b>
- только начинаете тренироваться
- устали от жёстких нагрузок
- хотите чувствовать тело лучше без перегрузки

<b>После тренировки вы почувствуете:</b>
- больше лёгкости и подвижности
- меньше напряжения в теле
- ощущение, что тело наконец стало хорошо

А если вам понравился такой подход, я подготовила <i>специальное предложение на пробную неделю в онлайн-клубе.</i>

<b>Там вас ждут полноценные тренировки, короткие зарядки, медитации, рецепты и системная работа с телом.</b>
"""
        kb = InlineKeyboardMarkup().add(InlineKeyboardButton("🌟 Начать пробную неделю", callback_data="sub_trial"))
        
        await bot.send_video(user_id, VIDEO_ID, caption=caption_text, reply_markup=kb, parse_mode="HTML")
        cur.execute("UPDATE users SET video_sent = TRUE, video_sent_at = NOW() WHERE telegram_id = %s", (user_id,))
        conn.commit()

    # ---------- 2. Для НЕактивных (нет подписки и не использовал триал) ----------
    cur.execute("""
        SELECT telegram_id FROM users 
        WHERE video_sent = TRUE 
        AND feedback_sent = FALSE 
        AND video_sent_at <= NOW() - INTERVAL '2 days'
        AND paid = FALSE 
        AND trial_used = FALSE
    """)
    users_for_feedback = cur.fetchall()
    for (user_id,) in users_for_feedback:
        await bot.send_message(user_id,
            "💬 <b>Как вам тренировка?</b>\n\n"
            "Поделитесь впечатлениями или задайте вопрос. "
            "Я передам ваш отзыв тренеру, и он ответит вам в этом чате.\n\n"
            "А если понравилось – у нас действует <b>пробная неделя за 15€</b>.",
            reply_markup=InlineKeyboardMarkup().add(
                InlineKeyboardButton("🌟 Начать пробную неделю", callback_data="sub_trial")
            ),
            parse_mode="HTML"
        )
        cur.execute("UPDATE users SET feedback_sent = TRUE, feedback_sent_at = NOW() WHERE telegram_id = %s", (user_id,))
        conn.commit()

    # ---------- 3. Повторное предложение для неактивных, кто не ответил через 2 дня ----------
    cur.execute("""
        SELECT telegram_id FROM users 
        WHERE feedback_sent = TRUE 
        AND reminder_sent = FALSE 
        AND feedback_received = FALSE 
        AND paid = FALSE 
        AND trial_used = FALSE
        AND feedback_sent_at <= NOW() - INTERVAL '2 days'
    """)
    users_for_reminder = cur.fetchall()
    for (user_id,) in users_for_reminder:
        await bot.send_message(user_id,
            "🌟 <b>Попробуйте пробную неделю за 15€</b>\n\n"
            "Если вы ещё не решили – у вас есть возможность оценить клуб в течение 7 дней.",
            reply_markup=InlineKeyboardMarkup().add(
                InlineKeyboardButton("Начать пробную неделю", callback_data="sub_trial")
            ),
            parse_mode="HTML"
        )
        cur.execute("UPDATE users SET reminder_sent = TRUE WHERE telegram_id = %s", (user_id,))
        conn.commit()

    # ---------- 4. Для тех, кто использовал триал, но не оплатил (trial_used = TRUE, paid = FALSE) ----------
    cur.execute("""
        SELECT telegram_id FROM users 
        WHERE video_sent = TRUE 
        AND trial_used = TRUE 
        AND paid = FALSE 
        AND reminder_sent = FALSE   # используем reminder_sent как флаг, что предложение уже отправлено
    """)
    users_for_trial_used = cur.fetchall()
    for (user_id,) in users_for_trial_used:
        kb = get_tariffs_keyboard(show_trial=False)  # тарифы без пробного
        await bot.send_message(user_id,
            "📢 <b>Ещё больше тренировок ждут вас в клубе!</b>\n\n"
            "Вы уже пробовали бесплатный урок. Оформите подписку, чтобы получить полный доступ к библиотеке, "
            "живым эфирам и поддержке.\n\n"
            "Выберите удобный тариф:",
            reply_markup=kb,
            parse_mode="HTML"
        )
        cur.execute("UPDATE users SET reminder_sent = TRUE WHERE telegram_id = %s", (user_id,))
        conn.commit()

    # ---------- 5. Для активных подписчиков (paid = TRUE) – только видео, без опросов и предложений ----------
    # (видео уже отправилось на шаге 1, ничего дополнительно не делаем)

    cur.close()
    conn.close()

@dp.message_handler(text="💬 Задать вопрос", state='*')
async def contact_admin(message: types.Message, state: FSMContext):
    await state.finish()
    kb = ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
    kb.add(KeyboardButton("❌ Отмена"))
    await message.answer(
        "📝 Напишите ваше сообщение администратору ниже.\n\n"
        "Или нажмите «Отмена», чтобы выйти.",
        reply_markup=kb
    )
    await ContactState.waiting_for_message.set()
    await bot.send_message(user_id, "🌟 Главное меню", reply_markup=get_main_keyboard())

# ---------- Пересылка сообщений от пользователей админу (с кнопкой "Ответить") ----------
@dp.message_handler(state='*')
async def forward_user_message(message: types.Message):
    menu_buttons = ["🎁 Бесплатный урок", "💬 Задать вопрос", "🆘 Правила клуба", "👤 Профиль и подписка"]
    
    if message.text and message.text.startswith('/'):
        return
    if message.from_user.id in ADMIN_IDS:
        return
    if message.text in menu_buttons:
        return
    
    current_state = await dp.current_state(chat=message.chat.id, user=message.from_user.id).get_state()
    if current_state == ContactState.waiting_for_message.state:
        return
    
    for admin_id in ADMIN_IDS:
        await bot.forward_message(admin_id, message.chat.id, message.message_id)
        kb = InlineKeyboardMarkup().add(
            InlineKeyboardButton("✍️ Ответить", callback_data=f"reply_to_{message.from_user.id}")
        )
        await bot.send_message(
            admin_id,
            f"👤 Пользователь: @{message.from_user.username or message.from_user.id} (ID: {message.from_user.id})",
            reply_markup=kb
        )

# ---------- Нажатие на кнопку "Ответить" -> активация режима ответа ----------
@dp.callback_query_handler(lambda c: c.data.startswith('reply_to_'), state='*')
async def start_reply_mode(callback: types.CallbackQuery, state: FSMContext):
    user_id = int(callback.data.split('_')[2])
    await state.update_data(reply_to_user=user_id)
    await ReplyState.waiting_for_reply.set()
    
    end_kb = InlineKeyboardMarkup().add(
        InlineKeyboardButton("🔚 Завершить ответ", callback_data="end_reply")
    )
    await callback.message.edit_text(
        f"✉️ Вы отвечаете пользователю {user_id}\n"
        f"Все ваши следующие сообщения будут отправлены ему.\n"
        f"Для завершения нажмите кнопку.",
        reply_markup=end_kb
    )
    await callback.answer()

# ---------- Админ пишет сообщение в режиме ответа -> отправка пользователю ----------
@dp.message_handler(state=ReplyState.waiting_for_reply, content_types=types.ContentTypes.ANY)
async def admin_reply(message: types.Message, state: FSMContext):
    data = await state.get_data()
    user_id = data.get('reply_to_user')
    if not user_id:
        await state.finish()
        return
    
    await bot.send_message(user_id, message.html_text, parse_mode="HTML")
    await message.reply(f"✅ Сообщение отправлено пользователю {user_id}")

# ---------- Кнопка "Завершить ответ" ----------
@dp.callback_query_handler(text="end_reply", state=ReplyState.waiting_for_reply)
async def end_reply_mode(callback: types.CallbackQuery, state: FSMContext):
    await state.finish()
    await callback.message.edit_text("✅ Режим ответа завершён.")
    await callback.answer()

# ---------- Обработка сообщений через кнопку "Задать вопрос" (уже есть выше, но остаётся) ----------
# (остальной код: кнопки меню, profile, rules, payment и т.д. – без изменений)

@dp.message_handler(state=ContactState.waiting_for_message, content_types=types.ContentTypes.ANY)
async def forward_to_admin(message: types.Message, state: FSMContext):
    # Если пользователь нажал «Отмена»
    if message.text == "❌ Отмена":
        await state.finish()
        await message.answer("🚫 Отправка отменена.", reply_markup=ReplyKeyboardRemove())
        await message.answer("🌟 Главное меню", reply_markup=get_main_keyboard())
        return

    # Пересылаем сообщение админам
    for admin_id in ADMIN_IDS:
        await bot.forward_message(admin_id, message.chat.id, message.message_id)
        await bot.send_message(
            admin_id,
            f"📬 Пользователь @{message.from_user.username or message.from_user.id} написал:\n"
            f"Ответьте, используя кнопку «Ответить» на его следующее сообщение (или через режим ответа)."
        )
    # После отправки – удаляем клавиатуру и показываем главное меню
    await message.answer("✅ Ваше сообщение отправлено администратору.", reply_markup=ReplyKeyboardRemove())
    await message.answer("🌟 Главное меню", reply_markup=get_main_keyboard())
    await state.finish()

# Кнопка "👤 Профиль и подписка" – вызывает команду /profile
@dp.message_handler(text="👤 Профиль и подписка", state='*')
async def profile_button_handler(message: types.Message, state: FSMContext):
    await state.finish()
    await profile(message)

# Кнопка "🆘 Правила клуба" – показывает правила (как в show_rules)
@dp.message_handler(text="🆘 Правила клуба", state='*')
async def rules_button_handler(message: types.Message, state: FSMContext):
    await state.finish()
    rules_text = """📜 <b>Правила клуба</b>

1. Уважайте других участников.
2. Без спама и рекламы.
3. Тренируйтесь в удовольствие!

"""
    await message.answer(rules_text, parse_mode="HTML")
    
@dp.message_handler(content_types=['video'], state=None)
async def reply_with_video_id(message: types.Message):
    file_id = message.video.file_id
    await message.reply(f"Ваш video file_id:\n{file_id}")

@dp.callback_query_handler(text="confirm_promo", state=PromoStates.waiting_for_text)
async def promo_send(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    text = data['text']
    media_type = data['media_type']
    file_id = data['file_id']

    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT telegram_id FROM users WHERE paid = FALSE")
    users = cur.fetchall()
    cur.close()
    conn.close()

    kb = InlineKeyboardMarkup().add(InlineKeyboardButton("Начать пробную неделю", callback_data="sub_trial"))

    success = 0
    blocked = 0
    for (user_id,) in users:
        try:
            if media_type == 'photo':
                await bot.send_photo(user_id, file_id, caption=text, reply_markup=kb, parse_mode="HTML")
            else:
                await bot.send_video(user_id, file_id, caption=text, reply_markup=kb, parse_mode="HTML")
            success += 1
        except BotBlocked:
            blocked += 1
        except Exception as e:
            logging.error(f"Ошибка отправки {user_id}: {e}")

    # Вместо редактирования исходного сообщения (которое может быть медиа) — шлём новое сообщение админу с результатом
    await callback.message.answer(f"✅ Рассылка завершена.\n📨 Успешно: {success}\n🚫 Заблокировали: {blocked}")
    await state.finish()
    await callback.answer()

@dp.callback_query_handler(text="cancel_promo", state=PromoStates.waiting_for_text)
async def promo_cancel(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text("❌ Рассылка отменена.")
    await state.finish()
    await callback.answer()

@dp.message_handler(content_types=['photo'])
async def reply_with_file_id(message: types.Message):
    file_id = message.photo[-1].file_id
    await message.reply(f"Ваш file_id:\n`{file_id}`", parse_mode="Markdown")

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
    if not db_url:
        await notify_admins("❌ Ошибка бэкапа: DATABASE_URL не задан!")
        return

    # Добавляем sslmode=require для Railway
    conn_string = db_url + "?sslmode=require"

    try:
        process = await asyncio.create_subprocess_exec(
            'pg_dump', conn_string,
            '--no-owner', '--no-privileges',
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()

        if process.returncode != 0:
            error_msg = stderr.decode('utf-8')
            logging.error(f"pg_dump failed (code {process.returncode}): {error_msg}")
            await notify_admins(f"❌ Ошибка дампа БД. Код: {process.returncode}. Подробности в логах.")
            return

        # Записываем дамп в файл
        with open(filename, 'wb') as f:
            f.write(stdout)

        logging.info(f"Бэкап создан: {filename} (размер: {len(stdout)} байт)")

        # Отправляем файл каждому админу
        for admin_id in ADMIN_IDS:
            try:
                with open(filename, 'rb') as f:
                    await bot.send_document(admin_id, f, caption=f"📦 Бэкап БД от {datetime.now().strftime('%d.%m.%Y %H:%M')}")
            except Exception as e:
                logging.error(f"Не удалось отправить бэкап админу {admin_id}: {e}")

    except Exception as e:
        logging.exception(f"Критическая ошибка бэкапа: {e}")
        await notify_admins(f"❌ Непредвиденная ошибка бэкапа: {e}")
    finally:
        if os.path.exists(filename):
            os.remove(filename)

# --- ХЕНДЛЕРЫ КОМАНД И КОЛБЭКОВ ---
@dp.message_handler(commands=['start'], state='*')
async def start(message: types.Message, state: FSMContext):
    await state.finish()
    user_id = message.from_user.id

    # Добавляем пользователя в БД (если его ещё нет)
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO users (telegram_id, paid)
            VALUES (%s, FALSE)
            ON CONFLICT (telegram_id) DO NOTHING
        """, (user_id,))
        conn.commit()
    except Exception as e:
        logging.error(f"Ошибка добавления {user_id}: {e}")
    finally:
        cur.close()
        conn.close()

    # Отправка приветствия
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
    cur.execute("SELECT paid, trial_used FROM users WHERE telegram_id = %s", (callback.from_user.id,))
    row = cur.fetchone()
    show_trial = not (row and (row[0] or row[1])) if row else True
    cur.close()
    conn.close()
    kb = get_tariffs_keyboard(show_trial=show_trial)
    cur.execute("UPDATE users SET registered_at = COALESCE(registered_at, NOW()) WHERE telegram_id = %s", (callback.from_user.id,))
    await bot.send_photo(callback.message.chat.id, PHOTO_URL_RULES, caption=text, reply_markup=kb, parse_mode="HTML")
    await callback.answer()
    await show_menu(callback.message)

@dp.callback_query_handler(lambda c: c.data.startswith('sub_'), state='*')
async def process_payment(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer("⏳ Проверяем...")
    sub_type = callback.data
    user_id = callback.from_user.id

    # Получаем данные пользователя из БД
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT trial_used, paid FROM users WHERE telegram_id = %s", (user_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()

    trial_used = row[0] if row else False
    paid = row[1] if row else False

    # Если нажата кнопка пробной недели
    if sub_type == "sub_trial":
        # Если пробный период уже использован ИЛИ у пользователя есть активная подписка
        if trial_used or paid:
            # Показываем клавиатуру с обычными тарифами (без пробного)
            await state.finish()
            kb = get_tariffs_keyboard(show_trial=False)
            text = "Вы уже использовали пробную неделю (или у вас активна подписка). Выберите платный тариф:"
            # Если сообщение имеет caption/текст, отредактируем, иначе отправим новое
            try:
                if callback.message.caption is not None:
                    await callback.message.edit_caption(caption=text, reply_markup=kb, parse_mode="HTML")
                elif callback.message.text:
                    await callback.message.edit_text(text=text, reply_markup=kb, parse_mode="HTML")
                else:
                    await callback.message.reply(text, reply_markup=kb, parse_mode="HTML")
            except Exception:
                await callback.message.reply(text, reply_markup=kb, parse_mode="HTML")
            return  # не создаём Stripe сессию

        # Иначе (пробный период не использован) – продолжаем создание оплаты пробной недели
        # (весь код ниже для sub_trial, но он такой же, как для остальных тарифов, поэтому вынесем общую логику)

    # Обработка всех тарифов (включая sub_trial, если прошли проверку)
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
        new_kb = InlineKeyboardMarkup(row_width=1).add(
            InlineKeyboardButton("💳 Перейти к оплате", url=session.url),
            InlineKeyboardButton("🔙 Назад к тарифам", callback_data="back_to_tariffs")
        )
        # Меняем клавиатуру исходного сообщения (безопасно)
        await callback.message.edit_reply_markup(reply_markup=new_kb)
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
    # Исправлено: получаем и paid, и trial_used
    cur.execute("SELECT paid, trial_used FROM users WHERE telegram_id = %s", (callback.from_user.id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    # Показываем триал только если нет ни paid, ни trial_used
    show_trial = not (row and (row[0] or row[1])) if row else True
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

@dp.message_handler(commands=['test_backup'])
async def test_backup(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("Нет прав.")
        return
    await message.answer("🔄 Запускаю бэкап...")
    await send_db_backup()
    await message.answer("✅ Бэкап завершён. Проверьте личные сообщения от бота (файл должен прийти админам).")
        
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
    scheduler.add_job(check_followup, 'cron', hour=12, minute=0)  # <--- ДОБАВЬТЕ СЮДА
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
