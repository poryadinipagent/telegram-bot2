import asyncio
import os
import json
from datetime import datetime

import aiosqlite
import feedparser
from aiocron import crontab
from aiogram import Bot, Dispatcher, F, types
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.client.default import DefaultBotProperties
from aiogram.types import (
    Message, CallbackQuery, FSInputFile,
    InlineKeyboardButton, InlineKeyboardMarkup,
    KeyboardButton, ReplyKeyboardMarkup
)
from aiogram.utils.keyboard import InlineKeyboardBuilder

# === CONFIG ===
TOKEN = os.getenv("TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
CHANNEL_USERNAME = "@poryadindom"
PDF_FILE_PATH = "file.pdf"
DB_PATH = "users.db"
NEWS_FEED = "https://yandex.ru/news/rubric/real_estate.rss"

# City districts from official sources
DISTRICTS = {
    "krasnodar": [
        "Центральный", "Прикубанский", "Фестивальный",
        "Северный", "Западный", "Юбилейный"
    ],
    "moscow": [
        "Центральный", "Северный", "Восточный",
        "Западный", "Юго-Западный", "Юго-Восточный"
    ],
    "spb": [
        "Адмиралтейский", "Василеостровский", "Московский",
        "Невский", "Приморский", "Калининский"
    ],
}

# === Bot initialization ===
bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(bot)

# === Database helpers ===
CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    name TEXT, goal TEXT, property TEXT,
    city TEXT, district TEXT, mortgage TEXT,
    handover TEXT, finishing TEXT, phone TEXT,
    created_at TEXT
);
"""
async def get_db():
    conn = await aiosqlite.connect(DB_PATH)
    await conn.execute(CREATE_TABLE)
    await conn.commit()
    return conn

async def upsert_user(uid: int, **fields):
    conn = await get_db()
    await conn.execute(
        "INSERT OR IGNORE INTO users(user_id, created_at) VALUES(?,?)",
        (uid, datetime.utcnow().isoformat())
    )
    if fields:
        cols = ", ".join(f"{k}=?" for k in fields)
        vals = list(fields.values()) + [uid]
        await conn.execute(f"UPDATE users SET {cols} WHERE user_id=?", vals)
    await conn.commit()
    await conn.close()

async def iterate_users():
    conn = await get_db()
    async with conn.execute("SELECT user_id FROM users") as cur:
        async for row in cur:
            yield row[0]
    await conn.close()

async def get_user_count():
    conn = await get_db()
    cur = await conn.execute("SELECT COUNT(*) FROM users")
    (count,) = await cur.fetchone()
    await conn.close()
    return count

async def mass_send(text: str, markup=None):
    async for uid in iterate_users():
        try:
            await bot.send_message(uid, text, reply_markup=markup)
        except:
            continue

# === Start & Subscription ===
@dp.message(CommandStart())
async def cmd_start(message: Message):
    user_id = message.from_user.id
    await upsert_user(user_id, name=message.from_user.full_name)
    member = await bot.get_chat_member(CHANNEL_USERNAME, user_id)
    if member.status not in ("member","creator","administrator"):
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(
                    text="➡️ Подписаться на канал",
                    url=f"https://t.me/{CHANNEL_USERNAME[1:]}"
                )]
            ]
        )
        await message.answer(
            "👋 Добро пожаловать! Чтобы начать, подпишитесь на наш канал.",
            reply_markup=kb
        )
        return
    await ask_goal(message)

async def ask_goal(message: Message):
    kb = InlineKeyboardBuilder()
    kb.button(text="🏡 Для проживания", callback_data="goal_live")
    kb.button(text="💼 Для инвестиций", callback_data="goal_invest")
    await message.answer(
        "Вы рассматриваете недвижимость для жизни или в качестве инвестиции?",
        reply_markup=kb.as_markup()
    )

@dp.callback_query(F.data.startswith("goal_"))
async def handle_goal(cb: CallbackQuery):
    goal = cb.data.split("_",1)[1]
    await upsert_user(cb.from_user.id, goal=goal)
    kb = InlineKeyboardBuilder()
    for txt, code in [("1-комнатная","1"),("2-комнатная","2"),("3-комнатная","3"),("🏠 Дом","house"),("Студия","studio")]:
        kb.button(text=txt, callback_data=f"type_{code}")
    await cb.message.answer("Какой тип объекта Вас интересует?", reply_markup=kb.as_markup())
    await cb.answer()

@dp.callback_query(F.data.startswith("type_"))
async def handle_type(cb: CallbackQuery):
    prop = cb.data.split("_",1)[1]
    await upsert_user(cb.from_user.id, property=prop)
    kb = InlineKeyboardBuilder()
    for city in DISTRICTS:
        label = city.upper() if city=="spb" else city.capitalize()
        kb.button(text=label, callback_data=f"city_{city}")
    await cb.message.answer("Выберите город / регион", reply_markup=kb.as_markup())
    await cb.answer()

@dp.callback_query(F.data.startswith("city_"))
async def handle_city(cb: CallbackQuery):
    city = cb.data.split("_",1)[1]
    await upsert_user(cb.from_user.id, city=city)
    districts = DISTRICTS.get(city, [])
    kb = InlineKeyboardBuilder()
    for d in districts:
        key = d.replace(" ","_").lower()
        kb.button(text=d, callback_data=f"district_{key}")
    await cb.message.answer("Уточните район:", reply_markup=kb.as_markup())
    await cb.answer()

@dp.callback_query(F.data.startswith("district_"))
async def handle_district(cb: CallbackQuery):
    district = cb.data.split("_",1)[1]
    await upsert_user(cb.from_user.id, district=district)
    await ask_mortgage(cb.message)
    await cb.answer()

async def ask_mortgage(message: Message):
    kb = InlineKeyboardBuilder()
    kb.button(text="👨‍👩‍👧 Семейная ипотека – да", callback_data="family_yes")
    kb.button(text="❌ Нет семейной", callback_data="family_no")
    await message.answer(
        "Рассматриваете ли Вы семейную ипотеку? (ребёнок до 7 лет или двое несовершеннолетних)",
        reply_markup=kb.as_markup()
    )

@dp.callback_query(F.data.startswith("family_"))
async def handle_family(cb: CallbackQuery):
    choice = cb.data.split("_",1)[1]
    val = "family" if choice=="yes" else "no_family"
    await upsert_user(cb.from_user.id, mortgage=val)
    if choice=="no":
        kb = InlineKeyboardBuilder()
        kb.button(text="🏖 Побережье КК", callback_data="install_coast")
        kb.button(text="🏙 Краснодар", callback_data="install_krasnodar")
        await cb.message.answer(
            "Тогда доступна рассрочка. Выберите локацию:",
            reply_markup=kb.as_markup()
        )
    else:
        await ask_handover(cb.message)
    await cb.answer()

@dp.callback_query(F.data.startswith("install_"))
async def handle_install(cb: CallbackQuery):
    loc = cb.data.split("_",1)[1]
    await upsert_user(cb.from_user.id, mortgage=f"install_{loc}")
    await ask_handover(cb.message)
    await cb.answer()

async def ask_handover(message: Message):
    kb = InlineKeyboardBuilder()
    kb.button(text="🏢 Только сданные", callback_data="hd_now")
    kb.button(text="⏳ Готов ждать", callback_data="hd_wait")
    await message.answer("Важно ли, чтобы дом уже сдан?", reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("hd_"))
async def handle_handover(cb: CallbackQuery):
    val = cb.data.split("_",1)[1]
    await upsert_user(cb.from_user.id, handover=val)
    kb = InlineKeyboardBuilder()
    kb.button(text="🔨 С ремонтом", callback_data="finish_ready")
    kb.button(text="🛠 Подчистовая", callback_data="finish_grey")
    await cb.message.answer("В каком состоянии предпочитаете жилье?", reply_markup=kb.as_markup())
    await cb.answer()

@dp.callback_query(F.data.startswith("finish_"))
async def handle_finish(cb: CallbackQuery):
    val = cb.data.split("_",1)[1]
    await upsert_user(cb.from_user.id, finishing=val)
    kb = ReplyKeyboardMarkup(        
        keyboard=[[KeyboardButton(text="📱 Оставить номер", request_contact=True)]],        
        resize_keyboard=True, one_time_keyboard=True    
    )
    await cb.message.answer("Пожалуйста, оставьте ваш номер телефона:", reply_markup=kb)    
    await cb.answer()

@dp.message(F.contact)
async def handle_contact(msg: Message):
    await upsert_user(msg.from_user.id, phone=msg.contact.phone_number)
    conn = await get_db()
    cur = await conn.execute("SELECT * FROM users WHERE user_id=?", (msg.from_user.id,))
    row = await cur.fetchone()
    await conn.close()
    parts = [f"Цель: {row[2]}", f"Тип: {row[3]}", f"Город: {row[4]}", f"Район: {row[5]}", f"Ипотека: {row[6]}", f"Сдача: {row[7]}", f"Отделка: {row[8]}", f"Телефон: {row[9]}"]
    await bot.send_message(ADMIN_ID, "📩 Заявка получили!")
" + "
".join((parts))
    if os.path.exists(PDF_FILE_PATH):
        await msg.answer_document(FSInputFile(PDF_FILE_PATH), caption="Выдача самого топового предложения на побережье с ПВ от 600 тысяч рублей")
    await msg.answer("Спасибо! Наш специалист свяжется с Вами. ✨", reply_markup=types.ReplyKeyboardRemove())

@dp.message()
async def smart_replies(message: Message):
    text = message.text.lower()
    if any(k in text for k in ("море","побережье")):
        await message.answer("🏖 Отличные варианты на побережье Краснодарского края! Напишите, какой формат интересует.")
    elif any(k in text for k in ("цена","стоимость")):
        await message.answer("💰 Уточните параметры – и мы подберем лучшие предложения.")
    elif any(k in text for k in ("ипотек","рассроч")):
        await message.answer("🏦 Мы предлагаем семейную ипотеку и рассрочку под ваш бюджет.")
    else:
        await message.answer("🤝 Спасибо за сообщение! Ответим в ближайшее время.")

# Scheduled tasks
@crontab("0 12 */2 * *")
async def scheduled_warmup():
    await mass_send("Здравствуйте! 👋 У нас появились новые варианты. Готовы получить подборку?")

@crontab("0 9 * * 1")
async def weekly_news():
    feed = feedparser.parse(NEWS_FEED)
    items = feed.entries[:3]
    text = "📰 Еженедельный дайджест новостей рынка недвижимости:
"
    for e in items:
        text += f"- <a href="{e.link}">{e.title}</a>
"
    await mass_send(text)

@dp.message(Command("stats"))
async def cmd_stats(message: Message):
    if message.from_user.id!=ADMIN_ID: return
    count = await get_user_count()
    await message.answer(f"Всего пользователей: {count}")

@dp.message(Command("broadcast"))
async def cmd_broadcast(message: Message):
    if message.from_user.id!=ADMIN_ID: return
    text = message.text.partition(" ")[2]
    if not text:
        await message.answer("Использование: /broadcast текст")
        return
    await mass_send(text)
    await message.answer("Рассылка завершена.")

async def main():
    await dp.start_polling()

if __name__=="__main__":
    asyncio.run(main())
