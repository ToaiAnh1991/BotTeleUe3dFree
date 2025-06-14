import os
import logging
import gspread
import pandas as pd
from fastapi import FastAPI, Request
from oauth2client.service_account import ServiceAccountCredentials
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# ENV
BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHANNEL_ID = int(os.environ.get("CHANNEL_ID", "-1000000000000"))
ADMIN_IDS = [id.strip() for id in os.environ.get("ADMIN_IDS", "").split(",") if id.strip().isdigit()]

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KEY_MAP = {}  # Global Key Map

# Load Google Sheet Function
def load_key_map_from_sheet():
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        json_key = os.environ.get("GOOGLE_SHEET_JSON")
        with open("temp_key.json", "w", encoding="utf-8") as f:
            f.write(json_key)

        credentials = ServiceAccountCredentials.from_json_keyfile_name("temp_key.json", scope)
        gc = gspread.authorize(credentials)

        sheet_name = os.environ.get("SHEET_NAME", "KeyData")
        sheet_file = gc.open(sheet_name)
        tabs = os.environ.get("SHEET_TABS", "1").split(",")

        combined_df = pd.DataFrame()
        for tab in tabs:
            worksheet = sheet_file.worksheet(tab.strip())
            df = pd.DataFrame(worksheet.get_all_records())
            df["key"] = df["key"].astype(str).str.strip().str.lower()
            combined_df = pd.concat([combined_df, df], ignore_index=True)

        key_map = {
            key: group[["name_file", "message_id"]].to_dict("records")
            for key, group in combined_df.groupby("key")
        }

        logger.info("✅ Google Sheet loaded successfully")
        return key_map

    except Exception as e:
        logger.error(f"❌ Google Sheet loaded Failed: {e}")
        return {}

# FastAPI App
app = FastAPI()

@app.on_event("startup")
async def startup():
    global bot_app, KEY_MAP

    # Load Google Sheet ngay khi bot khởi động
    KEY_MAP = load_key_map_from_sheet()

    # Khởi tạo Telegram Bot
    bot_app = Application.builder().token(BOT_TOKEN).build()

    bot_app.add_handler(CommandHandler("start", start))
    bot_app.add_handler(CommandHandler("reload", reload_sheet))
    bot_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_key))

    await bot_app.initialize()
    logger.info("✅ Bot initialized and sheet loaded.")


@app.post("/webhook/{token}")
async def telegram_webhook(token: str, request: Request):
    if token != BOT_TOKEN:
        return {"error": "Invalid token"}
    try:
        body = await request.json()
        update = Update.de_json(body, bot_app.bot)
        await bot_app.process_update(update)
    except Exception as e:
        logger.error(f"Webhook error: {e}")
    return {"ok": True}

# Bot Handlers

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "♥️ Hi. Please send your key UExxxxx to the Ue3dFreeBOT to receive the file.\n♥️ Contact Admin if file error: t.me/A911Studio"
    )

async def reload_sheet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("❌ You are not authorized to reload the sheet.")
        return

    global KEY_MAP
    KEY_MAP = load_key_map_from_sheet()

    if KEY_MAP:
        await update.message.reply_text("🔄 Google Sheet reloaded successfully.")
    else:
        await update.message.reply_text("❌ Google Sheet reloaded Failed.")

async def handle_key(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_input = update.message.text.strip().lower()
    chat_id = update.effective_chat.id

    if not KEY_MAP:
        await update.message.reply_text("🔄 Bot is not ready. Please wait or contact admin.\n♥️ Admin: t.me/A911Studio")
        return

    if user_input in KEY_MAP:
        files_info = KEY_MAP[user_input]
        errors = 0

        for file_info in files_info:
            try:
                await context.bot.copy_message(
                    chat_id=chat_id,
                    from_chat_id=CHANNEL_ID,
                    message_id=int(file_info["message_id"]),
                    protect_content=True
                )
                await update.message.reply_text(f"♥️ Your File \"{file_info['name_file']}\"")
            except Exception as e:
                logger.error(f"File send error: {e}")
                errors += 1

        if errors:
            await update.message.reply_text(
                "⚠️ Files not found. Please contact admin.\n♥️ Admin: t.me/A911Studio"
            )
    else:
        await update.message.reply_text("❌ KEY is incorrect. Please check again.")
