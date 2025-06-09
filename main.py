import os
import logging
import gspread
from fastapi import FastAPI, Request
from oauth2client.service_account import ServiceAccountCredentials
from telegram import Update, Bot
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

import pandas as pd

BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHANNEL_ID = int(os.environ.get("CHANNEL_ID", "-100..."))

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load Google Sheet
def load_key_map_from_sheet():
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        json_key = os.environ.get("GOOGLE_SHEET_JSON")
        with open("temp_key.json", "w", encoding="utf-8") as f:
            f.write(json_key)

        credentials = ServiceAccountCredentials.from_json_keyfile_name("temp_key.json", scope)
        gc = gspread.authorize(credentials)

        SHEET_NAME = os.environ.get("SHEET_NAME", "KeyData")
        sheet_file = gc.open(SHEET_NAME)
        tabs = os.environ.get("SHEET_TABS", "1").split(",")

        combined_df = pd.DataFrame()
        for tab_name in tabs:
            worksheet = sheet_file.worksheet(tab_name.strip())
            df = pd.DataFrame(worksheet.get_all_records())
            df["key"] = df["key"].astype(str).str.strip().str.lower()
            combined_df = pd.concat([combined_df, df], ignore_index=True)

        key_map = {
            key: group[["name_file", "message_id"]].to_dict("records")
            for key, group in combined_df.groupby("key")
        }
        return key_map
    except Exception as e:
        logger.error(f"Failed to load sheet: {e}")
        return {}

KEY_MAP = load_key_map_from_sheet()

# FastAPI
app = FastAPI()

@app.on_event("startup")
async def startup():
    global bot_app
    bot_app = Application.builder().token(BOT_TOKEN).build()

    bot_app.add_handler(CommandHandler("start", start))
    bot_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_key))

    await bot_app.initialize()
    logger.info("Bot initialized")

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
    await update.message.reply_text("♥️ Please send your KEY to receive the file.\n♥️ Admin: t.me/A911Studio")

async def handle_key(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_input = update.message.text.strip().lower()
    chat_id = update.effective_chat.id

    if user_input in KEY_MAP:
        files_info = KEY_MAP[user_input]
        errors = 0

        for file_info in files_info:
            try:
                sent_message = await context.bot.copy_message(
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
                "⚠️ File not found. Please contact admin for support.\n♥️ Admin: t.me/A911Studio"
            )
    else:
        await update.message.reply_text("❌ KEY is incorrect. Please check again.")
