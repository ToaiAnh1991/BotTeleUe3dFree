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
import asyncio
import json

# ENV
BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHANNEL_ID = int(os.environ.get("CHANNEL_ID", "-1000000000000"))
ADMIN_IDS = [id.strip() for id in os.environ.get("ADMIN_IDS", "").split(",") if id.strip().isdigit()]

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KEY_MAP = {}  # Global Key Map
PROCESSING_QUEUE = asyncio.Queue() 
RATE_LIMIT_SECONDS = 10 
USER_ACTIVE_REQUESTS = {} 

# Đường dẫn tới file cache cục bộ (trong thư mục /tmp trên Render)
CACHE_FILE_PATH = "/tmp/key_map_cache.json"

# Load Google Sheet Function (Đã điều chỉnh để ưu tiên cache và có tùy chọn buộc tải từ Sheet)
def load_key_map_from_sheet(force_from_sheet=False):
    """
    Tải KEY_MAP từ Google Sheet hoặc từ cache cục bộ.
    Nếu force_from_sheet=False, sẽ ưu tiên đọc từ cache nếu tồn tại.
    Nếu force_from_sheet=True, sẽ buộc tải từ Google Sheet.
    """
    global KEY_MAP 
    
    if not force_from_sheet and os.path.exists(CACHE_FILE_PATH):
        try:
            with open(CACHE_FILE_PATH, 'r', encoding='utf-8') as f:
                cached_data = json.load(f)
                KEY_MAP = cached_data 
                logger.info("✅ Key Map loaded from cache successfully.")
                return KEY_MAP
        except Exception as e:
            logger.warning(f"⚠️ Failed to load Key Map from cache: {e}. Attempting to load from Google Sheet.")

    logger.info("Attempting to load Key Map from Google Sheet...")
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        json_key_str = os.environ.get("GOOGLE_SHEET_JSON")
        if not json_key_str:
            logger.error("❌ GOOGLE_SHEET_JSON environment variable is missing. Cannot load from sheet.")
            return {}

        credentials = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(json_key_str), scope)
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

        new_key_map = {
            key: group[["name_file", "message_id"]].to_dict("records")
            for key, group in combined_df.groupby("key")
        }
        
        KEY_MAP = new_key_map 

        try:
            with open(CACHE_FILE_PATH, 'w', encoding='utf-8') as f:
                json.dump(KEY_MAP, f, ensure_ascii=False, indent=2)
            logger.info(f"✅ Key Map saved to cache: {CACHE_FILE_PATH}")
        except Exception as e:
            logger.error(f"❌ Failed to save Key Map to cache: {e}")

        logger.info("✅ Google Sheet loaded successfully.")
        return KEY_MAP

    except Exception as e:
        logger.error(f"❌ Google Sheet loaded Failed: {e}")
        return KEY_MAP 

# Hàm async để tải lại sheet từ Google Sheet và lưu cache (dùng cho lệnh /reload)
async def async_load_key_map_from_sheet_and_save_cache():
    logger.info("Initiating Google Sheet reload from command.")
    success = False
    try:
        global KEY_MAP
        temp_key_map = load_key_map_from_sheet(force_from_sheet=True) 
        if temp_key_map:
            KEY_MAP = temp_key_map 
            logger.info("Google Sheet reload from command completed successfully.")
            success = True
        else:
            logger.warning("Google Sheet reload from command failed: Empty key map returned.")
    except Exception as e:
        logger.error(f"Google Sheet reload from command failed with exception: {e}")
    return success

# FastAPI App
app = FastAPI()

@app.on_event("startup")
async def startup():
    global bot_app, KEY_MAP

    # Cố gắng tải KEY_MAP từ cache trước (hoặc từ sheet nếu không có cache/lỗi cache)
    load_key_map_from_sheet(force_from_sheet=False) 

    bot_app = Application.builder().token(BOT_TOKEN).build()

    bot_app.add_handler(CommandHandler("start", start))
    bot_app.add_handler(CommandHandler("reload", reload_sheet)) 
    bot_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, enqueue_key_request))

    await bot_app.initialize()
    logger.info("✅ Bot initialized.")

    # Khởi tạo tác vụ xử lý hàng đợi
    asyncio.create_task(process_queue_task())
    logger.info("✅ Queue processing task started.")
    
    # Gửi thông báo khi bot đã khởi động xong (cho kênh công khai và admin)
    if KEY_MAP: 
        try:
            await bot_app.bot.send_message(
                chat_id=CHANNEL_ID,
                text="🎉 Bot has started! You can send your KEY now."
            )
            logger.info(f"Sent startup success message to channel {CHANNEL_ID}.")
        except Exception as e:
            logger.error(f"Failed to send startup success message to channel {CHANNEL_ID}: {e}")

        for admin_id_str in ADMIN_IDS:
            try:
                admin_id = int(admin_id_str)
                await bot_app.bot.send_message(chat_id=admin_id, text="✨ Bot has started and is ready! Keymap loaded.")
            except Exception as e:
                logger.error(f"Failed to send startup message to admin {admin_id_str}: {e}")
    else:
         for admin_id_str in ADMIN_IDS:
            try:
                admin_id = int(admin_id_str)
                await bot_app.bot.send_message(chat_id=admin_id, text="⚠️ Bot started but failed to load Keymap! Please check logs.")
            except Exception as e:
                logger.error(f"Failed to send startup error message to admin {admin_id_str}: {e}")

@app.post("/webhook/{token}")
async def telegram_webhook(token: str, request: Request):
    if token != BOT_TOKEN:
        logger.warning(f"Received webhook with invalid token: {token}")
        return {"error": "Invalid token"}
    
    try:
        body = await request.json()
        if not body or 'update_id' not in body: 
            logger.info("Received non-Telegram JSON body on webhook endpoint. Ignoring.")
            return {"ok": True} 

        update = Update.de_json(body, bot_app.bot)
        await bot_app.process_update(update)
    except Exception as e:
        logger.error(f"Webhook error: {e}")
    return {"ok": True}

# Bot Handlers

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "♥️ Hi. Please send your key UExxxxx to the Ue3dFreeBOT to receive the file.\nContact Admin if file error: t.me/A911Studio"
    )

async def reload_sheet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("❌ You are not authorized to reload the sheet.")
        return

    await update.message.reply_text("🔄 Reloading Google Sheet. Please wait...")
    
    success = await async_load_key_map_from_sheet_and_save_cache()

    if success:
        await update.message.reply_text("✅ Google Sheet reloaded successfully.")
    else:
        await update.message.reply_text("❌ Google Sheet reloaded Failed. Check logs or try again.")

async def enqueue_key_request(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_input = update.message.text.strip().lower()

    if user_id in USER_ACTIVE_REQUESTS:
        await update.message.reply_text("⏳ Sending previous file. Please wait for current file to be received before sending another KEY !")
        logger.info(f"User {user_id} sent key '{user_input}' but already has an active request.")
        return

    if not KEY_MAP:
        await update.message.reply_text("⏰ Bot is starting or key data is not available. Please wait a few minutes and send your KEY again.")
        logger.info(f"User {user_id} sent key '{user_input}' but KEY_MAP is empty. Request not queued.")
        return 

    if user_input not in KEY_MAP:
        await update.message.reply_text("❌ KEY is incorrect. Please check again.")
        return

    await PROCESSING_QUEUE.put({"update": update, "context": context})
    USER_ACTIVE_REQUESTS[user_id] = True 
    await update.message.reply_text("✅ Sending file. Please wait a moment !")
    logger.info(f"Request for user {user_id} with key '{user_input}' added to queue.")

async def process_queue_task():
    while True:
        request_data = await PROCESSING_QUEUE.get()
        update = request_data["update"]
        context = request_data["context"]
        user_id = update.effective_user.id
        user_input = update.message.text.strip().lower() 

        logger.info(f"Processing queued request for user {user_id} with key '{user_input}'")

        if not KEY_MAP or user_input not in KEY_MAP:
            await update.message.reply_text(
                "⚠️ Sorry, Error processing file. Please try again later or contact admin.\n Admin: t.me/A911Studio"
            )
            logger.warning(f"Failed to process queued request for user {user_id}: KEY_MAP not ready or key '{user_input}' not found.")
        else:
            await handle_key_actual(update, context)

        if user_id in USER_ACTIVE_REQUESTS:
            del USER_ACTIVE_REQUESTS[user_id]
            logger.info(f"User {user_id} removed from active requests.")

        PROCESSING_QUEUE.task_done()
        await asyncio.sleep(RATE_LIMIT_SECONDS)

async def handle_key_actual(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_input = update.message.text.strip().lower()
    chat_id = update.effective_chat.id

    files_info = KEY_MAP.get(user_input, []) 
    errors = 0

    if not files_info: 
        await update.message.reply_text("❌ KEY is incorrect or file data not found. Please check again or try later.")
        logger.warning(f"User {update.effective_user.id} requested key '{user_input}' but no files_info found.")
        return

    for file_info in files_info:
        try:
            message_id = int(file_info["message_id"])
            if message_id <= 0:
                raise ValueError("Invalid message_id")

            await context.bot.copy_message(
                chat_id=chat_id,
                from_chat_id=CHANNEL_ID,
                message_id=message_id,
                protect_content=True
            )
            await update.message.reply_text(f"Your File: \"{file_info['name_file']}\"")
        except Exception as e:
            logger.error(f"File send error (user: {update.effective_user.id}, key: {user_input}, file: {file_info.get('name_file', 'N/A')}): {e}")
            errors += 1

    if errors:
        await update.message.reply_text(
            "⚠️ Files not found. Please contact admin.\n Admin: t.me/A911Studio"
        )
    else:
        await update.message.reply_text("✅ File sent successfully. You can send next KEY.")