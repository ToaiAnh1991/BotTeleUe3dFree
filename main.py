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
import time
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
PROCESSING_QUEUE = asyncio.Queue() # Hàng đợi để xử lý các yêu cầu
RATE_LIMIT_SECONDS = 10 # Thời gian chờ giữa các lần xử lý trong hàng đợi

# Thêm một dictionary để theo dõi các yêu cầu đang hoạt động của người dùng
USER_ACTIVE_REQUESTS = {} # user_id: True (đang có yêu cầu chờ/xử lý)

# Load Google Sheet Function
def load_key_map_from_sheet():
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        json_key_str = os.environ.get("GOOGLE_SHEET_JSON")
        if not json_key_str:
            logger.error("❌ GOOGLE_SHEET_JSON environment variable is missing.")
            return {}

        # Đọc JSON từ chuỗi biến môi trường trực tiếp, không ghi file tạm
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

    KEY_MAP = load_key_map_from_sheet()

    bot_app = Application.builder().token(BOT_TOKEN).build()

    bot_app.add_handler(CommandHandler("start", start))
    bot_app.add_handler(CommandHandler("reload", reload_sheet))
    bot_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, enqueue_key_request))

    await bot_app.initialize()
    logger.info("✅ Bot initialized and sheet loaded.")

    asyncio.create_task(process_queue_task())
    logger.info("✅ Queue processing task started.")

@app.post("/webhook/{token}")
async def telegram_webhook(token: str, request: Request):
    if token != BOT_TOKEN:
        return {"error": "Invalid token"}
    try:
        body = await request.json()
        
        # Xử lý Ping từ cron-job.org:
        # Nếu body rỗng (do bạn đã cấu hình {} trong cron-job.org)
        # hoặc nếu nó không chứa 'update_id' (một trường bắt buộc trong mỗi update Telegram)
        if not body or 'update_id' not in body: 
            logger.info("Received empty or non-Telegram JSON body. Likely a keep-alive ping.")
            return {"ok": True} # Trả về OK để xác nhận đã nhận request ping

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

    global KEY_MAP
    KEY_MAP = load_key_map_from_sheet()

    if KEY_MAP:
        await update.message.reply_text("🔄 Google Sheet reloaded successfully.")
    else:
        await update.message.reply_text("❌ Google Sheet reloaded Failed.")

async def enqueue_key_request(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_input = update.message.text.strip().lower()

    # Bước 1: Kiểm tra nếu người dùng đã có yêu cầu đang chờ/xử lý
    if user_id in USER_ACTIVE_REQUESTS:
        await update.message.reply_text("⏳ Sending previous file. Please wait for current file to be received before sending another KEY !")
        logger.info(f"User {user_id} sent key '{user_input}' but already has an active request.")
        return

    # Bước 2: Kiểm tra nếu bot chưa sẵn sàng (KEY_MAP rỗng)
    if not KEY_MAP:
        # Thông báo mới cho trường hợp bot đang sleep/khởi động
        await update.message.reply_text("⏰ Bot is starting. Please wait a few minutes and send your KEY again.")
        logger.info(f"User {user_id} sent key '{user_input}' while bot was starting. Request not queued.")
        return # Kết thúc xử lý ở đây nếu bot đang khởi động

    # Bước 3: Kiểm tra nếu KEY không hợp lệ ngay lập tức
    if user_input not in KEY_MAP:
        await update.message.reply_text("❌ KEY is incorrect. Please check again.")
        return

    # Nếu tất cả các kiểm tra đều vượt qua, thêm yêu cầu vào hàng đợi và đánh dấu người dùng
    await PROCESSING_QUEUE.put({"update": update, "context": context})
    USER_ACTIVE_REQUESTS[user_id] = True # Đánh dấu người dùng này đang có yêu cầu chờ
    await update.message.reply_text("✅ Sending file. Please wait a moment !")
    logger.info(f"Request for user {user_id} with key '{user_input}' added to queue.")

async def process_queue_task():
    while True:
        request_data = await PROCESSING_QUEUE.get()
        update = request_data["update"]
        context = request_data["context"]
        user_id = update.effective_user.id
        user_input = update.message.text.strip().lower() # Lấy user_input từ update

        logger.info(f"Processing queued request for user {user_id} with key '{user_input}'")

        # KIỂM TRA LẠI KEY_MAP TRƯỚC KHI XỬ LÝ TỪ HÀNG ĐỢI
        if not KEY_MAP or user_input not in KEY_MAP:
            await update.message.reply_text(
                "⚠️ Sorry, Error processing file. Please try again later or contact admin.\n Admin: t.me/A911Studio"
            )
            logger.warning(f"Failed to process queued request for user {user_id}: KEY_MAP not ready or key '{user_input}' not found.")
        else:
            await handle_key_actual(update, context)

        # Sau khi xử lý xong, xóa người dùng khỏi danh sách active requests
        if user_id in USER_ACTIVE_REQUESTS:
            del USER_ACTIVE_REQUESTS[user_id]
            logger.info(f"User {user_id} removed from active requests.")

        PROCESSING_QUEUE.task_done()
        await asyncio.sleep(RATE_LIMIT_SECONDS)

async def handle_key_actual(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_input = update.message.text.strip().lower()
    chat_id = update.effective_chat.id

    files_info = KEY_MAP[user_input]
    errors = 0

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
        # Thông báo khi tất cả file đã được gửi thành công
        await update.message.reply_text("✅ File sent successfully. You can send next KEY.")