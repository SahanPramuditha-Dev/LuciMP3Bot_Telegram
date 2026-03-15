import os
import re
import json
import asyncio
import logging
import time
import hashlib
import subprocess
from pathlib import Path

import yt_dlp
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder,
    MessageHandler,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)
from telegram.constants import ParseMode
from telegram.error import BadRequest

# ╔══════════════════════════════════════════╗
#   CONFIG  —  set via Railway Variables
# ╚══════════════════════════════════════════╝

BOT_TOKEN    = os.environ.get("BOT_TOKEN",    "YOUR_TOKEN")
WEBHOOK_URL  = os.environ.get("WEBHOOK_URL",  "")
PORT         = int(os.environ.get("PORT",     8443))

DOWNLOAD_FOLDER = Path("downloads")
CACHE_FOLDER    = Path("cache")
COOKIES_FILE    = Path("cookies.txt")
MAX_FILE_SIZE   = 50 * 1024 * 1024
MAX_QUEUE_SIZE  = 15
WORKERS         = 3
RATE_LIMIT_SEC  = 8
FFMPEG_LOCATION = os.environ.get("FFMPEG_LOCATION", None)

DOWNLOAD_FOLDER.mkdir(exist_ok=True)
CACHE_FOLDER.mkdir(exist_ok=True)

# ╔══════════════════════════════════════════╗
#   LOGGING
# ╚══════════════════════════════════════════╝

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("bot.log")],
)
logger = logging.getLogger(__name__)

# ╔══════════════════════════════════════════╗
#   PO TOKEN  (YouTube bot-detection bypass)
# ╚══════════════════════════════════════════╝

def get_po_token() -> tuple[str | None, str | None]:
    """
    Generate a YouTube PO token using the npm package
    youtube-po-token-generator (installed in Dockerfile).
    Returns (po_token, visitor_data) or (None, None) on failure.
    """
    try:
        result = subprocess.run(
            ["youtube-po-token-generator"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode != 0:
            logger.warning("PO token generator exited with code %d: %s",
                           result.returncode, result.stderr.strip())
            return None, None
        data = json.loads(result.stdout)
        po   = data.get("poToken")
        vis  = data.get("visitorData")
        if po:
            logger.info("PO token generated successfully.")
        return po, vis
    except FileNotFoundError:
        logger.warning("youtube-po-token-generator not found — skipping PO token.")
        return None, None
    except Exception as e:
        logger.warning("PO token generation failed: %s", e)
        return None, None

# Generate once at startup
PO_TOKEN, VISITOR_DATA = get_po_token()

# ╔══════════════════════════════════════════╗
#   SHARED YT-DLP OPTIONS
# ╚══════════════════════════════════════════╝

def build_extractor_args() -> dict:
    """Build extractor_args based on available PO token."""
    args = {"player_client": ["web", "android"]}
    if PO_TOKEN:
        args["player_client"] = ["web"]
        args["po_token"]      = [f"web+{PO_TOKEN}"]
    if VISITOR_DATA:
        args["visitor_data"]  = [VISITOR_DATA]
    return {"youtube": args}

YDL_COMMON: dict = {
    "quiet":       True,
    "no_warnings": True,
    "http_headers": {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/125.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    },
    "extractor_args": build_extractor_args(),
    "socket_timeout": 30,
    "retries":        5,
}

if COOKIES_FILE.exists():
    YDL_COMMON["cookiefile"] = str(COOKIES_FILE)
    logger.info("Loaded cookies from %s", COOKIES_FILE)

if FFMPEG_LOCATION:
    YDL_COMMON["ffmpeg_location"] = FFMPEG_LOCATION

# ╔══════════════════════════════════════════╗
#   GLOBALS
# ╚══════════════════════════════════════════╝

download_queue: asyncio.Queue = None
active_downloads: dict        = {}
user_last_request: dict       = {}

stats = {
    "users":      set(),
    "downloads":  0,
    "failed":     0,
    "start_time": time.time(),
}

# ╔══════════════════════════════════════════╗
#   ANIMATION FRAMES
# ╚══════════════════════════════════════════╝

SPINNER = ["◐", "◓", "◑", "◒"]
WAVE    = ["▁", "▂", "▃", "▄", "▅", "▆", "▇", "█", "▇", "▆", "▅", "▄", "▃", "▂"]

FETCHING_FRAMES = [
    "🔍 Fetching info{dots}",
    "📡 Connecting{dots}",
    "🌐 Retrieving metadata{dots}",
]

# ╔══════════════════════════════════════════╗
#   HELPERS
# ╚══════════════════════════════════════════╝

def fmt_size(n: float) -> str:
    for unit in ["B", "KB", "MB", "GB"]:
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"

def fmt_duration(sec: int) -> str:
    h, r = divmod(sec, 3600)
    m, s = divmod(r, 60)
    return (f"{h}h " if h else "") + (f"{m}m " if m else "") + f"{s}s"

def fmt_views(n: int) -> str:
    if n >= 1_000_000: return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:     return f"{n / 1_000:.1f}K"
    return str(n)

def fmt_uptime(sec: int) -> str:
    d, r = divmod(sec, 86400)
    h, r = divmod(r, 3600)
    m, s = divmod(r, 60)
    parts = []
    if d: parts.append(f"{d}d")
    if h: parts.append(f"{h}h")
    if m: parts.append(f"{m}m")
    parts.append(f"{s}s")
    return " ".join(parts)

def rich_progress_bar(pct: float, width: int = 14) -> str:
    filled    = int(pct / 100 * width)
    empty     = width - filled
    fill_char = "▓" if pct < 33 else ("█" if pct < 66 else "▉")
    return fill_char * filled + "░" * empty

def mini_wave(frame: int, width: int = 8) -> str:
    return "".join(WAVE[(i + frame) % len(WAVE)] for i in range(width))

def sanitize_title(title: str) -> str:
    title = re.sub(r'[\\/*?:"<>|#%&{}$!\'@+`=]', "_", title)
    title = re.sub(r'\s+', " ", title).strip()
    return title[:120]

def cache_key(url: str, typ: str, quality: str) -> str:
    h = hashlib.md5(f"{url}|{typ}|{quality}".encode()).hexdigest()[:12]
    return f"{h}_{typ}_{quality}"

async def safe_edit(msg, text: str, **kwargs):
    try:
        await msg.edit_text(text, **kwargs)
    except BadRequest as e:
        if "not modified" not in str(e).lower():
            logger.debug("Edit skipped: %s", e)

def error_hint(e: Exception) -> str:
    msg = str(e)
    if "Sign in" in msg or "bot" in msg.lower():
        if PO_TOKEN:
            return "\n\n💡 _PO token is active but YouTube still blocked — try adding `cookies.txt`_"
        return "\n\n💡 _YouTube bot detection triggered — PO token not available on this server_"
    if "403" in msg:
        return "\n\n💡 _HTTP 403 — server IP may be blocked by YouTube_"
    if "Private" in msg or "private" in msg:
        return "\n\n💡 _This video is private or age-restricted_"
    return ""

# ╔══════════════════════════════════════════╗
#   VIDEO INFO
# ╚══════════════════════════════════════════╝

def get_video_info(url: str) -> dict:
    opts = {**YDL_COMMON, "skip_download": True}
    with yt_dlp.YoutubeDL(opts) as ydl:
        return ydl.extract_info(url, download=False)

def build_caption(info: dict) -> str:
    title     = info.get("title", "Unknown")[:60]
    uploader  = info.get("uploader") or info.get("channel") or "—"
    duration  = fmt_duration(info.get("duration") or 0)
    views     = fmt_views(info.get("view_count") or 0)
    likes     = fmt_views(info.get("like_count") or 0)
    date      = info.get("upload_date", "")
    date_str  = f"{date[:4]}-{date[4:6]}-{date[6:]}" if len(date) == 8 else "—"
    extractor = info.get("extractor_key", "").upper()
    return (
        f"🎬 *{title}*\n\n"
        f"👤 {uploader}   📅 {date_str}\n"
        f"⏱ {duration}   👁 {views}   👍 {likes}\n"
        f"🔗 {extractor}\n\n"
        f"*Choose a format:*"
    )

def build_buttons(info: dict, url: str) -> list:
    formats = info.get("formats", [])
    heights = sorted(set(
        f.get("height") for f in formats
        if f.get("height") and f.get("height") in (360, 480, 720, 1080)
    ))
    rows  = []
    icons = {360: "📱", 480: "💻", 720: "🖥", 1080: "📺"}
    vid_row = []
    for h in heights:
        vid_row.append(InlineKeyboardButton(
            f"{icons.get(h, '📹')} {h}p",
            callback_data=f"mp4|{h}|{url}"
        ))
        if len(vid_row) == 2:
            rows.append(vid_row)
            vid_row = []
    if vid_row:
        rows.append(vid_row)
    if not heights:
        rows.append([InlineKeyboardButton("📹 Best quality", callback_data=f"mp4|best|{url}")])
    rows.append([
        InlineKeyboardButton("🎵 MP3 128kbps", callback_data=f"mp3|128|{url}"),
        InlineKeyboardButton("🎵 MP3 320kbps", callback_data=f"mp3|320|{url}"),
    ])
    rows.append([InlineKeyboardButton("❌  Cancel", callback_data="cancel")])
    return rows

# ╔══════════════════════════════════════════╗
#   ANIMATED FETCH STATUS
# ╚══════════════════════════════════════════╝

async def animated_fetch(msg, stop_event: asyncio.Event):
    frame = 0
    fidx  = 0
    while not stop_event.is_set():
        dots = "." * (frame % 4)
        text = FETCHING_FRAMES[fidx % len(FETCHING_FRAMES)].format(dots=dots)
        spin = SPINNER[frame % len(SPINNER)]
        await safe_edit(msg, f"{spin} _{text}_", parse_mode=ParseMode.MARKDOWN)
        frame += 1
        if frame % 4 == 0:
            fidx += 1
        await asyncio.sleep(0.7)

# ╔══════════════════════════════════════════╗
#   COMMANDS
# ╚══════════════════════════════════════════╝

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    stats["users"].add(update.effective_user.id)
    text = (
        "🎬 *Media Downloader Bot*\n\n"
        "Supported sites:\n"
        "• YouTube  • TikTok  • Instagram\n"
        "• Facebook  • Twitter/X  • Reddit\n"
        "• SoundCloud  • Twitch  • Vimeo\n\n"
        "Just paste a link and choose your format!\n\n"
        "📋 _Commands:_\n"
        "`/queue` — queue status\n"
        "`/stats` — bot statistics\n"
        "`/help`  — usage guide"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    mode          = "Webhook" if WEBHOOK_URL else "Polling"
    cookie_status = "✅ Loaded" if COOKIES_FILE.exists() else "❌ Not found"
    po_status     = "✅ Active" if PO_TOKEN else "❌ Not available"
    text = (
        "ℹ️ *How to use*\n\n"
        "1️⃣ Paste any supported video URL\n"
        "2️⃣ Wait for the format picker\n"
        "3️⃣ Choose MP4 resolution or MP3 quality\n"
        "4️⃣ File is sent with the song/video title as filename\n\n"
        "⚠️ *Limits*\n"
        f"• Max file size : `{fmt_size(MAX_FILE_SIZE)}`\n"
        f"• Queue slots   : `{MAX_QUEUE_SIZE}`\n"
        f"• Rate limit    : `{RATE_LIMIT_SEC}s` between requests\n\n"
        "💡 *Tips*\n"
        "• Use `MP3 320` for best audio quality\n"
        "• Use `360p` for fastest video download\n"
        "• Cached files are sent instantly ⚡\n\n"
        f"🔌 *Mode*       : `{mode}`\n"
        f"🔑 *PO Token*   : {po_status}\n"
        f"🍪 *Cookies*    : {cookie_status}"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uptime = int(time.time() - stats["start_time"])
    total  = stats["downloads"] + stats["failed"]
    rate   = (stats["downloads"] / total * 100) if total else 0
    text = (
        f"📊 *Bot Statistics*\n\n"
        f"👥 Unique users : `{len(stats['users'])}`\n"
        f"✅ Downloads    : `{stats['downloads']}`\n"
        f"❌ Failed       : `{stats['failed']}`\n"
        f"📈 Success rate : `{rate:.1f}%`\n"
        f"⏳ Queue now    : `{download_queue.qsize()}`\n"
        f"⚡ Workers      : `{WORKERS}`\n"
        f"🕐 Uptime       : `{fmt_uptime(uptime)}`"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

async def cmd_queue(update: Update, context: ContextTypes.DEFAULT_TYPE):
    running = len(active_downloads)
    bar_run = "🟢" * running + "⚪" * max(0, WORKERS - running)
    text = (
        f"📋 *Queue Status*\n\n"
        f"🔄 Running  : {bar_run} `{running}/{WORKERS}`\n"
        f"⏳ Pending  : `{download_queue.qsize()}`\n"
        f"🔢 Capacity : `{MAX_QUEUE_SIZE}`"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

# ╔══════════════════════════════════════════╗
#   LINK HANDLER
# ╚══════════════════════════════════════════╝

async def handle_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    url  = update.message.text.strip()
    user = update.effective_user.id

    if not re.match(r"https?://", url):
        return

    now = time.time()
    if user in user_last_request and now - user_last_request[user] < RATE_LIMIT_SEC:
        remaining = int(RATE_LIMIT_SEC - (now - user_last_request[user]))
        await update.message.reply_text(
            f"⏳ Please wait `{remaining}s` before sending another link.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    user_last_request[user] = now
    stats["users"].add(user)

    if download_queue.qsize() >= MAX_QUEUE_SIZE:
        await update.message.reply_text(
            "⚠️ *Queue is full!* Please try again in a moment.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    msg       = await update.message.reply_text("🔍 _Fetching info…_", parse_mode=ParseMode.MARKDOWN)
    stop_anim = asyncio.Event()
    anim_task = asyncio.create_task(animated_fetch(msg, stop_anim))

    try:
        info = await asyncio.get_event_loop().run_in_executor(None, get_video_info, url)
    except Exception as e:
        logger.error("Info fetch error: %s", e)
        stop_anim.set()
        anim_task.cancel()
        hint = error_hint(e)
        await safe_edit(
            msg,
            f"❌ *Could not fetch video info*\n\n`{str(e)[:200]}`{hint}",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    stop_anim.set()
    anim_task.cancel()
    await msg.delete()

    caption = build_caption(info)
    buttons = build_buttons(info, url)
    thumb   = info.get("thumbnail")

    try:
        await update.message.reply_photo(
            photo=thumb,
            caption=caption,
            reply_markup=InlineKeyboardMarkup(buttons),
            parse_mode=ParseMode.MARKDOWN,
        )
    except Exception:
        await update.message.reply_text(
            caption,
            reply_markup=InlineKeyboardMarkup(buttons),
            parse_mode=ParseMode.MARKDOWN,
        )

# ╔══════════════════════════════════════════╗
#   BUTTON HANDLER
# ╚══════════════════════════════════════════╝

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data == "cancel":
        await query.message.delete()
        return

    typ, quality, url = query.data.split("|", 2)
    pos = download_queue.qsize() + 1

    await query.edit_message_caption(
        caption=f"✅ *Added to queue — position #{pos}*",
        parse_mode=ParseMode.MARKDOWN,
    )

    label      = f"🎵 MP3 {quality}kbps" if typ == "mp3" else f"🎥 MP4 {quality}p"
    status_msg = await query.message.reply_text(
        f"🕐 *Queued* — {label}\n_Waiting for a free worker…_",
        parse_mode=ParseMode.MARKDOWN,
    )

    await download_queue.put((query, typ, quality, url, status_msg))

# ╔══════════════════════════════════════════╗
#   DOWNLOAD WORKER
# ╚══════════════════════════════════════════╝

async def worker(worker_id: int):
    logger.info("Worker %d started", worker_id)
    while True:
        try:
            query, typ, quality, url, msg = await download_queue.get()
            active_downloads[worker_id]   = url
            await process(query, typ, quality, url, msg, worker_id)
        except Exception as e:
            logger.exception("Worker %d crashed: %s", worker_id, e)
        finally:
            active_downloads.pop(worker_id, None)
            download_queue.task_done()

# ╔══════════════════════════════════════════╗
#   PROCESS / DOWNLOAD
# ╚══════════════════════════════════════════╝

async def process(query, typ: str, quality: str, url: str, msg, worker_id: int):
    loop = asyncio.get_event_loop()

    # ── Cache check ──────────────────────────────────────────────
    ck         = cache_key(url, typ, quality)
    cache_path = CACHE_FOLDER / ck

    if cache_path.exists():
        await safe_edit(msg, "⚡ *Sending from cache…*", parse_mode=ParseMode.MARKDOWN)
        try:
            with open(cache_path, "rb") as f:
                caption = f"✅ {'🎵 Audio' if typ == 'mp3' else '🎥 Video'} _(cached)_"
                if typ == "mp3":
                    await msg.reply_audio(
                        f, caption=caption, parse_mode=ParseMode.MARKDOWN,
                        read_timeout=120, write_timeout=120,
                    )
                else:
                    await msg.reply_video(
                        f, caption=caption, parse_mode=ParseMode.MARKDOWN,
                        supports_streaming=True, read_timeout=120, write_timeout=120,
                    )
            await msg.delete()
            stats["downloads"] += 1
        except Exception as e:
            logger.error("Cache send failed: %s", e)
            cache_path.unlink(missing_ok=True)
        return

    # ── Fetch title ───────────────────────────────────────────────
    try:
        info        = await loop.run_in_executor(None, get_video_info, url)
        raw_title   = info.get("title") or "download"
        clean_title = sanitize_title(raw_title)
    except Exception:
        clean_title = "download"

    # ── yt-dlp format ─────────────────────────────────────────────
    if typ == "mp3":
        fmt = "bestaudio/best"
    elif quality == "best":
        fmt = "bestvideo+bestaudio/best"
    else:
        fmt = f"bestvideo[height<={quality}]+bestaudio/best[height<={quality}]/best"

    output_template = str(DOWNLOAD_FOLDER / f"{clean_title}.%(ext)s")

    # ── Progress hook ─────────────────────────────────────────────
    frame_counter = {"n": 0, "last_pct": -1.0}

    def hook(d):
        if d["status"] != "downloading":
            return
        raw = d.get("_percent_str", "0").strip().replace("%", "")
        try:
            pct = float(raw)
        except ValueError:
            return
        if pct - frame_counter["last_pct"] < 3 and pct < 99:
            return
        frame_counter["last_pct"] = pct
        frame_counter["n"]       += 1

        bar      = rich_progress_bar(pct)
        wave     = mini_wave(frame_counter["n"], width=6)
        speed    = (d.get("_speed_str") or "—").strip()
        eta      = (d.get("_eta_str")   or "—").strip()
        size_str = (
            d.get("_total_bytes_str") or
            d.get("_total_bytes_estimate_str") or "—"
        ).strip()
        phase = "🎵 Audio" if typ == "mp3" else "🎥 Video"

        text = (
            f"{phase} — *{quality}{'kbps' if typ == 'mp3' else 'p'}*\n\n"
            f"`{bar}` *{pct:.0f}%*\n\n"
            f"{wave}\n\n"
            f"📦 `{size_str}`   ⚡ `{speed}`   ⏱ `{eta}`"
        )
        loop.call_soon_threadsafe(
            asyncio.ensure_future,
            safe_edit(msg, text, parse_mode=ParseMode.MARKDOWN),
        )

    ydl_opts = {
        **YDL_COMMON,
        "format":              fmt,
        "outtmpl":             output_template,
        "progress_hooks":      [hook],
        "merge_output_format": "mp4" if typ == "mp4" else None,
    }
    if typ == "mp3":
        ydl_opts["postprocessors"] = [{
            "key":              "FFmpegExtractAudio",
            "preferredcodec":   "mp3",
            "preferredquality": quality,
        }]

    await safe_edit(msg, "⬇️ _Starting download…_", parse_mode=ParseMode.MARKDOWN)

    try:
        file_path = await loop.run_in_executor(
            None, lambda: _run_ydl(ydl_opts, url, typ, clean_title)
        )
    except Exception as e:
        logger.error("Download error: %s", e)
        stats["failed"] += 1
        hint = error_hint(e)
        await safe_edit(
            msg,
            f"❌ *Download failed*\n\n`{str(e)[:300]}`{hint}",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    if not file_path or not Path(file_path).exists():
        stats["failed"] += 1
        await safe_edit(
            msg,
            "❌ *File not found after download*\n"
            "The video may be too long or geo-restricted.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    fp        = Path(file_path)
    file_size = fp.stat().st_size

    if file_size > MAX_FILE_SIZE:
        fp.unlink(missing_ok=True)
        stats["failed"] += 1
        await safe_edit(
            msg,
            f"⚠️ *File too large for Telegram*\n\n"
            f"Size : `{fmt_size(file_size)}`\n"
            f"Limit: `{fmt_size(MAX_FILE_SIZE)}`\n\n"
            f"Try:\n• MP3 instead of MP4\n• Lower resolution\n• Shorter video",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    await safe_edit(
        msg,
        f"📤 *Uploading* `{fp.name}` — `{fmt_size(file_size)}`…",
        parse_mode=ParseMode.MARKDOWN,
    )

    try:
        with open(fp, "rb") as f:
            caption = f"✅ *{fp.stem}*"
            if typ == "mp3":
                await msg.reply_audio(
                    audio=f, caption=caption, title=fp.stem,
                    parse_mode=ParseMode.MARKDOWN,
                    read_timeout=180, write_timeout=180,
                )
            else:
                await msg.reply_video(
                    video=f, caption=caption,
                    parse_mode=ParseMode.MARKDOWN,
                    supports_streaming=True,
                    read_timeout=180, write_timeout=180,
                )
        await msg.delete()
        stats["downloads"] += 1
        cache_path.write_bytes(fp.read_bytes())

    except Exception as e:
        logger.error("Upload error: %s", e)
        stats["failed"] += 1
        await safe_edit(
            msg,
            f"❌ *Upload failed*\n\n`{str(e)[:200]}`",
            parse_mode=ParseMode.MARKDOWN,
        )
    finally:
        fp.unlink(missing_ok=True)

# ╔══════════════════════════════════════════╗
#   BLOCKING YT-DLP CALL
# ╚══════════════════════════════════════════╝

def _run_ydl(opts: dict, url: str, typ: str, clean_title: str):
    with yt_dlp.YoutubeDL(opts) as ydl:
        info     = ydl.extract_info(url, download=True)
        raw_path = ydl.prepare_filename(info)

    if typ == "mp3":
        mp3_path = Path(raw_path).with_suffix(".mp3")
        if mp3_path.exists():
            return str(mp3_path)
        for f in DOWNLOAD_FOLDER.iterdir():
            if f.stem == clean_title and f.suffix == ".mp3":
                return str(f)
        for f in DOWNLOAD_FOLDER.iterdir():
            if f.suffix == ".mp3" and clean_title[:20].lower() in f.stem.lower():
                return str(f)
        return None

    return raw_path if Path(raw_path).exists() else None

# ╔══════════════════════════════════════════╗
#   UNKNOWN MESSAGE
# ╚══════════════════════════════════════════╝

async def handle_unknown(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "❓ Send a valid video URL or use /help for instructions."
    )

# ╔══════════════════════════════════════════╗
#   APP SETUP
# ╚══════════════════════════════════════════╝

async def post_init(app):
    global download_queue
    download_queue = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)
    for i in range(WORKERS):
        asyncio.create_task(worker(i + 1))
    logger.info(
        "%d workers started. Mode: %s | PO token: %s | Cookies: %s",
        WORKERS,
        f"webhook ({WEBHOOK_URL})" if WEBHOOK_URL else "polling",
        "yes" if PO_TOKEN else "no",
        "yes" if COOKIES_FILE.exists() else "no",
    )

def main():
    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .connect_timeout(60)
        .read_timeout(60)
        .write_timeout(180)
        .pool_timeout(60)
        .get_updates_connect_timeout(60)
        .get_updates_read_timeout(60)
        .get_updates_write_timeout(60)
        .get_updates_pool_timeout(60)
        .post_init(post_init)
        .build()
    )

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help",  cmd_help))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("queue", cmd_queue))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_link))
    app.add_handler(MessageHandler(filters.ALL, handle_unknown))

    if WEBHOOK_URL:
        logger.info("Starting in WEBHOOK mode on port %d", PORT)
        app.run_webhook(
            listen="0.0.0.0",
            port=PORT,
            webhook_url=WEBHOOK_URL,
            drop_pending_updates=True,
        )
    else:
        logger.info("Starting in POLLING mode")
        app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()