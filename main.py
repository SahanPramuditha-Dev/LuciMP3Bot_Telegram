"""
Telegram Media Downloader Bot
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
YouTube bot-detection bypass strategy (in priority order):
  1. tv_embedded client  — no sign-in required, works from server IPs
  2. iOS client          — mobile UA, rarely blocked
  3. android client      — fallback with broad format support
  4. PO token (web)      — if youtube-po-token-generator is installed
  5. cookies.txt         — manual cookie export for age-gated content

The extractor client list is tried left-to-right by yt-dlp automatically.
"""

import os
import re
import json
import shutil
import asyncio
import logging
import time
import hashlib
import subprocess
from pathlib import Path
from collections import OrderedDict

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
from telegram.error import BadRequest, NetworkError, RetryAfter

# ╔══════════════════════════════════════════╗
#   CONFIG  —  set via environment variables
# ╚══════════════════════════════════════════╝

BOT_TOKEN    = os.environ.get("BOT_TOKEN",    "YOUR_TOKEN")
WEBHOOK_URL  = os.environ.get("WEBHOOK_URL",  "")
PORT         = int(os.environ.get("PORT",     8443))

DOWNLOAD_FOLDER  = Path("downloads")
CACHE_FOLDER     = Path("cache")
COOKIES_FILE     = Path("cookies.txt")
MAX_FILE_SIZE    = 50 * 1024 * 1024
MAX_QUEUE_SIZE   = 15
WORKERS          = 3
RATE_LIMIT_SEC   = 8
DOWNLOAD_TIMEOUT = 600
MAX_RATE_CACHE   = 1000
FFMPEG_LOCATION  = os.environ.get("FFMPEG_LOCATION", None)

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
#   PO TOKEN  —  optional, refreshed hourly
# ╚══════════════════════════════════════════╝

_po_token:       str | None = None
_visitor_data:   str | None = None
_po_token_expiry: float     = 0.0
PO_TOKEN_TTL                = 3600

def _generate_po_token() -> tuple[str | None, str | None]:
    try:
        result = subprocess.run(
            ["youtube-po-token-generator"],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode != 0:
            logger.warning("PO token generator exited %d: %s",
                           result.returncode, result.stderr.strip())
            return None, None
        data = json.loads(result.stdout)
        po   = data.get("poToken")
        vis  = data.get("visitorData")
        if po:
            logger.info("PO token generated successfully.")
        return po, vis
    except FileNotFoundError:
        logger.info("youtube-po-token-generator not installed — PO token disabled.")
        return None, None
    except Exception as e:
        logger.warning("PO token generation failed: %s", e)
        return None, None

def get_po_token() -> tuple[str | None, str | None]:
    global _po_token, _visitor_data, _po_token_expiry
    if time.time() > _po_token_expiry:
        _po_token, _visitor_data = _generate_po_token()
        _po_token_expiry = time.time() + PO_TOKEN_TTL
    return _po_token, _visitor_data

# ╔══════════════════════════════════════════╗
#   YT-DLP OPTIONS
# ╚══════════════════════════════════════════╝

# tv_embedded: bypasses bot detection, no auth required from server IPs
# ios: mobile client, rarely flagged by YouTube
# android: broad format support
_YT_CLIENTS_BASE = ["tv_embedded", "ios", "android"]

def build_extractor_args() -> dict:
    po, vis = get_po_token()
    clients = list(_YT_CLIENTS_BASE)
    yt_args: dict = {"player_client": clients}
    if po:
        clients.insert(0, "web")
        yt_args["player_client"] = clients
        yt_args["po_token"]      = [f"web+{po}"]
    if vis:
        yt_args["visitor_data"]  = [vis]
    return {"youtube": yt_args}

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/125.0.0.0 Safari/537.36"
)

def build_ydl_common() -> dict:
    opts: dict = {
        "quiet":           True,
        "no_warnings":     True,
        "http_headers": {
            "User-Agent":      _UA,
            "Accept-Language": "en-US,en;q=0.9",
            "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
        "extractor_args":  build_extractor_args(),
        "socket_timeout":  30,
        "retries":         5,
        "fragment_retries": 5,
    }
    if COOKIES_FILE.exists():
        opts["cookiefile"] = str(COOKIES_FILE)
    if FFMPEG_LOCATION:
        opts["ffmpeg_location"] = FFMPEG_LOCATION
    return opts

# Fallback client configs tried in order when bot-detection fires
_FALLBACK_CLIENTS: list[dict] = [
    {"youtube": {"player_client": ["tv_embedded"]}},
    {"youtube": {"player_client": ["ios"]}},
    {"youtube": {"player_client": ["mweb"]}},
    {"youtube": {"player_client": ["android"]}},
    {"youtube": {"player_client": ["android_vr"]}},
]

def _is_bot_detection_error(e: Exception) -> bool:
    msg = str(e).lower()
    return any(kw in msg for kw in [
        "sign in", "signin", "bot", "confirm you", "403", "http error 429"
    ])

# ╔══════════════════════════════════════════╗
#   GLOBALS
# ╚══════════════════════════════════════════╝

download_queue:    asyncio.Queue    = None
active_downloads:  dict[int, str]   = {}
user_last_request: OrderedDict      = OrderedDict()

stats = {
    "users":      set(),
    "downloads":  0,
    "failed":     0,
    "start_time": time.time(),
}

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
    except RetryAfter as e:
        await asyncio.sleep(e.retry_after + 0.5)
        try:
            await msg.edit_text(text, **kwargs)
        except Exception:
            pass
    except BadRequest as e:
        if "not modified" not in str(e).lower():
            logger.debug("Edit skipped: %s", e)
    except NetworkError as e:
        logger.debug("Network error on edit: %s", e)

def record_rate_limit(user: int) -> None:
    user_last_request[user] = time.time()
    user_last_request.move_to_end(user)
    while len(user_last_request) > MAX_RATE_CACHE:
        user_last_request.popitem(last=False)

def user_facing_error(e: Exception) -> str:
    msg = str(e)
    low = msg.lower()
    if any(kw in low for kw in ["sign in", "bot", "confirm you"]):
        return (
            "❌ *YouTube blocked the download.*\n\n"
            "The bot tried multiple clients \\(tv\\_embedded, iOS, Android\\) "
            "but YouTube still requires authentication from this server IP\\.\n\n"
            "✅ *Fix options:*\n"
            "1\\. Add `cookies\\.txt` — export with:\n"
            "`yt\\-dlp \\-\\-cookies\\-from\\-browser chrome \\-\\-cookies cookies\\.txt`\n"
            "2\\. Install `youtube\\-po\\-token\\-generator`\n"
            "3\\. Set `HTTP_PROXY` env var to a residential proxy"
        )
    if "403" in msg:
        return "❌ *HTTP 403* — server IP blocked\\. Add cookies or a proxy\\."
    if "429" in msg:
        return "❌ *Rate limited \\(429\\)* — wait a few minutes and try again\\."
    if "not available" in low:
        return "❌ *Format not available\\.* Try a different resolution\\."
    if "private" in low:
        return "❌ *Video is private or age\\-restricted\\.* Add authenticated cookies\\."
    if "geo" in low or "not available in your country" in low:
        return "❌ *Video is geo\\-restricted* in the server's region\\."
    if "copyright" in low:
        return "❌ *Video blocked* due to a copyright claim\\."
    safe_msg = re.sub(r'([_*\[\]()~`>#+=|{}.!\\-])', r'\\\1', msg[:300])
    return f"❌ *Download failed*\n\n`{safe_msg}`"

# ╔══════════════════════════════════════════╗
#   VIDEO INFO  —  with client fallback
# ╚══════════════════════════════════════════╝

def _get_info_with_opts(url: str, opts: dict) -> dict:
    with yt_dlp.YoutubeDL(opts) as ydl:
        return ydl.extract_info(url, download=False)

def get_video_info(url: str) -> dict:
    """Try primary client config, then fall back through _FALLBACK_CLIENTS."""
    primary_opts = {**build_ydl_common(), "skip_download": True}
    try:
        return _get_info_with_opts(url, primary_opts)
    except Exception as e:
        if not _is_bot_detection_error(e):
            raise
        logger.warning("Primary client blocked (%s) — trying fallbacks…", e)

    for i, fb_args in enumerate(_FALLBACK_CLIENTS):
        try:
            opts = {**primary_opts, "extractor_args": fb_args}
            info = _get_info_with_opts(url, opts)
            logger.info("Fallback client #%d succeeded for info fetch.", i + 1)
            return info
        except Exception as fe:
            logger.debug("Fallback #%d failed: %s", i + 1, fe)

    raise Exception(
        "YouTube requires sign-in and no working client was found. "
        "Please add cookies.txt — see /help for instructions."
    )

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
    formats  = info.get("formats", [])
    heights  = sorted(set(
        f.get("height") for f in formats
        if f.get("height") and f.get("height") in (360, 480, 720, 1080)
    ))
    safe_url = url.replace("|", "%7C")
    rows     = []
    icons    = {360: "📱", 480: "💻", 720: "🖥", 1080: "📺"}
    vid_row  = []
    for h in heights:
        vid_row.append(InlineKeyboardButton(
            f"{icons.get(h, '📹')} {h}p",
            callback_data=f"mp4|{h}|{safe_url}"
        ))
        if len(vid_row) == 2:
            rows.append(vid_row)
            vid_row = []
    if vid_row:
        rows.append(vid_row)
    if not heights:
        rows.append([InlineKeyboardButton(
            "📹 Best quality", callback_data=f"mp4|best|{safe_url}"
        )])
    rows.append([
        InlineKeyboardButton("🎵 MP3 128kbps", callback_data=f"mp3|128|{safe_url}"),
        InlineKeyboardButton("🎵 MP3 320kbps", callback_data=f"mp3|320|{safe_url}"),
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
    await update.message.reply_text(
        "🎬 *Media Downloader Bot*\n\n"
        "Supported sites:\n"
        "• YouTube  • TikTok  • Instagram\n"
        "• Facebook  • Twitter/X  • Reddit\n"
        "• SoundCloud  • Twitch  • Vimeo\n\n"
        "Just paste a link and choose your format\\!\n\n"
        "📋 _Commands:_\n"
        "`/queue` — queue status\n"
        "`/stats` — bot statistics\n"
        "`/help`  — usage guide",
        parse_mode=ParseMode.MARKDOWN_V2,
    )

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    po, _         = get_po_token()
    mode          = "Webhook" if WEBHOOK_URL else "Polling"
    cookie_status = "✅ Loaded" if COOKIES_FILE.exists() else "❌ Not found"
    po_status     = "✅ Active" if po else "❌ Not available"
    await update.message.reply_text(
        "ℹ️ *How to use*\n\n"
        "1️⃣ Paste any supported video URL\n"
        "2️⃣ Wait for the format picker\n"
        "3️⃣ Choose MP4 resolution or MP3 quality\n"
        "4️⃣ File is sent with the song/video title as filename\n\n"
        "⚠️ *Limits*\n"
        f"• Max file size : `{fmt_size(MAX_FILE_SIZE)}`\n"
        f"• Queue slots   : `{MAX_QUEUE_SIZE}`\n"
        f"• Rate limit    : `{RATE_LIMIT_SEC}s` between requests\n\n"
        "🛡 *YouTube bot-detection fix*\n"
        "If YouTube blocks downloads, export cookies from your browser:\n"
        "`yt-dlp --cookies-from-browser chrome --cookies cookies.txt`\n"
        "Then place `cookies.txt` next to `bot.py`.\n\n"
        "💡 *Tips*\n"
        "• `MP3 320` = best audio quality\n"
        "• `360p` = fastest video download\n"
        "• Cached files are sent instantly ⚡\n\n"
        f"🔌 *Mode*       : `{mode}`\n"
        f"🔑 *PO Token*   : {po_status}\n"
        f"🍪 *Cookies*    : {cookie_status}",
        parse_mode=ParseMode.MARKDOWN,
    )

async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uptime = int(time.time() - stats["start_time"])
    total  = stats["downloads"] + stats["failed"]
    rate   = (stats["downloads"] / total * 100) if total else 0
    qsize  = download_queue.qsize() if download_queue else 0
    await update.message.reply_text(
        f"📊 *Bot Statistics*\n\n"
        f"👥 Unique users : `{len(stats['users'])}`\n"
        f"✅ Downloads    : `{stats['downloads']}`\n"
        f"❌ Failed       : `{stats['failed']}`\n"
        f"📈 Success rate : `{rate:.1f}%`\n"
        f"⏳ Queue now    : `{qsize}`\n"
        f"⚡ Workers      : `{WORKERS}`\n"
        f"🕐 Uptime       : `{fmt_uptime(uptime)}`",
        parse_mode=ParseMode.MARKDOWN,
    )

async def cmd_queue(update: Update, context: ContextTypes.DEFAULT_TYPE):
    running = len(active_downloads)
    bar_run = "🟢" * running + "⚪" * max(0, WORKERS - running)
    qsize   = download_queue.qsize() if download_queue else 0
    await update.message.reply_text(
        f"📋 *Queue Status*\n\n"
        f"🔄 Running  : {bar_run} `{running}/{WORKERS}`\n"
        f"⏳ Pending  : `{qsize}`\n"
        f"🔢 Capacity : `{MAX_QUEUE_SIZE}`",
        parse_mode=ParseMode.MARKDOWN,
    )

# ╔══════════════════════════════════════════╗
#   LINK HANDLER
# ╚══════════════════════════════════════════╝

async def handle_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    url  = update.message.text.strip()
    user = update.effective_user.id

    if not re.match(r"https?://", url):
        await update.message.reply_text(
            "❓ Send a valid video URL or use /help for instructions."
        )
        return

    now  = time.time()
    last = user_last_request.get(user, 0)
    if now - last < RATE_LIMIT_SEC:
        remaining = int(RATE_LIMIT_SEC - (now - last))
        await update.message.reply_text(
            f"⏳ Please wait `{remaining}s` before sending another link.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    record_rate_limit(user)
    stats["users"].add(user)

    if download_queue and download_queue.qsize() >= MAX_QUEUE_SIZE:
        await update.message.reply_text(
            "⚠️ *Queue is full!* Please try again in a moment.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    msg       = await update.message.reply_text("🔍 _Fetching info…_", parse_mode=ParseMode.MARKDOWN)
    stop_anim = asyncio.Event()
    anim_task = asyncio.create_task(animated_fetch(msg, stop_anim))

    try:
        info = await asyncio.wait_for(
            asyncio.get_event_loop().run_in_executor(None, get_video_info, url),
            timeout=60,
        )
    except asyncio.TimeoutError:
        stop_anim.set()
        await anim_task
        await safe_edit(
            msg,
            "❌ *Timed out fetching video info.* The site may be slow or unreachable.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return
    except Exception as e:
        logger.error("Info fetch error: %s", e)
        stop_anim.set()
        await anim_task
        await safe_edit(msg, user_facing_error(e), parse_mode=ParseMode.MARKDOWN_V2)
        return
    else:
        stop_anim.set()
        await anim_task

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
        try:
            await query.message.delete()
        except Exception:
            pass
        return

    parts = query.data.split("|", 2)
    if len(parts) != 3 or parts[0] not in ("mp3", "mp4"):
        await query.answer("⚠️ Invalid selection.", show_alert=True)
        return

    typ, quality, safe_url = parts
    url = safe_url.replace("%7C", "|")
    pos = (download_queue.qsize() if download_queue else 0) + 1

    try:
        await query.edit_message_caption(
            caption=f"✅ *Added to queue — position #{pos}*",
            parse_mode=ParseMode.MARKDOWN,
        )
    except BadRequest:
        try:
            await query.edit_message_text(
                text=f"✅ *Added to queue — position #{pos}*",
                parse_mode=ParseMode.MARKDOWN,
            )
        except Exception as e:
            logger.debug("Could not edit queued message: %s", e)

    label      = f"🎵 MP3 {quality}kbps" if typ == "mp3" else f"🎥 MP4 {quality}p"
    status_msg = await query.message.reply_text(
        f"🕐 *Queued* — {label}\n_Waiting for a free worker…_",
        parse_mode=ParseMode.MARKDOWN,
    )

    if download_queue:
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
            try:
                await asyncio.wait_for(
                    process(query, typ, quality, url, msg, worker_id),
                    timeout=DOWNLOAD_TIMEOUT,
                )
            except asyncio.TimeoutError:
                logger.error("Worker %d timed out on %s", worker_id, url)
                stats["failed"] += 1
                await safe_edit(msg, "❌ *Download timed out* — video may be too long.",
                                parse_mode=ParseMode.MARKDOWN)
        except asyncio.CancelledError:
            logger.info("Worker %d shutting down.", worker_id)
            break
        except Exception as e:
            logger.exception("Worker %d crashed: %s", worker_id, e)
        finally:
            active_downloads.pop(worker_id, None)
            download_queue.task_done()

# ╔══════════════════════════════════════════╗
#   PROCESS / DOWNLOAD  (with client fallback)
# ╚══════════════════════════════════════════╝

async def process(query, typ: str, quality: str, url: str, msg, worker_id: int):
    loop = asyncio.get_event_loop()

    # ── Cache check ──────────────────────────────────────────────
    ck         = cache_key(url, typ, quality)
    cache_path = CACHE_FOLDER / ck

    if cache_path.exists():
        await safe_edit(msg, "⚡ *Sending from cache…*", parse_mode=ParseMode.MARKDOWN)
        try:
            caption = f"✅ {'🎵 Audio' if typ == 'mp3' else '🎥 Video'} _(cached)_"
            with open(cache_path, "rb") as f:
                if typ == "mp3":
                    await msg.reply_audio(f, caption=caption, parse_mode=ParseMode.MARKDOWN,
                                          read_timeout=120, write_timeout=120)
                else:
                    await msg.reply_video(f, caption=caption, parse_mode=ParseMode.MARKDOWN,
                                          supports_streaming=True,
                                          read_timeout=120, write_timeout=120)
            await msg.delete()
            stats["downloads"] += 1
            return
        except Exception as e:
            logger.error("Cache send failed (%s) — re-downloading.", e)
            cache_path.unlink(missing_ok=True)

    # ── Fetch clean title ─────────────────────────────────────────
    try:
        info        = await loop.run_in_executor(None, get_video_info, url)
        clean_title = sanitize_title(info.get("title") or "download")
    except Exception:
        clean_title = "download"

    # ── Build yt-dlp format string ────────────────────────────────
    if typ == "mp3":
        fmt = "bestaudio/best"
    elif quality == "best":
        fmt = "bestvideo+bestaudio/best"
    else:
        fmt = (
            f"bestvideo[height<={quality}][ext=mp4]+bestaudio[ext=m4a]"
            f"/bestvideo[height<={quality}]+bestaudio"
            f"/best[height<={quality}]/best"
        )

    output_template = str(DOWNLOAD_FOLDER / f"{clean_title}.%(ext)s")

    # ── Progress hook ─────────────────────────────────────────────
    frame_counter = {"n": 0, "last_pct": -1.0}

    def hook(d: dict) -> None:
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
        speed    = (d.get("_speed_str")   or "—").strip()
        eta      = (d.get("_eta_str")     or "—").strip()
        size_str = (d.get("_total_bytes_str") or
                    d.get("_total_bytes_estimate_str") or "—").strip()
        phase    = "🎵 Audio" if typ == "mp3" else "🎥 Video"

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

    # ── Build base ydl opts ───────────────────────────────────────
    base_ydl_opts = {
        **build_ydl_common(),
        "format":         fmt,
        "outtmpl":        output_template,
        "progress_hooks": [hook],
    }
    if typ == "mp4":
        base_ydl_opts["merge_output_format"] = "mp4"
    if typ == "mp3":
        base_ydl_opts["postprocessors"] = [{
            "key":              "FFmpegExtractAudio",
            "preferredcodec":   "mp3",
            "preferredquality": quality,
        }]

    await safe_edit(msg, "⬇️ _Starting download…_", parse_mode=ParseMode.MARKDOWN)

    # ── Download with automatic client fallback ───────────────────
    file_path  = None
    last_error = None

    try:
        file_path = await loop.run_in_executor(
            None, lambda: _run_ydl(base_ydl_opts, url, typ, clean_title)
        )
    except Exception as e:
        last_error = e
        if not _is_bot_detection_error(e):
            stats["failed"] += 1
            await safe_edit(msg, user_facing_error(e), parse_mode=ParseMode.MARKDOWN_V2)
            return
        logger.warning("Primary download blocked (%s) — trying fallbacks…", e)

    if file_path is None and last_error is not None:
        for i, fb_args in enumerate(_FALLBACK_CLIENTS):
            client_name = fb_args["youtube"]["player_client"][0]
            await safe_edit(
                msg,
                f"🔄 _Retrying with `{client_name}` client "
                f"({i + 1}/{len(_FALLBACK_CLIENTS)})…_",
                parse_mode=ParseMode.MARKDOWN,
            )
            try:
                fb_opts   = {**base_ydl_opts, "extractor_args": fb_args}
                file_path = await loop.run_in_executor(
                    None, lambda o=fb_opts: _run_ydl(o, url, typ, clean_title)
                )
                if file_path:
                    logger.info("Fallback `%s` succeeded.", client_name)
                    last_error = None
                    break
            except Exception as fe:
                last_error = fe
                logger.debug("Fallback `%s` failed: %s", client_name, fe)

    if last_error and not file_path:
        stats["failed"] += 1
        await safe_edit(msg, user_facing_error(last_error), parse_mode=ParseMode.MARKDOWN_V2)
        return

    if not file_path or not Path(file_path).exists():
        stats["failed"] += 1
        await safe_edit(
            msg,
            "❌ *File not found after download.*\n"
            "The video may be geo-restricted or too long.",
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
            "Try:\n• MP3 instead of MP4\n• Lower resolution\n• Shorter video",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    await safe_edit(
        msg,
        f"📤 *Uploading* `{fp.name}` — `{fmt_size(file_size)}`…",
        parse_mode=ParseMode.MARKDOWN,
    )

    # Cache BEFORE sending (and before finally-delete)
    try:
        shutil.copy2(fp, cache_path)
    except Exception as ce:
        logger.warning("Cache write failed: %s", ce)

    try:
        with open(fp, "rb") as f:
            caption = f"✅ *{fp.stem}*"
            if typ == "mp3":
                await msg.reply_audio(audio=f, caption=caption, title=fp.stem,
                                      parse_mode=ParseMode.MARKDOWN,
                                      read_timeout=180, write_timeout=180)
            else:
                await msg.reply_video(video=f, caption=caption,
                                      parse_mode=ParseMode.MARKDOWN,
                                      supports_streaming=True,
                                      read_timeout=180, write_timeout=180)
        await msg.delete()
        stats["downloads"] += 1

    except Exception as e:
        logger.error("Upload error: %s", e)
        stats["failed"] += 1
        cache_path.unlink(missing_ok=True)
        await safe_edit(msg, f"❌ *Upload failed*\n\n`{str(e)[:200]}`",
                        parse_mode=ParseMode.MARKDOWN)
    finally:
        fp.unlink(missing_ok=True)

# ╔══════════════════════════════════════════╗
#   BLOCKING YT-DLP CALL  (runs in executor)
# ╚══════════════════════════════════════════╝

def _run_ydl(opts: dict, url: str, typ: str, clean_title: str) -> str | None:
    with yt_dlp.YoutubeDL(opts) as ydl:
        info     = ydl.extract_info(url, download=True)
        raw_path = ydl.prepare_filename(info)

    if typ == "mp3":
        mp3_path = Path(raw_path).with_suffix(".mp3")
        if mp3_path.exists():
            return str(mp3_path)
        candidates = sorted(
            (f for f in DOWNLOAD_FOLDER.iterdir() if f.suffix == ".mp3"),
            key=lambda p: p.stat().st_mtime, reverse=True,
        )
        return str(candidates[0]) if candidates else None

    final = Path(raw_path)
    if final.exists():
        return str(final)

    for ext in (".mp4", ".mkv", ".webm"):
        candidates = sorted(
            (f for f in DOWNLOAD_FOLDER.iterdir() if f.suffix == ext),
            key=lambda p: p.stat().st_mtime, reverse=True,
        )
        if candidates:
            return str(candidates[0])
    return None

# ╔══════════════════════════════════════════╗
#   GLOBAL ERROR HANDLER
# ╚══════════════════════════════════════════╝

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.error("Unhandled exception", exc_info=context.error)

# ╔══════════════════════════════════════════╗
#   APP SETUP
# ╚══════════════════════════════════════════╝

async def post_init(app):
    global download_queue
    download_queue = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)
    for i in range(WORKERS):
        asyncio.create_task(worker(i + 1))
    po, _ = get_po_token()
    logger.info(
        "%d workers started | mode=%s | PO token=%s | cookies=%s",
        WORKERS,
        "webhook" if WEBHOOK_URL else "polling",
        "yes" if po else "no",
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
    app.add_error_handler(error_handler)

    if WEBHOOK_URL:
        logger.info("Starting WEBHOOK mode on port %d", PORT)
        app.run_webhook(
            listen="0.0.0.0",
            port=PORT,
            webhook_url=WEBHOOK_URL,
            drop_pending_updates=True,
        )
    else:
        logger.info("Starting POLLING mode")
        app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()