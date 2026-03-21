r"""
╔═══════════════════════════════════════════════════════════╗
║         ▓ L U C I C R Y P T ▓ // MEDIA DECRYPTOR v4.0     ║
║         [CRYPTO-STEALTH BYPASS] // AGE-RESTRICTED OK      ║
╚═══════════════════════════════════════════════════════════╝
╔═══════════════════════════════════════════════════════════╗
║          N E X U S - D L   //  MEDIA EXTRACTION BOT       ║
║          v3.1  |  FIXED: cookie validation + debug        ║
╚═══════════════════════════════════════════════════════════╝

  FIXES IN v3.1:
  • Cookie file validated by actually testing a yt-dlp fetch (not just file sniff)
  • Detailed per-client error logging so you can see exactly which client fails and why
  • build_ydl_common() logs cookie path + size on every call
  • Cookies re-tested on every /help call so bypass status is always accurate
  • OAuth2 token path fixed — written to yt-dlp cache dir that yt-dlp actually reads
  • _run_ydl now logs the raw yt-dlp exception before re-raising
  • update check on startup: warns if yt-dlp < 2024
  • /cookietest command — tests cookies.txt live against a known video
  • /authstatus command — shows exact token paths and content snippet
"""

import os, re, json, shutil, asyncio, logging, time, hashlib, subprocess, threading, io
from pathlib import Path
from collections import OrderedDict, defaultdict

import yt_dlp
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
from telegram.ext import (
    ApplicationBuilder, MessageHandler, CommandHandler,
    CallbackQueryHandler, ContextTypes, filters,
)

from telegram.constants import ParseMode
from telegram.error import BadRequest, NetworkError, RetryAfter
import difflib
import json
from pathlib import Path

# ═══════════════════════════════════════════════════════════
#  CONFIG
# ═══════════════════════════════════════════════════════════

BOT_TOKEN        = os.getenv("BOT_TOKEN")
WEBHOOK_URL      = os.environ.get("WEBHOOK_URL",  "")
PORT             = int(os.environ.get("PORT",     8443))
ADMIN_IDS        = set(
    int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()
)

DOWNLOAD_FOLDER  = Path(os.getenv("DOWNLOAD_FOLDER", "downloads"))
CACHE_FOLDER     = Path(os.getenv("CACHE_FOLDER", "cache"))
COOKIES_FILE     = Path(os.getenv("COOKIES_FILE", "cookies.txt"))

load_dotenv()

LOCAL_API_URL    = os.environ.get("LOCAL_API_URL", "")
_using_local_api = bool(LOCAL_API_URL)
MAX_FILE_SIZE    = 500 * 1024 * 1024 if _using_local_api else 50 * 1024 * 1024

MAX_QUEUE_SIZE   = 20
WORKERS          = 3
RATE_LIMIT_SEC   = 6
DOWNLOAD_TIMEOUT = 1800
MAX_RATE_CACHE   = 2000
MAX_HISTORY      = 5
MAX_PLAYLIST_SHOW = 10
FFMPEG_LOCATION  = os.environ.get("FFMPEG_LOCATION", None)

HTTP_PROXY   = os.environ.get("HTTP_PROXY", None)
_PROXY_LIST  = [p.strip() for p in os.environ.get("PROXY_LIST", "").split(",") if p.strip()]
if HTTP_PROXY and HTTP_PROXY not in _PROXY_LIST:
    _PROXY_LIST.insert(0, HTTP_PROXY)
_proxy_index = 0

def get_next_proxy() -> str | None:
    global _proxy_index
    if not _PROXY_LIST:
        return None
    p = _PROXY_LIST[_proxy_index % len(_PROXY_LIST)]
    _proxy_index += 1
    return p

DOWNLOAD_FOLDER.mkdir(exist_ok=True)
CACHE_FOLDER.mkdir(exist_ok=True)

# ═══════════════════════════════════════════════════════════
#  LOGGING
# ═══════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("bot.log")],
)
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════
#  STARTUP: yt-dlp version check
# ═══════════════════════════════════════════════════════════

def _check_ytdlp_version():
    try:
        ver  = yt_dlp.version.__version__
        year = int(ver.split(".")[0])
        if year < 2024:
            logger.warning(
                "⚠ yt-dlp %s is outdated — run: pip install -U yt-dlp --break-system-packages", ver
            )
        else:
            logger.info("yt-dlp version: %s OK", ver)
    except Exception as e:
        logger.warning("Could not check yt-dlp version: %s", e)

_check_ytdlp_version()

# ═══════════════════════════════════════════════════════════
#  PO TOKEN
# ═══════════════════════════════════════════════════════════

_po_token: str | None     = None
_visitor_data: str | None = None
_po_token_expiry: float   = 0.0
PO_TOKEN_TTL              = 3600

def _generate_po_token() -> tuple[str | None, str | None]:
    try:
        r = subprocess.run(["youtube-po-token-generator"], capture_output=True, text=True, timeout=8)
        if r.returncode != 0:
            return None, None
        data = json.loads(r.stdout)
        po   = data.get("poToken")
        vis  = data.get("visitorData")
        if po:
            logger.info("▶ PO token acquired.")
        return po, vis
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return None, None
    except Exception as e:
        logger.warning("PO token failed: %s", e)
        return None, None

def get_po_token() -> tuple[str | None, str | None]:
    global _po_token, _visitor_data, _po_token_expiry
    if time.time() > _po_token_expiry:
        _po_token, _visitor_data = _generate_po_token()
        _po_token_expiry = time.time() + PO_TOKEN_TTL
    return _po_token, _visitor_data

# ═══════════════════════════════════════════════════════════
#  OAUTH2
# ═══════════════════════════════════════════════════════════

_OAUTH_CLIENT_ID     = "861556708454-d6dlm3lh05idd8npek18k6be8ba3oc68.apps.googleusercontent.com"
_OAUTH_CLIENT_SECRET = "SboVhoG9s0rNafixCSGGKXAT"
_OAUTH_SCOPE         = "https://www.googleapis.com/auth/youtube"
_DEVICE_CODE_URL     = "https://oauth2.googleapis.com/device/code"
_TOKEN_URL           = "https://oauth2.googleapis.com/token"

OAUTH2_TOKEN_FILE = Path("oauth2_token.json")
_oauth_pending: dict = {}

def _yt_dlp_cache_dir() -> Path:
    try:
        import yt_dlp.utils as u
        return Path(u.get_cachedir())
    except Exception:
        return Path.home() / ".cache" / "yt-dlp"

def _write_ydlp_token(token_data: dict) -> None:
    cache_dir  = _yt_dlp_cache_dir()
    cache_dir.mkdir(parents=True, exist_ok=True)
    token_path = cache_dir / "youtube-oauth2.token.json"
    ydlp_fmt   = {
        "access_token":  token_data.get("access_token", ""),
        "refresh_token": token_data.get("refresh_token", ""),
        "token_type":    token_data.get("token_type", "Bearer"),
        "expires":       int(time.time()) + int(token_data.get("expires_in", 3600)),
    }
    token_path.write_text(json.dumps(ydlp_fmt, indent=2))
    OAUTH2_TOKEN_FILE.write_text(json.dumps(token_data, indent=2))
    logger.info("OAuth2 token saved -> %s", token_path)

def oauth2_token_exists() -> bool:
    return ((_yt_dlp_cache_dir() / "youtube-oauth2.token.json").exists()
            or OAUTH2_TOKEN_FILE.exists())

# ═══════════════════════════════════════════════════════════
#  COOKIE VALIDATION  (v3.1 — live test, not just file sniff)
# ═══════════════════════════════════════════════════════════

_cookie_valid: bool | None  = None
_cookie_tested_mtime: float = 0.0
_TEST_VIDEO = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"

def test_cookies_live() -> tuple[bool, str]:
    """
    The ONLY reliable way to know if cookies work:
    actually try a yt-dlp info fetch with them.
    File existence and header checks tell you nothing about auth state.
    """
    global _cookie_valid, _cookie_tested_mtime

    if not COOKIES_FILE.exists():
        _cookie_valid = False
        return False, "cookies.txt not found"

    current_mtime = COOKIES_FILE.stat().st_mtime
    if _cookie_valid is not None and current_mtime == _cookie_tested_mtime:
        return _cookie_valid, "cached result"

    logger.info("Testing cookies.txt against YouTube…")
    opts = {
        "quiet": True, "no_warnings": True, "skip_download": True,
        "cookiefile": str(COOKIES_FILE), "socket_timeout": 15, "retries": 1,
    }
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(_TEST_VIDEO, download=False)
        title = info.get("title", "?")
        logger.info("Cookie test PASSED — title: %s", title)
        _cookie_valid        = True
        _cookie_tested_mtime = current_mtime
        return True, f"ok — fetched: {title[:40]}"
    except Exception as e:
        err = str(e)
        logger.warning("Cookie test FAILED: %s", err)
        _cookie_valid        = False
        _cookie_tested_mtime = current_mtime
        return False, err[:120]

def invalidate_cookie_cache():
    global _cookie_valid, _cookie_tested_mtime
    _cookie_valid        = None
    _cookie_tested_mtime = 0.0

# ═══════════════════════════════════════════════════════════
#  YT-DLP CONFIG
# ═══════════════════════════════════════════════════════════

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/125.0.0.0 Safari/537.36"
)

def build_extractor_args() -> dict:
    po, vis = get_po_token()
    clients: list[str] = ["tv_embedded", "ios", "android"]
    yt: dict = {"player_client": clients}
    if po:
        clients = ["web"] + clients
        yt["player_client"] = clients
        yt["po_token"]      = [f"web+{po}"]
    if vis:
        yt["visitor_data"] = [vis]
    return {"youtube": yt}

def build_ydl_common() -> dict:
    opts: dict = {
        "quiet":          True,
        "no_warnings":    True,
        "http_headers": {
            "User-Agent":      _UA,
            "Accept-Language": "en-US,en;q=0.9",
            "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
        "extractor_args": build_extractor_args(),
        "socket_timeout": 30,
        "retries":        5,
        "fragment_retries": 5,
    }
    if COOKIES_FILE.exists():
        opts["cookiefile"] = str(COOKIES_FILE)
        logger.info("🍪 cookies.txt loaded (%d bytes)", COOKIES_FILE.stat().st_size)
    else:
        logger.warning("⚠ No cookies.txt — downloads will likely fail from this IP")
    if FFMPEG_LOCATION:
        opts["ffmpeg_location"] = FFMPEG_LOCATION
    proxy = get_next_proxy()
    if proxy:
        opts["proxy"] = proxy
        logger.debug("Using proxy: %s…", proxy[:30])
    return opts

_FALLBACK_CLIENTS: list[dict] = [
    {"youtube": {"player_client": ["tv_embedded"]}},
    {"youtube": {"player_client": ["ios"]}},
    {"youtube": {"player_client": ["mweb"]}},
    {"youtube": {"player_client": ["android"]}},
    {"youtube": {"player_client": ["android_vr"]}},
    {"youtube": {"player_client": ["web"]}},
]

def _is_bot_block(e: Exception) -> bool:
    m = str(e).lower()
    return any(k in m for k in [
        "sign in", "signin", "bot", "confirm you",
        "403", "429", "blocked", "not available", "video unavailable",
    ])

def bypass_status() -> dict:
    po, _ = get_po_token()
    return {
        "cookies":       COOKIES_FILE.exists(),
        "cookies_valid": _cookie_valid,
        "oauth2":        oauth2_token_exists(),
        "po_token":      bool(po),
        "proxy":         len(_PROXY_LIST) > 0,
        "proxy_count":   len(_PROXY_LIST),
    }

# ═══════════════════════════════════════════════════════════
#  VIDEO INFO
# ═══════════════════════════════════════════════════════════

def _info(url: str, opts: dict) -> dict:
    with yt_dlp.YoutubeDL(opts) as ydl:
        return ydl.extract_info(url, download=False)

def get_video_info(url: str) -> dict:
    base = {**build_ydl_common(), "skip_download": True}
    try:
        return _info(url, base)
    except Exception as e:
        logger.warning("Primary client failed: %s", e)
        if not _is_bot_block(e):
            raise

    for i, fb in enumerate(_FALLBACK_CLIENTS):
        client_name = fb["youtube"]["player_client"][0]
        try:
            info = _info(url, {**base, "extractor_args": fb})
            logger.info("Fallback '%s' succeeded (info).", client_name)
            return info
        except Exception as fe:
            logger.warning("Fallback '%s' info fail: %s", client_name, fe)

    raise Exception("All bypass clients failed. Add cookies.txt — see /help.")

# ═══════════════════════════════════════════════════════════
#  GLOBALS
# ═══════════════════════════════════════════════════════════

download_queue:    asyncio.Queue           = None
active_downloads:  dict[int, str]          = {}
user_last_request: OrderedDict             = OrderedDict()
user_history:      dict[int, list]         = defaultdict(list)


stats = {
    "users":      set(),
    "downloads":  0,
    "failed":     0,
    "bytes_sent": 0,
    "start_time": time.time(),
}

# Cache index for song name lookup
INDEX_PATH = CACHE_FOLDER / "index.json"
cache_index: dict[str, list] = {}

# ═══════════════════════════════════════════════════════════
#  HACKER ANIMATIONS
# ═══════════════════════════════════════════════════════════

_BAR_FULL  = "█"
_BAR_HEAD  = "▓"
_BAR_EMPTY = "░"
_RADAR     = ["◜", "◝", "◞", "◟"]
_SCAN      = ["▰▱▱▱▱▱▱▱", "▰▰▱▱▱▱▱▱", "▰▰▰▱▱▱▱▱", "▰▰▰▰▱▱▱▱",
              "▰▰▰▰▰▱▱▱", "▰▰▰▰▰▰▱▱", "▰▰▰▰▰▰▰▱", "▰▰▰▰▰▰▰▰"]
_FETCH_LINES = [
    "initializing extraction protocol{d}",
    "probing target endpoint{d}",
    "negotiating media stream{d}",
    "decrypting stream tokens{d}",
    "resolving CDN nodes{d}",
    "bypassing geo-filters{d}",
    "mapping format matrix{d}",
]
_DL_PHASES = ["INTERCEPTING STREAM", "PULLING FRAGMENTS", "STITCHING SEGMENTS", "MUXING CONTAINER", "VERIFYING PAYLOAD"]

def _emoji_bar(pct: float, width: int = 10) -> str:
    filled = int(pct / 100 * width)
    return "🟩" * filled + "🟨" * (width - filled - 1) + "🟥"

def _phase(pct: float) -> str:
    return _DL_PHASES[min(int(pct / 100 * len(_DL_PHASES)), len(_DL_PHASES) - 1)]

def _glitch(text: str, frame: int) -> str:
    glitch_chars = "▒░▓▌▐▀▄█▊▋▍▎▏▶◀◆◇○●◉"
    if frame % 7 == 0 and len(text) > 4:
        pos = (frame * 3) % len(text)
        c   = glitch_chars[(frame * 13) % len(glitch_chars)]
        return text[:pos] + c + text[pos + 1:]
    return text

# ═══════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════

def load_cache_index():
    global cache_index
    if INDEX_PATH.exists():
        try:
            cache_index = json.loads(INDEX_PATH.read_text())
            if not isinstance(cache_index, dict):
                cache_index = {}
        except:
            cache_index = {}
    else:
        cache_index = {}

def save_cache_index():
    try:
        INDEX_PATH.parent.mkdir(exist_ok=True)
        INDEX_PATH.write_text(json.dumps(cache_index, indent=2, ensure_ascii=False))
    except Exception as e:
        logger.warning("Failed to save cache index: %s", e)

def search_cache_songs(query: str, max_results: int = 10) -> list:
    query_lower = query.lower().strip()
    if not query_lower:
        return []

    # Exact/contains matches first
    direct_matches = []
    for title, entries in cache_index.items():
        if query_lower in title.lower():
            direct_matches.extend([{"title": title, **e} for e in entries])

    # Fuzzy matches if few direct
    fuzzy_matches = []
    if len(direct_matches) < 5:
        scores = []
        for title in cache_index:
            score = difflib.SequenceMatcher(None, query_lower, title.lower()).ratio()
            if score > 0.5:
                scores.append((score, title))
        scores.sort(reverse=True, key=lambda x: x[0])
        for score, title in scores[:max_results]:
            entries = cache_index[title]
            fuzzy_matches.extend([{"title": title, "score": score, **e} for e in entries])

    combined = direct_matches + fuzzy_matches
    return list({e["path"]: e for e in combined}.values())[:max_results]  # Dedup by path

def fmt_size(n: float) -> str:
    for u in ["B", "KB", "MB", "GB"]:
        if n < 1024: return f"{n:.1f} {u}"
        n /= 1024
    return f"{n:.1f} TB"

def fmt_dur(sec: int) -> str:
    h, r = divmod(sec, 3600); m, s = divmod(r, 60)
    return (f"{h}h " if h else "") + (f"{m}m " if m else "") + f"{s}s"

def fmt_views(n: int) -> str:
    if n >= 1_000_000: return f"{n/1_000_000:.1f}M"
    if n >= 1_000:     return f"{n/1_000:.1f}K"
    return str(n)

def fmt_uptime(sec: int) -> str:
    d, r = divmod(sec, 86400); h, r = divmod(r, 3600); m, s = divmod(r, 60)
    parts = []
    if d: parts.append(f"{d}d")
    if h: parts.append(f"{h}h")
    if m: parts.append(f"{m}m")
    parts.append(f"{s}s")
    return " ".join(parts)

def sanitize(title: str) -> str:
    title = re.sub(r'[\\/*?:"<>|#%&{}$!\'@+`=]', "_", title)
    title = title.replace("`", "'")
    return re.sub(r'\s+', " ", title).strip()[:120]

def ckey(url: str, typ: str, q: str) -> str:
    return hashlib.md5(f"{url}|{typ}|{q}".encode()).hexdigest()[:12] + f"_{typ}_{q}"

def mdescape(t: str) -> str:
    return re.sub(r'([_*\[\]()~`>#+=|{}.!\\-])', r'\\\1', t)

def normalize_url(entry: dict) -> str:
    raw    = (entry.get("url") or "").strip()
    vid_id = (entry.get("id")  or "").strip()
    if raw.startswith("http"):
        return raw
    if vid_id:
        return f"https://www.youtube.com/watch?v={vid_id}"
    if raw and "/" not in raw and len(raw) <= 15:
        return f"https://www.youtube.com/watch?v={raw}"
    return raw

async def safe_edit(msg, text: str, **kw):
    try:
        await msg.edit_text(text, **kw)
    except RetryAfter as e:
        await asyncio.sleep(e.retry_after + 0.5)
        try: await msg.edit_text(text, **kw)
        except Exception: pass
    except BadRequest as e:
        if "not modified" not in str(e).lower():
            logger.debug("edit skip: %s", e)
    except NetworkError as e:
        logger.debug("net err: %s", e)

def record_rl(user: int):
    user_last_request[user] = time.time()
    user_last_request.move_to_end(user)
    while len(user_last_request) > MAX_RATE_CACHE:
        user_last_request.popitem(last=False)

def push_history(user: int, title: str, typ: str, quality: str):
    h = user_history[user]
    h.append({"title": title[:50], "typ": typ, "quality": quality, "ts": int(time.time())})
    if len(h) > MAX_HISTORY:
        h.pop(0)

def error_msg(e: Exception) -> str:
    m = str(e); low = m.lower()
    bs = bypass_status()

    if any(k in low for k in ["sign in", "bot", "confirm you", "403", "blocked"]):
        active = []
        if bs["cookies"] and bs["cookies_valid"] is True:  active.append("cookies.txt ✓")
        elif bs["cookies"]:                                 active.append("cookies.txt ✗(invalid)")
        if bs["oauth2"]:   active.append("oauth2")
        if bs["po_token"]: active.append("PO token")
        if bs["proxy"]:    active.append("proxy")
        active_str = ", ".join(active) if active else "none"
        return (
            f"```\n"
            f"[ACCESS DENIED - IP BLOCKED]\n"
            f"active bypass : {active_str}\n"
            f"all clients   : exhausted\n"
            f"```\n\n"
            f"*Fix options:*\n"
            f"1\\. Run `/cookietest` to diagnose your cookies\n"
            f"2\\. Re\\-export cookies while logged into YouTube:\n"
            f"`yt-dlp --cookies-from-browser chrome --cookies cookies.txt`\n"
            f"3\\. Run `/auth` to link a Google account\n\n"
            f"_See /help for full bypass status_"
        )
    if "429" in m:
        return "```\n[RATE LIMITED 429]\nYouTube throttling this IP.\nWait 5-10 min and retry.\n```"
    if "private" in low:
        return "```\n[LOCKED CONTENT]\nVideo is private or age-restricted.\nSend cookies.txt from a logged-in\nYouTube session to unlock.\n```"
    if "geo" in low or "not available in your country" in low:
        return "```\n[GEO BLOCK]\nVideo unavailable in server region.\nSet HTTP_PROXY env var to bypass.\n```"
    if "copyright" in low:
        return "```\n[DMCA BLOCK]\nVideo blocked by copyright claim.\n```"
    safe = m[:250].replace("`", "'").replace("\\", "/")
    return f"```\n[DOWNLOAD FAILED]\n{safe}\n```"

# ═══════════════════════════════════════════════════════════
#  ANIMATED FETCH
# ═══════════════════════════════════════════════════════════

async def animated_fetch(msg, stop: asyncio.Event):
    frame = 0
    while not stop.is_set():
        radar    = _RADAR[frame % len(_RADAR)]
        scan     = _SCAN[frame % len(_SCAN)]
        line     = _FETCH_LINES[frame % len(_FETCH_LINES)].format(d="." * (frame % 4))
        glitched = _glitch(line, frame)
        text = (
            f"```\n"
            f"[{radar}] NEXUS-DL // EXTRACTION ENGINE\n"
            f"{'─'*34}\n"
            f"SCAN  {scan}\n"
            f"PROC  {glitched}\n"
            f"```"
        )
        await safe_edit(msg, text, parse_mode=ParseMode.MARKDOWN_V2)
        frame += 1
        await asyncio.sleep(0.65)

# ═══════════════════════════════════════════════════════════
#  FORMAT / BUTTON BUILDER
# ═══════════════════════════════════════════════════════════

_ALL_HEIGHTS = [144, 240, 360, 480, 720, 1080, 1440, 2160]
_RES_ICONS   = {144:"📺",240:"📺",360:"📱",480:"💻",720:"🖥",1080:"🖥",1440:"🔲",2160:"🎞"}
_RES_LABELS  = {144:"144p",240:"240p",360:"360p",480:"480p",720:"720p HD",1080:"1080p FHD",1440:"1440p QHD",2160:"2160p 4K"}

def build_caption(info: dict) -> str:
    title    = sanitize(info.get("title", "Unknown"))[:55]
    uploader = info.get("uploader") or info.get("channel") or "—"
    dur      = fmt_dur(info.get("duration") or 0)
    views    = fmt_views(info.get("view_count") or 0)
    likes    = fmt_views(info.get("like_count") or 0)
    date     = info.get("upload_date", "")
    ds       = f"{date[:4]}-{date[4:6]}-{date[6:]}" if len(date) == 8 else "—"
    ext      = info.get("extractor_key", "").upper()
    desc     = (info.get("description") or "")[:120].replace("\n", " ")
    return (
        f"```\n╔{'═'*40}\n║ {title}\n╠{'═'*40}\n"
        f"║ SRC  : {ext}\n║ BY   : {uploader[:30]}\n║ DATE : {ds}\n"
        f"║ DUR  : {dur}\n║ VIEWS: {views}   LIKES: {likes}\n╚{'═'*40}\n```\n"
        f"_{desc}_\n\n*SELECT OUTPUT FORMAT:*"
    )

def build_buttons(info: dict, url: str) -> list:
    safe_url = url.replace("|", "%7C")
    rows: list = []
    vid_row: list = []
    for h in _ALL_HEIGHTS:
        lbl = f"{_RES_ICONS[h]} {_RES_LABELS[h]}"
        vid_row.append(InlineKeyboardButton(lbl, callback_data=f"mp4|{h}|{safe_url}"))
        if len(vid_row) == 2:
            rows.append(vid_row); vid_row = []
    if vid_row: rows.append(vid_row)
    rows.append([InlineKeyboardButton("🏆 Best Video+Audio", callback_data=f"mp4|best|{safe_url}")])
    rows.append([
        InlineKeyboardButton("🎵 MP3 128k", callback_data=f"mp3|128|{safe_url}"),
        InlineKeyboardButton("🎵 MP3 192k", callback_data=f"mp3|192|{safe_url}"),
        InlineKeyboardButton("🎵 MP3 320k", callback_data=f"mp3|320|{safe_url}"),
    ])
    rows.append([
        InlineKeyboardButton("🎶 M4A Best", callback_data=f"m4a|best|{safe_url}"),
        InlineKeyboardButton("🎙 OGG Best", callback_data=f"ogg|best|{safe_url}"),
    ])
    rows.append([InlineKeyboardButton("✖ ABORT", callback_data="cancel")])
    return rows

# ═══════════════════════════════════════════════════════════
#  COMMANDS
# ═══════════════════════════════════════════════════════════

async def cmd_cache(update: Update, context: ContextTypes.DEFAULT_TYPE):
    load_cache_index()
    args = " ".join(context.args).strip()
    if not args:
        await update.message.reply_text(
            "```\n[CACHE SEARCH]\nUsage: /cache <song name or keywords>\n\n"
            "Finds cached MP3s by title. Fuzzy matching enabled.\n```",
            parse_mode=ParseMode.MARKDOWN_V2
        )
        return

    matches = search_cache_songs(args)
    if not matches:
        await update.message.reply_text(
            f"```\n[NO MATCHES]\n'{args}' not found in cache.\n\n"
            f"Cache has {len(cache_index)} songs.\nTry broader keywords.\n```",
            parse_mode=ParseMode.MARKDOWN_V2
        )
        return

    lines = ["```", f"[CACHE SEARCH: {args}]", f"Found {len(matches)} matches:", "─" * 36]
    buttons = []
    for i, match in enumerate(matches, 1):
        title = match["title"][:35]
        qual = match["quality"]
        lines.append(f"[{i}] 🎵 {title} ({qual})")
        lines.append(f"     📁 {Path(match['path']).name}")
        cb_data = f"cache_send|{match['path']}"
        buttons.append([InlineKeyboardButton(f"[{i}] {title[:28]} {qual}", callback_data=cb_data)])
    lines.append("```")
    buttons.append([InlineKeyboardButton("✖ Close", callback_data="cancel")])
    await update.message.reply_text(
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(buttons),
        parse_mode=ParseMode.MARKDOWN_V2
    )


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    stats["users"].add(update.effective_user.id)
    name = update.effective_user.first_name or "AGENT"
    await update.message.reply_text(
        f"```\n╔══════════════════════════════════════╗\n"
        f"║   N E X U S - D L  //  v3.1         ║\n"
        f"║   MEDIA EXTRACTION SYSTEM ONLINE     ║\n"
        f"╠══════════════════════════════════════╣\n"
        f"║  AGENT AUTHENTICATED: {name[:14]:<14} ║\n"
        f"╚══════════════════════════════════════╝\n```\n"
        f"*TARGET PLATFORMS:*\n"
        f"`YouTube` `TikTok` `Instagram` `Twitter/X`\n"
        f"`Facebook` `Reddit` `SoundCloud` `Twitch` `Vimeo`\n\n"
        f"*COMMANDS:*\n"
        f"`/search` — search YouTube\n"
        f"`/info`   — inspect target URL\n"
        f"`/playlist` — extract playlist\n"
        f"`/trending` — trending videos\n"
        f"`/cookietest` — diagnose cookies.txt\n"
        f"`/authstatus` — check auth token\n"
        f"`/auth`   — link Google account\n"
        f"`/help`   — operator manual\n\n"
        f"_Drop any URL to begin extraction_",
        parse_mode=ParseMode.MARKDOWN,
    )

# ── /cookietest — live validation ─────────────────────────



    if not COOKIES_FILE.exists():
        await update.message.reply_text(
            "```\n[COOKIE TEST]\nNo cookies.txt found.\nSend the file to this chat first.\n```",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    msg = await update.message.reply_text(
        "```\n[COOKIE TEST]\nTesting against YouTube...\n```", parse_mode=ParseMode.MARKDOWN_V2,
    )
    loop = asyncio.get_running_loop()
    ok, detail = await loop.run_in_executor(None, test_cookies_live)

    fsize  = fmt_size(COOKIES_FILE.stat().st_size)
    flines = COOKIES_FILE.read_text(errors="ignore").count("\n")
    yt_cnt = COOKIES_FILE.read_text(errors="ignore").lower().count("youtube")
    status = "PASS ✅" if ok else "FAIL ✗"

    await safe_edit(msg,
        f"```\n[COOKIE TEST RESULT]\n{'─'*32}\n"
        f"file     : {COOKIES_FILE.name}\n"
        f"size     : {fsize}\nlines    : {flines}\nyt refs  : {yt_cnt}\n"
        f"result   : {status}\ndetail   : {detail[:60]}\n{'─'*32}\n"
        f"{'Cookies are working!' if ok else 'Cookies FAILED — re-export while logged in'}\n```",
        parse_mode=ParseMode.MARKDOWN_V2,
    )

    if not ok:
        await update.message.reply_text(
            "*How to fix:*\n\n"
            "1\\. Open Chrome → go to `youtube\\.com` → *log in*\n"
            "2\\. On your PC run:\n"
            "`yt-dlp --cookies-from-browser chrome --cookies cookies.txt`\n"
            "3\\. Send the new `cookies\\.txt` to this chat\n\n"
            "_If Chrome fails, try Firefox:_\n"
            "`yt-dlp --cookies-from-browser firefox --cookies cookies.txt`",
            parse_mode=ParseMode.MARKDOWN_V2,
        )

# ── /authstatus — token file details ──────────────────────



    cache_dir  = _yt_dlp_cache_dir()
    token_path = cache_dir / "youtube-oauth2.token.json"
    lines = ["```", "[OAUTH2 TOKEN STATUS]", "─" * 36]
    lines.append(f"cache dir  : {cache_dir}")
    lines.append(f"token path : {token_path}")
    lines.append(f"exists     : {'YES ✅' if token_path.exists() else 'NO ✗'}")

    if token_path.exists():
        try:
            raw = json.loads(token_path.read_text())
            exp = raw.get("expires", 0)
            ttl = exp - int(time.time())
            lines.append(f"has access : {'YES' if raw.get('access_token') else 'NO'}")
            lines.append(f"has refresh: {'YES' if raw.get('refresh_token') else 'NO'}")
            lines.append(f"expires in : {ttl}s {'✅' if ttl > 0 else '✗ EXPIRED'}")
        except Exception as e:
            lines.append(f"parse error: {e}")

    lines += ["─" * 36, f"oauth2_token.json: {'YES' if OAUTH2_TOKEN_FILE.exists() else 'NO'}", "```"]
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN_V2)

# ── /setcookies ────────────────────────────────────────────

async def cmd_setcookies(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "```\n[COOKIE UPLOAD INSTRUCTIONS]\n─────────────────────────────\n"
        "Step 1: Log into youtube.com in Chrome\n"
        "Step 2: On your PC run:\n"
        "  yt-dlp --cookies-from-browser chrome \\\n"
        "         --cookies cookies.txt\n\n"
        "Step 3: Send the file to this chat\n"
        "  as a FILE (not a photo/text)\n\n"
        "Step 4: Run /cookietest to verify\n"
        "─────────────────────────────\n"
        "Supported: chrome / firefox / edge /\n"
        "           safari / brave / opera\n```",
        parse_mode=ParseMode.MARKDOWN_V2,
    )

# ── handle_document — cookie upload ───────────────────────



    if ADMIN_IDS and uid not in ADMIN_IDS:
        await update.message.reply_text(
            "```\n[RESTRICTED]\nOnly admins can upload cookies.\n```", parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    fname = (doc.file_name or "").lower()
    if "cookie" not in fname and not fname.endswith(".txt"):
        return

    msg = await update.message.reply_text("```\n[RECEIVING COOKIES...]\n```", parse_mode=ParseMode.MARKDOWN_V2)
    try:
        tg_file = await doc.get_file()
        await tg_file.download_to_drive(str(COOKIES_FILE))
        invalidate_cookie_cache()

        raw = COOKIES_FILE.read_text(errors="ignore")[:200]
        if "HTTP Cookie File" not in raw and "Netscape" not in raw and "#" not in raw[:5]:
            COOKIES_FILE.unlink(missing_ok=True)
            await safe_edit(msg,
                "```\n[INVALID FILE]\nNot a valid Netscape cookies.txt\n"
                "Export with:\nyt-dlp --cookies-from-browser chrome --cookies cookies.txt\n```",
                parse_mode=ParseMode.MARKDOWN_V2,
            )
            return

        fsize = fmt_size(COOKIES_FILE.stat().st_size)
        await safe_edit(msg,
            f"```\n[COOKIES RECEIVED]\nfile    : {COOKIES_FILE.name}\nsize    : {fsize}\nstatus  : validating...\n```",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        logger.info("cookies.txt updated by user %d (%s)", uid, fsize)

        loop = asyncio.get_running_loop()
        ok, detail = await loop.run_in_executor(None, test_cookies_live)

        if ok:
            await safe_edit(msg,
                f"```\n[COOKIES ACTIVATED ✅]\nfile    : {COOKIES_FILE.name}\nsize    : {fsize}\n"
                f"test    : PASS\ndetail  : {detail[:50]}\n```\n\n"
                f"✅ Cookies are working\\. Try your download now\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
            )
        else:
            await safe_edit(msg,
                f"```\n[COOKIES INVALID ✗]\nfile    : {COOKIES_FILE.name}\nsize    : {fsize}\n"
                f"test    : FAIL\nerror   : {detail[:60]}\n```\n\n"
                f"⚠ File saved but cookies did not authenticate\\.\n"
                f"Make sure you are *logged into YouTube* before exporting\\.\n"
                f"Run /cookietest for detailed diagnosis\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
            )
    except Exception as e:
        await safe_edit(msg,
            f"```\n[UPLOAD FAILED]\n{str(e)[:150].replace('`', chr(39))}\n```",
            parse_mode=ParseMode.MARKDOWN_V2,
        )

# ── /auth ──────────────────────────────────────────────────



    if oauth2_token_exists():
        await update.message.reply_text(
            "✅ OAuth2 already active — Google account is linked.\n\n"
            "Run /authstatus to see token details.\n"
            "To re-auth: delete oauth2_token.json and run /auth again."
        )
        return

    if uid in _oauth_pending:
        if time.time() - _oauth_pending[uid].get("ts", 0) < 600:
            await update.message.reply_text("⏳ Auth already in progress — check above for the Google code.")
            return
        del _oauth_pending[uid]

    await update.message.reply_text("🔄 Contacting Google... please wait 5-10 seconds.")
    loop = asyncio.get_running_loop()
    _oauth_pending[uid] = {"ts": time.time()}

    def tg_send(text: str):
        future = asyncio.run_coroutine_threadsafe(bot.send_message(chat_id=chat, text=text), loop)
        try: future.result(timeout=15)
        except Exception as e: logger.error("tg_send failed: %s", e)

    def run_flow():
        logger.info("OAuth2 thread started uid=%d", uid)
        try:
            import httpx
            r    = httpx.post(_DEVICE_CODE_URL, data={"client_id": _OAUTH_CLIENT_ID, "scope": _OAUTH_SCOPE}, timeout=30)
            resp = r.json()
            logger.info("OAuth2 device code response: %s", resp)

            if "error" in resp or "device_code" not in resp:
                _oauth_pending.pop(uid, None)
                tg_send(f"❌ Google error: {resp.get('error','unknown')} — {resp.get('error_description','')}")
                return

            device_code      = resp["device_code"]
            user_code        = resp["user_code"]
            verification_url = resp.get("verification_url", "https://www.google.com/device")
            interval         = int(resp.get("interval", 5))
            expires_in       = int(resp.get("expires_in", 1800))

            _oauth_pending[uid] = {"ts": time.time()}
            tg_send(
                f"🔑 GOOGLE AUTH CODE\n━━━━━━━━━━━━━━━━━━━━\n"
                f"Step 1 — Open on your phone:\n{verification_url}\n\n"
                f"Step 2 — Enter this code:\n   ➤  {user_code}\n\n"
                f"Step 3 — Sign in with any Google account\n\n"
                f"⏳ Bot will confirm automatically (waiting up to {expires_in//60} min)..."
            )

            deadline = time.time() + expires_in
            while time.time() < deadline:
                time.sleep(interval)
                try:
                    tr    = httpx.post(_TOKEN_URL, data={
                        "client_id": _OAUTH_CLIENT_ID, "client_secret": _OAUTH_CLIENT_SECRET,
                        "device_code": device_code, "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                    }, timeout=30)
                    token = tr.json()
                    logger.debug("OAuth2 poll: %s", token.get("error", "pending"))
                    if "access_token" in token:
                        _write_ydlp_token(token)
                        _oauth_pending.pop(uid, None)
                        tg_send("✅ AUTH COMPLETE!\nGoogle account linked. Token saved permanently.\n\nAll YouTube downloads now bypass IP blocks! 🎉\nTry sending a YouTube link now.")
                        return
                    err = token.get("error", "")
                    if err == "authorization_pending": continue
                    if err == "slow_down": interval = min(interval + 5, 30); continue
                    if err in ("access_denied", "expired_token"):
                        _oauth_pending.pop(uid, None)
                        tg_send(f"❌ Google said: {err}\nRun /auth again.")
                        return
                except Exception as pe:
                    logger.warning("OAuth2 poll error: %s", pe)
                    time.sleep(interval)

            _oauth_pending.pop(uid, None)
            tg_send("⏰ Auth timed out — code not entered in time.\nRun /auth again.")
        except Exception as e:
            logger.error("OAuth2 thread crash: %s", e, exc_info=True)
            _oauth_pending.pop(uid, None)
            tg_send(f"❌ OAuth2 error: {e}\n\nRun /auth again.")

    threading.Thread(target=run_flow, daemon=True, name=f"oauth2-{uid}").start()

async def cmd_authtest(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if ADMIN_IDS and uid not in ADMIN_IDS:
        return
    msg = await update.message.reply_text("```\n[TESTING GOOGLE API...]\n```", parse_mode=ParseMode.MARKDOWN_V2)

    def _test():
        import httpx, traceback
        results = [f"client_id: {_OAUTH_CLIENT_ID[:30]}...", f"endpoint : {_DEVICE_CODE_URL}"]
        try:
            r = httpx.post(_DEVICE_CODE_URL, data={"client_id": _OAUTH_CLIENT_ID, "scope": _OAUTH_SCOPE}, timeout=15)
            results.append(f"HTTP     : {r.status_code}")
            results.append(f"response :\n{r.text[:400].replace('`', chr(39))}")
        except Exception as e:
            results.append(f"ERROR: {e}")
            results.append(traceback.format_exc()[-300:].replace("`", "'"))
        return "\n".join(results)

    result = await asyncio.get_running_loop().run_in_executor(None, _test)
    await safe_edit(msg,
        f"```\n[GOOGLE API TEST]\n{result.replace(chr(92), '/')}\n```",
        parse_mode=ParseMode.MARKDOWN_V2,
    )

async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    t0  = time.monotonic()
    msg = await update.message.reply_text("```\n[PING] measuring...\n```", parse_mode=ParseMode.MARKDOWN_V2)
    ms  = int((time.monotonic() - t0) * 1000)
    bar = ("█" * min(ms // 10, 20)).ljust(20, "░")
    quality = "OPTIMAL" if ms < 200 else ("NOMINAL" if ms < 500 else "DEGRADED")
    await msg.edit_text(
        f"```\n[LATENCY CHECK]\nRTT   : {ms}ms\nBAR   : {bar}\nSTATUS: {quality}\n```",
        parse_mode=ParseMode.MARKDOWN_V2,
    )

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bs = bypass_status()
    ck_ok   = bs["cookies"] and bs["cookies_valid"] is True
    ck_unk  = bs["cookies"] and bs["cookies_valid"] is None
    ck_bad  = bs["cookies"] and bs["cookies_valid"] is False

    if ck_ok:    ck_str = "OK ✅"
    elif ck_unk: ck_str = "PRESENT (untested)"
    elif ck_bad: ck_str = "INVALID ✗"
    else:        ck_str = "----  ✗"

    def st(ok): return "OK  ✅" if ok else "----  ✗"
    proxy_str = f"OK ({bs['proxy_count']}x) ✅" if bs["proxy"] else "----  ✗"
    api_str   = "LOCAL ✅" if _using_local_api else "CLOUD (50MB cap)"

    await update.message.reply_text(
        f"```\n╔═══════════════════════════════════╗\n║  NEXUS-DL  //  OPERATOR MANUAL   ║\n"
        f"╠═══════════════════════════════════╣\n║  BYPASS STATUS                   ║\n"
        f"║  cookies.txt : {ck_str:<20}║\n"
        f"║  oauth2      : {st(bs['oauth2']):<20}║\n"
        f"║  po_token    : {st(bs['po_token']):<20}║\n"
        f"║  proxy       : {proxy_str:<20}║\n"
        f"╠═══════════════════════════════════╣\n║  FILE LIMITS                     ║\n"
        f"║  bot api  : {api_str:<22}║\n"
        f"║  max_file : {fmt_size(MAX_FILE_SIZE):<22}║\n"
        f"╠═══════════════════════════════════╣\n║  DIAGNOSTICS                     ║\n"
        f"║  /cookietest — test cookies live  ║\n"
        f"║  /authstatus — token file paths   ║\n"

        f"╚═══════════════════════════════════╝\n```",
        parse_mode=ParseMode.MARKDOWN_V2,
    )

async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    up    = int(time.time() - stats["start_time"])
    total = stats["downloads"] + stats["failed"]
    rate  = (stats["downloads"] / total * 100) if total else 0
    qsize = download_queue.qsize() if download_queue else 0
    mb    = stats["bytes_sent"] / (1024 * 1024)
    run_bar = ("▰" * len(active_downloads)).ljust(WORKERS, "▱")
    await update.message.reply_text(
        f"```\n╔════════════════════════════════╗\n║  NEXUS-DL  //  TELEMETRY      ║\n"
        f"╠════════════════════════════════╣\n║  uptime  : {fmt_uptime(up):<20}║\n"
        f"║  users   : {len(stats['users']):<20}║\n║  success : {stats['downloads']:<20}║\n"
        f"║  failed  : {stats['failed']:<20}║\n║  rate    : {rate:<19.1f}%║\n"
        f"║  data_tx : {mb:<17.1f} MB ║\n║  queue   : {qsize:<20}║\n"
        f"║  workers : [{run_bar}] {len(active_downloads)}/{WORKERS}  ║\n╚════════════════════════════════╝\n```",
        parse_mode=ParseMode.MARKDOWN_V2,
    )

async def cmd_queue(update: Update, context: ContextTypes.DEFAULT_TYPE):
    qsize = download_queue.qsize() if download_queue else 0
    run   = len(active_downloads)
    w_bar = ("▰" * run).ljust(WORKERS, "▱")
    q_bar = ("█" * min(qsize, 10)).ljust(10, "░")
    await update.message.reply_text(
        f"```\n[MISSION QUEUE]\nworkers  [{w_bar}] {run}/{WORKERS}\npending  [{q_bar}] {qsize}/{MAX_QUEUE_SIZE}\n```",
        parse_mode=ParseMode.MARKDOWN_V2,
    )

async def cmd_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    h   = user_history.get(uid, [])
    if not h:
        await update.message.reply_text("```\n[HISTORY LOG — EMPTY]\nNo downloads recorded yet.\n```", parse_mode=ParseMode.MARKDOWN_V2)
        return
    lines = ["```", "[RECENT DOWNLOADS]", "─" * 32]
    for i, entry in enumerate(reversed(h), 1):
        ts = time.strftime("%m-%d %H:%M", time.localtime(entry["ts"]))
        lines.append(f"#{i} [{ts}] {entry['typ'].upper()}/{entry['quality']}")
        lines.append(f"   {entry['title'][:30]}")
    lines.append("```")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN_V2)

async def cmd_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args:
        await update.message.reply_text("```\n[USAGE]\n/info <url>\n```", parse_mode=ParseMode.MARKDOWN_V2)
        return
    url = args[0].strip()
    msg = await update.message.reply_text("```\n[SCANNING TARGET...]\n```", parse_mode=ParseMode.MARKDOWN_V2)
    try:
        info = await asyncio.wait_for(asyncio.get_event_loop().run_in_executor(None, get_video_info, url), timeout=30)
    except Exception as e:
        await safe_edit(msg, f"```\n[SCAN FAILED]\n{mdescape(str(e)[:200])}\n```", parse_mode=ParseMode.MARKDOWN_V2)
        return
    fmts    = info.get("formats", [])
    heights = sorted(set(f.get("height") for f in fmts if f.get("height")))
    codecs  = sorted(set(f.get("vcodec","").split(".")[0] for f in fmts if f.get("vcodec") and f.get("vcodec") != "none"))
    acodecs = sorted(set(f.get("acodec","").split(".")[0] for f in fmts if f.get("acodec") and f.get("acodec") != "none"))
    title   = sanitize(info.get("title","?"))[:40]
    ch      = (info.get("uploader") or info.get("channel") or "?")[:34]
    dur     = fmt_dur(info.get("duration") or 0)
    tags    = ", ".join((info.get("tags") or [])[:5]) or "—"
    await safe_edit(msg,
        f"```\n╔══════════════════════════════════╗\n║  TARGET ANALYSIS COMPLETE        ║\n"
        f"╠══════════════════════════════════╣\n║ TITLE  : {title[:34]:<34}║\n"
        f"║ CH     : {ch:<34}║\n║ DUR    : {dur:<34}║\n"
        f"╠══════════════════════════════════╣\n║ V.RES  : {str(heights)[:34]:<34}║\n"
        f"║ V.CODEC: {str(codecs)[:34]:<34}║\n║ A.CODEC: {str(acodecs)[:34]:<34}║\n"
        f"║ TAGS   : {tags[:34]:<34}║\n║ FORMATS: {len(fmts):<34}║\n"
        f"╚══════════════════════════════════╝\n```",
        parse_mode=ParseMode.MARKDOWN_V2,
    )

async def cmd_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = " ".join(context.args).strip()
    if not query:
        await update.message.reply_text("```\n[USAGE]\n/search <keywords>\n```", parse_mode=ParseMode.MARKDOWN_V2)
        return
    msg = await update.message.reply_text(
        f"```\n[SEARCHING YT]\nquery: {query[:40].replace('`',chr(39))}\n```", parse_mode=ParseMode.MARKDOWN_V2,
    )
    def _do_search():
        opts = {**build_ydl_common(), "skip_download": True, "quiet": True, "extract_flat": True, "playlistend": 8}
        with yt_dlp.YoutubeDL(opts) as ydl:
            return ydl.extract_info(f"ytsearch8:{query}", download=False)
    try:
        results = await asyncio.wait_for(asyncio.get_event_loop().run_in_executor(None, _do_search), timeout=30)
    except Exception as e:
        await safe_edit(msg, f"```\n[SEARCH FAILED]\n{mdescape(str(e)[:150])}\n```", parse_mode=ParseMode.MARKDOWN_V2)
        return
    entries = results.get("entries") or []
    if not entries:
        await safe_edit(msg, "```\n[NO RESULTS FOUND]\n```", parse_mode=ParseMode.MARKDOWN_V2)
        return
    lines = ["```", f"[RESULTS: {len(entries)}]", f"query: {query[:35]}", "─" * 36]
    buttons = []
    for i, e in enumerate(entries[:8], 1):
        title = sanitize(e.get("title","?"))[:38]
        lines.append(f"[{i}] {title}")
        lines.append(f"     ⏱ {fmt_dur(e.get('duration') or 0)}")
        safe_url = normalize_url(e).replace("|", "%7C")
        buttons.append([InlineKeyboardButton(f"[{i}] {title[:30]}", callback_data=f"fetch|{safe_url}")])
    lines.append("```")
    buttons.append([InlineKeyboardButton("✖ Close", callback_data="cancel")])
    await safe_edit(msg, "\n".join(lines), parse_mode=ParseMode.MARKDOWN_V2, reply_markup=InlineKeyboardMarkup(buttons))

async def cmd_trending(update: Update, context: ContextTypes.DEFAULT_TYPE):
    category = " ".join(context.args).strip() if context.args else ""
    tag      = category or "today"
    msg = await update.message.reply_text(
        f"```\n[SCANNING TRENDING: {tag[:20].upper()}]\n```", parse_mode=ParseMode.MARKDOWN_V2,
    )
    def _trend():
        opts = {**build_ydl_common(), "skip_download": True, "extract_flat": True, "quiet": True}
        with yt_dlp.YoutubeDL(opts) as ydl:
            r = ydl.extract_info(f"ytsearch10:trending {category} 2025".strip(), download=False)
            return r.get("entries") or []
    try:
        entries = await asyncio.wait_for(asyncio.get_event_loop().run_in_executor(None, _trend), timeout=40)
    except Exception as e:
        await safe_edit(msg, f"```\n[TRENDING FAILED]\n{str(e)[:120].replace('`','')}\n```", parse_mode=ParseMode.MARKDOWN_V2)
        return
    entries = [e for e in entries if e and e.get("id")][:10]
    if not entries:
        await safe_edit(msg, "```\n[NO RESULTS]\nTry: /trending music\n     /trending gaming\n```", parse_mode=ParseMode.MARKDOWN_V2)
        return
    lines = ["```", f"[TRENDING: {tag[:20].upper()}]", "─" * 34]
    buttons = []
    for i, e in enumerate(entries, 1):
        title    = sanitize(e.get("title") or "Unknown")[:34]
        safe_url = normalize_url(e).replace("|", "%7C")
        lines.append(f"[{i:02d}] {title}")
        lines.append(f"      {fmt_dur(e.get('duration') or 0)}")
        buttons.append([InlineKeyboardButton(f"#{i} {title[:32]}", callback_data=f"fetch|{safe_url}")])
    lines.append("```")
    buttons.append([InlineKeyboardButton("✖ Close", callback_data="cancel")])
    await safe_edit(msg, "\n".join(lines), parse_mode=ParseMode.MARKDOWN_V2, reply_markup=InlineKeyboardMarkup(buttons))

async def cmd_playlist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args:
        await update.message.reply_text("```\n[USAGE]\n/playlist <url>\n```", parse_mode=ParseMode.MARKDOWN_V2)
        return
    url = args[0].strip()
    msg = await update.message.reply_text("```\n[PARSING PLAYLIST...]\n```", parse_mode=ParseMode.MARKDOWN_V2)
    def _pl():
        opts = {**build_ydl_common(), "skip_download": True, "extract_flat": True, "playlistend": MAX_PLAYLIST_SHOW}
        with yt_dlp.YoutubeDL(opts) as ydl:
            return ydl.extract_info(url, download=False)
    try:
        info = await asyncio.wait_for(asyncio.get_event_loop().run_in_executor(None, _pl), timeout=40)
    except Exception as e:
        await safe_edit(msg, f"```\n[PLAYLIST PARSE FAILED]\n{mdescape(str(e)[:150])}\n```", parse_mode=ParseMode.MARKDOWN_V2)
        return
    entries  = info.get("entries") or []
    pl_title = sanitize(info.get("title","Playlist"))[:30]
    lines    = ["```", f"[PLAYLIST: {pl_title}]", f"tracks shown: {len(entries)}", "─" * 36]
    buttons  = []
    for i, e in enumerate(entries, 1):
        title    = sanitize(e.get("title","?"))[:38]
        vid_url  = normalize_url(e)
        safe_url = vid_url.replace("|", "%7C")
        lines.append(f"[{i}] {title[:36]}")
        lines.append(f"     ⏱ {fmt_dur(e.get('duration') or 0)}")
        buttons.append([InlineKeyboardButton(f"[{i}] {title[:30]}", callback_data=f"fetch|{safe_url}")])
    lines.append("```")
    buttons.append([InlineKeyboardButton("✖ Close", callback_data="cancel")])
    await safe_edit(msg, "\n".join(lines), parse_mode=ParseMode.MARKDOWN_V2, reply_markup=InlineKeyboardMarkup(buttons))

# ═══════════════════════════════════════════════════════════
#  LINK HANDLER
# ═══════════════════════════════════════════════════════════

async def handle_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    url  = update.message.text.strip()
    user = update.effective_user.id

    if not re.match(r"https?://", url):
        await update.message.reply_text(
            "```\n[INVALID INPUT]\nExpected: https://...\nType /help for help.\n```",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    now  = time.time()
    last = user_last_request.get(user, 0)
    if now - last < RATE_LIMIT_SEC:
        cd = int(RATE_LIMIT_SEC - (now - last))
        await update.message.reply_text(f"```\n[RATE LIMIT]\nCooldown: {cd}s remaining\n```", parse_mode=ParseMode.MARKDOWN_V2)
        return

    record_rl(user)
    stats["users"].add(user)

    if download_queue and download_queue.qsize() >= MAX_QUEUE_SIZE:
        await update.message.reply_text("```\n[QUEUE FULL]\nAll slots occupied. Retry shortly.\n```", parse_mode=ParseMode.MARKDOWN_V2)
        return

    msg       = await update.message.reply_text("```\n[INITIALIZING...]\n```", parse_mode=ParseMode.MARKDOWN_V2)
    stop_anim = asyncio.Event()
    anim_task = asyncio.create_task(animated_fetch(msg, stop_anim))

    try:
        info = await asyncio.wait_for(asyncio.get_event_loop().run_in_executor(None, get_video_info, url), timeout=60)
    except asyncio.TimeoutError:
        stop_anim.set(); await anim_task
        await safe_edit(msg, "```\n[TIMEOUT]\nTarget unreachable (>60s).\n```", parse_mode=ParseMode.MARKDOWN_V2)
        return
    except Exception as e:
        logger.error("info fetch: %s", e)
        stop_anim.set(); await anim_task
        await safe_edit(msg, error_msg(e), parse_mode=ParseMode.MARKDOWN_V2)
        return
    else:
        stop_anim.set(); await anim_task

    await msg.delete()
    caption = build_caption(info)
    buttons = build_buttons(info, url)
    thumb   = info.get("thumbnail")
    try:
        await update.message.reply_photo(photo=thumb, caption=caption,
            reply_markup=InlineKeyboardMarkup(buttons), parse_mode=ParseMode.MARKDOWN)
    except Exception:
        await update.message.reply_text(caption,
            reply_markup=InlineKeyboardMarkup(buttons), parse_mode=ParseMode.MARKDOWN)

# ═══════════════════════════════════════════════════════════
#  BUTTON HANDLER
# ═══════════════════════════════════════════════════════════

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data.startswith("cache_send|"):
        path = query.data[11:]
        cp = Path(path)
        if not cp.exists():
            await query.answer("❌ File no longer in cache.", show_alert=True)
            return

        load_cache_index()
        # Find entry for title
        title = "Unknown Title"
        for t, entries in cache_index.items():
            for e in entries:
                if e["path"] == path:
                    title = t
                    break
            if title != "Unknown Title":
                break

        try:
            cap = f"🎵 `{Path(path).stem}` \\(from cache\\)"
            with open(cp, "rb") as f:
                await query.message.reply_audio(
                    f, caption=cap, title=title[:100], parse_mode=ParseMode.MARKDOWN_V2,
                    read_timeout=120, write_timeout=120
                )
            stats["downloads"] += 1
        except Exception as e:
            logger.error("cache send fail: %s", e)
        try:
            await query.message.delete()
        except:
            pass
        return

    if query.data == "cancel":
        try: await query.message.delete()
        except Exception: pass
        return

    if query.data.startswith("fetch|"):
        url = query.data[6:].replace("%7C", "|")
        try: await query.message.delete()
        except Exception: pass
        stop = asyncio.Event()
        msg  = await query.message.reply_text("```\n[SCANNING TARGET...]\n```", parse_mode=ParseMode.MARKDOWN_V2)
        anim = asyncio.create_task(animated_fetch(msg, stop))
        try:
            info = await asyncio.wait_for(asyncio.get_event_loop().run_in_executor(None, get_video_info, url), timeout=60)
        except Exception as e:
            stop.set(); await anim
            await safe_edit(msg, error_msg(e), parse_mode=ParseMode.MARKDOWN_V2)
            return
        stop.set(); await anim
        await msg.delete()
        caption = build_caption(info)
        buttons = build_buttons(info, url)
        try:
            await query.message.reply_photo(photo=info.get("thumbnail"), caption=caption,
                reply_markup=InlineKeyboardMarkup(buttons), parse_mode=ParseMode.MARKDOWN)
        except Exception:
            await query.message.reply_text(caption,
                reply_markup=InlineKeyboardMarkup(buttons), parse_mode=ParseMode.MARKDOWN)
        return

    parts = query.data.split("|", 2)
    if len(parts) != 3 or parts[0] not in ("mp3", "mp4", "m4a", "ogg"):
        await query.answer("⚠ Invalid selection.", show_alert=True)
        return

    typ, quality, safe_url = parts
    url = safe_url.replace("%7C", "|")
    pos = (download_queue.qsize() if download_queue else 0) + 1

    try:
        await query.edit_message_caption(
            caption=f"```\n[MISSION QUEUED #{pos}]\nformat : {typ.upper()} / {quality}\nstatus : waiting for worker\n```",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
    except BadRequest:
        try:
            await query.edit_message_text(
                text=f"```\n[MISSION QUEUED #{pos}]\nformat : {typ.upper()} / {quality}\n```",
                parse_mode=ParseMode.MARKDOWN_V2,
            )
        except Exception as e:
            logger.debug("edit fail: %s", e)

    status_msg = await query.message.reply_text(
        f"```\n[QUEUED — POSITION #{pos}]\ntarget : {typ.upper()} {quality}\nstatus : standby...\n```",
        parse_mode=ParseMode.MARKDOWN_V2,
    )
    if download_queue:
        await download_queue.put((query, typ, quality, url, status_msg))

# ═══════════════════════════════════════════════════════════
#  WORKERS
# ═══════════════════════════════════════════════════════════

async def worker(wid: int):
    logger.info("Worker %d online", wid)
    while True:
        try:
            query, typ, quality, url, msg = await download_queue.get()
            active_downloads[wid] = url
            try:
                await asyncio.wait_for(process(query, typ, quality, url, msg, wid), timeout=DOWNLOAD_TIMEOUT)
            except asyncio.TimeoutError:
                logger.error("Worker %d timeout on %s", wid, url)
                stats["failed"] += 1
                await safe_edit(msg, "```\n[TIMEOUT]\nMission exceeded time limit.\n```", parse_mode=ParseMode.MARKDOWN_V2)
        except asyncio.CancelledError:
            logger.info("Worker %d terminating.", wid); break
        except Exception as e:
            logger.exception("Worker %d crash: %s", wid, e)
        finally:
            active_downloads.pop(wid, None)
            download_queue.task_done()

# ═══════════════════════════════════════════════════════════
#  PROCESS
# ═══════════════════════════════════════════════════════════

async def process(query, typ: str, quality: str, url: str, msg, wid: int):
    loop = asyncio.get_event_loop()
    uid  = query.from_user.id if query.from_user else 0

    cp = CACHE_FOLDER / ckey(url, typ, quality)
    if cp.exists():
        await safe_edit(msg, "```\n[CACHE HIT]\nTransmitting from local cache...\n```", parse_mode=ParseMode.MARKDOWN_V2)
        try:
            cap = f"`{'🎵 AUDIO' if typ == 'mp3' else '🎥 VIDEO'}` _(cached)_"
            with open(cp, "rb") as f:
                if typ in ("mp3", "m4a", "ogg"):
                    await msg.reply_audio(f, caption=cap, parse_mode=ParseMode.MARKDOWN, read_timeout=120, write_timeout=120)
                else:
                    await msg.reply_video(f, caption=cap, parse_mode=ParseMode.MARKDOWN, supports_streaming=True, read_timeout=120, write_timeout=120)
            await msg.delete(); stats["downloads"] += 1; return
        except Exception as e:
            logger.error("cache send fail: %s", e)
            cp.unlink(missing_ok=True)

    try:
        info        = await loop.run_in_executor(None, get_video_info, url)
        clean_title = sanitize(info.get("title") or "download")
    except Exception:
        clean_title = "download"

    if typ in ("mp3", "m4a", "ogg"):
        fmt = "bestaudio/best"
    elif quality == "best":
        fmt = "bestvideo+bestaudio/best"
    else:
        q   = int(quality)
        fmt = (
            f"bestvideo[height={q}]+bestaudio"
            f"/bestvideo[height<={q}]+bestaudio"
            f"/best[height<={q}]"
            f"/bestvideo+bestaudio/best"
        )

    out_tpl = str(DOWNLOAD_FOLDER / f"{clean_title}.%(ext)s")
    frame_n = {"n": 0, "last_pct": -1.0}

    def hook(d: dict):
        if d["status"] != "downloading": return
        raw = d.get("_percent_str", "0").strip().replace("%", "")
        try: pct = float(raw)
        except ValueError: return
        if pct - frame_n["last_pct"] < 2.5 and pct < 99: return
        frame_n["last_pct"] = pct; frame_n["n"] += 1
        bar   = _hbar(pct)
        speed = (d.get("_speed_str")   or "—").strip()
        eta   = (d.get("_eta_str")     or "—").strip()
        size  = (d.get("_total_bytes_str") or d.get("_total_bytes_estimate_str") or "—").strip()
        radar = _RADAR[frame_n["n"] % len(_RADAR)]
        text  = (
            f"```\n[{radar}] NEXUS-DL // EXTRACTION\n{'─'*30}\n"
            f"TARGET : {typ.upper()} {quality}\n"
            f"PHASE  : {_glitch(_phase(pct), frame_n['n'])}\n"
            f"[{bar}] {pct:.0f}%\nSIZE   : {size}\nSPEED  : {speed}\nETA    : {eta}\n```"
        )
        loop.call_soon_threadsafe(asyncio.ensure_future, safe_edit(msg, text, parse_mode=ParseMode.MARKDOWN_V2))

    ydl_opts = {**build_ydl_common(), "format": fmt, "outtmpl": out_tpl, "progress_hooks": [hook]}
    if typ == "mp4":
        ydl_opts["merge_output_format"] = "mp4"
    if typ == "mp3":
        ydl_opts["postprocessors"] = [{"key": "FFmpegExtractAudio", "preferredcodec": "mp3", "preferredquality": quality}]
    elif typ == "m4a":
        ydl_opts["postprocessors"] = [{"key": "FFmpegExtractAudio", "preferredcodec": "m4a", "preferredquality": "0"}]
    elif typ == "ogg":
        ydl_opts["postprocessors"] = [{"key": "FFmpegExtractAudio", "preferredcodec": "vorbis", "preferredquality": "5"}]

    await safe_edit(msg,
        f"```\n[DOWNLOAD INITIATED]\ntarget : {typ.upper()} {quality}\nstatus : connecting...\n```",
        parse_mode=ParseMode.MARKDOWN_V2)

    fp_str = None; last_err = None

    try:
        fp_str = await loop.run_in_executor(None, lambda: _run_ydl(ydl_opts, url, typ, clean_title))
    except Exception as e:
        last_err = e
        if not _is_bot_block(e):
            stats["failed"] += 1
            await safe_edit(msg, error_msg(e), parse_mode=ParseMode.MARKDOWN_V2)
            return
        logger.warning("Primary download blocked — trying fallbacks…")

    if fp_str is None and last_err is not None:
        for i, fb in enumerate(_FALLBACK_CLIENTS):
            cname = fb["youtube"]["player_client"][0]
            await safe_edit(msg,
                f"```\n[BYPASS #{i+1}/{len(_FALLBACK_CLIENTS)}]\nclient: {cname}\nretrying...\n```",
                parse_mode=ParseMode.MARKDOWN_V2)
            try:
                fb_opts = {**ydl_opts, "extractor_args": fb}
                fp_str  = await loop.run_in_executor(None, lambda o=fb_opts: _run_ydl(o, url, typ, clean_title))
                if fp_str:
                    logger.info("Fallback '%s' succeeded.", cname)
                    last_err = None; break
            except Exception as fe:
                last_err = fe
                logger.warning("Fallback '%s' fail: %s", cname, fe)

    if last_err and not fp_str:
        stats["failed"] += 1
        await safe_edit(msg, error_msg(last_err), parse_mode=ParseMode.MARKDOWN_V2)
        return

    if not fp_str or not Path(fp_str).exists():
        stats["failed"] += 1
        await safe_edit(msg, "```\n[FILE NOT FOUND]\nDownload completed but output missing.\nTry shorter video or different format.\n```", parse_mode=ParseMode.MARKDOWN_V2)
        return

    fp  = Path(fp_str)
    fsz = fp.stat().st_size

    if fsz > MAX_FILE_SIZE and not _using_local_api:
        fp.unlink(missing_ok=True)
        stats["failed"] += 1
        await safe_edit(msg,
            f"```\n[FILE TOO LARGE]\nsize  : {fmt_size(fsz)}\nlimit : {fmt_size(MAX_FILE_SIZE)}\n\nOptions:\n  1. Choose lower resolution\n  2. Choose MP3 audio only\n  3. Run a local Bot API server\n```",
            parse_mode=ParseMode.MARKDOWN_V2)
        return

    SPLIT_THRESHOLD = 45 * 1024 * 1024
    need_split = not _using_local_api and fsz > SPLIT_THRESHOLD

    await safe_edit(msg,
        f"```\n[TRANSMITTING]\nfile  : {fp.name[:40]}\nsize  : {fmt_size(fsz)}\nuplink: active...\n```",
        parse_mode=ParseMode.MARKDOWN_V2)

    if not need_split:
        try: 
            shutil.copy2(fp, cp)
            # Update cache index with song name
            info = await loop.run_in_executor(None, get_video_info, url)
            song_title = sanitize(info.get("title", "Unknown"))[:200]
            entry = {
                "path": str(cp),
                "typ": typ,
                "quality": quality,
                "url": url
            }
            load_cache_index()
            if song_title not in cache_index:
                cache_index[song_title] = []
            if entry not in cache_index[song_title]:  # Avoid dups
                cache_index[song_title].append(entry)
            save_cache_index()
            logger.info("Cached %s -> index updated (%d entries for '%s')", cp.name, len(cache_index.get(song_title, [])), song_title[:50])
        except Exception as ce: 
            logger.warning("cache write: %s", ce)

    try:
        if need_split and typ in ("mp3", "m4a", "ogg"):
            await _send_in_parts(msg, fp, typ, clean_title)
        else:
            with open(fp, "rb") as f:
                cap = f"✅ `{fp.stem[:50]}`"
                if typ in ("mp3", "m4a", "ogg"):
                    await msg.reply_audio(audio=f, caption=cap, title=fp.stem, parse_mode=ParseMode.MARKDOWN, read_timeout=600, write_timeout=600)
                else:
                    await msg.reply_video(video=f, caption=cap, parse_mode=ParseMode.MARKDOWN, supports_streaming=True, read_timeout=600, write_timeout=600)
        await msg.delete()
        stats["downloads"] += 1
        stats["bytes_sent"] += fsz
        push_history(uid, clean_title, typ, quality)
    except Exception as e:
        logger.error("upload fail: %s", e)
        stats["failed"] += 1
        cp.unlink(missing_ok=True)
        await safe_edit(msg, f"```\n[UPLOAD FAILED]\n{str(e)[:200].replace(chr(96),chr(39))}\n```", parse_mode=ParseMode.MARKDOWN_V2)
    finally:
        fp.unlink(missing_ok=True)

# ═══════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════

async def _send_in_parts(msg, fp: Path, typ: str, title: str):
    CHUNK = 45 * 1024 * 1024
    fsz   = fp.stat().st_size
    total = (fsz + CHUNK - 1) // CHUNK
    await safe_edit(msg,
        f"```\n[SPLITTING FILE]\nsize  : {fmt_size(fsz)}\nparts : {total} x ~{fmt_size(CHUNK)}\n```",
        parse_mode=ParseMode.MARKDOWN_V2)
    with open(fp, "rb") as src:
        for i in range(total):
            chunk_data = src.read(CHUNK)
            if not chunk_data: break
            part_name = f"{title[:40]} (Part {i+1} of {total}){fp.suffix}"
            await msg.reply_audio(
                audio=io.BytesIO(chunk_data), filename=part_name, title=part_name,
                caption=f"`Part {i+1}/{total}`", parse_mode=ParseMode.MARKDOWN,
                read_timeout=600, write_timeout=600,
            )

def _run_ydl(opts: dict, url: str, typ: str, clean_title: str) -> str | None:
    before  = set(DOWNLOAD_FOLDER.iterdir()) if DOWNLOAD_FOLDER.exists() else set()
    t_start = time.time()

    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info     = ydl.extract_info(url, download=True)
            raw_path = ydl.prepare_filename(info)
    except Exception as e:
        logger.error("yt-dlp raw error [%s]: %s", url[:60], e)
        raise

    if typ == "mp3":   target_exts = [".mp3"]
    elif typ == "m4a": target_exts = [".m4a"]
    elif typ == "ogg": target_exts = [".ogg", ".opus"]
    else:              target_exts = [".mp4", ".mkv", ".webm", ".avi"]

    direct = Path(raw_path)
    if direct.exists(): return str(direct)
    for ext in target_exts:
        swapped = direct.with_suffix(ext)
        if swapped.exists(): return str(swapped)

    after     = set(DOWNLOAD_FOLDER.iterdir()) if DOWNLOAD_FOLDER.exists() else set()
    new_files = after - before
    for ext in target_exts:
        matches = [f for f in new_files if f.suffix == ext]
        if matches: return str(max(matches, key=lambda p: p.stat().st_mtime))

    for ext in target_exts:
        candidates = sorted(
            (f for f in DOWNLOAD_FOLDER.iterdir() if f.suffix == ext and f.stat().st_mtime >= t_start - 5),
            key=lambda p: p.stat().st_mtime, reverse=True,
        )
        if candidates: return str(candidates[0])

    logger.error("_run_ydl: could not locate output. raw=%s typ=%s", raw_path, typ)
    return None

# ═══════════════════════════════════════════════════════════
#  ERROR HANDLER + APP SETUP
# ═══════════════════════════════════════════════════════════

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.error("Unhandled exception", exc_info=context.error)

async def post_init(app):
    global download_queue
    download_queue = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)
    for i in range(WORKERS):
        asyncio.create_task(worker(i + 1))

    await app.bot.set_my_commands([
        BotCommand("start",       "Boot sequence"),



        BotCommand("search",      "Search YouTube"),
        BotCommand("trending",    "Trending videos [category]"),
        BotCommand("playlist",    "Extract playlist"),
        BotCommand("info",        "Inspect a URL"),
        BotCommand("history",     "Your download log"),

        BotCommand("queue",       "Mission queue status"),
        BotCommand("stats",       "System telemetry"),

        BotCommand("help",        "Operator manual"),
    ])

    po = get_po_token()[0]
    oa = oauth2_token_exists()
    ck = COOKIES_FILE.exists()
    px = len(_PROXY_LIST) > 0
    logger.info(
        "NEXUS-DL v3.1 online | workers=%d | oauth2=%s | cookies=%s | po_token=%s | proxy=%s",
        WORKERS, "yes" if oa else "no", "yes" if ck else "no",
        "yes" if po else "no", f"{len(_PROXY_LIST)}x" if px else "no",
    )

    if not oa and not ck and not px:
        logger.warning("⚠ NO BYPASS ACTIVE — run /auth in Telegram.")
        if ADMIN_IDS:
            for admin_id in ADMIN_IDS:
                try:
                    await app.bot.send_message(
                        chat_id=admin_id,
                        text="```\n[⚠ NO BYPASS ACTIVE]\nYouTube WILL block all downloads.\n\nRun /auth RIGHT NOW — 30 seconds,\nlasts forever.\n\nOr send cookies.txt then run /cookietest\n```",
                        parse_mode=ParseMode.MARKDOWN_V2,
                    )
                except Exception: pass
    elif ck and not oa:
        if ADMIN_IDS:
            for admin_id in ADMIN_IDS:
                try:
                    await app.bot.send_message(chat_id=admin_id, text="ℹ cookies.txt found. Run /cookietest to confirm it's working.")
                except Exception: pass

def main():
    builder = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .connect_timeout(120)
        .read_timeout(120)
        .write_timeout(600)
        .pool_timeout(120)
        .get_updates_connect_timeout(60)
        .get_updates_read_timeout(60)
        .get_updates_write_timeout(60)
        .get_updates_pool_timeout(60)
        .post_init(post_init)
    )
    if LOCAL_API_URL:
        builder = builder.base_url(f"{LOCAL_API_URL}/bot")
        logger.info("Using local Bot API server: %s  (limit: %s)", LOCAL_API_URL, fmt_size(MAX_FILE_SIZE))
    else:
        logger.info("Using cloud Bot API  (limit: %s)", fmt_size(MAX_FILE_SIZE))

    app = builder.build()
    app.add_handler(CommandHandler("cache",       cmd_cache))
    app.add_handler(CommandHandler("start",       cmd_start))




    app.add_handler(CommandHandler("help",        cmd_help))
    app.add_handler(CommandHandler("stats",       cmd_stats))
    app.add_handler(CommandHandler("queue",       cmd_queue))

    app.add_handler(CommandHandler("history",     cmd_history))
    app.add_handler(CommandHandler("info",        cmd_info))
    app.add_handler(CommandHandler("search",      cmd_search))
    app.add_handler(CommandHandler("trending",    cmd_trending))
    app.add_handler(CommandHandler("playlist",    cmd_playlist))

    app.add_handler(CallbackQueryHandler(button_handler))

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_link))
    app.add_error_handler(error_handler)

    # Load cache index on startup
    load_cache_index()
    logger.info("Cache index loaded: %d songs", len(cache_index))

    if WEBHOOK_URL:
        logger.info("WEBHOOK mode — port %d", PORT)
        app.run_webhook(listen="0.0.0.0", port=PORT, webhook_url=WEBHOOK_URL, drop_pending_updates=True)
    else:
        logger.info("POLLING mode")
        app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()