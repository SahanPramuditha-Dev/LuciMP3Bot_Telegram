"""
╔═══════════════════════════════════════════════════════════╗
║          N E X U S - D L   //  MEDIA EXTRACTION BOT      ║
║          v3.0  |  by  0xNEXUS  |  classified: PUBLIC     ║
╚═══════════════════════════════════════════════════════════╝

  [BYPASS CHAIN]
  tv_embedded → ios → android → mweb → android_vr
  + PO token (web client) if youtube-po-token-generator found
  + cookies.txt if present (strongest bypass)

  [NEW FEATURES]
  • All resolutions: 144p / 240p / 360p / 480p / 720p / 1080p / 1440p / 2160p (4K)
  • Audio-only: MP3 128/192/320kbps + M4A best
  • /search <query>  — search YouTube without leaving Telegram
  • /trending        — trending YouTube videos
  • /playlist <url>  — list & pick tracks from a playlist
  • /info <url>      — detailed video info card (no download)
  • /history         — your last 5 downloads
  • /cancel          — cancel your queued download
  • /ping            — latency check
  • Hacker-terminal  — matrix-style progress bars & status
"""

import os, re, json, shutil, asyncio, logging, time, hashlib, subprocess, threading, io
from pathlib import Path
from collections import OrderedDict, defaultdict

import yt_dlp
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
)
from telegram.ext import (
    ApplicationBuilder, MessageHandler, CommandHandler,
    CallbackQueryHandler, ContextTypes, filters,
)
from telegram.constants import ParseMode
from telegram.error import BadRequest, NetworkError, RetryAfter

# ═══════════════════════════════════════════════════════════
#  CONFIG
# ═══════════════════════════════════════════════════════════

BOT_TOKEN        = os.environ.get("BOT_TOKEN",    "YOUR_TOKEN")
WEBHOOK_URL      = os.environ.get("WEBHOOK_URL",  "")
PORT             = int(os.environ.get("PORT",     8443))
ADMIN_IDS        = set(
    int(x) for x in os.environ.get("ADMIN_IDS", "").split(",") if x.strip().isdigit()
)

DOWNLOAD_FOLDER  = Path("downloads")
CACHE_FOLDER     = Path("cache")
COOKIES_FILE     = Path("cookies.txt")

# ── File size limit ──────────────────────────────────────────────────────────
# Standard Telegram Bot API hard limit = 50 MB (uploads) / 20 MB (downloads).
# To support up to 2 GB you MUST run a local Bot API server:
#   https://github.com/tdlib/telegram-bot-api
# Set LOCAL_API_URL=http://localhost:8081 (or wherever your server runs).
# Without it, MAX_FILE_SIZE is capped at 50 MB regardless of this setting.
LOCAL_API_URL    = os.environ.get("LOCAL_API_URL", "")   # e.g. http://localhost:8081
_using_local_api = bool(LOCAL_API_URL)
MAX_FILE_SIZE    = 500 * 1024 * 1024  if _using_local_api else 50 * 1024 * 1024

MAX_QUEUE_SIZE   = 20
WORKERS          = 3
RATE_LIMIT_SEC   = 6
DOWNLOAD_TIMEOUT = 1800   # 30 min — large files take time
MAX_RATE_CACHE   = 2000
MAX_HISTORY      = 5
MAX_PLAYLIST_SHOW = 10
FFMPEG_LOCATION  = os.environ.get("FFMPEG_LOCATION", None)

# ── Proxy config ────────────────────────────────────────────────────────────
# Single proxy:  HTTP_PROXY=http://user:pass@host:port
# Multi/rotating: PROXY_LIST=http://u:p@h1:p1,http://u:p@h2:p2,...
# The bot round-robins across the list on each download.
HTTP_PROXY   = os.environ.get("HTTP_PROXY", None)
_PROXY_LIST  = [p.strip() for p in os.environ.get("PROXY_LIST", "").split(",") if p.strip()]
if HTTP_PROXY and HTTP_PROXY not in _PROXY_LIST:
    _PROXY_LIST.insert(0, HTTP_PROXY)
_proxy_index = 0

def get_next_proxy() -> str | None:
    """Round-robin over the proxy list. Returns None if no proxies configured."""
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
#  PO TOKEN  —  refreshed hourly
# ═══════════════════════════════════════════════════════════

_po_token: str | None       = None
_visitor_data: str | None   = None
_po_token_expiry: float     = 0.0
PO_TOKEN_TTL                = 3600

def _generate_po_token() -> tuple[str | None, str | None]:
    try:
        r = subprocess.run(
            ["youtube-po-token-generator"],
            capture_output=True, text=True, timeout=30,
        )
        if r.returncode != 0:
            logger.warning("PO token generator: exit %d — %s", r.returncode, r.stderr.strip())
            return None, None
        data = json.loads(r.stdout)
        po   = data.get("poToken")
        vis  = data.get("visitorData")
        if po:
            logger.info("▶ PO token acquired.")
        return po, vis
    except FileNotFoundError:
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
#  YT-DLP CONFIG  —  multi-layer bypass
# ═══════════════════════════════════════════════════════════
#
#  Why server IPs get blocked:
#    YouTube detects datacenter/VPS ASNs and requires proof of a real
#    browser session before serving video data.
#
#  Bypass priority (strongest first):
#    1. cookies.txt  — exported from a logged-in Chrome/Firefox session
#    2. OAuth2 token — yt-dlp's built-in --username oauth2 flow
#    3. PO token     — requires youtube-po-token-generator npm package
#    4. tv_embedded  — embedded TV client, often skips auth check
#    5. ios/android  — mobile clients, different code path
#    6. HTTP_PROXY   — residential/ISP proxy env var (best permanent fix)
#
# ═══════════════════════════════════════════════════════════

# ── OAuth2  —  Google Device Authorization Flow ──────────────────────────────
#
#  HOW IT WORKS (direct Google API — no yt-dlp wrapper needed):
#   1. POST to Google device auth endpoint -> get short user_code + URL
#   2. Send URL + code to admin via Telegram (takes 5 seconds)
#   3. Admin opens URL on their phone, enters code, signs into Google
#   4. Bot polls Google until approved, writes token in yt-dlp cache format
#   5. All future downloads use the token automatically — works from any IP
#   6. Refresh token never expires unless you revoke it
# ─────────────────────────────────────────────────────────────────────────────

import urllib.request
import urllib.parse

# yt-dlp's public YouTube TV client credentials (from yt-dlp source)
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
    """Write token in yt-dlp exact format so it auto-loads on every download."""
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
    """Return True if a valid OAuth2 token is cached."""
    return ((_yt_dlp_cache_dir() / "youtube-oauth2.token.json").exists()
            or OAUTH2_TOKEN_FILE.exists())

def _http_post(url: str, data: dict) -> dict:
    """Synchronous HTTPS POST using httpx (already a dependency)."""
    import httpx
    r = httpx.post(url, data=data, timeout=30)
    r.raise_for_status()
    return r.json()

def _run_oauth2_flow(notify_callback) -> None:
    """
    Background thread — complete Google device auth flow.
    notify_callback(url, code, error=None) — url+code ready, or error string
    notify_callback(None, None) — finished (check oauth2_token_exists())
    """
    try:
        # Step 1: request device code from Google
        resp = _http_post(_DEVICE_CODE_URL, {
            "client_id": _OAUTH_CLIENT_ID,
            "scope":     _OAUTH_SCOPE,
        })

        # Surface any error Google returns (e.g. invalid client)
        if "error" in resp:
            notify_callback(None, None, error=f"Google error: {resp['error']} — {resp.get('error_description','')}")
            return

        device_code      = resp["device_code"]
        user_code        = resp["user_code"]
        verification_url = resp.get("verification_url", "https://www.google.com/device")
        interval         = int(resp.get("interval", 5))
        expires_in       = int(resp.get("expires_in", 300))
        logger.info("OAuth2 device_code ready, user_code=%s url=%s", user_code, verification_url)

        # Step 2: send URL+code to admin
        notify_callback(verification_url, user_code)

        # Step 3: poll until approved or expired
        deadline = time.time() + expires_in
        while time.time() < deadline:
            time.sleep(interval)
            try:
                token = _http_post(_TOKEN_URL, {
                    "client_id":     _OAUTH_CLIENT_ID,
                    "client_secret": _OAUTH_CLIENT_SECRET,
                    "device_code":   device_code,
                    "grant_type":    "urn:ietf:params:oauth:grant-type:device_code",
                })
                if "access_token" in token:
                    _write_ydlp_token(token)
                    notify_callback(None, None)
                    return
                err = token.get("error", "")
                if err == "authorization_pending":
                    continue
                if err == "slow_down":
                    interval = min(interval + 5, 30)
                    continue
                if err in ("access_denied", "expired_token"):
                    notify_callback(None, None, error=f"Google: {err}")
                    return
            except Exception as pe:
                logger.debug("OAuth2 poll: %s", pe)
                time.sleep(interval)

    except Exception as e:
        logger.error("OAuth2 flow crash: %s", e)
        notify_callback(None, None, error=str(e))
        return

    notify_callback(None, None)  # timeout

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/125.0.0.0 Safari/537.36"
)

def build_extractor_args() -> dict:
    po, vis = get_po_token()
    # tv_embedded works without cookies on most IPs; ios/android as fallback
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
        "quiet":            True,
        "no_warnings":      True,
        "http_headers": {
            "User-Agent":      _UA,
            "Accept-Language": "en-US,en;q=0.9",
            "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
        "extractor_args":   build_extractor_args(),
        "socket_timeout":   30,
        "retries":          5,
        "fragment_retries": 5,
    }
    # cookies.txt  — strongest auth signal
    if COOKIES_FILE.exists():
        opts["cookiefile"] = str(COOKIES_FILE)
        logger.debug("Using cookies.txt")
    # OAuth2 — yt-dlp's own cached refresh token (survives IP changes, lasts forever)
    if oauth2_token_exists():
        opts["username"] = "oauth2"
        opts["password"] = ""
        logger.debug("Using OAuth2 token")
    if FFMPEG_LOCATION:
        opts["ffmpeg_location"] = FFMPEG_LOCATION
    proxy = get_next_proxy()
    if proxy:
        opts["proxy"] = proxy
        logger.debug("Using proxy: %s…", proxy[:30])
    return opts

# Fallback extractor_args tried one-by-one when primary is blocked
_FALLBACK_CLIENTS: list[dict] = [
    {"youtube": {"player_client": ["tv_embedded"]}},
    {"youtube": {"player_client": ["ios"]}},
    {"youtube": {"player_client": ["mweb"]}},
    {"youtube": {"player_client": ["android"]}},
    {"youtube": {"player_client": ["android_vr"]}},
    # Last resort: web with no po_token (sometimes works on fresh IPs)
    {"youtube": {"player_client": ["web"]}},
]

def _is_bot_block(e: Exception) -> bool:
    m = str(e).lower()
    return any(k in m for k in [
        "sign in", "signin", "bot", "confirm you",
        "403", "429", "blocked", "not available",
        "video unavailable",
    ])

def bypass_status() -> dict:
    """Return current state of each bypass method for display."""
    po, _ = get_po_token()
    return {
        "cookies":     COOKIES_FILE.exists(),
        "oauth2":      oauth2_token_exists(),
        "po_token":    bool(po),
        "proxy":       len(_PROXY_LIST) > 0,
        "proxy_count": len(_PROXY_LIST),
    }

# ═══════════════════════════════════════════════════════════
#  GLOBALS
# ═══════════════════════════════════════════════════════════

download_queue:    asyncio.Queue           = None
active_downloads:  dict[int, str]          = {}
user_last_request: OrderedDict             = OrderedDict()
# user_id → list of dicts {title, typ, quality, ts}
user_history:      dict[int, list]         = defaultdict(list)
# user_id → queue item (for /cancel)
user_queue_item:   dict[int, asyncio.Task] = {}

stats = {
    "users":      set(),
    "downloads":  0,
    "failed":     0,
    "bytes_sent": 0,
    "start_time": time.time(),
}

# ═══════════════════════════════════════════════════════════
#  HACKER-STYLE ANIMATIONS  ("terminal" aesthetic)
# ═══════════════════════════════════════════════════════════

# Monospace block chars for progress bar
_BAR_FULL  = "█"
_BAR_HEAD  = "▓"
_BAR_EMPTY = "░"

# Spinning "radar sweep"
_RADAR = ["◜", "◝", "◞", "◟"]
_SCAN  = ["▰▱▱▱▱▱▱▱", "▰▰▱▱▱▱▱▱", "▰▰▰▱▱▱▱▱", "▰▰▰▰▱▱▱▱",
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

_DL_PHASES = [
    "INTERCEPTING STREAM",
    "PULLING FRAGMENTS",
    "STITCHING SEGMENTS",
    "MUXING CONTAINER",
    "VERIFYING PAYLOAD",
]

def _hbar(pct: float, width: int = 16) -> str:
    filled = int(pct / 100 * width)
    head   = 1 if filled < width else 0
    empty  = width - filled - head
    return _BAR_FULL * filled + (_BAR_HEAD if head else "") + _BAR_EMPTY * max(empty, 0)

def _phase(pct: float) -> str:
    idx = min(int(pct / 100 * len(_DL_PHASES)), len(_DL_PHASES) - 1)
    return _DL_PHASES[idx]

def _glitch(text: str, frame: int) -> str:
    """Occasionally corrupt one char — only block chars safe inside ``` fences."""
    glitch_chars = "▒░▓▌▐▀▄█▊▋▍▎▏▶◀◆◇○●◉"
    if frame % 7 == 0 and len(text) > 4:
        pos = (frame * 3) % len(text)
        c   = glitch_chars[(frame * 13) % len(glitch_chars)]
        return text[:pos] + c + text[pos + 1:]
    return text

# ═══════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════

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
    # backticks break ``` fences in Telegram markdown
    title = re.sub(r'[\\/*?:"<>|#%&{}$!\'@+`=]', "_", title)
    title = title.replace("`", "'")
    return re.sub(r'\s+', " ", title).strip()[:120]

def ckey(url: str, typ: str, q: str) -> str:
    return hashlib.md5(f"{url}|{typ}|{q}".encode()).hexdigest()[:12] + f"_{typ}_{q}"

def mdescape(t: str) -> str:
    return re.sub(r'([_*\[\]()~`>#+=|{}.!\\-])', r'\\\1', t)

def normalize_url(entry: dict) -> str:
    """
    extract_flat entries sometimes have 'url' as a bare video ID.
    Always return a canonical https://www.youtube.com/watch?v=... URL.
    """
    raw    = (entry.get("url") or "").strip()
    vid_id = (entry.get("id")  or "").strip()
    if raw.startswith("http"):
        return raw
    # Prefer the explicit id field
    if vid_id:
        return f"https://www.youtube.com/watch?v={vid_id}"
    # Fall back to treating raw as an id if it looks like one
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
        if bs["cookies"]:  active.append("cookies.txt")
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
            f"*Fix — send your cookies to this bot:*\n"
            f"1\\. On your PC, run:\n"
            f"`yt-dlp --cookies-from-browser chrome --cookies cookies.txt`\n"
            f"2\\. Send the `cookies.txt` file to this chat\n"
            f"   The bot will auto\\-detect and save it\\.\n\n"
            f"_Or set `HTTP\\_PROXY` env var to a residential proxy\\._"
        )
    if "429" in m:
        return (
            "```\n[RATE LIMITED 429]\n"
            "YouTube throttling this IP.\n"
            "Wait 5-10 min and retry.\n```"
        )
    if "private" in low:
        return (
            "```\n[LOCKED CONTENT]\n"
            "Video is private or age-restricted.\n"
            "Send cookies.txt from a logged-in\n"
            "YouTube session to unlock.\n```"
        )
    if "geo" in low or "not available in your country" in low:
        return (
            "```\n[GEO BLOCK]\n"
            "Video unavailable in server region.\n"
            "Set HTTP_PROXY env var to bypass.\n```"
        )
    if "copyright" in low:
        return "```\n[DMCA BLOCK]\nVideo blocked by copyright claim.\n```"
    safe = m[:250].replace("`", "'").replace("\\", "/")
    return f"```\n[DOWNLOAD FAILED]\n{safe}\n```"

# ═══════════════════════════════════════════════════════════
#  VIDEO INFO  —  with client fallback chain
# ═══════════════════════════════════════════════════════════

def _info(url: str, opts: dict) -> dict:
    with yt_dlp.YoutubeDL(opts) as ydl:
        return ydl.extract_info(url, download=False)

def get_video_info(url: str) -> dict:
    base = {**build_ydl_common(), "skip_download": True}
    try:
        return _info(url, base)
    except Exception as e:
        if not _is_bot_block(e): raise
        logger.warning("Primary client blocked — falling back…")

    for i, fb in enumerate(_FALLBACK_CLIENTS):
        try:
            info = _info(url, {**base, "extractor_args": fb})
            logger.info("Fallback #%d succeeded (info).", i + 1)
            return info
        except Exception as fe:
            logger.debug("Fallback #%d info fail: %s", i + 1, fe)

    raise Exception("All bypass clients failed. Add cookies.txt — see /help.")

# ═══════════════════════════════════════════════════════════
#  FORMAT / BUTTON BUILDER
# ═══════════════════════════════════════════════════════════

# All resolutions we want to offer
_ALL_HEIGHTS = [144, 240, 360, 480, 720, 1080, 1440, 2160]
_RES_ICONS   = {
    144:  "📺",
    240:  "📺",
    360:  "📱",
    480:  "💻",
    720:  "🖥",
    1080: "🖥",
    1440: "🔲",
    2160: "🎞",
}
_RES_LABELS = {
    144:  "144p",
    240:  "240p",
    360:  "360p",
    480:  "480p",
    720:  "720p HD",
    1080: "1080p FHD",
    1440: "1440p QHD",
    2160: "2160p 4K",
}

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
        f"```\n╔{'═'*40}\n"
        f"║ {title}\n"
        f"╠{'═'*40}\n"
        f"║ SRC  : {ext}\n"
        f"║ BY   : {uploader[:30]}\n"
        f"║ DATE : {ds}\n"
        f"║ DUR  : {dur}\n"
        f"║ VIEWS: {views}   LIKES: {likes}\n"
        f"╚{'═'*40}\n```\n"
        f"_{desc}_\n\n"
        f"*SELECT OUTPUT FORMAT:*"
    )

def build_buttons(info: dict, url: str) -> list:
    """
    Always show ALL standard resolution tiers — no format-list detection.

    Root cause of the detection approach failing:
      tv_embedded / ios / android clients used for bot-bypass return sparse or
      completely empty format lists during info-fetch (heights are null or missing).
      Any logic that tries to infer max_height from those lists will silently
      cap buttons at whatever low-res formats happen to be reported.

    Correct approach:
      Show every tier unconditionally. yt-dlp's format fallback chain
        bestvideo[height=N] / bestvideo[height<=N] / best
      handles unavailable resolutions gracefully at download time — if the
      video is only 720p and the user picks 4K, they just get 720p.
      No harm done, and the user always sees the full menu.
    """
    safe_url = url.replace("|", "%7C")
    rows: list = []

    # ── All 8 video tiers, 2 per row ──────────────────────────────
    # Layout:  [144p]  [240p]
    #          [360p]  [480p]
    #          [720p HD]  [1080p FHD]
    #          [1440p QHD]  [2160p 4K]
    vid_row: list = []
    for h in _ALL_HEIGHTS:
        lbl = f"{_RES_ICONS[h]} {_RES_LABELS[h]}"
        vid_row.append(InlineKeyboardButton(lbl, callback_data=f"mp4|{h}|{safe_url}"))
        if len(vid_row) == 2:
            rows.append(vid_row)
            vid_row = []
    if vid_row:
        rows.append(vid_row)

    # ── Best-quality shortcut (let yt-dlp decide) ─────────────────
    rows.append([
        InlineKeyboardButton("🏆 Best Video+Audio", callback_data=f"mp4|best|{safe_url}")
    ])

    # ── Audio formats ─────────────────────────────────────────────
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
#  ANIMATED FETCH  (hacker terminal style)
# ═══════════════════════════════════════════════════════════

async def animated_fetch(msg, stop: asyncio.Event):
    frame = 0
    while not stop.is_set():
        radar = _RADAR[frame % len(_RADAR)]
        scan  = _SCAN[frame % len(_SCAN)]
        line  = _FETCH_LINES[frame % len(_FETCH_LINES)].format(d="." * (frame % 4))
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
#  COMMANDS
# ═══════════════════════════════════════════════════════════

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    stats["users"].add(update.effective_user.id)
    name = update.effective_user.first_name or "AGENT"
    await update.message.reply_text(
        f"```\n"
        f"╔══════════════════════════════════════╗\n"
        f"║   N E X U S - D L  //  v3.0         ║\n"
        f"║   MEDIA EXTRACTION SYSTEM ONLINE     ║\n"
        f"╠══════════════════════════════════════╣\n"
        f"║  AGENT AUTHENTICATED: {name[:14]:<14} ║\n"
        f"╚══════════════════════════════════════╝\n"
        f"```\n"
        f"*TARGET PLATFORMS:*\n"
        f"`YouTube` `TikTok` `Instagram` `Twitter/X`\n"
        f"`Facebook` `Reddit` `SoundCloud` `Twitch` `Vimeo`\n\n"
        f"*COMMANDS:*\n"
        f"`/search` — search YouTube\n"
        f"`/info`   — inspect target URL\n"
        f"`/playlist` — extract playlist\n"
        f"`/trending` — trending [music/gaming/news]\n"
        f"`/history` — your download log\n"
        f"`/queue`  — mission queue status\n"
        f"`/stats`  — system telemetry\n"
        f"`/ping`   — latency check\n"
        f"`/help`   — operator manual\n\n"
        f"_Drop any URL to begin extraction_",
        parse_mode=ParseMode.MARKDOWN,
    )

async def cmd_setcookies(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Tell the user exactly how to upload cookies."""
    await update.message.reply_text(
        "```\n"
        "[COOKIE UPLOAD INSTRUCTIONS]\n"
        "─────────────────────────────\n"
        "Step 1: Export on your PC\n"
        "  yt-dlp --cookies-from-browser chrome \\\n"
        "         --cookies cookies.txt\n"
        "\n"
        "Step 2: Send the file\n"
        "  Attach cookies.txt to this chat\n"
        "  as a FILE (not a photo/text)\n"
        "\n"
        "The bot will auto-save and activate it.\n"
        "─────────────────────────────\n"
        "Supported browsers:\n"
        "  chrome / firefox / edge /\n"
        "  safari / brave / opera\n"
        "```",
        parse_mode=ParseMode.MARKDOWN_V2,
    )

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Auto-detect cookies.txt uploaded as a Telegram document.
    Any admin (or any user if ADMIN_IDS is empty) can upload.
    """
    doc  = update.message.document
    uid  = update.effective_user.id

    # Restrict to admins if configured
    if ADMIN_IDS and uid not in ADMIN_IDS:
        await update.message.reply_text(
            "```\n[RESTRICTED]\nOnly admins can upload cookies.\n```",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    fname = (doc.file_name or "").lower()
    if "cookie" not in fname and not fname.endswith(".txt"):
        # Not a cookies file — ignore silently
        return

    msg = await update.message.reply_text(
        "```\n[RECEIVING COOKIES...]\n```", parse_mode=ParseMode.MARKDOWN_V2
    )
    try:
        tg_file = await doc.get_file()
        await tg_file.download_to_drive(str(COOKIES_FILE))

        # Quick sanity check — valid Netscape cookie files start with # Netscape
        raw = COOKIES_FILE.read_text(errors="ignore")[:200]
        if "HTTP Cookie File" not in raw and "Netscape" not in raw and "#" not in raw[:5]:
            COOKIES_FILE.unlink(missing_ok=True)
            await safe_edit(msg,
                "```\n[INVALID FILE]\nNot a valid Netscape cookies.txt\n"
                "Export using:\n"
                "yt-dlp --cookies-from-browser chrome --cookies cookies.txt\n```",
                parse_mode=ParseMode.MARKDOWN_V2,
            )
            return

        line_count = raw.count("\n")
        await safe_edit(msg,
            f"```\n"
            f"[COOKIES ACTIVATED]\n"
            f"file    : {COOKIES_FILE.name}\n"
            f"size    : {fmt_size(COOKIES_FILE.stat().st_size)}\n"
            f"lines   : ~{line_count}\n"
            f"status  : ACTIVE\n"
            f"```\n\n"
            f"✅ Cookie bypass is now active\\. Try your download again\\.",
            parse_mode=ParseMode.MARKDOWN,
        )
        logger.info("cookies.txt updated by user %d (%d bytes)", uid, COOKIES_FILE.stat().st_size)
    except Exception as e:
        await safe_edit(msg,
            f"```\n[UPLOAD FAILED]\n{str(e)[:150].replace('`',chr(39))}\n```",
            parse_mode=ParseMode.MARKDOWN_V2,
        )

async def cmd_auth(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /auth — One-time Google OAuth2 setup that fixes IP blocking permanently.
    Uses Google's TV device-auth flow directly (no yt-dlp wrapper).
    After approval the token auto-refreshes forever.
    """
    uid = update.effective_user.id

    if ADMIN_IDS and uid not in ADMIN_IDS:
        await update.message.reply_text(
            "```\n[RESTRICTED]\nOnly admins can run /auth.\n```",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    if oauth2_token_exists():
        await update.message.reply_text(
            "```\n"
            "[OAUTH2: ALREADY ACTIVE]\n"
            "Google account is linked.\n"
            "Downloads bypass IP blocks.\n"
            "\n"
            "To re-auth, delete token file:\n"
            "  rm ~/.cache/yt-dlp/youtube-oauth2.token.json\n"
            "  rm oauth2_token.json\n"
            "Then run /auth again.\n"
            "```",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    if uid in _oauth_pending:
        # Clear stuck state older than 10 minutes
        entry = _oauth_pending[uid]
        if isinstance(entry, dict) and time.time() - entry.get("ts", 0) > 600:
            del _oauth_pending[uid]
        elif isinstance(entry, bool):
            del _oauth_pending[uid]
        else:
            await update.message.reply_text(
                "```\n[AUTH IN PROGRESS]\nAlready running. Check above for the code.\nIf stuck, wait 2 min and try again.\n```",
                parse_mode=ParseMode.MARKDOWN_V2,
            )
            return

    status_msg = await update.message.reply_text(
        "```\n"
        "[CONTACTING GOOGLE...]\n"
        "Requesting device auth code.\n"
        "Takes 3-10 seconds.\n"
        "```",
        parse_mode=ParseMode.MARKDOWN_V2,
    )

    # Capture the running loop BEFORE entering the thread
    loop = asyncio.get_running_loop()
    _oauth_pending[uid] = {"ts": time.time()}

    def on_event(url: str | None, code: str | None, error: str | None = None):
        """Called from background thread. url=None means flow is done."""

        if url is not None:
            # Show the auth URL + code to the admin
            _oauth_pending[uid]["ts"] = time.time()
            text = (
                "```\n"
                "[ACTION REQUIRED]\n"
                "─────────────────────────────\n"
                "1. Open on your phone:\n"
                "```\n"
                f"https://google.com/device\n"
                "```\n"
                "2. Enter this code:\n"
                "```\n"
                f"  {code}\n"
                "```\n"
                "3. Sign in with any Google account\n\n"
                f"_Full URL: {url}_\n\n"
                "_Bot confirms automatically. Waiting up to 5 min..._"
            )
            asyncio.run_coroutine_threadsafe(
                safe_edit(status_msg, text, parse_mode=ParseMode.MARKDOWN),
                loop,
            )
            return

        # url is None → flow finished
        _oauth_pending.pop(uid, None)

        if error:
            # Show the real error so we know what went wrong
            err_safe = error[:200].replace("`", "'")
            fail_text = (
                f"```\n"
                f"[AUTH ERROR]\n"
                f"{err_safe}\n"
                f"\n"
                f"Run /auth again to retry.\n"
                f"```"
            )
            asyncio.run_coroutine_threadsafe(
                safe_edit(status_msg, fail_text, parse_mode=ParseMode.MARKDOWN_V2),
                loop,
            )
            return

        if oauth2_token_exists():
            success_text = (
                "```\n"
                "[AUTH COMPLETE]\n"
                "Google account linked!\n"
                "Token saved permanently.\n"
                "\n"
                "All downloads now bypass\n"
                "YouTube IP blocks.\n"
                "No further setup needed.\n"
                "```"
            )
            asyncio.run_coroutine_threadsafe(
                safe_edit(status_msg, success_text, parse_mode=ParseMode.MARKDOWN_V2),
                loop,
            )
        else:
            fail_text = (
                "```\n"
                "[AUTH TIMED OUT]\n"
                "Code not entered in time.\n"
                "Run /auth again to retry.\n"
                "```"
            )
            asyncio.run_coroutine_threadsafe(
                safe_edit(status_msg, fail_text, parse_mode=ParseMode.MARKDOWN_V2),
                loop,
            )

    def run_with_error_guard():
        """Wrapper that catches all exceptions and reports them."""
        try:
            _run_oauth2_flow(on_event)
        except Exception as e:
            logger.error("cmd_auth thread crash: %s", e)
            _oauth_pending.pop(uid, None)
            err = str(e)[:120].replace("`", "'")
            asyncio.run_coroutine_threadsafe(
                safe_edit(
                    status_msg,
                    f"```\n[OAUTH2 ERROR]\n{err}\nRun /auth to retry.\n```",
                    parse_mode=ParseMode.MARKDOWN_V2,
                ),
                loop,
            )

    threading.Thread(target=run_with_error_guard, daemon=True, name="oauth2").start()


async def cmd_authtest(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /authtest — test the Google OAuth2 endpoint directly and show raw response.
    Use this to diagnose /auth failures.
    """
    uid = update.effective_user.id
    if ADMIN_IDS and uid not in ADMIN_IDS:
        return

    msg = await update.message.reply_text(
        "```\n[TESTING GOOGLE API...]\n```", parse_mode=ParseMode.MARKDOWN_V2
    )

    def _test():
        import httpx, traceback
        results = []
        try:
            results.append(f"client_id: {_OAUTH_CLIENT_ID[:30]}...")
            results.append(f"endpoint : {_DEVICE_CODE_URL}")
            r = httpx.post(_DEVICE_CODE_URL, data={
                "client_id": _OAUTH_CLIENT_ID,
                "scope":     _OAUTH_SCOPE,
            }, timeout=15)
            results.append(f"HTTP     : {r.status_code}")
            body = r.text[:400].replace("`", "'")
            results.append(f"response :\n{body}")
        except Exception as e:
            results.append(f"ERROR: {e}")
            results.append(traceback.format_exc()[-300:].replace("`","'"))
        return "\n".join(results)

    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(None, _test)
    safe_result = result.replace("\\", "/")
    await safe_edit(
        msg,
        f"```\n[GOOGLE API TEST]\n{safe_result}\n```",
        parse_mode=ParseMode.MARKDOWN_V2,
    )

async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    t0  = time.monotonic()
    msg = await update.message.reply_text("```\n[PING] measuring...\n```", parse_mode=ParseMode.MARKDOWN_V2)
    ms  = int((time.monotonic() - t0) * 1000)
    bar = ("█" * min(ms // 10, 20)).ljust(20, "░")
    quality = "OPTIMAL" if ms < 200 else ("NOMINAL" if ms < 500 else "DEGRADED")
    await msg.edit_text(
        f"```\n"
        f"[LATENCY CHECK]\n"
        f"RTT   : {ms}ms\n"
        f"BAR   : {bar}\n"
        f"STATUS: {quality}\n"
        f"```",
        parse_mode=ParseMode.MARKDOWN_V2,
    )

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bs    = bypass_status()
    def st(ok): return "OK  ✅" if ok else "----  ✗"
    proxy_str = f"OK ({bs['proxy_count']}x) ✅" if bs["proxy"] else "----  ✗"
    api_str   = "LOCAL ✅" if _using_local_api else "CLOUD (50MB cap)"
    await update.message.reply_text(
        f"```\n"
        f"╔═══════════════════════════════════╗\n"
        f"║  NEXUS-DL  //  OPERATOR MANUAL   ║\n"
        f"╠═══════════════════════════════════╣\n"
        f"║  BYPASS STATUS                   ║\n"
        f"║  cookies.txt : {st(bs['cookies']):<20}║\n"
        f"║  oauth2      : {st(bs['oauth2']):<20}║\n"
        f"║  po_token    : {st(bs['po_token']):<20}║\n"
        f"║  proxy       : {proxy_str:<20}║\n"
        f"╠═══════════════════════════════════╣\n"
        f"║  FILE LIMITS                     ║\n"
        f"║  bot api  : {api_str:<22}║\n"
        f"║  max_file : {fmt_size(MAX_FILE_SIZE):<22}║\n"
        f"║  queue    : {MAX_QUEUE_SIZE:<22}║\n"
        f"║  rate     : {RATE_LIMIT_SEC}s cooldown{'':<13}║\n"
        f"╠═══════════════════════════════════╣\n"
        f"║  VIDEO FORMATS                   ║\n"
        f"║  144p / 240p / 360p / 480p        ║\n"
        f"║  720p / 1080p / 1440p / 2160p     ║\n"
        f"║  Best Video+Audio (auto)          ║\n"
        f"║  AUDIO FORMATS                   ║\n"
        f"║  MP3 128k / 192k / 320k           ║\n"
        f"║  M4A best  /  OGG best            ║\n"
        f"╠═══════════════════════════════════╣\n"
        f"║  RAISE FILE LIMIT TO 2 GB        ║\n"
        f"║  Run local Bot API server:        ║\n"
        f"║  docker run -d                    ║\n"
        f"║    aiogram/telegram-bot-api       ║\n"
        f"║  Set env: LOCAL_API_URL=          ║\n"
        f"║    http://localhost:8081          ║\n"
        f"╚═══════════════════════════════════╝\n"
        f"```\n\n"
        f"*Fix IP block:* send `cookies.txt` to this chat\n"
        f"or run `/auth` to link a Google account\\.",
        parse_mode=ParseMode.MARKDOWN,
    )

async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    up    = int(time.time() - stats["start_time"])
    total = stats["downloads"] + stats["failed"]
    rate  = (stats["downloads"] / total * 100) if total else 0
    qsize = download_queue.qsize() if download_queue else 0
    mb    = stats["bytes_sent"] / (1024 * 1024)
    run_bar = ("▰" * len(active_downloads)).ljust(WORKERS, "▱")
    await update.message.reply_text(
        f"```\n"
        f"╔════════════════════════════════╗\n"
        f"║  NEXUS-DL  //  TELEMETRY      ║\n"
        f"╠════════════════════════════════╣\n"
        f"║  uptime  : {fmt_uptime(up):<20}║\n"
        f"║  users   : {len(stats['users']):<20}║\n"
        f"║  success : {stats['downloads']:<20}║\n"
        f"║  failed  : {stats['failed']:<20}║\n"
        f"║  rate    : {rate:<19.1f}%║\n"
        f"║  data_tx : {mb:<17.1f} MB ║\n"
        f"║  queue   : {qsize:<20}║\n"
        f"║  workers : [{run_bar}] {len(active_downloads)}/{WORKERS}  ║\n"
        f"╚════════════════════════════════╝\n"
        f"```",
        parse_mode=ParseMode.MARKDOWN_V2,
    )

async def cmd_queue(update: Update, context: ContextTypes.DEFAULT_TYPE):
    qsize = download_queue.qsize() if download_queue else 0
    run   = len(active_downloads)
    w_bar = ("▰" * run).ljust(WORKERS, "▱")
    q_bar = ("█" * min(qsize, 10)).ljust(10, "░")
    await update.message.reply_text(
        f"```\n"
        f"[MISSION QUEUE]\n"
        f"workers  [{w_bar}] {run}/{WORKERS}\n"
        f"pending  [{q_bar}] {qsize}/{MAX_QUEUE_SIZE}\n"
        f"```",
        parse_mode=ParseMode.MARKDOWN_V2,
    )

async def cmd_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    h   = user_history.get(uid, [])
    if not h:
        await update.message.reply_text(
            "```\n[HISTORY LOG — EMPTY]\nNo downloads recorded yet.\n```",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return
    lines = ["```", "[RECENT DOWNLOADS]", "─" * 32]
    for i, entry in enumerate(reversed(h), 1):
        ts  = time.strftime("%m-%d %H:%M", time.localtime(entry["ts"]))
        typ = entry["typ"].upper()
        q   = entry["quality"]
        lines.append(f"#{i} [{ts}] {typ}/{q}")
        lines.append(f"   {entry['title'][:30]}")
    lines.append("```")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN_V2)

async def cmd_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args:
        await update.message.reply_text(
            "```\n[USAGE]\n/info <url>\n```", parse_mode=ParseMode.MARKDOWN_V2
        )
        return
    url = args[0].strip()
    msg = await update.message.reply_text(
        "```\n[SCANNING TARGET...]\n```", parse_mode=ParseMode.MARKDOWN_V2
    )
    try:
        info = await asyncio.wait_for(
            asyncio.get_event_loop().run_in_executor(None, get_video_info, url),
            timeout=30,
        )
    except Exception as e:
        await safe_edit(msg, f"```\n[SCAN FAILED]\n{mdescape(str(e)[:200])}\n```",
                        parse_mode=ParseMode.MARKDOWN_V2)
        return

    fmts    = info.get("formats", [])
    heights = sorted(set(f.get("height") for f in fmts if f.get("height")))
    codecs  = sorted(set(f.get("vcodec","").split(".")[0] for f in fmts if f.get("vcodec") and f.get("vcodec") != "none"))
    acodecs = sorted(set(f.get("acodec","").split(".")[0] for f in fmts if f.get("acodec") and f.get("acodec") != "none"))
    title   = sanitize(info.get("title","?"))[:40]
    ch      = info.get("uploader") or info.get("channel") or "?"
    dur     = fmt_dur(info.get("duration") or 0)
    views   = fmt_views(info.get("view_count") or 0)
    likes   = fmt_views(info.get("like_count") or 0)
    tags    = ", ".join((info.get("tags") or [])[:5]) or "—"

    await safe_edit(
        msg,
        f"```\n"
        f"╔══════════════════════════════════╗\n"
        f"║  TARGET ANALYSIS COMPLETE        ║\n"
        f"╠══════════════════════════════════╣\n"
        f"║ TITLE  : {title[:34]:<34}║\n"
        f"║ CH     : {ch[:34]:<34}║\n"
        f"║ DUR    : {dur:<34}║\n"
        f"║ VIEWS  : {views:<34}║\n"
        f"║ LIKES  : {likes:<34}║\n"
        f"╠══════════════════════════════════╣\n"
        f"║ V.RES  : {str(heights)[:34]:<34}║\n"
        f"║ V.CODEC: {str(codecs)[:34]:<34}║\n"
        f"║ A.CODEC: {str(acodecs)[:34]:<34}║\n"
        f"║ TAGS   : {tags[:34]:<34}║\n"
        f"║ FORMATS: {len(fmts):<34}║\n"
        f"╚══════════════════════════════════╝\n"
        f"```",
        parse_mode=ParseMode.MARKDOWN_V2,
    )

async def cmd_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = " ".join(context.args).strip()
    if not query:
        await update.message.reply_text(
            "```\n[USAGE]\n/search <keywords>\n```", parse_mode=ParseMode.MARKDOWN_V2
        )
        return

    safe_q = query[:40].replace("`", "'").replace("\\", "/")
    msg = await update.message.reply_text(
        f"```\n[SEARCHING YT]\nquery: {safe_q}\n```",
        parse_mode=ParseMode.MARKDOWN_V2,
    )

    def _do_search():
        opts = {**build_ydl_common(), "skip_download": True, "quiet": True,
                "extract_flat": True, "playlistend": 8}
        with yt_dlp.YoutubeDL(opts) as ydl:
            return ydl.extract_info(f"ytsearch8:{query}", download=False)

    try:
        results = await asyncio.wait_for(
            asyncio.get_event_loop().run_in_executor(None, _do_search),
            timeout=30,
        )
    except Exception as e:
        await safe_edit(msg, f"```\n[SEARCH FAILED]\n{mdescape(str(e)[:150])}\n```",
                        parse_mode=ParseMode.MARKDOWN_V2)
        return

    entries = results.get("entries") or []
    if not entries:
        await safe_edit(msg, "```\n[NO RESULTS FOUND]\n```", parse_mode=ParseMode.MARKDOWN_V2)
        return

    lines   = ["```", f"[RESULTS: {len(entries)}]", f"query: {query[:35]}", "─" * 36]
    buttons = []
    for i, e in enumerate(entries[:8], 1):
        title = sanitize(e.get("title","?"))[:38]
        dur   = fmt_dur(e.get("duration") or 0)
        lines.append(f"[{i}] {title}")
        lines.append(f"     ⏱ {dur}")
        url      = normalize_url(e)
        safe_url = url.replace("|", "%7C")
        buttons.append([InlineKeyboardButton(
            f"[{i}] {title[:30]}",
            callback_data=f"fetch|{safe_url}"
        )])
    lines.append("```")
    buttons.append([InlineKeyboardButton("✖ Close", callback_data="cancel")])

    await safe_edit(msg, "\n".join(lines), parse_mode=ParseMode.MARKDOWN_V2,
                    reply_markup=InlineKeyboardMarkup(buttons))

async def cmd_trending(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    YouTube's /feed/trending wall requires authentication from server IPs and
    is blocked by most yt-dlp clients. Use targeted ytsearch queries instead —
    these work on any IP without cookies.
    Optional arg: category keyword e.g. /trending music
    """
    category = " ".join(context.args).strip() if context.args else ""
    tag      = category or "today"
    safe_tag = tag[:20].replace("`", "'")

    msg = await update.message.reply_text(
        f"```\n[SCANNING TRENDING: {safe_tag.upper()}]\n```",
        parse_mode=ParseMode.MARKDOWN_V2,
    )

    search_q = f"ytsearch10:trending {category} 2025".strip()

    def _trend():
        opts = {**build_ydl_common(), "skip_download": True,
                "extract_flat": True, "quiet": True}
        with yt_dlp.YoutubeDL(opts) as ydl:
            r = ydl.extract_info(search_q, download=False)
            return r.get("entries") or []

    try:
        entries = await asyncio.wait_for(
            asyncio.get_event_loop().run_in_executor(None, _trend),
            timeout=40,
        )
    except Exception as e:
        err = str(e)[:120].replace("`", "'")
        await safe_edit(msg,
            f"```\n[TRENDING FAILED]\n{err}\n```",
            parse_mode=ParseMode.MARKDOWN_V2)
        return

    entries = [e for e in entries if e and e.get("id")][:10]
    if not entries:
        await safe_edit(msg,
            "```\n[NO RESULTS]\nTry: /trending music\n     /trending gaming\n     /trending news\n```",
            parse_mode=ParseMode.MARKDOWN_V2)
        return

    lines   = ["```", f"[TRENDING: {safe_tag.upper()}]", "─" * 34]
    buttons = []
    for i, e in enumerate(entries, 1):
        title    = sanitize(e.get("title") or "Unknown")[:34]
        dur      = fmt_dur(e.get("duration") or 0)
        url      = normalize_url(e)
        safe_url = url.replace("|", "%7C")
        lines.append(f"[{i:02d}] {title}")
        lines.append(f"      {dur}")
        buttons.append([InlineKeyboardButton(
            f"#{i} {title[:32]}",
            callback_data=f"fetch|{safe_url}"
        )])
    lines.append("```")
    buttons.append([InlineKeyboardButton("✖ Close", callback_data="cancel")])

    await safe_edit(msg, "\n".join(lines), parse_mode=ParseMode.MARKDOWN_V2,
                    reply_markup=InlineKeyboardMarkup(buttons))

async def cmd_playlist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args:
        await update.message.reply_text(
            "```\n[USAGE]\n/playlist <url>\n```", parse_mode=ParseMode.MARKDOWN_V2
        )
        return
    url = args[0].strip()
    msg = await update.message.reply_text(
        "```\n[PARSING PLAYLIST...]\n```", parse_mode=ParseMode.MARKDOWN_V2
    )

    def _pl():
        opts = {**build_ydl_common(), "skip_download": True,
                "extract_flat": True, "playlistend": MAX_PLAYLIST_SHOW}
        with yt_dlp.YoutubeDL(opts) as ydl:
            return ydl.extract_info(url, download=False)

    try:
        info = await asyncio.wait_for(
            asyncio.get_event_loop().run_in_executor(None, _pl),
            timeout=40,
        )
    except Exception as e:
        await safe_edit(msg, f"```\n[PLAYLIST PARSE FAILED]\n{mdescape(str(e)[:150])}\n```",
                        parse_mode=ParseMode.MARKDOWN_V2)
        return

    entries = info.get("entries") or []
    pl_title = sanitize(info.get("title","Playlist"))[:30]
    lines    = ["```", f"[PLAYLIST: {pl_title}]",
                f"tracks shown: {len(entries)}", "─" * 36]
    buttons  = []
    for i, e in enumerate(entries, 1):
        title    = sanitize(e.get("title","?"))[:38]
        dur      = fmt_dur(e.get("duration") or 0)
        lines.append(f"[{i}] {title[:36]}")
        lines.append(f"     ⏱ {dur}")
        vid_url  = normalize_url(e)
        safe_url = vid_url.replace("|", "%7C")
        buttons.append([InlineKeyboardButton(
            f"[{i}] {title[:30]}",
            callback_data=f"fetch|{safe_url}"
        )])
    lines.append("```")
    buttons.append([InlineKeyboardButton("✖ Close", callback_data="cancel")])

    await safe_edit(msg, "\n".join(lines), parse_mode=ParseMode.MARKDOWN_V2,
                    reply_markup=InlineKeyboardMarkup(buttons))

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
        await update.message.reply_text(
            f"```\n[RATE LIMIT]\nCooldown: {cd}s remaining\n```",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    record_rl(user)
    stats["users"].add(user)

    if download_queue and download_queue.qsize() >= MAX_QUEUE_SIZE:
        await update.message.reply_text(
            "```\n[QUEUE FULL]\nAll slots occupied. Retry shortly.\n```",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    msg       = await update.message.reply_text(
        "```\n[INITIALIZING...]\n```", parse_mode=ParseMode.MARKDOWN_V2
    )
    stop_anim = asyncio.Event()
    anim_task = asyncio.create_task(animated_fetch(msg, stop_anim))

    try:
        info = await asyncio.wait_for(
            asyncio.get_event_loop().run_in_executor(None, get_video_info, url),
            timeout=60,
        )
    except asyncio.TimeoutError:
        stop_anim.set(); await anim_task
        await safe_edit(msg, "```\n[TIMEOUT]\nTarget unreachable (>60s).\n```",
                        parse_mode=ParseMode.MARKDOWN_V2)
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
        await update.message.reply_photo(
            photo=thumb, caption=caption,
            reply_markup=InlineKeyboardMarkup(buttons),
            parse_mode=ParseMode.MARKDOWN,
        )
    except Exception:
        await update.message.reply_text(
            caption, reply_markup=InlineKeyboardMarkup(buttons),
            parse_mode=ParseMode.MARKDOWN,
        )

# ═══════════════════════════════════════════════════════════
#  BUTTON HANDLER
# ═══════════════════════════════════════════════════════════

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data == "cancel":
        try: await query.message.delete()
        except Exception: pass
        return

    # "fetch|<url>" — from search/trending/playlist results
    if query.data.startswith("fetch|"):
        url = query.data[6:].replace("%7C", "|")
        # Synthesise a fake Update.message to reuse handle_link logic
        try: await query.message.delete()
        except Exception: pass

        stop = asyncio.Event()
        msg  = await query.message.reply_text(
            "```\n[SCANNING TARGET...]\n```", parse_mode=ParseMode.MARKDOWN_V2
        )
        anim = asyncio.create_task(animated_fetch(msg, stop))
        try:
            info = await asyncio.wait_for(
                asyncio.get_event_loop().run_in_executor(None, get_video_info, url),
                timeout=60,
            )
        except Exception as e:
            stop.set(); await anim
            await safe_edit(msg, error_msg(e), parse_mode=ParseMode.MARKDOWN_V2)
            return
        stop.set(); await anim
        await msg.delete()
        caption = build_caption(info)
        buttons = build_buttons(info, url)
        thumb   = info.get("thumbnail")
        try:
            await query.message.reply_photo(
                photo=thumb, caption=caption,
                reply_markup=InlineKeyboardMarkup(buttons),
                parse_mode=ParseMode.MARKDOWN,
            )
        except Exception:
            await query.message.reply_text(
                caption, reply_markup=InlineKeyboardMarkup(buttons),
                parse_mode=ParseMode.MARKDOWN,
            )
        return

    # Normal download selection
    parts = query.data.split("|", 2)
    if len(parts) != 3 or parts[0] not in ("mp3", "mp4", "m4a", "ogg"):
        await query.answer("⚠ Invalid selection.", show_alert=True)
        return

    typ, quality, safe_url = parts
    url  = safe_url.replace("%7C", "|")
    pos  = (download_queue.qsize() if download_queue else 0) + 1

    try:
        await query.edit_message_caption(
            caption=(
                f"```\n[MISSION QUEUED #{pos}]\n"
                f"format : {typ.upper()} / {quality}\n"
                f"status : waiting for worker\n```"
            ),
            parse_mode=ParseMode.MARKDOWN_V2,
        )
    except BadRequest:
        try:
            await query.edit_message_text(
                text=(
                    f"```\n[MISSION QUEUED #{pos}]\n"
                    f"format : {typ.upper()} / {quality}\n```"
                ),
                parse_mode=ParseMode.MARKDOWN_V2,
            )
        except Exception as e:
            logger.debug("edit fail: %s", e)

    status_msg = await query.message.reply_text(
        f"```\n"
        f"[QUEUED — POSITION #{pos}]\n"
        f"target : {typ.upper()} {quality}\n"
        f"status : standby...\n"
        f"```",
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
                await asyncio.wait_for(
                    process(query, typ, quality, url, msg, wid),
                    timeout=DOWNLOAD_TIMEOUT,
                )
            except asyncio.TimeoutError:
                logger.error("Worker %d timeout on %s", wid, url)
                stats["failed"] += 1
                await safe_edit(msg,
                    "```\n[TIMEOUT]\nMission exceeded time limit.\n```",
                    parse_mode=ParseMode.MARKDOWN_V2)
        except asyncio.CancelledError:
            logger.info("Worker %d terminating.", wid); break
        except Exception as e:
            logger.exception("Worker %d crash: %s", wid, e)
        finally:
            active_downloads.pop(wid, None)
            download_queue.task_done()

# ═══════════════════════════════════════════════════════════
#  PROCESS  —  full download pipeline with fallback
# ═══════════════════════════════════════════════════════════

async def process(query, typ: str, quality: str, url: str, msg, wid: int):
    loop = asyncio.get_event_loop()
    uid  = query.from_user.id if query.from_user else 0

    # ── Cache check ──────────────────────────────────────
    cp = CACHE_FOLDER / ckey(url, typ, quality)
    if cp.exists():
        await safe_edit(msg,
            "```\n[CACHE HIT]\nTransmitting from local cache...\n```",
            parse_mode=ParseMode.MARKDOWN_V2)
        try:
            cap = f"`{'🎵 AUDIO' if typ == 'mp3' else '🎥 VIDEO'}` _(cached)_"
            with open(cp, "rb") as f:
                if typ in ("mp3", "m4a", "ogg"):
                    await msg.reply_audio(f, caption=cap, parse_mode=ParseMode.MARKDOWN,
                                          read_timeout=120, write_timeout=120)
                else:
                    await msg.reply_video(f, caption=cap, parse_mode=ParseMode.MARKDOWN,
                                          supports_streaming=True,
                                          read_timeout=120, write_timeout=120)
            await msg.delete()
            stats["downloads"] += 1
            return
        except Exception as e:
            logger.error("cache send fail: %s", e)
            cp.unlink(missing_ok=True)

    # ── Title fetch ───────────────────────────────────────
    try:
        info        = await loop.run_in_executor(None, get_video_info, url)
        clean_title = sanitize(info.get("title") or "download")
    except Exception:
        clean_title = "download"

    # ── Format string ─────────────────────────────────────────────────────
    # ROOT CAUSE of "Requested format is not available":
    #   Codec-locked selectors like [ext=mp4] or [ext=m4a] silently fail
    #   when a video only has VP9/AV1/webm streams (very common on YouTube).
    #
    # RULE: NEVER filter by codec/container in the format selector.
    #   Let yt-dlp pick the best available streams by quality only,
    #   then let FFmpeg convert/remux into the target container.
    #   This works for 100% of videos regardless of what codecs YouTube serves.
    # ─────────────────────────────────────────────────────────────────────────
    if typ == "mp3":
        # Download best audio stream, FFmpeg converts to MP3
        fmt = "bestaudio/best"

    elif typ == "m4a":
        # Best audio, FFmpeg remuxes/converts to M4A
        fmt = "bestaudio/best"

    elif typ == "ogg":
        # Best audio, FFmpeg converts to OGG Vorbis
        fmt = "bestaudio/best"

    elif quality == "best":
        # Absolute best video+audio — let yt-dlp decide everything
        fmt = "bestvideo+bestaudio/best"

    else:
        q = int(quality)
        # Priority: exact height → closest height below → best available
        # NO codec filters — accept any codec, FFmpeg will remux to mp4
        fmt = (
            f"bestvideo[height={q}]+bestaudio"
            f"/bestvideo[height<={q}]+bestaudio"
            f"/best[height<={q}]"
            f"/bestvideo+bestaudio"
            f"/best"
        )

    out_tpl  = str(DOWNLOAD_FOLDER / f"{clean_title}.%(ext)s")
    frame_n  = {"n": 0, "last_pct": -1.0, "phase_idx": 0}

    # ── Progress hook ─────────────────────────────────────
    def hook(d: dict):
        if d["status"] != "downloading": return
        raw = d.get("_percent_str", "0").strip().replace("%", "")
        try: pct = float(raw)
        except ValueError: return
        if pct - frame_n["last_pct"] < 2.5 and pct < 99: return
        frame_n["last_pct"] = pct
        frame_n["n"]       += 1

        bar     = _hbar(pct)
        phase   = _phase(pct)
        speed   = (d.get("_speed_str")   or "—").strip()
        eta     = (d.get("_eta_str")     or "—").strip()
        size    = (d.get("_total_bytes_str") or
                   d.get("_total_bytes_estimate_str") or "—").strip()
        radar   = _RADAR[frame_n["n"] % len(_RADAR)]
        glbl    = _glitch(phase, frame_n["n"])

        text = (
            f"```\n"
            f"[{radar}] NEXUS-DL // EXTRACTION\n"
            f"{'─'*30}\n"
            f"TARGET : {typ.upper()} {quality}{'kbps' if typ=='mp3' else 'p' if quality.isdigit() else ''}\n"
            f"PHASE  : {glbl}\n"
            f"[{bar}] {pct:.0f}%\n"
            f"SIZE   : {size}\n"
            f"SPEED  : {speed}\n"
            f"ETA    : {eta}\n"
            f"```"
        )
        loop.call_soon_threadsafe(
            asyncio.ensure_future,
            safe_edit(msg, text, parse_mode=ParseMode.MARKDOWN_V2),
        )

    # ── Build opts ────────────────────────────────────────
    ydl_opts = {
        **build_ydl_common(),
        "format":         fmt,
        "outtmpl":        out_tpl,
        "progress_hooks": [hook],
    }

    # Video: always remux to mp4 so Telegram plays it inline
    if typ == "mp4":
        ydl_opts["merge_output_format"] = "mp4"

    # Audio: postprocessor chain converts whatever yt-dlp downloaded → target codec
    if typ == "mp3":
        ydl_opts["postprocessors"] = [{
            "key":              "FFmpegExtractAudio",
            "preferredcodec":   "mp3",
            "preferredquality": quality,   # "128", "192", "320"
        }]
    elif typ == "m4a":
        ydl_opts["postprocessors"] = [{
            "key":            "FFmpegExtractAudio",
            "preferredcodec": "m4a",
            "preferredquality": "0",
        }]
    elif typ == "ogg":
        ydl_opts["postprocessors"] = [{
            "key":            "FFmpegExtractAudio",
            "preferredcodec": "vorbis",
            "preferredquality": "5",
        }]

    await safe_edit(msg,
        f"```\n[DOWNLOAD INITIATED]\ntarget : {typ.upper()} {quality}\nstatus : connecting...\n```",
        parse_mode=ParseMode.MARKDOWN_V2)

    # ── Download with fallback ────────────────────────────
    fp_str = None; last_err = None

    try:
        fp_str = await loop.run_in_executor(
            None, lambda: _run_ydl(ydl_opts, url, typ, clean_title)
        )
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
                fp_str  = await loop.run_in_executor(
                    None, lambda o=fb_opts: _run_ydl(o, url, typ, clean_title)
                )
                if fp_str:
                    logger.info("Fallback `%s` succeeded.", cname)
                    last_err = None; break
            except Exception as fe:
                last_err = fe
                logger.debug("Fallback `%s` fail: %s", cname, fe)

    if last_err and not fp_str:
        stats["failed"] += 1
        await safe_edit(msg, error_msg(last_err), parse_mode=ParseMode.MARKDOWN_V2)
        return

    if not fp_str or not Path(fp_str).exists():
        stats["failed"] += 1
        await safe_edit(msg,
            "```\n[FILE NOT FOUND]\nDownload completed but output missing.\nTry shorter video or different format.\n```",
            parse_mode=ParseMode.MARKDOWN_V2)
        return

    fp   = Path(fp_str)
    fsz  = fp.stat().st_size

    # ── Size check + smart split ─────────────────────────────────────────────
    SPLIT_THRESHOLD = 45 * 1024 * 1024   # split if > 45 MB AND no local API
    need_split = (fsz > MAX_FILE_SIZE) or (not _using_local_api and fsz > SPLIT_THRESHOLD)

    if fsz > MAX_FILE_SIZE and _using_local_api:
        # Local API supports up to 2 GB — just proceed normally
        need_split = False

    if need_split and fsz > MAX_FILE_SIZE and not _using_local_api:
        # Hard limit exceeded, cannot split video sensibly — reject
        fp.unlink(missing_ok=True)
        stats["failed"] += 1
        await safe_edit(msg,
            f"```\n"
            f"[FILE TOO LARGE]\n"
            f"size  : {fmt_size(fsz)}\n"
            f"limit : {fmt_size(MAX_FILE_SIZE)}\n"
            f"\n"
            f"Options:\n"
            f"  1. Choose lower resolution\n"
            f"  2. Choose MP3 audio only\n"
            f"  3. Run a local Bot API server\n"
            f"     (raises limit to 2 GB)\n"
            f"     See /help for setup guide\n"
            f"```",
            parse_mode=ParseMode.MARKDOWN_V2)
        return

    await safe_edit(msg,
        f"```\n[TRANSMITTING]\nfile  : {fp.name[:40]}\nsize  : {fmt_size(fsz)}\nuplink: active...\n```",
        parse_mode=ParseMode.MARKDOWN_V2)

    # Cache before send (only if fits in one shot)
    if not need_split:
        try: shutil.copy2(fp, cp)
        except Exception as ce: logger.warning("cache write: %s", ce)

    try:
        if need_split and typ in ("mp3", "m4a", "ogg"):
            # Split audio into 45 MB chunks and send sequentially
            await _send_in_parts(msg, fp, typ, clean_title)
        else:
            with open(fp, "rb") as f:
                cap = f"✅ `{fp.stem[:50]}`"
                if typ in ("mp3", "m4a", "ogg"):
                    await msg.reply_audio(audio=f, caption=cap, title=fp.stem,
                                          parse_mode=ParseMode.MARKDOWN,
                                          read_timeout=600, write_timeout=600)
                else:
                    await msg.reply_video(video=f, caption=cap,
                                          parse_mode=ParseMode.MARKDOWN,
                                          supports_streaming=True,
                                          read_timeout=600, write_timeout=600)
        await msg.delete()
        stats["downloads"] += 1
        stats["bytes_sent"] += fsz
        push_history(uid, clean_title, typ, quality)

    except Exception as e:
        logger.error("upload fail: %s", e)
        stats["failed"] += 1
        cp.unlink(missing_ok=True)
        await safe_edit(msg, f"```\n[UPLOAD FAILED]\n{str(e)[:200].replace(chr(96), chr(39))}\n```",
                        parse_mode=ParseMode.MARKDOWN_V2)
    finally:
        fp.unlink(missing_ok=True)

# ═══════════════════════════════════════════════════════════
#  YT-DLP RUNNER  (blocking, runs in executor)
# ═══════════════════════════════════════════════════════════

async def _send_in_parts(msg, fp: Path, typ: str, title: str):
    """
    Split a large audio file into 45 MB chunks and send them one by one.
    Used when no local Bot API server is configured and the file is too
    large for the standard 50 MB Telegram limit.
    Only used for audio (mp3/m4a/ogg) — video cannot be cleanly split
    without re-encoding (which we avoid to keep things fast).
    """
    CHUNK = 45 * 1024 * 1024   # 45 MB per part
    fsz   = fp.stat().st_size
    total = (fsz + CHUNK - 1) // CHUNK

    await safe_edit(msg,
        f"```\n[SPLITTING FILE]\n"
        f"size  : {fmt_size(fsz)}\n"
        f"parts : {total} x ~{fmt_size(CHUNK)}\n"
        f"```",
        parse_mode=ParseMode.MARKDOWN_V2)

    with open(fp, "rb") as src:
        for i in range(total):
            chunk_data = src.read(CHUNK)
            if not chunk_data:
                break
            part_name = f"{title[:40]} (Part {i+1} of {total}){fp.suffix}"
            await msg.reply_audio(
                audio=io.BytesIO(chunk_data),
                filename=part_name,
                title=part_name,
                caption=f"`Part {i+1}/{total}`",
                parse_mode=ParseMode.MARKDOWN,
                read_timeout=600,
                write_timeout=600,
            )

def _run_ydl(opts: dict, url: str, typ: str, clean_title: str) -> str | None:
    """
    Run yt-dlp download and reliably locate the output file.

    The challenge: FFmpeg postprocessors rename/replace files after download,
    so ydl.prepare_filename() returns the PRE-postprocessing name which may
    not exist. We snapshot the download folder before and after, then pick
    the newest file matching the expected extension.
    """
    # Snapshot files in download folder before we start
    before: set[Path] = set(DOWNLOAD_FOLDER.iterdir()) if DOWNLOAD_FOLDER.exists() else set()
    t_start = time.time()

    with yt_dlp.YoutubeDL(opts) as ydl:
        info     = ydl.extract_info(url, download=True)
        raw_path = ydl.prepare_filename(info)

    # Expected extensions after postprocessing
    if typ == "mp3":
        target_exts = [".mp3"]
    elif typ == "m4a":
        target_exts = [".m4a", ".m4a"]
    elif typ == "ogg":
        target_exts = [".ogg", ".opus"]
    else:
        # Video: mp4 preferred (merge_output_format="mp4"), then mkv/webm
        target_exts = [".mp4", ".mkv", ".webm", ".avi"]

    # 1. Try the direct path yt-dlp reported (works when no postprocessing)
    direct = Path(raw_path)
    if direct.exists():
        return str(direct)

    # 2. Try swapping the extension to the expected postprocessed one
    for ext in target_exts:
        swapped = direct.with_suffix(ext)
        if swapped.exists():
            return str(swapped)

    # 3. Find any NEW file created after t_start with matching extension
    after: set[Path] = set(DOWNLOAD_FOLDER.iterdir()) if DOWNLOAD_FOLDER.exists() else set()
    new_files = after - before
    for ext in target_exts:
        matches = [f for f in new_files if f.suffix == ext]
        if matches:
            return str(max(matches, key=lambda p: p.stat().st_mtime))

    # 4. Last resort: newest file with matching extension (could be concurrent download)
    for ext in target_exts:
        candidates = sorted(
            (f for f in DOWNLOAD_FOLDER.iterdir()
             if f.suffix == ext and f.stat().st_mtime >= t_start - 5),
            key=lambda p: p.stat().st_mtime, reverse=True,
        )
        if candidates:
            return str(candidates[0])

    logger.error("_run_ydl: could not locate output. raw=%s typ=%s", raw_path, typ)
    return None

# ═══════════════════════════════════════════════════════════
#  ERROR HANDLER
# ═══════════════════════════════════════════════════════════

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.error("Unhandled exception", exc_info=context.error)

# ═══════════════════════════════════════════════════════════
#  APP SETUP
# ═══════════════════════════════════════════════════════════

async def post_init(app):
    global download_queue
    download_queue = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)
    for i in range(WORKERS):
        asyncio.create_task(worker(i + 1))
    # Register bot commands for the menu button
    await app.bot.set_my_commands([
        BotCommand("start",      "Boot sequence"),
        BotCommand("auth",       "Link Google account (fixes IP block)"),
        BotCommand("search",     "Search YouTube"),
        BotCommand("trending",   "Trending videos [category]"),
        BotCommand("playlist",   "Extract playlist"),
        BotCommand("info",       "Inspect a URL"),
        BotCommand("history",    "Your download log"),
        BotCommand("setcookies", "Upload cookies.txt"),
        BotCommand("queue",      "Mission queue status"),
        BotCommand("stats",      "System telemetry"),
        BotCommand("ping",       "Latency check"),
        BotCommand("help",       "Operator manual"),
    ])
    po, _ = get_po_token()
    oa    = oauth2_token_exists()
    ck    = COOKIES_FILE.exists()
    px    = len(_PROXY_LIST) > 0
    logger.info(
        "NEXUS-DL online | workers=%d | mode=%s | oauth2=%s | cookies=%s | po_token=%s | proxy=%s",
        WORKERS,
        "webhook" if WEBHOOK_URL else "polling",
        "yes" if oa else "no",
        "yes" if ck else "no",
        "yes" if po else "no",
        f"{len(_PROXY_LIST)}x" if px else "no",
    )
    if not oa and not ck and not px:
        logger.warning(
            "⚠ NO BYPASS ACTIVE — YouTube will block downloads from this IP.\n"
            "  Fix: send /auth in Telegram and approve the Google login.\n"
            "  This is a one-time setup. Token lasts forever."
        )
        # DM every configured admin so they see the warning in Telegram too
        if ADMIN_IDS:
            for admin_id in ADMIN_IDS:
                try:
                    await app.bot.send_message(
                        chat_id=admin_id,
                        text=(
                            "```\n"
                            "[⚠ NO BYPASS ACTIVE]\n"
                            "YouTube WILL block all downloads\n"
                            "from this server IP.\n"
                            "\n"
                            "Fix: run /auth RIGHT NOW\n"
                            "Takes 30 seconds, lasts forever.\n"
                            "```"
                        ),
                        parse_mode=ParseMode.MARKDOWN_V2,
                    )
                except Exception:
                    pass

def main():
    builder = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .connect_timeout(120)
        .read_timeout(120)
        .write_timeout(600)          # large uploads need long write timeout
        .pool_timeout(120)
        .get_updates_connect_timeout(60)
        .get_updates_read_timeout(60)
        .get_updates_write_timeout(60)
        .get_updates_pool_timeout(60)
        .post_init(post_init)
    )
    # Point to local Bot API server when configured — removes the 50 MB cap
    if LOCAL_API_URL:
        builder = builder.base_url(f"{LOCAL_API_URL}/bot")
        logger.info("Using local Bot API server: %s  (limit: %s)",
                    LOCAL_API_URL, fmt_size(MAX_FILE_SIZE))
    else:
        logger.info("Using cloud Bot API  (limit: %s)", fmt_size(MAX_FILE_SIZE))
    app = builder.build()

    app.add_handler(CommandHandler("start",      cmd_start))
    app.add_handler(CommandHandler("auth",       cmd_auth))
    app.add_handler(CommandHandler("authtest",   cmd_authtest))
    app.add_handler(CommandHandler("help",       cmd_help))
    app.add_handler(CommandHandler("stats",      cmd_stats))
    app.add_handler(CommandHandler("queue",      cmd_queue))
    app.add_handler(CommandHandler("ping",       cmd_ping))
    app.add_handler(CommandHandler("history",    cmd_history))
    app.add_handler(CommandHandler("info",       cmd_info))
    app.add_handler(CommandHandler("search",     cmd_search))
    app.add_handler(CommandHandler("trending",   cmd_trending))
    app.add_handler(CommandHandler("playlist",   cmd_playlist))
    app.add_handler(CommandHandler("setcookies", cmd_setcookies))
    app.add_handler(CallbackQueryHandler(button_handler))
    # Document handler — catches cookies.txt uploads (must be before TEXT handler)
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_link))
    app.add_error_handler(error_handler)

    if WEBHOOK_URL:
        logger.info("WEBHOOK mode — port %d", PORT)
        app.run_webhook(
            listen="0.0.0.0", port=PORT,
            webhook_url=WEBHOOK_URL, drop_pending_updates=True,
        )
    else:
        logger.info("POLLING mode")
        app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()