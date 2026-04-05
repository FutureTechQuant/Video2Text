import json
import os
import re
import subprocess
import time
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import parse_qs, urlparse

from faster_whisper import WhisperModel

SOURCE_URL = os.getenv("BILIBILI_SOURCE_URL", "").strip()
COOKIES_FILE = Path(os.getenv("BILIBILI_COOKIES_FILE", "cookies.txt"))

MODEL_NAME = os.getenv("WHISPER_MODEL", "small")
DEVICE = os.getenv("WHISPER_DEVICE", "cpu")
COMPUTE_TYPE = os.getenv("WHISPER_COMPUTE_TYPE", "int8")
LANGUAGE = os.getenv("WHISPER_LANGUAGE", "zh")
AUDIO_FORMAT = os.getenv("AUDIO_FORMAT", "mp3")
AUDIO_QUALITY = os.getenv("AUDIO_QUALITY", "7")
BEAM_SIZE = int(os.getenv("BEAM_SIZE", "1"))
VAD_FILTER = os.getenv("VAD_FILTER", "1") == "1"
GIT_BRANCH = os.getenv("GITHUB_REF_NAME", "").strip()


def log(msg: str):
    print(msg, flush=True)


def sanitize_key(name: str, max_len: int = 80) -> str:
    name = re.sub(r"[^0-9A-Za-z._-]+", "_", (name or "").strip())
    name = name.strip("._ ")
    return (name[:max_len] or "default")


DESTINATION_RAW = os.getenv("DESTINATION", "").strip()
DESTINATION = sanitize_key(DESTINATION_RAW or "default")

STATE_DIR = Path("state_collections") / DESTINATION
OUTPUT_DIR = Path("destinations") / DESTINATION
WITH_TS_DIR = OUTPUT_DIR / "with_timestamps"
PLAIN_DIR = OUTPUT_DIR / "plain"
TMP_DIR = Path("tmp_audio") / DESTINATION
ERRORS_DIR = STATE_DIR / "errors"

QUEUE_FILE = STATE_DIR / "queue.json"
DONE_FILE = STATE_DIR / "done.txt"
FAILED_FILE = STATE_DIR / "failed.txt"
PROGRESS_FILE = STATE_DIR / "progress.json"
CONTINUE_FLAG = STATE_DIR / "continue.flag"
MANIFEST_FILE = OUTPUT_DIR / "_manifest.json"


def ensure_dirs():
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    WITH_TS_DIR.mkdir(parents=True, exist_ok=True)
    PLAIN_DIR.mkdir(parents=True, exist_ok=True)
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    ERRORS_DIR.mkdir(parents=True, exist_ok=True)


def atomic_write_text(path: Path, content: str, encoding: str = "utf-8"):
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(content, encoding=encoding)
    tmp.replace(path)


def append_line(path: Path, line: str):
    with path.open("a", encoding="utf-8") as f:
        f.write(line.rstrip("\n") + "\n")


def load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def save_json(path: Path, data):
    atomic_write_text(path, json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def load_set(path: Path) -> set:
    if not path.exists():
        return set()
    return {x.strip() for x in path.read_text(encoding="utf-8").splitlines() if x.strip()}


def run(cmd: List[str], capture: bool = False, check: bool = True):
    log("[cmd] " + " ".join(cmd))
    return subprocess.run(cmd, text=True, capture_output=capture, check=check)


def git_run(cmd: List[str], check: bool = True):
    log("[git] " + " ".join(cmd))
    return subprocess.run(cmd, text=True, check=check)


def detect_bvid(text: str) -> str:
    m = re.search(r"(BV[0-9A-Za-z]+)", text or "")
    return m.group(1) if m else ""


def detect_page(url: str) -> Optional[int]:
    try:
        query = parse_qs(urlparse(url).query)
        p = query.get("p", [None])[0]
        return int(p) if p else None
    except Exception:
        return None


def normalize_bilibili_base_url(url: str) -> str:
    bvid = detect_bvid(url)
    if not bvid:
        raise ValueError(f"cannot detect BV id from url: {url}")
    return f"https://www.bilibili.com/video/{bvid}"


def format_video_url(entry: Dict, fallback_bvid: str = "") -> Optional[str]:
    url = (entry.get("url") or entry.get("webpage_url") or "").strip()
    if url.startswith("http://") or url.startswith("https://"):
        return url
    if url.startswith("/video/"):
        return "https://www.bilibili.com" + url

    vid = (entry.get("id") or "").strip()
    if not vid:
        vid = detect_bvid(url) or fallback_bvid

    page = entry.get("page") or entry.get("page_num") or entry.get("page_number")
    if vid.startswith("BV"):
        full = f"https://www.bilibili.com/video/{vid}"
        if page:
            full += f"?p={page}"
        return full

    return None


def mmss_mmm(sec: float) -> str:
    total_ms = max(0, int(round(sec * 1000)))
    total_seconds, ms = divmod(total_ms, 1000)
    minutes, seconds = divmod(total_seconds, 60)
    return f"{minutes:02d}:{seconds:02d}.{ms:03d}"


def build_output_basename(item: Dict) -> str:
    bvid = item.get("bvid") or "NOID"
    return f"{DESTINATION}_{item['index']:04d}_{bvid}"


def plain_output_path(item: Dict) -> Path:
    return PLAIN_DIR / f"{build_output_basename(item)}.txt"


def ts_output_path(item: Dict) -> Path:
    return WITH_TS_DIR / f"{build_output_basename(item)}.txt"


def save_progress(status: str, note: str = "", current: Optional[Dict] = None, queue_total: int = 0, queue_index: int = 0):
    payload = {
        "status": status,
        "note": note,
        "destination": DESTINATION,
        "source_url": SOURCE_URL,
        "queue_total": queue_total,
        "queue_index": queue_index,
        "updated_at": int(time.time()),
        "updated_at_readable": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
    }
    if current:
        payload["current_item_id"] = current.get("item_id", "")
        payload["current_bvid"] = current.get("bvid", "")
        payload["current_title"] = current.get("title", "")
        payload["current_url"] = current.get("url", "")
    save_json(PROGRESS_FILE, payload)


def record_done(item_id: str):
    done = load_set(DONE_FILE)
    if item_id not in done:
        append_line(DONE_FILE, item_id)


def record_failed(item_id: str):
    failed = load_set(FAILED_FILE)
    if item_id not in failed:
        append_line(FAILED_FILE, item_id)


def write_error_file(item: Dict, err: Exception):
    path = ERRORS_DIR / f"{item['item_id']}.txt"
    body = (
        f"item_id: {item['item_id']}\n"
        f"bvid: {item.get('bvid', '')}\n"
        f"title: {item.get('title', '')}\n"
        f"url: {item.get('url', '')}\n"
        f"time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}\n"
        f"error: {repr(err)}\n"
    )
    atomic_write_text(path, body)


def fetch_json_via_yt_dlp(url: str) -> Dict:
    result = run([
        "yt-dlp",
        "--cookies", str(COOKIES_FILE),
        "--dump-single-json",
        url
    ], capture=True)
    raw = result.stdout.strip()
    if not raw:
        raise RuntimeError(f"empty json from yt-dlp: {url}")
    return json.loads(raw)


def try_extract_entries_from_data(data: Dict, base_bvid: str) -> List[Dict]:
    entries = data.get("entries") or []
    queue = []

    for i, e in enumerate(entries, start=1):
        url = format_video_url(e, fallback_bvid=base_bvid)
        bvid = detect_bvid((e.get("id") or "") + " " + (url or "") + " " + base_bvid) or base_bvid
        title = (e.get("title") or f"item_{i}").strip()
        page = detect_page(url or "") or e.get("page") or e.get("page_num") or e.get("page_number") or i
        item_id = f"{i:04d}_{bvid or 'NOID'}"

        if not url and bvid:
            url = f"https://www.bilibili.com/video/{bvid}?p={page}"

        if not url:
            continue

        queue.append({
            "item_id": item_id,
            "index": i,
            "bvid": bvid,
            "page": page,
            "title": title,
            "url": url,
        })
    return queue


def try_extract_pages_from_webpage(base_url: str, base_bvid: str) -> List[Dict]:
    result = run([
        "yt-dlp",
        "--cookies", str(COOKIES_FILE),
        "--print", "webpage",
        "--skip-download",
        base_url
    ], capture=True)

    html = result.stdout or ""
    if not html:
        return []

    patterns = [
        r'(?s)window\.__INITIAL_STATE__\s*=\s*(\{.*?\})\s*;\s*\(function',
        r'(?s)window\.__INITIAL_STATE__\s*=\s*(\{.*?\})\s*;\s*$',
        r'(?s)__INITIAL_STATE__=(\{.*?\});',
    ]

    state = None
    for pat in patterns:
        m = re.search(pat, html)
        if not m:
            continue
        blob = m.group(1)
        try:
            state = json.loads(blob)
            break
        except Exception:
            continue

    if not state:
        return []

    video_data = state.get("videoData") or {}
    pages = video_data.get("pages") or state.get("pages") or []
    title = (video_data.get("title") or base_bvid).strip()

    queue = []
    for i, p in enumerate(pages, start=1):
        page_no = p.get("page") or i
        part = (p.get("part") or title or f"item_{i}").strip()
        url = f"https://www.bilibili.com/video/{base_bvid}?p={page_no}"
        queue.append({
            "item_id": f"{i:04d}_{base_bvid or 'NOID'}",
            "index": i,
            "bvid": base_bvid,
            "page": page_no,
            "title": part,
            "url": url,
        })
    return queue


def build_single_item_queue(base_url: str, base_bvid: str) -> List[Dict]:
    return [{
        "item_id": f"{1:04d}_{base_bvid or 'NOID'}",
        "index": 1,
        "bvid": base_bvid,
        "page": 1,
        "title": base_bvid or DESTINATION,
        "url": base_url,
    }]


def extract_queue_from_source() -> List[Dict]:
    if not SOURCE_URL:
        raise ValueError("BILIBILI_SOURCE_URL is empty")
    if not COOKIES_FILE.exists():
        raise FileNotFoundError(f"cookies file not found: {COOKIES_FILE}")

    base_url = normalize_bilibili_base_url(SOURCE_URL)
    base_bvid = detect_bvid(base_url)

    data = fetch_json_via_yt_dlp(base_url)
    queue = try_extract_entries_from_data(data, base_bvid)

    if len(queue) <= 1:
        log("[warn] yt-dlp entries not enough, fallback to webpage pages")
        fallback_queue = try_extract_pages_from_webpage(base_url, base_bvid)
        if len(fallback_queue) > len(queue):
            queue = fallback_queue

    if not queue:
        queue = build_single_item_queue(base_url, base_bvid)

    if not queue:
        raise RuntimeError("queue is empty")

    return queue


def write_manifest(queue: List[Dict]):
    payload = {
        "destination": DESTINATION,
        "source_url": SOURCE_URL,
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        "items": queue,
    }
    save_json(MANIFEST_FILE, payload)


def load_or_build_queue() -> List[Dict]:
    if QUEUE_FILE.exists():
        queue = load_json(QUEUE_FILE, [])
        if queue:
            log(f"[info] loaded queue from state: {len(queue)} items")
            return queue

    log("[info] building queue from source url")
    queue = extract_queue_from_source()
    save_json(QUEUE_FILE, queue)
    write_manifest(queue)
    log(f"[info] queue saved: {len(queue)} items")
    return queue


def is_item_completed(item: Dict) -> bool:
    return plain_output_path(item).exists() and ts_output_path(item).exists()


def find_next_item(queue: List[Dict], done: set, failed: set) -> Optional[Dict]:
    for item in queue:
        if item["item_id"] in done or item["item_id"] in failed or is_item_completed(item):
            continue
        return item
    return None


def has_more_pending(queue: List[Dict], done: set, failed: set) -> bool:
    return find_next_item(queue, done, failed) is not None


def download_audio(video_url: str, item_id: str) -> Path:
    outtmpl = str(TMP_DIR / f"{item_id}.%(ext)s")
    run([
        "yt-dlp",
        "--cookies", str(COOKIES_FILE),
        "--no-playlist",
        "-f", "ba/bestaudio",
        "-x",
        "--audio-format", AUDIO_FORMAT,
        "--audio-quality", AUDIO_QUALITY,
        "-o", outtmpl,
        video_url
    ])

    files = [p for p in TMP_DIR.glob(f"{item_id}.*") if p.is_file() and not p.name.endswith(".part")]
    if not files:
        raise RuntimeError(f"audio file not found for {item_id}")
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0]


def load_model() -> WhisperModel:
    log(f"[info] loading model: {MODEL_NAME}, device={DEVICE}, compute_type={COMPUTE_TYPE}")
    return WhisperModel(MODEL_NAME, device=DEVICE, compute_type=COMPUTE_TYPE)


def transcribe_audio(model: WhisperModel, audio_path: Path) -> Dict:
    segments, info = model.transcribe(
        str(audio_path),
        language=LANGUAGE if LANGUAGE else None,
        beam_size=BEAM_SIZE,
        vad_filter=VAD_FILTER,
        condition_on_previous_text=False,
    )

    ts_lines = []
    plain_lines = []
    kept_segments = 0

    for seg in segments:
        text = (seg.text or "").strip()
        if not text:
            continue
        kept_segments += 1
        ts_lines.append(f"[{mmss_mmm(seg.start)} --> {mmss_mmm(seg.end)}] {text}")
        plain_lines.append(text)

    return {
        "language": getattr(info, "language", ""),
        "language_probability": getattr(info, "language_probability", ""),
        "segments": kept_segments,
        "timestamp_text": "\n".join(ts_lines).strip(),
        "plain_text": "\n".join(plain_lines).strip(),
    }


def write_outputs(item: Dict, result: Dict):
    ts_body = (result.get("timestamp_text", "").strip() + "\n") if result.get("timestamp_text") else ""
    plain_body = (result.get("plain_text", "").strip() + "\n") if result.get("plain_text") else ""
    atomic_write_text(ts_output_path(item), ts_body)
    atomic_write_text(plain_output_path(item), plain_body)


def cleanup_temp_file(path: Optional[Path]):
    try:
        if path and path.exists():
            path.unlink(missing_ok=True)
    except Exception as e:
        log(f"[warn] cleanup failed: {e}")


def touch_continue():
    CONTINUE_FLAG.write_text("1\n", encoding="utf-8")


def clear_continue():
    CONTINUE_FLAG.unlink(missing_ok=True)


def git_commit_and_push(message: str):
    git_run(["git", "add", "destinations", "state_collections"], check=False)
    diff = subprocess.run(["git", "diff", "--cached", "--quiet"])
    if diff.returncode == 0:
        log("[info] no git changes to commit")
        return

    git_run(["git", "commit", "-m", message])

    if GIT_BRANCH:
        subprocess.run(["git", "pull", "--rebase", "origin", GIT_BRANCH], check=False)
        git_run(["git", "push", "origin", f"HEAD:{GIT_BRANCH}"])
    else:
        git_run(["git", "push"])


def main():
    ensure_dirs()
    clear_continue()

    queue = load_or_build_queue()
    done = load_set(DONE_FILE)
    failed = load_set(FAILED_FILE)

    next_item = find_next_item(queue, done, failed)
    if not next_item:
        save_progress("finished", note="all items completed", queue_total=len(queue), queue_index=len(queue))
        git_commit_and_push(f"collection: finished {DESTINATION}")
        log("[info] all items completed")
        return

    model = load_model()
    audio_path = None
    idx = next((i for i, x in enumerate(queue) if x["item_id"] == next_item["item_id"]), 0)

    try:
        save_progress("downloading_audio", current=next_item, queue_total=len(queue), queue_index=idx)
        audio_path = download_audio(next_item["url"], next_item["item_id"])

        save_progress("transcribing", current=next_item, queue_total=len(queue), queue_index=idx)
        result = transcribe_audio(model, audio_path)

        save_progress("writing_outputs", current=next_item, queue_total=len(queue), queue_index=idx)
        write_outputs(next_item, result)

        record_done(next_item["item_id"])
        done.add(next_item["item_id"])

        if has_more_pending(queue, done, failed):
            touch_continue()
            save_progress(
                "done_one",
                note="one video processed, more pending",
                current=next_item,
                queue_total=len(queue),
                queue_index=idx + 1
            )
        else:
            clear_continue()
            save_progress(
                "finished",
                note="one video processed, no more pending",
                current=next_item,
                queue_total=len(queue),
                queue_index=idx + 1
            )

        git_commit_and_push(f"collection: {DESTINATION} {next_item['item_id']}")
        log(f"[ok] completed: {next_item['item_id']} {next_item['url']}")

    except Exception as e:
        record_failed(next_item["item_id"])
        failed.add(next_item["item_id"])
        write_error_file(next_item, e)

        if has_more_pending(queue, done, failed):
            touch_continue()
            save_progress(
                "error",
                note=repr(e),
                current=next_item,
                queue_total=len(queue),
                queue_index=idx + 1
            )
        else:
            clear_continue()
            save_progress(
                "finished_with_errors",
                note=repr(e),
                current=next_item,
                queue_total=len(queue),
                queue_index=idx + 1
            )

        git_commit_and_push(f"collection: failed {DESTINATION} {next_item['item_id']}")
        log(f"[error] {next_item['item_id']}: {e}")
        raise

    finally:
        cleanup_temp_file(audio_path)


if __name__ == "__main__":
    main()
