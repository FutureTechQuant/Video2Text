import json
import os
import re
import subprocess
import time
from pathlib import Path
from typing import Dict, Optional
from urllib.parse import urlsplit

from faster_whisper import WhisperModel


SOURCE_URL = os.getenv("SOURCE_URL", "").strip()
DESTINATION_RAW = os.getenv("DESTINATION", "").strip()
PLATFORM = os.getenv("PLATFORM", "auto").strip().lower()
USE_COOKIES = str(os.getenv("USE_COOKIES", "true")).strip().lower() in {"1", "true", "yes", "on"}

YOUTUBE_COOKIES_FILE = Path(os.getenv("YOUTUBE_COOKIES_FILE", "youtube_cookies.txt"))
BILIBILI_COOKIES_FILE = Path(os.getenv("BILIBILI_COOKIES_FILE", "bilibili_cookies.txt"))

MODEL_NAME = os.getenv("WHISPER_MODEL", "small")
DEVICE = os.getenv("WHISPER_DEVICE", "cpu")
COMPUTE_TYPE = os.getenv("WHISPER_COMPUTE_TYPE", "int8")
LANGUAGE = os.getenv("WHISPER_LANGUAGE", "zh")
INITIAL_PROMPT = os.getenv("WHISPER_INITIAL_PROMPT", "").strip()

AUDIO_FORMAT = os.getenv("AUDIO_FORMAT", "mp3")
AUDIO_QUALITY = os.getenv("AUDIO_QUALITY", "7")
BEAM_SIZE = int(os.getenv("BEAM_SIZE", "1"))
VAD_FILTER = os.getenv("VAD_FILTER", "1") in {"1", "true", "True"}

GIT_BRANCH = os.getenv("GITHUB_REF_NAME", "").strip()


def log(msg: str):
    print(msg, flush=True)


def clean_url(text: str) -> str:
    text = (text or "").strip()
    if not text:
        return text
    if text.startswith("[") and "](" in text and text.endswith(")"):
        left = text.find("](")
        return text[left + 2:-1].strip()
    m = re.search(r'https?://[^\s]+', text)
    if m:
        return m.group(0).rstrip(")")
    return text


def sanitize_key(name: str, max_len: int = 80) -> str:
    name = (name or "").strip()
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]+', "_", name)
    name = re.sub(r"\s+", "_", name)
    name = name.strip("._ ")
    return (name[:max_len] or "default")


DESTINATION = sanitize_key(DESTINATION_RAW or "default")

STATE_DIR = Path("state_single_video") / DESTINATION
OUTPUT_DIR = Path("single_videos") / DESTINATION
WITH_TS_DIR = OUTPUT_DIR / "with_timestamps"
PLAIN_DIR = OUTPUT_DIR / "plain"
TMP_DIR = Path("tmp_audio") / DESTINATION
PROGRESS_FILE = STATE_DIR / "progress.json"
META_FILE = OUTPUT_DIR / "_meta.json"


def ensure_dirs():
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    WITH_TS_DIR.mkdir(parents=True, exist_ok=True)
    PLAIN_DIR.mkdir(parents=True, exist_ok=True)
    TMP_DIR.mkdir(parents=True, exist_ok=True)


def atomic_write_text(path: Path, content: str, encoding: str = "utf-8"):
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(content, encoding=encoding)
    tmp.replace(path)


def save_json(path: Path, data):
    atomic_write_text(path, json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def run(cmd, capture=False, check=True):
    log("[cmd] " + " ".join(cmd))
    if capture:
        return subprocess.run(cmd, text=True, capture_output=True, check=check)
    return subprocess.run(cmd, text=True, check=check)


def git_run(cmd, check=True):
    log("[git] " + " ".join(cmd))
    return subprocess.run(cmd, text=True, check=check)


def save_progress(status: str, note: str = "", extra: Optional[Dict] = None):
    payload = {
        "status": status,
        "note": note,
        "source_url": clean_url(SOURCE_URL),
        "destination": DESTINATION,
        "platform": PLATFORM,
        "use_cookies": USE_COOKIES,
        "updated_at": int(time.time()),
        "updated_at_readable": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
    }
    if extra:
        payload.update(extra)
    save_json(PROGRESS_FILE, payload)


def detect_platform(url: str) -> str:
    url = clean_url(url)
    host = (urlsplit(url).netloc or "").lower()
    if "youtube.com" in host or "youtu.be" in host:
        return "youtube"
    if "bilibili.com" in host or "b23.tv" in host:
        return "bilibili"
    raise ValueError(f"unsupported platform: {url}")


def get_platform() -> str:
    if PLATFORM in {"youtube", "bilibili"}:
        return PLATFORM
    return detect_platform(SOURCE_URL)


def seconds_to_mmss_mmm(sec: float) -> str:
    total_ms = max(0, int(round(sec * 1000)))
    total_seconds, ms = divmod(total_ms, 1000)
    minutes, seconds = divmod(total_seconds, 60)
    return f"{minutes:02d}:{seconds:02d}.{ms:03d}"


def cookies_file_for(platform: str) -> Optional[Path]:
    if not USE_COOKIES:
        return None
    if platform == "youtube" and YOUTUBE_COOKIES_FILE.exists() and YOUTUBE_COOKIES_FILE.stat().st_size > 0:
        return YOUTUBE_COOKIES_FILE
    if platform == "bilibili" and BILIBILI_COOKIES_FILE.exists() and BILIBILI_COOKIES_FILE.stat().st_size > 0:
        return BILIBILI_COOKIES_FILE
    return None


def yt_dlp_base_cmd(platform: str):
    cmd = ["yt-dlp"]
    if platform == "youtube":
        cmd.extend(["--remote-components", "ejs:github"])
    cookie_path = cookies_file_for(platform)
    if cookie_path:
        cmd.extend(["--cookies", str(cookie_path)])
    return cmd


def fetch_video_info(url: str, platform: str) -> Dict:
    url = clean_url(url)
    cmd = yt_dlp_base_cmd(platform)
    cmd.extend(["--no-playlist", "--dump-single-json", url])
    res = run(cmd, capture=True)
    data = json.loads(res.stdout)

    video_id = data.get("id") or "NOID"
    title = (data.get("title") or video_id).strip()
    webpage_url = data.get("webpage_url") or url

    return {
        "id": video_id,
        "title": title,
        "url": webpage_url,
        "uploader": data.get("uploader", ""),
        "channel": data.get("channel", ""),
        "duration": data.get("duration"),
        "platform": platform,
    }


def build_output_basename(info: Dict) -> str:
    video_id = info.get("id") or "NOID"
    return f"{DESTINATION}_{video_id}"


def plain_output_path(info: Dict) -> Path:
    return PLAIN_DIR / f"{build_output_basename(info)}.txt"


def ts_output_path(info: Dict) -> Path:
    return WITH_TS_DIR / f"{build_output_basename(info)}.txt"


def meta_output_path() -> Path:
    return META_FILE


def download_audio(info: Dict) -> Path:
    platform = info["platform"]
    url = clean_url(info["url"])
    video_id = info["id"]

    outtmpl = str(TMP_DIR / f"{video_id}.%(ext)s")
    cmd = yt_dlp_base_cmd(platform)
    cmd.extend([
        "--no-playlist",
        "-f", "ba/bestaudio",
        "-x",
        "--audio-format", AUDIO_FORMAT,
        "--audio-quality", AUDIO_QUALITY,
        "-o", outtmpl,
        url,
    ])
    run(cmd)

    files = [p for p in TMP_DIR.glob(f"{video_id}.*") if p.is_file() and not p.name.endswith(".part")]
    if not files:
        raise RuntimeError(f"audio file not found for {video_id}")
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0]


def load_model() -> WhisperModel:
    log(f"[info] loading model: {MODEL_NAME}, device={DEVICE}, compute_type={COMPUTE_TYPE}")
    return WhisperModel(MODEL_NAME, device=DEVICE, compute_type=COMPUTE_TYPE)


def transcribe_audio(model: WhisperModel, audio_path: Path) -> Dict:
    kwargs = {
        "language": LANGUAGE if LANGUAGE else None,
        "beam_size": BEAM_SIZE,
        "vad_filter": VAD_FILTER,
        "condition_on_previous_text": False,
    }
    if INITIAL_PROMPT:
        kwargs["initial_prompt"] = INITIAL_PROMPT

    segments, info = model.transcribe(str(audio_path), **kwargs)

    ts_lines = []
    plain_lines = []
    kept_segments = 0

    for seg in segments:
        text = (seg.text or "").strip()
        if not text:
            continue
        kept_segments += 1
        ts_lines.append(f"[{seconds_to_mmss_mmm(seg.start)} --> {seconds_to_mmss_mmm(seg.end)}] {text}")
        plain_lines.append(text)

    return {
        "language": getattr(info, "language", ""),
        "language_probability": getattr(info, "language_probability", ""),
        "segments": kept_segments,
        "timestamp_text": "\n".join(ts_lines).strip(),
        "plain_text": "\n".join(plain_lines).strip(),
    }


def write_outputs(info: Dict, result: Dict):
    ts_body = (result.get("timestamp_text", "").strip() + "\n") if result.get("timestamp_text") else ""
    plain_body = (result.get("plain_text", "").strip() + "\n") if result.get("plain_text") else ""
    atomic_write_text(ts_output_path(info), ts_body)
    atomic_write_text(plain_output_path(info), plain_body)

    meta = {
        "id": info.get("id", ""),
        "title": info.get("title", ""),
        "url": clean_url(info.get("url", "")),
        "uploader": info.get("uploader", ""),
        "channel": info.get("channel", ""),
        "duration": info.get("duration"),
        "platform": info.get("platform", ""),
        "destination": DESTINATION,
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        "whisper_model": MODEL_NAME,
        "language": result.get("language", ""),
        "language_probability": result.get("language_probability", ""),
        "segments": result.get("segments", 0),
    }
    save_json(meta_output_path(), meta)


def cleanup_temp_file(path: Optional[Path]):
    try:
        if path and path.exists():
            path.unlink(missing_ok=True)
    except Exception as e:
        log(f"[warn] cleanup failed: {e}")


def git_commit_and_push(message: str):
    git_run(["git", "add", "single_videos", "state_single_video"], check=False)
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

    source_url = clean_url(SOURCE_URL)
    if not source_url:
        raise ValueError("SOURCE_URL is empty")

    platform = get_platform()
    save_progress("fetching_info", extra={"resolved_platform": platform, "source_url": source_url})

    info = fetch_video_info(source_url, platform)
    save_progress("downloading_audio", extra=info)

    model = load_model()
    audio_path = None

    try:
        audio_path = download_audio(info)

        save_progress("transcribing", extra=info)
        result = transcribe_audio(model, audio_path)

        save_progress("writing_outputs", extra=info)
        write_outputs(info, result)

        save_progress("finished", extra=info)
        git_commit_and_push(f"single-video: {DESTINATION} {info['id']}")
        log(f"[ok] completed: {info['id']} {info['title']}")

    except Exception as e:
        save_progress("error", note=repr(e), extra=info if 'info' in locals() else {"source_url": source_url})
        raise
    finally:
        cleanup_temp_file(audio_path)


if __name__ == "__main__":
    main()
