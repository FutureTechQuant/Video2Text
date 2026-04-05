import os
import re
import json
import time
import requests
from pathlib import Path

BVID = os.getenv("BVID", "BV1TC1jYmEve")
OUT_DIR = Path("subtitles") / BVID
OUT_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Referer": f"https://www.bilibili.com/video/{BVID}",
    "Cookie": os.getenv("BILIBILI_COOKIES", "").strip(),
}

session = requests.Session()
session.headers.update(HEADERS)

def safe_name(name: str) -> str:
    name = re.sub(r'[\\\\/:*?"<>|\\n\\r\\t]+', "_", name)
    return name[:120].strip(" ._") or "untitled"

def sec_to_srt_time(sec: float) -> str:
    ms = int(round(sec * 1000))
    h = ms // 3600000
    ms %= 3600000
    m = ms // 60000
    ms %= 60000
    s = ms // 1000
    ms %= 1000
    return f"{h:02d}:{m:02d}:{s:02d},{ms:03d}"

def json_to_srt(body):
    lines = []
    for idx, item in enumerate(body, start=1):
        start = sec_to_srt_time(item["from"])
        end = sec_to_srt_time(item["to"])
        content = item["content"].strip()
        lines.append(f"{idx}\\n{start} --> {end}\\n{content}\\n")
    return "\\n".join(lines)

def get_video_info(bvid):
    url = "https://api.bilibili.com/x/web-interface/view"
    r = session.get(url, params={"bvid": bvid}, timeout=30)
    r.raise_for_status()
    data = r.json()
    if data["code"] != 0:
        raise RuntimeError(f"view api failed: {data}")
    return data["data"]

def get_pages(bvid):
    url = "https://api.bilibili.com/x/player/pagelist"
    r = session.get(url, params={"bvid": bvid}, timeout=30)
    r.raise_for_status()
    data = r.json()
    if data["code"] != 0:
        raise RuntimeError(f"pagelist api failed: {data}")
    return data["data"]

def get_subtitles_meta(bvid, cid):
    url = "https://api.bilibili.com/x/player/v2"
    r = session.get(url, params={"bvid": bvid, "cid": cid}, timeout=30)
    r.raise_for_status()
    data = r.json()
    if data["code"] != 0:
        raise RuntimeError(f"player v2 api failed: {data}")
    subtitle = data.get("data", {}).get("subtitle", {})
    return subtitle.get("subtitles", [])

def pick_subtitle(subtitles):
    if not subtitles:
        return None
    preferred = ["zh-CN", "zh-Hans", "zh", "ai-zh", "zh-TW"]
    for lan in preferred:
        for s in subtitles:
            if s.get("lan") == lan:
                return s
    return subtitles[0]

def download_subtitle_json(subtitle_url):
    if subtitle_url.startswith("//"):
        subtitle_url = "https:" + subtitle_url
    r = session.get(subtitle_url, timeout=30)
    r.raise_for_status()
    return r.json()

def main():
    info = get_video_info(BVID)
    title = info["title"]
    pages = get_pages(BVID)

    meta = {
        "bvid": BVID,
        "title": title,
        "pages": []
    }

    for page in pages:
        p = page["page"]
        cid = page["cid"]
        part = page["part"]
        base = f"{p:03d}-{safe_name(part)}"

        try:
            subtitles = get_subtitles_meta(BVID, cid)
            chosen = pick_subtitle(subtitles)
            if not chosen:
                print(f"[skip] p{p} {part} -> no subtitle")
                meta["pages"].append({
                    "page": p,
                    "cid": cid,
                    "part": part,
                    "subtitle": None
                })
                continue

            sub_json = download_subtitle_json(chosen["subtitle_url"])
            body = sub_json.get("body", [])

            json_path = OUT_DIR / f"{base}.json"
            srt_path = OUT_DIR / f"{base}.srt"

            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(sub_json, f, ensure_ascii=False, indent=2)

            with open(srt_path, "w", encoding="utf-8") as f:
                f.write(json_to_srt(body))

            meta["pages"].append({
                "page": p,
                "cid": cid,
                "part": part,
                "subtitle": {
                    "lan": chosen.get("lan"),
                    "lan_doc": chosen.get("lan_doc"),
                    "json": str(json_path).replace("\\\\", "/"),
                    "srt": str(srt_path).replace("\\\\", "/")
                }
            })

            print(f"[ok] p{p} {part}")
            time.sleep(1)

        except Exception as e:
            print(f"[error] p{p} {part}: {e}")
            meta["pages"].append({
                "page": p,
                "cid": cid,
                "part": part,
                "error": str(e)
            })

    with open(OUT_DIR / "index.json", "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
