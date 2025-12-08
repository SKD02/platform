# yandex_ocr.py
import os, io, base64, json, time, random, requests, re
from typing import List, Dict, Any, Optional, Tuple
from pdf2image import convert_from_bytes
import fitz
from hashlib import sha256
from threading import Lock
from PIL import Image

YC_API_KEY   = os.getenv("YC_API_KEY")         
YC_FOLDER_ID = os.getenv("YC_FOLDER_ID")     
VISION_URL   = os.getenv(
    "YC_VISION_URL",
    "https://vision.api.cloud.yandex.net/vision/v1/batchAnalyze"
)


OCR_MIN_INTERVAL_SEC = 1.1  
OCR_MAX_RETRIES      = 2
OCR_BACKOFF_BASE     = 1.8
OCR_BACKOFF_MAX      = 30


MAX_PDF_BYTES        = 10 * 1024 * 1024   
MAX_IMAGE_MEGAPIX    = 20.0              
BATCH_PAGE_CHUNK     = 3

PDF_DPI              = 300
JPEG_QUALITY         = 80

_last_ocr_ts = 0.0
_last_ocr_lock = Lock()
_OCR_CACHE: Dict[str, Dict[str, Any]] = {}

class YCOCRError(Exception):
    pass

def _headers() -> Dict[str, str]:
    if not YC_API_KEY:
        raise YCOCRError("YC_API_KEY is not set")
    if not YC_FOLDER_ID:
        raise YCOCRError("YC_FOLDER_ID is not set")
    return {
        "Authorization": f"Api-Key {YC_API_KEY}",
        "X-Folder-Id": YC_FOLDER_ID,
        "Content-Type": "application/json",
    }

def _b64(b: bytes) -> str:
    return base64.b64encode(b).decode("utf-8")

def _fp(b: bytes) -> str:
    return sha256(b).hexdigest()

def _chunked(lst: List[bytes], n: int) -> List[List[bytes]]:
    return [lst[i:i+n] for i in range(0, len(lst), n)]

def has_embedded_text(pdf_bytes: bytes) -> bool:
    try:
        with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
            for p in doc:
                if p.get_text().strip():
                    return True
    except Exception:
        pass
    return False

def extract_text_from_pdf_bytes(pdf_bytes: bytes) -> str:
    parts = []
    with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
        for p in doc:
            t = p.get_text("text")
            if t:
                parts.append(t)
    return "\n".join(parts).strip()

def get_pdf_page_count(pdf_bytes: bytes) -> int:
    try:
        with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
            return len(doc)
    except Exception:
        return 0

def _cap_megapixels(img: Image.Image, max_mp: float = MAX_IMAGE_MEGAPIX) -> Image.Image:
    w, h = img.size
    mp = (w * h) / 1e6
    if mp <= max_mp:
        return img
    scale = (max_mp / mp) ** 0.5
    nw, nh = max(1, int(w * scale)), max(1, int(h * scale))
    return img.resize((nw, nh), Image.LANCZOS)

def pdf_to_jpegs_capped(pdf_bytes: bytes,
                        dpi: int = PDF_DPI,
                        quality: int = JPEG_QUALITY,
                        max_pages: Optional[int] = None) -> List[bytes]:
    pil_pages = convert_from_bytes(pdf_bytes, dpi=dpi)
    out: List[bytes] = []
    for i, im in enumerate(pil_pages):
        if max_pages is not None and i >= max_pages:
            break
        im = _cap_megapixels(im, MAX_IMAGE_MEGAPIX)
        buf = io.BytesIO()
        im.save(buf, format="JPEG", quality=quality, optimize=True)
        out.append(buf.getvalue())
    return out

def _post_with_retry(url: str, headers: dict, payload: dict) -> requests.Response:
    global _last_ocr_ts
    attempt = 0
    while True:
        with _last_ocr_lock:
            now = time.time()
            d = now - _last_ocr_ts
            if d < OCR_MIN_INTERVAL_SEC:
                time.sleep(OCR_MIN_INTERVAL_SEC - d)
            _last_ocr_ts = time.time()

        try:
            r = requests.post(url, headers=headers, json=payload)
        except (requests.ConnectionError,
                requests.Timeout,
                requests.exceptions.ChunkedEncodingError) as e:
            if attempt < OCR_MAX_RETRIES:
                sleep_s = min((OCR_BACKOFF_BASE ** attempt) + random.uniform(0, 0.5), OCR_BACKOFF_MAX)
                time.sleep(sleep_s)
                attempt += 1
                continue
            raise
        if r.status_code < 400:
            return r

        if r.status_code == 413:
            r.raise_for_status()

        if r.status_code in (429, 503) and attempt < OCR_MAX_RETRIES:
            retry_after = r.headers.get("Retry-After")
            if retry_after:
                try:
                    sleep_s = float(retry_after)
                except Exception:
                    sleep_s = OCR_BACKOFF_BASE ** attempt
            else:
                sleep_s = OCR_BACKOFF_BASE ** attempt
            sleep_s = min(sleep_s + random.uniform(0, 0.5), OCR_BACKOFF_MAX)
            time.sleep(sleep_s)
            attempt += 1
            continue

        r.raise_for_status()

def yc_vision_ocr_document(file_bytes: bytes, mime_type: str) -> Dict[str, Any]:
    payload = {
        "analyze_specs": [{
            "content": _b64(file_bytes),
            "mimeType": mime_type,
            "features": [{
                "type": "TEXT_DETECTION",
                "text_detection_config": {"language_codes": ["ru", "en"]}
            }]
        }]
    }
    r = _post_with_retry(VISION_URL, _headers(), payload)
    return r.json()

def yc_vision_ocr_images(img_bytes_list: List[bytes], langs: Optional[List[str]] = None) -> Dict[str, Any]:
    langs = langs or ["ru", "en"]
    headers = _headers()

    def make_payload(chunk: List[bytes]) -> dict:
        specs = []
        for b in chunk:
            specs.append({
                "content": _b64(b),
                "mimeType": "image/jpeg",
                "features": [{
                    "type": "TEXT_DETECTION",
                    "text_detection_config": {"language_codes": langs}
                }]
            })
        return {"analyze_specs": specs}

    all_results: List[Dict[str, Any]] = []
    for chunk in _chunked(img_bytes_list, BATCH_PAGE_CHUNK):
        r = _post_with_retry(VISION_URL, headers, make_payload(chunk))
        j = r.json() or {}
        if "results" in j:
            all_results.extend(j["results"])
    return {"results": all_results}

def parse_vision_response_to_text(resp: Dict[str, Any]) -> str:
    lines: List[str] = []
    for spec_result in resp.get("results", []) or []:
        for feat in spec_result.get("results", []) or []:
            td = feat.get("textDetection") or feat.get("textAnnotation") or {}
            full = td.get("fullText")
            if isinstance(full, str) and full.strip():
                lines.append(full.strip())
                continue
            for page in td.get("pages", []) or []:
                for block in page.get("blocks", []) or []:
                    for line in block.get("lines", []) or []:
                        if line.get("text"):
                            lines.append(str(line["text"]))
                        else:
                            words = [w.get("text", "") for w in (line.get("words") or []) if w.get("text")]
                            if words:
                                lines.append(" ".join(words))
    
    text = "\n".join(s.strip() for s in lines if str(s).strip())
    text = re.sub(r"<(hw|rot)_[0-9]+>", "", text)
    text = re.sub(r"\s+", " ", text).strip()

    return text


def extract_text_smart(file_bytes: bytes, mime: str) -> str:
    mime = (mime or "").lower().strip()
    if mime == "application/pdf":
        embedded = ""
        if has_embedded_text(file_bytes):
            return extract_text_from_pdf_bytes(file_bytes)
        
        if len(embedded) >= 100:
            return embedded
        if len(file_bytes) <= MAX_PDF_BYTES:
            try:
                resp = yc_vision_ocr_document(file_bytes, mime_type="application/pdf")
                return parse_vision_response_to_text(resp)
            except requests.HTTPError as e:
                if e.response is None or e.response.status_code in (413, 429, 503):
                    pass
                else:
                    raise

        imgs = pdf_to_jpegs_capped(file_bytes, dpi=PDF_DPI, quality=JPEG_QUALITY)
        resp = yc_vision_ocr_images(imgs)
        return parse_vision_response_to_text(resp)

    if "png" in mime:
        mt = "image/png"
    else:
        mt = "image/jpeg"
    try:
        resp = yc_vision_ocr_document(file_bytes, mime_type=mt)
        return parse_vision_response_to_text(resp)
    except requests.HTTPError:
        try:
            im = Image.open(io.BytesIO(file_bytes))
            im = _cap_megapixels(im, MAX_IMAGE_MEGAPIX)
            buf = io.BytesIO()
            im.save(buf, format="JPEG", quality=JPEG_QUALITY, optimize=True)
            resp = yc_vision_ocr_document(buf.getvalue(), mime_type="image/jpeg")
            return parse_vision_response_to_text(resp)
        except Exception:
            raise

def extract_text_with_meta(file_bytes: bytes, mime: str) -> Tuple[str, Dict[str, Any]]:
    t0 = time.perf_counter()
    mime_norm = (mime or "").lower().strip()
    pages = get_pdf_page_count(file_bytes)

    text = ""
    source = ""
    ocr_used = False

    if mime_norm == "application/pdf":
        embedded_txt = ""
        if has_embedded_text(file_bytes):
            embedded_txt = extract_text_from_pdf_bytes(file_bytes)

        if len(embedded_txt) >= 100:
            text = embedded_txt
            source = "embedded"
            ocr_used = False
        else:
            if len(file_bytes) <= MAX_PDF_BYTES:
                resp = yc_vision_ocr_document(file_bytes, mime_type="application/pdf")
            else:
                imgs = pdf_to_jpegs_capped(file_bytes, dpi=PDF_DPI, quality=JPEG_QUALITY)
                resp = yc_vision_ocr_images(imgs)

            text = parse_vision_response_to_text(resp)
            source = "yandex_vision"
            ocr_used = True
    else:
        mt = "image/png" if "png" in mime_norm else "image/jpeg"
        resp = yc_vision_ocr_document(file_bytes, mime_type=mt)
        text = parse_vision_response_to_text(resp)
        source = "yandex_vision"
        ocr_used = True
        pages = 1

    meta = {
        "source": source,
        "ocr_used": ocr_used,
        "engine": "yandex-vision" if ocr_used else "none",
        "mime": mime_norm,
        "pages": pages,
        "chars": len(text or ""),
        "ms": int((time.perf_counter() - t0) * 1000),
        "cache_hit": False
    }
    fp = _fp(file_bytes)
    _OCR_CACHE[fp] = {"text": text, "meta": meta}
    return text, meta
