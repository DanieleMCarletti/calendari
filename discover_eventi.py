"""Discovery automatica di eventi (concerti) in zona San Siro / Lampugnano / Ippodromo SNAI
via GitHub Models. Output: file JSON in discovered/eventi_YYYY_MM.json.

NON modifica i file Python in dati_grezzi/ direttamente. Il workflow di generazione
ICS legge entrambe le fonti; questo script si limita a proporre candidati nuovi.

Esecuzione: python discover_eventi.py
Variabili env richieste:
  GITHUB_TOKEN (o GH_MODELS_TOKEN) - PAT o token Actions con accesso a GitHub Models
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sys
import time
from datetime import datetime, date, timedelta
from pathlib import Path

import pytz
import requests
from bs4 import BeautifulSoup
from jsonschema import Draft202012Validator
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


SCRIPT_DIR = Path(__file__).resolve().parent
DISCOVERED_DIR = SCRIPT_DIR / "discovered"
DATI_GREZZI_DIR = SCRIPT_DIR / "dati_grezzi"
SCHEMA_PATH = DISCOVERED_DIR / "SCHEMA.json"

TARGET_TIMEZONE = pytz.timezone("Europe/Rome")
NOW = datetime.now(TARGET_TIMEZONE)
WINDOW_DAYS = 180  # cerca eventi fino a 6 mesi nel futuro

GH_MODELS_ENDPOINT = "https://models.github.ai/inference/chat/completions"
GH_MODELS_MODEL = "openai/gpt-4o-mini"
GH_MODELS_MAX_TOKENS = 2000

# Fonti da scrappare. Inizio con UNA sola fonte (vedi piano Step 2).
# Altre saranno aggiunte in Step 3 dopo aver misurato la qualita' di output.
SOURCES = [
    # NB: MilanoToday usa rendering lato-client per i listing → il body server-side
    # contiene poco contenuto. Lasciato come fonte di prova in attesa di Step 3
    # (aggiunta fonti server-rendered o playwright). Failure mode: 0 eventi, niente PR.
    {
        "name": "milanotoday-concerti",
        "url": "https://www.milanotoday.it/eventi/concerti/",
    },
    {
        "name": "milanotoday-san-siro",
        "url": "https://www.milanotoday.it/eventi/location/san-siro/",
    },
    {
        "name": "milanotoday-ippodromo-san-siro",
        "url": "https://www.milanotoday.it/eventi/location/ippodromo-di-san-siro/",
    },
]

LOCATION_REGEX = re.compile(
    r"san\s*siro|meazza|lampugnano|la\s*maura|ippodromo",
    re.IGNORECASE,
)


def log(msg: str) -> None:
    print(f"[{datetime.now().isoformat(timespec='seconds')}] {msg}", flush=True)


def make_http_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=frozenset(["GET"]),
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://", HTTPAdapter(max_retries=retry))
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
        ),
        "Accept-Language": "it-IT,it;q=0.9,en;q=0.5",
    })
    return s


def fetch_source_text(session: requests.Session, source: dict) -> str | None:
    url = source["url"]
    try:
        r = session.get(url, timeout=20)
        r.raise_for_status()
    except Exception as e:
        log(f"  ERRORE fetch {url}: {e}")
        return None
    soup = BeautifulSoup(r.text, "html.parser")
    # Rimuovo elementi non utili (rumore: chrome, navigazione, embed, cookie banner, ecc.)
    for tag in soup(["script", "style", "noscript", "iframe", "header", "footer", "nav", "aside", "form"]):
        tag.decompose()
    # Rimuovo nodi con classi tipiche di banner/cookie/menu (snapshot per evitare
    # mutazione concorrente durante decompose).
    noise_keywords = ("cookie", "newsletter", "site-footer", "site-header",
                      "share", "social", "advert", "banner-")
    candidates = list(soup.find_all(True, class_=True))
    for el in candidates:
        if not el.attrs:
            continue
        classes = " ".join(el.get("class") or []).lower()
        if any(k in classes for k in noise_keywords):
            el.decompose()
    root = soup.body or soup
    text = root.get_text(separator=" ", strip=True)
    text = re.sub(r"\s+", " ", text)
    # Tronco a ~8000 caratteri (~2000 token) per non sporcare il prompt
    if len(text) > 8000:
        text = text[:8000] + " ...[truncated]"
    return text


def call_github_models(token: str, source_url: str, page_text: str) -> dict | None:
    today_iso = NOW.date().isoformat()
    horizon_iso = (NOW + timedelta(days=WINDOW_DAYS)).date().isoformat()
    system_prompt = (
        "Sei un estrattore di eventi pubblici. Riceverai il testo di una pagina web italiana "
        "e devi estrarre SOLO eventi che si svolgono in una di queste location di Milano:\n"
        "- Stadio San Siro / Stadio Giuseppe Meazza\n"
        "- Ippodromo SNAI San Siro\n"
        "- Ippodromo SNAI La Maura (zona Lampugnano)\n"
        f"Filtra solo eventi futuri (data >= {today_iso}) fino a {horizon_iso}.\n"
        "Restituisci JSON puro nel formato:\n"
        '{"events": [{"summary": "...", "dtstart_str": "YYYY-MM-DDTHH:MM:SS", '
        '"dtend_str": "YYYY-MM-DDTHH:MM:SS", "location_name": "...", '
        '"location_address": "...", "description": "...", "confidence": "high|medium|low"}]}'
        "\nRegole:\n"
        "- Usa orari plausibili: concerti tipicamente 21:00-23:30 (dtend = dtstart + 2h30m se assente).\n"
        "- Se non sei sicuro della location esatta, METTI 'confidence': 'low' o salta l'evento.\n"
        "- NON inventare eventi non menzionati nel testo. NON inventare date se non esplicite.\n"
        "- Il 'summary' DEVE contenere il nome di un artista/banda/show specifico "
        "(es. 'Cesare Cremonini - LIVE25', 'Bruce Springsteen Tour'). "
        "SALTA titoli generici di sezione o link di categoria tipo 'Concerti a San Siro', "
        "'Eventi al Meazza' che NON sono singoli eventi.\n"
        "- Salta menu, footer, banner cookie, articoli generici senza data certa.\n"
        '- Se non trovi eventi validi, restituisci {"events": []}.'
    )
    user_prompt = f"Pagina sorgente: {source_url}\n\nTesto:\n{page_text}"
    body = {
        "model": GH_MODELS_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "response_format": {"type": "json_object"},
        "temperature": 0.0,
        "max_tokens": GH_MODELS_MAX_TOKENS,
    }
    try:
        r = requests.post(
            GH_MODELS_ENDPOINT,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json=body,
            timeout=60,
        )
        r.raise_for_status()
    except Exception as e:
        log(f"  ERRORE chiamata GitHub Models: {e}")
        return None
    try:
        data = r.json()
        content = data["choices"][0]["message"]["content"]
        parsed = json.loads(content)
        return parsed
    except Exception as e:
        log(f"  ERRORE parsing risposta LLM: {e}")
        return None


def load_existing_manual_signatures() -> set[tuple[str, str]]:
    """Legge tutti i file dati_grezzi/eventi_YYYY_MM.py e ritorna firme (summary_norm, date)
    per dedup pre-PR (non riproporre eventi gia' curati a mano)."""
    sigs: set[tuple[str, str]] = set()
    if not DATI_GREZZI_DIR.is_dir():
        return sigs
    for py_file in DATI_GREZZI_DIR.glob("eventi_*.py"):
        try:
            ns: dict = {}
            exec(py_file.read_text(encoding="utf-8"), ns)
            for ev in ns.get("event_list", []):
                sig = _signature(ev.get("summary", ""), ev.get("dtstart_str", ""))
                if sig:
                    sigs.add(sig)
        except Exception as e:
            log(f"  WARN: impossibile leggere {py_file}: {e}")
    return sigs


def load_existing_discovered_signatures() -> set[tuple[str, str]]:
    """Firme di eventi gia' presenti nei JSON discovered/ committati."""
    sigs: set[tuple[str, str]] = set()
    if not DISCOVERED_DIR.is_dir():
        return sigs
    for jf in DISCOVERED_DIR.glob("eventi_*.json"):
        try:
            doc = json.loads(jf.read_text(encoding="utf-8"))
            for ev in doc.get("events", []):
                sig = _signature(ev.get("summary", ""), ev.get("dtstart_str", ""))
                if sig:
                    sigs.add(sig)
        except Exception as e:
            log(f"  WARN: impossibile leggere {jf}: {e}")
    return sigs


def _normalize_summary(s: str) -> str:
    s = (s or "").lower().strip()
    s = re.sub(r"[^a-z0-9àèéìòù\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    # rimuovo parole comuni rumorose
    s = re.sub(r"\b(live|concerto|tour|show|stadi|i-days)\b", "", s).strip()
    return s


def _signature(summary: str, dtstart_str: str) -> tuple[str, str] | None:
    norm = _normalize_summary(summary)
    if not norm or not dtstart_str:
        return None
    date_part = dtstart_str[:10]  # YYYY-MM-DD
    return (norm, date_part)


def filter_and_dedup(
    raw_events: list[dict],
    existing_manual: set[tuple[str, str]],
    existing_discovered: set[tuple[str, str]],
    source_url: str,
) -> list[dict]:
    out: list[dict] = []
    today = NOW.date()
    horizon = today + timedelta(days=WINDOW_DAYS)
    for ev in raw_events:
        if not isinstance(ev, dict):
            continue
        summary = (ev.get("summary") or "").strip()
        dtstart_str = (ev.get("dtstart_str") or "").strip()
        location_name = (ev.get("location_name") or "").strip()
        if not summary or not dtstart_str or not location_name:
            continue
        # Validazione data
        try:
            dt = datetime.strptime(dtstart_str, "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            log(f"    SKIP: dtstart_str non parsabile: {dtstart_str!r}")
            continue
        if not (today <= dt.date() <= horizon):
            continue
        # Location deve matchare regex
        if not LOCATION_REGEX.search(location_name):
            log(f"    SKIP: location non in scope: {location_name!r}")
            continue
        sig = _signature(summary, dtstart_str)
        if not sig:
            continue
        if sig in existing_manual:
            log(f"    SKIP: gia' presente in dati_grezzi: {summary!r} {dtstart_str}")
            continue
        if sig in existing_discovered:
            log(f"    SKIP: gia' presente in discovered: {summary!r} {dtstart_str}")
            continue
        # Default dtend se mancante
        if not ev.get("dtend_str"):
            dt_end = dt + timedelta(hours=2, minutes=30)
            ev["dtend_str"] = dt_end.strftime("%Y-%m-%dT%H:%M:%S")
        # Default address se la location matcha
        if not ev.get("location_address"):
            loc_lower = location_name.lower()
            if "san siro" in loc_lower or "meazza" in loc_lower:
                ev["location_address"] = "Piazzale Angelo Moratti, 20151 Milano MI, Italy"
            elif "la maura" in loc_lower or "lampugnano" in loc_lower:
                ev["location_address"] = "Via Lampugnano 95, 20151 Milano MI, Italy"
            elif "ippodromo" in loc_lower:
                ev["location_address"] = "Piazzale dello Sport 16, 20151 Milano MI, Italy"
        ev["source_url"] = source_url
        out.append(ev)
        # Aggiungo subito alla blacklist per non duplicare tra fonti diverse in questo run
        existing_discovered.add(sig)
    return out


def group_by_month(events: list[dict]) -> dict[str, list[dict]]:
    grouped: dict[str, list[dict]] = {}
    for ev in events:
        ym = ev["dtstart_str"][:7].replace("-", "_")  # YYYY_MM
        grouped.setdefault(ym, []).append(ev)
    return grouped


def write_monthly_json(month_key: str, events: list[dict], source_urls: list[str]) -> Path:
    DISCOVERED_DIR.mkdir(parents=True, exist_ok=True)
    target = DISCOVERED_DIR / f"eventi_{month_key}.json"
    # Se esiste, unisco con quelli già committati (mantenendo dedup per firma)
    existing_events: list[dict] = []
    if target.exists():
        try:
            doc = json.loads(target.read_text(encoding="utf-8"))
            existing_events = doc.get("events", [])
        except Exception:
            existing_events = []
    # Indice firme degli eventi già committati
    seen_sigs: set[tuple[str, str]] = set()
    merged: list[dict] = []
    for ev in existing_events + events:
        sig = _signature(ev.get("summary", ""), ev.get("dtstart_str", ""))
        if not sig or sig in seen_sigs:
            continue
        seen_sigs.add(sig)
        merged.append(ev)
    # Ordino per dtstart per output deterministico
    merged.sort(key=lambda e: (e.get("dtstart_str", ""), e.get("summary", "")))
    doc = {
        "generated_at": NOW.astimezone(pytz.UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source_urls": sorted(set(source_urls)),
        "events": merged,
    }
    target.write_text(json.dumps(doc, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return target


def validate_against_schema(doc: dict) -> list[str]:
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema)
    errors = []
    for err in validator.iter_errors(doc):
        errors.append(f"{list(err.absolute_path)}: {err.message}")
    return errors


def main() -> int:
    token = os.environ.get("GH_MODELS_TOKEN") or os.environ.get("GITHUB_TOKEN")
    if not token:
        log("ERRORE: GH_MODELS_TOKEN o GITHUB_TOKEN non impostato.")
        return 2

    log(f"--- Discovery eventi (orizzonte {WINDOW_DAYS} giorni) ---")
    existing_manual = load_existing_manual_signatures()
    existing_discovered = load_existing_discovered_signatures()
    log(f"  Eventi manuali noti: {len(existing_manual)}")
    log(f"  Eventi discovered noti: {len(existing_discovered)}")

    session = make_http_session()
    all_new_events: list[dict] = []
    source_urls_used: list[str] = []

    for source in SOURCES:
        log(f"--- Sorgente: {source['name']} ({source['url']}) ---")
        text = fetch_source_text(session, source)
        if not text:
            log("  Skip sorgente (fetch fallito).")
            continue
        log(f"  Testo estratto: {len(text)} chars")
        parsed = call_github_models(token, source["url"], text)
        if not parsed:
            log("  Skip sorgente (LLM fallito).")
            continue
        raw_events = parsed.get("events", []) if isinstance(parsed, dict) else []
        log(f"  LLM ha proposto {len(raw_events)} eventi candidati")
        new_events = filter_and_dedup(raw_events, existing_manual, existing_discovered, source["url"])
        log(f"  Dopo filtro/dedup: {len(new_events)} eventi nuovi")
        all_new_events.extend(new_events)
        source_urls_used.append(source["url"])

    if not all_new_events:
        log("Nessun evento nuovo da proporre.")
        return 0

    log(f"--- Scrittura JSON: {len(all_new_events)} eventi totali ---")
    grouped = group_by_month(all_new_events)
    written_files: list[Path] = []
    for month_key, evs in grouped.items():
        path = write_monthly_json(month_key, evs, source_urls_used)
        # Valida il file scritto contro lo schema
        doc = json.loads(path.read_text(encoding="utf-8"))
        errs = validate_against_schema(doc)
        if errs:
            log(f"  ERRORE schema su {path.name}:")
            for e in errs:
                log(f"    - {e}")
            return 3
        log(f"  OK {path.name}: {len(evs)} nuovi (+ esistenti, dedup applicato).")
        written_files.append(path)

    # Stampa riepilogo per body PR
    log("--- Riepilogo eventi proposti (per body PR) ---")
    for ev in sorted(all_new_events, key=lambda e: e.get("dtstart_str", "")):
        conf = ev.get("confidence", "?")
        log(f"  [{conf}] {ev['dtstart_str']} - {ev['summary']} @ {ev['location_name']}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
