# genera_calendari_mensili.py

from icalendar import Calendar, Event
from datetime import datetime, date, timedelta
import pytz
import os
import sys
import hashlib
import shutil
from pathlib import Path
import re
import importlib.util
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- Configurazione Globale ---
TARGET_TIMEZONE_STR = 'Europe/Rome'
TARGET_TIMEZONE_OBJ = pytz.timezone(TARGET_TIMEZONE_STR)

DATA_SOURCE_FOLDER_NAME = "dati_grezzi"
OUTPUT_ICS_FOLDER_NAME = "calendari_output"
CURRENT_YEAR = datetime.now().year
AGGREGATED_ICS_FILENAME = "eventi_san_siro_aggregato.ics"

CALENDAR_URLS = {
    "inter": "https://ics.fixtur.es/v2/inter.ics",
    "milan": "https://ics.fixtur.es/v2/ac-milan.ics",
}

STADIO_SAN_SIRO_KEYWORDS = [
    "san siro", "giuseppe meazza"
]

LOCATION_ALIASES = {
    "ippodromo snai la maura": ["ippodromo la maura", "la maura", "via lampugnano 95"],
    "ippodromo snai san siro": ["ippodromo san siro", "piazzale dello sport 16", "piazzale dello sport"],
    "stadio san siro": ["stadio giuseppe meazza", "san siro", "piazzale angelo moratti"]
}

LAMPUGNANO_CANONICAL_LOCATION = "ippodromo snai la maura"
UID_DOMAIN = "calendari.danielecarletti"
MIN_AGGREGATED_EVENTS = 5
SHRINK_TOLERANCE = 0.5  # se nuovi < 50% dei precedenti, abortisci senza scrivere


def log(msg):
    print(f"[{datetime.now().isoformat(timespec='seconds')}] {msg}")


def _make_http_session():
    """Session HTTP con retry+timeout per i fetch dei feed ICS."""
    s = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=frozenset(["GET"]),
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount('https://', adapter)
    s.mount('http://', adapter)
    return s


def stable_uid(summary, dtstart_iso, location_name):
    """UID deterministico (sha256 troncato) basato su summary+dtstart+location normalizzati.
    Stabile tra run a parità di contenuto → niente più eventi duplicati su Google Calendar."""
    norm_summary = normalize_summary_for_signature(summary or '')
    norm_loc = normalize_location_for_signature(location_name or '')
    key = f"{norm_summary}|{dtstart_iso or ''}|{norm_loc}"
    digest = hashlib.sha256(key.encode('utf-8')).hexdigest()[:16]
    return f"{digest}@{UID_DOMAIN}"


def stable_dtstamp(dtstart_aware):
    """DTSTAMP stabile derivato da dtstart (in UTC). Necessario per idempotenza dell'output ICS."""
    if not dtstart_aware:
        return datetime(2024, 1, 1, tzinfo=pytz.UTC)
    return dtstart_aware.astimezone(pytz.UTC).replace(microsecond=0)


def count_events_in_ics_file(path):
    """Conta i VEVENT in un file ICS esistente; ritorna 0 se il file non esiste o non parsabile."""
    if not path.exists():
        return 0
    try:
        with open(path, 'rb') as f:
            cal = Calendar.from_ical(f.read())
        return sum(1 for _ in cal.walk('VEVENT'))
    except Exception as e:
        log(f"WARN: impossibile parsare {path} per conteggio storico: {e}")
        return 0


# --- Funzioni di Download e Parsing URL ---
_HTTP_SESSION = None


def get_calendar_from_url(url):
    global _HTTP_SESSION
    if _HTTP_SESSION is None:
        _HTTP_SESSION = _make_http_session()
    try:
        response = _HTTP_SESSION.get(url, timeout=20)
        response.raise_for_status()
        return Calendar.from_ical(response.text)
    except requests.exceptions.Timeout:
        log(f"ERRORE: Timeout durante il download del calendario da {url}")
    except requests.exceptions.RequestException as e:
        log(f"ERRORE nel scaricare il calendario da {url}: {e}")
    except Exception as e:
        log(f"ERRORE nel parsare il calendario da {url}: {e}")
    return None

def is_location_relevant_for_feed(location_text):
    if not location_text: return False
    normalized_loc = normalize_location_for_signature(location_text)
    return normalized_loc in LOCATION_ALIASES

# --- Funzioni di Normalizzazione e Utilità ---
def parse_datetime_str(dt_str):
    if not dt_str: return None
    try:
        return datetime.strptime(dt_str, '%Y-%m-%dT%H:%M:%S')
    except ValueError:
        return None

def make_timezone_aware(dt_obj, timezone_obj=TARGET_TIMEZONE_OBJ):
    if not dt_obj: return None
    if isinstance(dt_obj, date) and not isinstance(dt_obj, datetime):
        dt_obj = datetime.combine(dt_obj, datetime.min.time())
    if not isinstance(dt_obj, datetime): return None
    if dt_obj.tzinfo is None or dt_obj.tzinfo.utcoffset(dt_obj) is None:
        return timezone_obj.localize(dt_obj)
    return dt_obj.astimezone(timezone_obj)

def normalize_summary_for_signature(summary):
    if not summary: return ""
    s = str(summary).lower().strip()
    s = re.sub(r'\s*\[(cl|el|cop|serie a|campionato)\]\s*$', '', s, flags=re.IGNORECASE)
    s = re.sub(r'\b(live|world tour|concerto|evento|show|i-days milano|stadi \d{4}|partita)\b', '', s, flags=re.IGNORECASE)
    s = re.sub(r'\(data \d+(?: - ipotizzata)?\)', '', s, flags=re.IGNORECASE)
    s = re.sub(r'[^\w\s-]', '', s)
    s = re.sub(r'\s+', ' ', s).strip()
    s = re.sub(r'\s*\(\d+-\d+\)\s*$', '', s).strip()
    parts = sorted([p.strip() for p in re.split(r'\s+vs\s+|\s+-\s+', s) if p.strip()])
    s = " vs ".join(parts)
    return s

def normalize_location_for_signature(location_name):
    if not location_name: return ""
    loc_lower = str(location_name).lower().strip()
    for canonical, aliases in LOCATION_ALIASES.items():
        if canonical in loc_lower: return canonical
        for alias in aliases:
            if alias in loc_lower: return canonical
    loc_lower = re.sub(r'[^\w\s-,]', '', loc_lower)
    loc_lower = re.sub(r'\s+', ' ', loc_lower).strip()
    return loc_lower if loc_lower else "unknown_location" # Ritorna "unknown_location" se vuota

def create_event_signatures(event_data_dict):
    """Crea sia una firma debole (summary+data) sia una forte (summary+data+location)."""
    norm_summary = normalize_summary_for_signature(event_data_dict.get('summary'))
    dt_start_obj_naive = parse_datetime_str(event_data_dict.get('dtstart_str'))
    event_date_str = dt_start_obj_naive.date().isoformat() if dt_start_obj_naive else ""
    
    weak_signature = (norm_summary, event_date_str)
    
    raw_location = event_data_dict.get('location_name', event_data_dict.get('location', ''))
    norm_loc_specific = normalize_location_for_signature(raw_location) # Restituisce 'unknown_location' se vuota
    
    strong_signature = (norm_summary, event_date_str, norm_loc_specific)
    return weak_signature, strong_signature

def load_event_list_from_file(file_path):
    module_name = file_path.stem
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if not spec or not spec.loader: return None
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
        # Assicurati di aggiungere 'source_type': 'manual' ai dizionari se non già presente
        loaded_list = getattr(module, 'event_list', None)
        if isinstance(loaded_list, list):
            for item in loaded_list:
                if 'source_type' not in item:
                    item['source_type'] = 'manual_from_file' # O 'dati_grezzi'
            return loaded_list
        return None
    except Exception as e:
        print(f"Errore durante l'importazione di {file_path}: {e}")
    return None

def ical_event_component_to_dict(component):
    event_dict = {'source_type': 'ics_feed'} # Importante per la logica di merge
    event_dict['summary'] = str(component.get('summary', 'Evento da Feed ICS'))
    dtstart_prop = component.get('dtstart')
    if dtstart_prop:
        dtstart_obj_orig = dtstart_prop.dt
        dtstart_obj_aware = make_timezone_aware(dtstart_obj_orig)
        if dtstart_obj_aware:
            event_dict['dtstart_str'] = dtstart_obj_aware.strftime('%Y-%m-%dT%H:%M:%S')
    dtend_prop = component.get('dtend')
    if dtend_prop:
        dtend_obj_orig = dtend_prop.dt
        dtend_obj_aware = make_timezone_aware(dtend_obj_orig)
        if dtend_obj_aware:
            event_dict['dtend_str'] = dtend_obj_aware.strftime('%Y-%m-%dT%H:%M:%S')
    event_dict['location_name'] = str(component.get('location', ''))
    event_dict['location_address'] = ''
    event_dict['description'] = str(component.get('description', ''))
    event_dict['google_maps_url_str'] = ''
    event_dict['original_uid_from_feed'] = str(component.get('uid', ''))
    return event_dict

def apply_deduplication_and_merge(event_list_of_dicts):
    print(f"  Inizio de-duplicazione per {len(event_list_of_dicts)} eventi candidati...")
    # Mappa da firma FORTE a dizionario evento
    processed_events_by_strong_signature = {}
    # Mappa da firma DEBOLE a firma FORTE (per trovare corrispondenze deboli)
    weak_to_strong_map = {}

    for current_event_data in event_list_of_dicts:
        weak_sig_curr, strong_sig_curr = create_event_signatures(current_event_data)
        
        dt_start_curr_obj = make_timezone_aware(parse_datetime_str(current_event_data.get('dtstart_str')))
        if not dt_start_curr_obj: continue # Evento non valido

        # Controlla se c'è già un evento con la stessa firma DEBOLE
        if weak_sig_curr in weak_to_strong_map:
            existing_event_strong_sig = weak_to_strong_map[weak_sig_curr]
            existing_event_data = processed_events_by_strong_signature[existing_event_strong_sig]
            
            print(f"    INFO: Trovata corrispondenza debole (Summary+Data) tra NUOVO '{current_event_data.get('summary')}' e ESISTENTE '{existing_event_data.get('summary')}'")

            is_current_from_feed = current_event_data.get('source_type') == 'ics_feed'
            is_existing_manual = existing_event_data.get('source_type') != 'ics_feed' # o 'manual_from_file'
            
            current_loc_norm = normalize_location_for_signature(current_event_data.get('location_name',''))
            existing_loc_norm = normalize_location_for_signature(existing_event_data.get('location_name',''))

            # CASO 1: Nuovo da FEED senza location, Esistente MANUALE con location -> Priorità al manuale
            if is_current_from_feed and is_existing_manual and \
               current_loc_norm == 'unknown_location' and existing_loc_norm != 'unknown_location':
                
                print(f"      Merge: Feed senza loc. ('{current_event_data.get('summary')}') con Manuale con loc. ('{existing_event_data.get('summary')}'). Dettagli manuali mantenuti.")
                # Aggiorna orari dell'evento manuale esistente se quelli del feed sono "migliori"
                # (Questa logica di "migliore" potrebbe essere solo il timestamp o se uno è più preciso)
                # Per ora, semplice sovrascrittura se diversi per semplicità
                dt_end_curr_obj = make_timezone_aware(parse_datetime_str(current_event_data.get('dtend_str')))
                dt_end_existing_obj = make_timezone_aware(parse_datetime_str(existing_event_data.get('dtend_str')))

                if dt_start_curr_obj < make_timezone_aware(parse_datetime_str(existing_event_data.get('dtstart_str'))):
                    existing_event_data['dtstart_str'] = current_event_data['dtstart_str']
                if dt_end_curr_obj and dt_end_existing_obj:
                    if dt_end_curr_obj > dt_end_existing_obj:
                        existing_event_data['dtend_str'] = current_event_data['dtend_str']
                elif dt_end_curr_obj and not dt_end_existing_obj:
                    existing_event_data['dtend_str'] = current_event_data['dtend_str']
                # Non toccare description, url mappa, ecc. del manuale.
                processed_events_by_strong_signature[existing_event_strong_sig] = existing_event_data
                continue # L'evento corrente dal feed è stato "assorbito"

            # CASO 2: Altre corrispondenze deboli -> Procedi con il confronto della firma forte e merge standard
            if strong_sig_curr == existing_event_strong_sig: # Stessa firma forte, sono lo stesso evento
                print(f"      Merge: Stessa firma forte per '{current_event_data.get('summary')}'. Applico merge standard.")
                # Logica di merge standard (come l'avevamo prima)
                dt_end_curr_obj = make_timezone_aware(parse_datetime_str(current_event_data.get('dtend_str')))
                dt_start_existing_obj = make_timezone_aware(parse_datetime_str(existing_event_data.get('dtstart_str')))
                dt_end_existing_obj = make_timezone_aware(parse_datetime_str(existing_event_data.get('dtend_str')))

                if dt_start_curr_obj < dt_start_existing_obj:
                    existing_event_data['dtstart_str'] = current_event_data['dtstart_str']
                if dt_end_curr_obj and dt_end_existing_obj:
                    if dt_end_curr_obj > dt_end_existing_obj:
                        existing_event_data['dtend_str'] = current_event_data['dtend_str']
                elif dt_end_curr_obj and not dt_end_existing_obj:
                    existing_event_data['dtend_str'] = current_event_data['dtend_str']
                
                ex_desc = str(existing_event_data.get('description', '')).strip()
                new_desc = str(current_event_data.get('description', '')).strip()
                if new_desc and new_desc.lower() != ex_desc.lower(): # Unisci solo se diverse
                    merged_desc = f"{ex_desc}\n---\n{new_desc}" if ex_desc else new_desc
                    existing_event_data['description'] = merged_desc
                
                # Arricchisci con URL mappa e indirizzo se il corrente è manuale e l'esistente no
                if current_event_data.get('source_type') != 'ics_feed' and current_event_data.get('google_maps_url_str'):
                    if not existing_event_data.get('google_maps_url_str'):
                        existing_event_data['google_maps_url_str'] = current_event_data.get('google_maps_url_str')
                        existing_event_data['location_address'] = current_event_data.get('location_address', existing_event_data.get('location_address',''))
                
                processed_events_by_strong_signature[strong_sig_curr] = existing_event_data
                continue # Evento mergiato
            else:
                # Firme deboli uguali, ma forti diverse (es. stessa partita, date diverse ma location normalizzate diverse)
                # Li trattiamo come eventi distinti per ora, aggiungendo quello nuovo.
                # Potrebbe essere un'area di ulteriore affinamento se questo produce duplicati indesiderati.
                print(f"    INFO: Firme deboli uguali ma forti diverse. Aggiungo '{current_event_data.get('summary')}' come nuovo.")
                processed_events_by_strong_signature[strong_sig_curr] = current_event_data.copy()
                weak_to_strong_map[weak_sig_curr] = strong_sig_curr # Aggiorna la mappa debole alla firma forte del *nuovo* evento se lo consideriamo "dominante" o più recente. O NON aggiornare, se vogliamo che il primo match debole sia il "master". Da decidere. Per ora, sovrascrivo.
        else:
            # Nessuna corrispondenza debole, è un evento nuovo per summary+data
            processed_events_by_strong_signature[strong_sig_curr] = current_event_data.copy()
            weak_to_strong_map[weak_sig_curr] = strong_sig_curr
            # print(f"  Aggiunto nuovo evento (firma debole unica): {current_event_data.get('summary')}")
    
    final_list = list(processed_events_by_strong_signature.values())
    print(f"  De-duplicazione completata. Eventi unici/mergiati: {len(final_list)}")
    return final_list

def create_calendar_from_event_dicts(event_dictionaries, calendar_display_name):
    final_calendar = Calendar()
    final_calendar.add('prodid', f'-//Generated Calendar ({TARGET_TIMEZONE_STR})//calendari.danielecarletti//')
    final_calendar.add('version', '2.0')
    final_calendar.add('X-WR-CALNAME', calendar_display_name)
    final_calendar.add('X-WR-TIMEZONE', TARGET_TIMEZONE_STR)
    # Ordino gli eventi per dtstart per garantire output deterministico (stesso ordine tra run)
    sorted_events = sorted(
        event_dictionaries,
        key=lambda e: (e.get('dtstart_str') or '', e.get('summary') or '')
    )
    for event_dict in sorted_events:
        ics_event = Event()
        dtstart = make_timezone_aware(parse_datetime_str(event_dict.get('dtstart_str')))
        dtend = make_timezone_aware(parse_datetime_str(event_dict.get('dtend_str')))
        if not dtstart: continue
        summary_val = event_dict.get('summary', 'Evento Senza Titolo')
        loc_name = event_dict.get('location_name', '')
        loc_addr = event_dict.get('location_address', '')
        location_string = f"{loc_name} - {loc_addr}".strip().strip('-').strip() if loc_name and loc_addr else loc_name or loc_addr
        ics_event.add('summary', summary_val)
        ics_event.add('dtstart', dtstart)
        if dtend: ics_event.add('dtend', dtend)
        # DTSTAMP stabile (derivato da dtstart) → idempotenza dell'ICS, niente diff fittizi tra run
        ics_event.add('dtstamp', stable_dtstamp(dtstart))
        if location_string: ics_event.add('location', location_string)
        if event_dict.get('description'): ics_event.add('description', event_dict.get('description'))
        if event_dict.get('google_maps_url_str'): ics_event.add('url', event_dict.get('google_maps_url_str'))
        # UID deterministico: hash sha256(summary+dtstart+location) → stabile tra run
        ics_event.add('uid', stable_uid(summary_val, event_dict.get('dtstart_str'), loc_name))
        final_calendar.add_component(ics_event)
    return final_calendar


def write_calendar_with_validation(cal_obj, target_path, label):
    """Scrive l'ICS solo se l'output rispetta le soglie minime di sanità.
    Protegge da feed temporaneamente vuoto che svuoterebbe il calendario pubblico."""
    new_count = sum(1 for _ in cal_obj.walk('VEVENT'))
    old_count = count_events_in_ics_file(target_path)
    if new_count < MIN_AGGREGATED_EVENTS:
        log(f"ERRORE [{label}]: nuovo conteggio eventi {new_count} < soglia minima {MIN_AGGREGATED_EVENTS}. NON sovrascrivo {target_path}.")
        return False
    if old_count > 0 and new_count < old_count * SHRINK_TOLERANCE:
        log(f"ERRORE [{label}]: nuovo conteggio {new_count} < {SHRINK_TOLERANCE*100:.0f}% del precedente ({old_count}). NON sovrascrivo {target_path}.")
        return False
    try:
        with open(target_path, 'wb') as f:
            f.write(cal_obj.to_ical())
        log(f"  OK [{label}] salvato in {target_path} ({new_count} eventi, prima {old_count}).")
        return True
    except Exception as e:
        log(f"ERRORE [{label}] in scrittura {target_path}: {e}")
        return False

# --- Script Principale ---
def main():
    script_dir = Path(__file__).resolve().parent
    data_source_dir = script_dir / DATA_SOURCE_FOLDER_NAME
    output_dir = script_dir / OUTPUT_ICS_FOLDER_NAME
    if not data_source_dir.is_dir():
        log(f"ERRORE FATALE: La cartella dei dati sorgente '{data_source_dir}' non è stata trovata.")
        sys.exit(1)
    output_dir.mkdir(parents=True, exist_ok=True)
    all_events_for_aggregation = []

    log(f"--- Fase 1: Processamento Dati Grezzi Mensili da '{data_source_dir}' ---")
    monthly_files = sorted(data_source_dir.glob("eventi_*.py"))
    if not monthly_files:
        log(f"  WARN: nessun file mensile in {data_source_dir}. Continuo solo con i feed.")
    for data_file_path in monthly_files:
        log(f"  Processando file dati: {data_file_path.name}")
        raw_events_monthly = load_event_list_from_file(data_file_path)
        if not raw_events_monthly:
            log(f"    Nessun evento caricato da {data_file_path.name} (file vuoto o malformato). Skip.")
            continue

        processed_monthly_event_dicts = apply_deduplication_and_merge(raw_events_monthly)
        all_events_for_aggregation.extend(processed_monthly_event_dicts)

        output_file_basename = data_file_path.stem
        calendar_name_part = output_file_basename.replace("eventi_", "")
        display_name_monthly = f'Eventi San Siro - {calendar_name_part}'
        monthly_calendar_obj = create_calendar_from_event_dicts(processed_monthly_event_dicts, display_name_monthly)

        if len(monthly_calendar_obj.walk('VEVENT')) > 0:
            output_ics_file_path = output_dir / f"{output_file_basename}.ics"
            try:
                with open(output_ics_file_path, 'wb') as f: f.write(monthly_calendar_obj.to_ical())
                log(f"    Calendario mensile salvato in: {output_ics_file_path}")
            except Exception as e:
                log(f"    ERRORE nello scrivere il file ICS mensile {output_ics_file_path}: {e}")

    log(f"--- Fase 2: Processamento Calendari Partite da URL ---")
    giorni_passato_da_includere = 7
    data_riferimento_feed = datetime.now(TARGET_TIMEZONE_OBJ) - timedelta(days=giorni_passato_da_includere)

    feed_failures = 0
    for team_key, url in CALENDAR_URLS.items():
        club_name_for_feed = "AC Milan" if team_key.lower() == "milan" else team_key.capitalize()
        if team_key.lower() == "inter": club_name_for_feed = "Inter"

        log(f"  Scaricando calendario per: {club_name_for_feed} da {url}")
        calendar_data_from_url = get_calendar_from_url(url)
        if not calendar_data_from_url:
            feed_failures += 1
            continue

        team_events_added_count = 0
        for component in calendar_data_from_url.walk('VEVENT'):
            summary_text = str(component.get('summary', ''))
            location_text = str(component.get('location', ''))
            dtstart_prop = component.get('dtstart')
            if not dtstart_prop: continue
            
            dtstart_event_obj_orig = dtstart_prop.dt
            dtstart_event_obj_aware = make_timezone_aware(dtstart_event_obj_orig, TARGET_TIMEZONE_OBJ)
            if not dtstart_event_obj_aware: continue

            # Filtro Data per i feed
            # Se usi il filtro stagione per test:
            # if not (data_riferimento_feed_inizio_stagione <= dtstart_event_obj_aware <= data_riferimento_feed_fine_stagione):
            #     continue
            # Filtro per produzione (eventi futuri o recenti):
            if dtstart_event_obj_aware < data_riferimento_feed: # Usa questo per la produzione
                 continue

            is_home_match_candidate = False
            summary_parts = [p.strip() for p in re.split(r'\s+vs\s+|\s+-\s+', summary_text, 1)]
            if len(summary_parts) > 0 and club_name_for_feed.lower() in summary_parts[0].lower():
                remaining_summary_part = re.sub(re.escape(club_name_for_feed), '', summary_parts[0], flags=re.IGNORECASE).strip()
                if not re.search(r'[a-zA-Z0-9]', remaining_summary_part):
                    is_home_match_candidate = True

            # Detection casa: AND rigido. Richiediamo SIA che il club sia primo nel summary
            # SIA che la location sia presente e normalizzata in LOCATION_ALIASES.
            # Senza il vincolo sulla location, un cambio di formato del feed (es. ordine
            # invertito di "home vs away") farebbe passare silenziosamente trasferte.
            is_truly_relevant_match = False
            if is_home_match_candidate and location_text:
                normalized_feed_location = normalize_location_for_signature(location_text)
                if normalized_feed_location in LOCATION_ALIASES:
                    is_truly_relevant_match = True
            
            if is_truly_relevant_match:
                event_dict = ical_event_component_to_dict(component)
                if event_dict.get('dtstart_str'):
                    all_events_for_aggregation.append(event_dict)
                    team_events_added_count += 1
        log(f"    Aggiunti {team_events_added_count} eventi da {club_name_for_feed} dopo filtro casa/location e data.")

    # Safety net: se TUTTI i feed sono falliti, non sovrascrivere l'aggregato (rischio calendario vuoto)
    if feed_failures == len(CALENDAR_URLS):
        log(f"ERRORE FATALE: tutti i {feed_failures} feed sono falliti. ABORT senza scrivere l'aggregato.")
        sys.exit(1)

    log(f"--- Fase 3: Creazione Calendario Aggregato Finale ---")
    log(f"  Eventi totali prima della de-duplicazione: {len(all_events_for_aggregation)}")
    final_unique_event_dicts = apply_deduplication_and_merge(all_events_for_aggregation)
    aggregated_calendar_obj = create_calendar_from_event_dicts(final_unique_event_dicts, 'Eventi San Siro (Aggregato)')

    aggregated_ics_file_path = output_dir / AGGREGATED_ICS_FILENAME
    ok_agg = write_calendar_with_validation(aggregated_calendar_obj, aggregated_ics_file_path, 'aggregato')
    if not ok_agg:
        log("ABORT: validazione aggregato fallita.")
        sys.exit(1)

    # Mantengo per compatibilità l'URL pubblico storico /eventi_san_siro_merged.ics
    # come copia esatta dell'aggregato (chi era già iscritto via webcal continua a funzionare).
    root_merged_path = script_dir / "eventi_san_siro_merged.ics"
    try:
        shutil.copyfile(aggregated_ics_file_path, root_merged_path)
        log(f"  OK [compat] copia in {root_merged_path}")
    except Exception as e:
        log(f"  WARN: impossibile aggiornare {root_merged_path}: {e}")

    # --- Fase 4: Calendario Lampugnano (sottoinsieme filtrato per Ippodromo SNAI La Maura) ---
    log("--- Fase 4: Generazione calendario Lampugnano (Ippodromo SNAI La Maura) ---")
    lampugnano_events = [
        e for e in final_unique_event_dicts
        if normalize_location_for_signature(e.get('location_name', '')) == LAMPUGNANO_CANONICAL_LOCATION
    ]
    lampugnano_path = output_dir / "eventi_lampugnano.ics"
    if lampugnano_events:
        lamp_cal = create_calendar_from_event_dicts(lampugnano_events, 'Eventi Ippodromo La Maura (Lampugnano)')
        # Validazione più permissiva per Lampugnano: pochi eventi nominali; non applico la soglia min globale
        try:
            with open(lampugnano_path, 'wb') as f:
                f.write(lamp_cal.to_ical())
            log(f"  OK [lampugnano] salvato in {lampugnano_path} ({len(lampugnano_events)} eventi).")
            # Copia in root per compat con URL pubblico storico
            shutil.copyfile(lampugnano_path, script_dir / "eventi_lampugnano.ics")
            log(f"  OK [compat] copia in {script_dir / 'eventi_lampugnano.ics'}")
        except Exception as e:
            log(f"  ERRORE in scrittura Lampugnano: {e}")
    else:
        log("  Nessun evento Lampugnano per questo run; non sovrascrivo l'output esistente.")

    log("Processamento completato.")


if __name__ == "__main__":
    main()
