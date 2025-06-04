# genera_calendari_mensili.py

from icalendar import Calendar, Event, vDDDTypes
from datetime import datetime, date, timedelta
import pytz
import os
from pathlib import Path
import uuid
import re
import importlib.util # Per importare dinamicamente i file di dati
import requests # Per scaricare i calendari URL

# --- Configurazione Globale ---
TARGET_TIMEZONE_STR = 'Europe/Rome'
TARGET_TIMEZONE_OBJ = pytz.timezone(TARGET_TIMEZONE_STR)

DATA_SOURCE_FOLDER_NAME = "dati_grezzi"
OUTPUT_ICS_FOLDER_NAME = "calendari_output"
CURRENT_YEAR = datetime.now().year # Definito per filtri data o nomi visualizzati
AGGREGATED_ICS_FILENAME = "eventi_san_siro_aggregato.ics" # Nome file senza anno

CALENDAR_URLS = {
    "inter": "https://ics.fixtur.es/v2/inter.ics",
    "milan": "https://ics.fixtur.es/v2/ac-milan.ics",
}

STADIO_SAN_SIRO_KEYWORDS = [ # Usato come fallback se il summary non è chiaro
    "san siro", "giuseppe meazza"
]

LOCATION_ALIASES = {
    "ippodromo snai la maura": ["ippodromo la maura", "la maura", "via lampugnano 95"],
    "ippodromo snai san siro": ["ippodromo san siro", "piazzale dello sport 16", "piazzale dello sport"],
    "stadio san siro": ["stadio giuseppe meazza", "san siro", "piazzale angelo moratti"]
}


# --- Funzioni di Download e Parsing URL ---
def get_calendar_from_url(url):
    try:
        response = requests.get(url, timeout=20) # Aumentato timeout
        response.raise_for_status()
        return Calendar.from_ical(response.text)
    except requests.exceptions.Timeout:
        print(f"Timeout durante il download del calendario da {url}")
    except requests.exceptions.RequestException as e:
        print(f"Errore nel scaricare il calendario da {url}: {e}")
    except Exception as e:
        print(f"Errore nel parsare il calendario da {url}: {e}")
    return None

def is_location_relevant_for_feed(location_text):
    """Verifica se la location da un feed ICS è una di quelle di nostro interesse (San Siro/La Maura)."""
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
    # Rimuovi qualificatori di competizione comuni dai summary per il confronto
    s = re.sub(r'\s*\[(cl|el|cop|serie a|campionato)\]\s*$', '', s, flags=re.IGNORECASE) # Rimuove [CL], [EL], [COP] etc. alla fine
    s = re.sub(r'\b(live|world tour|concerto|evento|show|i-days milano|stadi \d{4}|partita)\b', '', s, flags=re.IGNORECASE)
    s = re.sub(r'\(data \d+(?: - ipotizzata)?\)', '', s, flags=re.IGNORECASE)
    s = re.sub(r'[^\w\s-]', '', s) # Rimuove la maggior parte della punteggiatura
    s = re.sub(r'\s+', ' ', s).strip()
    
    # Normalizza "SquadraA - SquadraB (punteggio)" in "SquadraA - SquadraB"
    s = re.sub(r'\s*\(\d+-\d+\)\s*$', '', s).strip()

    # Normalizza "SquadraA vs SquadraB" in ordine alfabetico per la firma
    # per rendere "A vs B" e "B vs A" la stessa firma se si riferiscono alla stessa partita
    # Nota: Questo potrebbe non essere sempre desiderabile se A vs B è diverso da B vs A per la location
    # Ma lo applichiamo solo al summary normalizzato per la firma.
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
    return loc_lower

def create_event_signature(event_data_dict):
    norm_summary = normalize_summary_for_signature(event_data_dict.get('summary'))
    dt_start_obj_naive = parse_datetime_str(event_data_dict.get('dtstart_str'))
    event_date_str = dt_start_obj_naive.date().isoformat() if dt_start_obj_naive else ""
    # Per la firma, usiamo una location normalizzata, se la location originale è San Siro/La Maura.
    # Se la location non è San Siro/La Maura (es. trasferta), usiamo quella specifica per la firma.
    raw_location = event_data_dict.get('location_name', event_data_dict.get('location', ''))
    norm_loc_specific = normalize_location_for_signature(raw_location)
    
    # Se la location normalizzata è una delle nostre principali, usiamo quella per la firma.
    # Altrimenti, usiamo la location normalizzata specifica (per distinguere le trasferte).
    if norm_loc_specific in LOCATION_ALIASES:
        signature_location = norm_loc_specific
    else: 
        # Per le trasferte o location non mappate, usiamo la stringa normalizzata originale
        # per evitare che tutte le trasferte abbiano la stessa firma di location vuota.
        signature_location = norm_loc_specific if norm_loc_specific else "unknown_location"

    return (norm_summary, event_date_str, signature_location)


def load_event_list_from_file(file_path):
    module_name = file_path.stem
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if not spec or not spec.loader: return None
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
        return getattr(module, 'event_list', None) if hasattr(module, 'event_list') and isinstance(module.event_list, list) else None
    except Exception as e:
        print(f"Errore durante l'importazione di {file_path}: {e}")
    return None

def ical_event_component_to_dict(component):
    event_dict = {'source_type': 'ics_feed'}
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
    processed_events_by_signature = {}
    for raw_event_data in event_list_of_dicts:
        signature = create_event_signature(raw_event_data)
        dt_start_new_str = raw_event_data.get('dtstart_str')
        dt_end_new_str = raw_event_data.get('dtend_str')
        dt_start_new_obj = make_timezone_aware(parse_datetime_str(dt_start_new_str))
        if not dt_start_new_obj: continue

        if signature in processed_events_by_signature:
            existing_event_data = processed_events_by_signature[signature]
            dt_start_existing_obj = make_timezone_aware(parse_datetime_str(existing_event_data.get('dtstart_str')))
            dt_end_existing_obj = make_timezone_aware(parse_datetime_str(existing_event_data.get('dtend_str')))
            dt_end_new_obj = make_timezone_aware(parse_datetime_str(dt_end_new_str))

            if dt_start_new_obj < dt_start_existing_obj:
                existing_event_data['dtstart_str'] = dt_start_new_str
            if dt_end_new_obj and dt_end_existing_obj:
                if dt_end_new_obj > dt_end_existing_obj:
                    existing_event_data['dtend_str'] = dt_end_new_str
            elif dt_end_new_obj and not dt_end_existing_obj:
                existing_event_data['dtend_str'] = dt_end_new_str
            
            ex_desc = str(existing_event_data.get('description', '')).strip()
            new_desc = str(raw_event_data.get('description', '')).strip()
            if new_desc and new_desc.lower() != ex_desc.lower():
                merged_desc = f"{ex_desc}\n---\n{new_desc}" if ex_desc else new_desc
                existing_event_data['description'] = merged_desc
            
            if raw_event_data.get('source_type') != 'ics_feed' and raw_event_data.get('google_maps_url_str'):
                if not existing_event_data.get('google_maps_url_str'):
                    existing_event_data['google_maps_url_str'] = raw_event_data.get('google_maps_url_str')
                    existing_event_data['location_address'] = raw_event_data.get('location_address', existing_event_data.get('location_address',''))
            processed_events_by_signature[signature] = existing_event_data
        else:
            processed_events_by_signature[signature] = raw_event_data.copy()
    
    final_list = list(processed_events_by_signature.values())
    print(f"  De-duplicazione completata. Eventi unici/mergiati: {len(final_list)}")
    return final_list

def create_calendar_from_event_dicts(event_dictionaries, calendar_display_name):
    final_calendar = Calendar()
    final_calendar.add('prodid', f'-//Generated Calendar ({TARGET_TIMEZONE_STR})//example.com//')
    final_calendar.add('version', '2.0')
    final_calendar.add('X-WR-CALNAME', calendar_display_name)
    final_calendar.add('X-WR-TIMEZONE', TARGET_TIMEZONE_STR)
    for event_dict in event_dictionaries:
        ics_event = Event()
        dtstart = make_timezone_aware(parse_datetime_str(event_dict.get('dtstart_str')))
        dtend = make_timezone_aware(parse_datetime_str(event_dict.get('dtend_str')))
        if not dtstart: continue
        ics_event.add('summary', event_dict.get('summary', 'Evento Senza Titolo'))
        ics_event.add('dtstart', dtstart)
        if dtend: ics_event.add('dtend', dtend)
        ics_event.add('dtstamp', make_timezone_aware(datetime.now()))
        loc_name = event_dict.get('location_name', '')
        loc_addr = event_dict.get('location_address', '')
        location_string = f"{loc_name} - {loc_addr}".strip().strip('-').strip() if loc_name and loc_addr else loc_name or loc_addr
        if location_string: ics_event.add('location', location_string)
        if event_dict.get('description'): ics_event.add('description', event_dict.get('description'))
        if event_dict.get('google_maps_url_str'): ics_event.add('url', event_dict.get('google_maps_url_str'))
        ics_event.add('uid', str(uuid.uuid4()))
        final_calendar.add_component(ics_event)
    return final_calendar

# --- Script Principale ---
def main():
    script_dir = Path(__file__).resolve().parent
    data_source_dir = script_dir / DATA_SOURCE_FOLDER_NAME
    output_dir = script_dir / OUTPUT_ICS_FOLDER_NAME
    if not data_source_dir.is_dir():
        print(f"Errore: La cartella dei dati sorgente '{data_source_dir}' non è stata trovata.")
        return
    output_dir.mkdir(parents=True, exist_ok=True)
    all_events_for_aggregation = []

    print(f"\n--- Fase 1: Processamento Dati Grezzi Mensili da '{data_source_dir}' ---")
    for data_file_path in sorted(data_source_dir.glob("*.py")): # Processa in ordine alfabetico
        print(f"  Processando file dati: {data_file_path.name}")
        raw_events_monthly = load_event_list_from_file(data_file_path)
        if not raw_events_monthly:
            print(f"    Nessun evento caricato da {data_file_path.name}.\n")
            continue
        
        # Applica un primo filtro opzionale solo sui dati grezzi (es. per escludere eventi non a San Siro/La Maura dai file grezzi)
        # filtered_raw_monthly = [e for e in raw_events_monthly if is_location_relevant_for_feed(e.get('location_name', ''))] # Esempio
        # print(f"    Eventi dopo filtro location su dati grezzi: {len(filtered_raw_monthly)}")
        # processed_monthly_event_dicts = apply_deduplication_and_merge(filtered_raw_monthly)
        
        processed_monthly_event_dicts = apply_deduplication_and_merge(raw_events_monthly) # Usa tutti i dati grezzi
        all_events_for_aggregation.extend(processed_monthly_event_dicts)

        output_file_basename = data_file_path.stem
        calendar_name_part = output_file_basename.replace("eventi_", "")
        display_name_monthly = f'Eventi San Siro - {calendar_name_part}'
        monthly_calendar_obj = create_calendar_from_event_dicts(processed_monthly_event_dicts, display_name_monthly)
        
        if len(monthly_calendar_obj.walk('VEVENT')) > 0:
            output_ics_file_path = output_dir / f"{output_file_basename}.ics"
            try:
                with open(output_ics_file_path, 'wb') as f: f.write(monthly_calendar_obj.to_ical())
                print(f"    Calendario mensile salvato in: {output_ics_file_path}")
            except Exception as e:
                print(f"    Errore nello scrivere il file ICS mensile {output_ics_file_path}: {e}")
        print("")

    print(f"\n--- Fase 2: Processamento Calendari Partite da URL ---")
    # Filtro Data per testare con partite passate (ESEMPIO: stagione 2023/2024)
    # Se vuoi tutte le partite future, commenta o modifica questo filtro.
    # Per testare con dati storici e vedere se il filtro "in casa" funziona.
    # considera_eventi_da_anno = CURRENT_YEAR - 2 # Inizia a considerare eventi da 2 anni prima
    # considera_eventi_da_mese = 7 # da Luglio/Agosto (inizio stagione calcistica tipica)
    # data_limite_per_feed = datetime(considera_eventi_da_anno, considera_eventi_da_mese, 1)
    # print(f"    Considerando partite dai feed ICS a partire da: {data_limite_per_feed.date()}")


    for team_key, url in CALENDAR_URLS.items():
        club_name_for_feed = "AC Milan" if team_key.lower() == "milan" else team_key.capitalize()
        if team_key.lower() == "inter": club_name_for_feed = "Inter"
        
        print(f"  Scaricando calendario per: {club_name_for_feed} da {url}")
        calendar_data_from_url = get_calendar_from_url(url)
        if not calendar_data_from_url: continue

        team_events_added_count = 0
        for component in calendar_data_from_url.walk('VEVENT'):
            summary_text = str(component.get('summary', ''))
            location_text = str(component.get('location', ''))
            
            dtstart_prop = component.get('dtstart')
            if not dtstart_prop: continue
            dtstart_obj_orig_naive = dtstart_prop.dt # Questo è naive se UTC (Z), o già aware
            if isinstance(dtstart_obj_orig_naive, datetime) and dtstart_obj_orig_naive.tzinfo is not None: # Se è già aware (es. da feed con fusi specifici)
                dtstart_obj_utc_equiv = dtstart_obj_orig_naive.astimezone(pytz.utc)
            elif isinstance(dtstart_obj_orig_naive, datetime): # Se è naive (assumiamo sia UTC come da 'Z')
                dtstart_obj_utc_equiv = pytz.utc.localize(dtstart_obj_orig_naive)
            elif isinstance(dtstart_obj_orig_naive, date): # Se è solo una data
                 dtstart_obj_utc_equiv = pytz.utc.localize(datetime.combine(dtstart_obj_orig_naive, datetime.min.time()))
            else:
                continue # Non possiamo processare

            # --- INIZIO BLOCCO FILTRO DATA PER TEST ---
            # SCOMMENTA E ADATTA QUESTO BLOCCO SE VUOI TESTARE CON PARTITE PASSATE SPECIFICHE
            # Ad esempio, per la stagione 2023/2024:
            test_stagione_inizio_anno = 2023
            test_stagione_fine_anno = 2024
            if not ( (dtstart_obj_utc_equiv.year == test_stagione_inizio_anno and dtstart_obj_utc_equiv.month >= 7) or \
                     (dtstart_obj_utc_equiv.year == test_stagione_fine_anno and dtstart_obj_utc_equiv.month <= 6) ):
                # print(f"      Partita feed fuori stagione di test: {summary_text} ({dtstart_obj_utc_equiv.date()})")
                continue
            # --- FINE BLOCCO FILTRO DATA PER TEST ---


            is_home_match = False
            # Prima controlla se il nome del club è all'inizio del summary
            # e la location (se presente) è una delle nostre
            summary_parts = [p.strip() for p in re.split(r'\s+vs\s+|\s+-\s+', summary_text, 1)] # Split su 'vs' o '-' una volta
            
            if len(summary_parts) > 0 and club_name_for_feed.lower() in summary_parts[0].lower():
                # Rimuovi il nome del club e controlla se rimane vuoto o solo punteggiatura/spazi
                # Questo aiuta a distinguere "AC Milan vs Juventus" da "AC Milan Primavera vs Juventus Primavera"
                remaining_summary_part = re.sub(re.escape(club_name_for_feed), '', summary_parts[0], flags=re.IGNORECASE).strip()
                if not re.search(r'[a-zA-Z0-9]', remaining_summary_part): # Se non ci sono più lettere/numeri
                    is_home_match = True
                    # print(f"DEBUG: Partita in casa per summary: {summary_text}")


            # Se non identificata come in casa dal summary, ma la location è una delle nostre, considerala rilevante
            # (potrebbe essere un evento generico a San Siro, non una partita di quel club, ma va bene per ora)
            if not is_home_match and location_text and is_location_relevant_for_feed(location_text):
                is_home_match = True # La consideriamo "rilevante" per la location
                # print(f"DEBUG: Evento rilevante per location: {summary_text} @ {location_text}")


            if is_home_match:
                event_dict = ical_event_component_to_dict(component)
                if event_dict.get('dtstart_str'):
                    all_events_for_aggregation.append(event_dict)
                    team_events_added_count += 1
        print(f"    Aggiunti {team_events_added_count} eventi da {club_name_for_feed} dopo filtro casa/location.")
    
    print(f"\n--- Fase 3: Creazione Calendario Aggregato Finale ---")
    print(f"  Numero totale di eventi prima della de-duplicazione finale: {len(all_events_for_aggregation)}")
    final_unique_event_dicts = apply_deduplication_and_merge(all_events_for_aggregation)
    display_name_aggregated = f'Eventi San Siro (Aggregato)' # Nome senza anno
    aggregated_calendar_obj = create_calendar_from_event_dicts(final_unique_event_dicts, display_name_aggregated)

    if len(aggregated_calendar_obj.walk('VEVENT')) > 0:
        aggregated_ics_file_path = output_dir / AGGREGATED_ICS_FILENAME
        try:
            with open(aggregated_ics_file_path, 'wb') as f: f.write(aggregated_calendar_obj.to_ical())
            print(f"  Calendario aggregato salvato in: {aggregated_ics_file_path}")
            print(f"  Eventi totali nel calendario aggregato: {len(aggregated_calendar_obj.walk('VEVENT'))}")
        except Exception as e:
            print(f"  Errore nello scrivere il file ICS aggregato {aggregated_ics_file_path}: {e}")
    else:
        print("  Nessun evento da scrivere nel calendario aggregato finale.")
    print("\nProcessamento completato.")

if __name__ == "__main__":
    main()
