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
AGGREGATED_ICS_FILENAME = f"eventi_san_siro_aggregato.ics"

CALENDAR_URLS = {
    "inter": "https://www.stanza.news/api/calendar/inter/all.ics",
    "milan": "https://www.stanza.news/api/calendar/milan/all.ics",
}

# Nomi per il filtro is_home_game (da affinare se necessario)
STADIO_SAN_SIRO_KEYWORDS = [
    "san siro", "giuseppe meazza" # Aggiungere altri se servono
]

LOCATION_ALIASES = {
    "ippodromo snai la maura": ["ippodromo la maura", "la maura", "via lampugnano 95"],
    "ippodromo snai san siro": ["ippodromo san siro", "piazzale dello sport 16", "piazzale dello sport"],
    "stadio san siro": ["stadio giuseppe meazza", "san siro", "piazzale angelo moratti"]
}


# --- Funzioni di Download e Parsing URL ---
def get_calendar_from_url(url):
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        return Calendar.from_ical(response.text)
    except requests.exceptions.RequestException as e:
        print(f"Errore nel scaricare il calendario da {url}: {e}")
    except Exception as e:
        print(f"Errore nel parsare il calendario da {url}: {e}")
    return None

def is_relevant_location(location_text):
    """Verifica se la location è una di quelle di nostro interesse."""
    if not location_text: return False
    normalized_loc = normalize_location_for_signature(location_text) # Usa la stessa normalizzazione
    # Verifica se la location normalizzata corrisponde a una delle chiavi canoniche
    # che sappiamo essere a San Siro / La Maura.
    # Questo assume che le chiavi di LOCATION_ALIASES siano le location di interesse.
    return normalized_loc in LOCATION_ALIASES 

# --- Funzioni di Normalizzazione e Utilità (esistenti e nuove) ---
def parse_datetime_str(dt_str):
    if not dt_str: return None
    try:
        return datetime.strptime(dt_str, '%Y-%m-%dT%H:%M:%S')
    except ValueError:
        # print(f"Attenzione: formato data/ora non valido: {dt_str}")
        return None

def make_timezone_aware(dt_obj, timezone_obj=TARGET_TIMEZONE_OBJ):
    if not dt_obj: return None
    if isinstance(dt_obj, date) and not isinstance(dt_obj, datetime): # Se è solo date
        # Converti in datetime all'inizio del giorno per poter localizzare
        dt_obj = datetime.combine(dt_obj, datetime.min.time())
        
    if not isinstance(dt_obj, datetime): return None # Se ancora non è datetime, non possiamo procedere

    if dt_obj.tzinfo is None or dt_obj.tzinfo.utcoffset(dt_obj) is None:
        return timezone_obj.localize(dt_obj)
    return dt_obj.astimezone(timezone_obj)

def normalize_summary_for_signature(summary):
    if not summary: return ""
    s = str(summary).lower().strip()
    s = re.sub(r'\b(live|world tour|concerto|evento|show|i-days milano|stadi \d{4}|partita|campionato|serie a|coppa italia|champions league|europa league)\b', '', s, flags=re.IGNORECASE)
    s = re.sub(r'\(data \d+(?: - ipotizzata)?\)', '', s, flags=re.IGNORECASE)
    s = re.sub(r'[^\w\s-]', '', s)
    s = re.sub(r'\s+', ' ', s).strip()
    # Tentativo di normalizzare Inter vs Milan -> Milan vs Inter
    parts = sorted([p.strip() for p in s.split(" vs ")])
    s = " vs ".join(parts)
    return s

def normalize_location_for_signature(location_name):
    if not location_name: return ""
    loc_lower = str(location_name).lower().strip()
    for canonical, aliases in LOCATION_ALIASES.items():
        if canonical in loc_lower: return canonical
        for alias in aliases:
            if alias in loc_lower: return canonical
    # Normalizzazione base se non trovato alias (potrebbe essere meno efficace)
    loc_lower = re.sub(r'[^\w\s-,]', '', loc_lower) # Mantieni virgole e trattini
    loc_lower = re.sub(r'\s+', ' ', loc_lower).strip()
    return loc_lower

def create_event_signature(event_data_dict):
    norm_summary = normalize_summary_for_signature(event_data_dict.get('summary'))
    dt_start_obj_naive = parse_datetime_str(event_data_dict.get('dtstart_str'))
    event_date_str = dt_start_obj_naive.date().isoformat() if dt_start_obj_naive else ""
    norm_loc = normalize_location_for_signature(event_data_dict.get('location_name', event_data_dict.get('location', ''))) # Considera anche campo 'location'
    return (norm_summary, event_date_str, norm_loc)

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
    """Converte un componente VEVENT da icalendar in un nostro dizionario standard."""
    event_dict = {'source_type': 'ics_feed'} # Per tracciare l'origine
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
    
    event_dict['location_name'] = str(component.get('location', '')) # Mettiamo tutto in location_name
    event_dict['location_address'] = '' # I feed ICS raramente lo separano
    event_dict['description'] = str(component.get('description', ''))
    event_dict['google_maps_url_str'] = '' # Non presente solitamente nei feed generici
    
    # Mantieni UID originale per eventuale logica di merge più avanzata, ma non per la firma base
    event_dict['original_uid_from_feed'] = str(component.get('uid', ''))
    return event_dict

def apply_deduplication_and_merge(event_list_of_dicts):
    """Applica la logica di de-duplicazione e merge a una lista di dizionari evento."""
    print(f"  Inizio de-duplicazione per {len(event_list_of_dicts)} eventi...")
    processed_events_by_signature = {}
    for raw_event_data in event_list_of_dicts:
        signature = create_event_signature(raw_event_data)
        
        dt_start_new_str = raw_event_data.get('dtstart_str')
        dt_end_new_str = raw_event_data.get('dtend_str')

        dt_start_new_obj = make_timezone_aware(parse_datetime_str(dt_start_new_str))
        if not dt_start_new_obj: continue # Salta se dtstart non è valido

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
            
            # Priorità ai dati "manuali" per URL mappa se l'esistente è da feed
            if raw_event_data.get('source_type') != 'ics_feed' and raw_event_data.get('google_maps_url_str'):
                if not existing_event_data.get('google_maps_url_str'):
                    existing_event_data['google_maps_url_str'] = raw_event_data.get('google_maps_url_str')
                    existing_event_data['location_address'] = raw_event_data.get('location_address', existing_event_data.get('location_address',''))


            processed_events_by_signature[signature] = existing_event_data
        else:
            processed_events_by_signature[signature] = raw_event_data.copy()
    
    print(f"  De-duplicazione completata. Eventi unici/mergiati: {len(processed_events_by_signature)}")
    return list(processed_events_by_signature.values())


def create_calendar_from_event_dicts(event_dictionaries, calendar_display_name):
    """Crea un oggetto icalendar.Calendar da una lista di dizionari evento processati."""
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
    
    all_events_for_aggregation = [] # Lista di dizionari evento da tutte le fonti

    # 1. Processa file di dati grezzi mensili
    print(f"\n--- Fase 1: Processamento Dati Grezzi Mensili da '{data_source_dir}' ---")
    for data_file_path in data_source_dir.glob("*.py"):
        print(f"  Processando file dati: {data_file_path.name}")
        raw_events_monthly = load_event_list_from_file(data_file_path)
        if not raw_events_monthly:
            print(f"    Nessun evento caricato da {data_file_path.name}.\n")
            continue
        
        # De-duplica e mergia eventi per questo mese specifico
        processed_monthly_event_dicts = apply_deduplication_and_merge(raw_events_monthly)
        all_events_for_aggregation.extend(processed_monthly_event_dicts) # Aggiungi alla lista aggregata

        # Genera e salva il calendario ICS mensile
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

    # 2. Processa calendari URL (Inter/Milan)
    print(f"\n--- Fase 2: Processamento Calendari Partite da URL ---")
    for team_name, url in CALENDAR_URLS.items():
        print(f"  Scaricando calendario per: {team_name.capitalize()} da {url}")
        calendar_component = get_calendar_from_url(url)
        if not calendar_component: continue

        team_events_count = 0
        for component in calendar_component.walk('VEVENT'):
            location_text = str(component.get('location', ''))
            if is_relevant_location(location_text): # Filtra per partite a San Siro/La Maura
                event_dict = ical_event_component_to_dict(component)
                if event_dict.get('dtstart_str'): # Assicurati che ci sia una data di inizio valida
                    all_events_for_aggregation.append(event_dict)
                    team_events_count += 1
        print(f"    Aggiunti {team_events_count} eventi rilevanti da {team_name.capitalize()}.")
    
    # 3. De-duplicazione finale e creazione del calendario aggregato
    print(f"\n--- Fase 3: Creazione Calendario Aggregato Finale ---")
    print(f"  Numero totale di eventi prima della de-duplicazione finale: {len(all_events_for_aggregation)}")
    final_unique_event_dicts = apply_deduplication_and_merge(all_events_for_aggregation)
    
    display_name_aggregated = f'Eventi San Siro {CURRENT_YEAR} (Aggregato)'
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
