# genera_calendari_mensili.py

from icalendar import Calendar, Event, vDDDTypes
from datetime import datetime, date, timedelta
import pytz
import os
from pathlib import Path
import uuid
import re
import importlib.util # Per importare dinamicamente i file di dati

# --- Configurazione Globale ---
TARGET_TIMEZONE_STR = 'Europe/Rome'
TARGET_TIMEZONE_OBJ = pytz.timezone(TARGET_TIMEZONE_STR)

# Nomi delle cartelle (relative alla posizione dello script)
DATA_SOURCE_FOLDER_NAME = "dati_grezzi"
OUTPUT_ICS_FOLDER_NAME = "calendari_output"


LOCATION_ALIASES = {
    "ippodromo snai la maura": ["ippodromo la maura", "la maura", "via lampugnano 95"],
    "ippodromo snai san siro": ["ippodromo san siro", "piazzale dello sport 16", "piazzale dello sport"],
    "stadio san siro": ["stadio giuseppe meazza", "san siro", "piazzale angelo moratti"]
}


# --- Funzioni di Normalizzazione e Utilità ---
def parse_datetime_str(dt_str):
    if not dt_str: return None
    try:
        return datetime.strptime(dt_str, '%Y-%m-%dT%H:%M:%S')
    except ValueError:
        print(f"Attenzione: formato data/ora non valido: {dt_str}")
        return None

def make_timezone_aware(dt_obj):
    if not dt_obj or not isinstance(dt_obj, datetime): return None
    if dt_obj.tzinfo is None or dt_obj.tzinfo.utcoffset(dt_obj) is None:
        return TARGET_TIMEZONE_OBJ.localize(dt_obj)
    return dt_obj.astimezone(TARGET_TIMEZONE_OBJ)

def normalize_summary_for_signature(summary):
    if not summary: return ""
    s = str(summary).lower().strip()
    s = re.sub(r'\b(live|world tour|concerto|evento|show|i-days milano|stadi \d{4})\b', '', s, flags=re.IGNORECASE)
    s = re.sub(r'\(data \d+(?: - ipotizzata)?\)', '', s, flags=re.IGNORECASE)
    s = re.sub(r'[^\w\s-]', '', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def normalize_location_for_signature(location_name):
    if not location_name: return ""
    loc_lower = str(location_name).lower().strip()
    for canonical, aliases in LOCATION_ALIASES.items():
        if canonical in loc_lower: return canonical
        for alias in aliases:
            if alias in loc_lower: return canonical
    loc_lower = re.sub(r'[^\w\s-]', '', loc_lower)
    loc_lower = re.sub(r'\s+', ' ', loc_lower).strip()
    return loc_lower

def create_event_signature(event_data_dict):
    norm_summary = normalize_summary_for_signature(event_data_dict.get('summary'))
    dt_start_obj = parse_datetime_str(event_data_dict.get('dtstart_str'))
    event_date_str = dt_start_obj.date().isoformat() if dt_start_obj else ""
    norm_loc = normalize_location_for_signature(event_data_dict.get('location_name'))
    return (norm_summary, event_date_str, norm_loc)

def load_event_list_from_file(file_path):
    """Carica dinamicamente la variabile 'event_list' da un file .py."""
    module_name = file_path.stem # Nome del file senza estensione
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if not spec or not spec.loader:
        print(f"Errore: Impossibile creare lo spec per {file_path}")
        return None
        
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
        if hasattr(module, 'event_list') and isinstance(module.event_list, list):
            return module.event_list
        else:
            print(f"Errore: 'event_list' non trovata o non è una lista in {file_path}")
            return None
    except Exception as e:
        print(f"Errore durante l'importazione di {file_path}: {e}")
        return None

# --- Funzione Principale di Processamento per una Lista di Eventi ---
def process_raw_event_list(raw_event_list, calendar_name_suffix):
    """Processa una lista di dati grezzi e restituisce un oggetto Calendar."""
    
    processed_events_by_signature = {}
    
    for raw_event_data in raw_event_list:
        signature = create_event_signature(raw_event_data)
        dt_start_new_str = raw_event_data.get('dtstart_str')
        dt_end_new_str = raw_event_data.get('dtend_str')

        dt_start_new_obj = make_timezone_aware(parse_datetime_str(dt_start_new_str))

        if not dt_start_new_obj:
            print(f"  Evento saltato (dtstart mancante o non valido): {raw_event_data.get('summary')}")
            continue

        if signature in processed_events_by_signature:
            # print(f"  Trovata firma duplicata per: {raw_event_data.get('summary')} (Firma: {signature})")
            existing_event_data = processed_events_by_signature[signature]
            
            dt_start_existing_obj = make_timezone_aware(parse_datetime_str(existing_event_data.get('dtstart_str')))
            dt_end_existing_obj = make_timezone_aware(parse_datetime_str(existing_event_data.get('dtend_str')))
            dt_end_new_obj = make_timezone_aware(parse_datetime_str(dt_end_new_str))

            if dt_start_new_obj < dt_start_existing_obj:
                existing_event_data['dtstart_str'] = dt_start_new_str
                # print(f"    Aggiornato DTSTART (più presto) per {existing_event_data.get('summary')}")

            if dt_end_new_obj and dt_end_existing_obj:
                if dt_end_new_obj > dt_end_existing_obj:
                    existing_event_data['dtend_str'] = dt_end_new_str
                    # print(f"    Aggiornato DTEND (più tardi) per {existing_event_data.get('summary')}")
            elif dt_end_new_obj and not dt_end_existing_obj:
                existing_event_data['dtend_str'] = dt_end_new_str
                # print(f"    Aggiunto DTEND da nuovo evento per {existing_event_data.get('summary')}")
            
            ex_desc = str(existing_event_data.get('description', '')).strip()
            new_desc = str(raw_event_data.get('description', '')).strip()
            if new_desc and new_desc.lower() != ex_desc.lower():
                merged_desc = f"{ex_desc}\n---\n{new_desc}" if ex_desc else new_desc
                existing_event_data['description'] = merged_desc
                # print(f"    Unita/Aggiornata DESCRIPTION per {existing_event_data.get('summary')}.")
            
            processed_events_by_signature[signature] = existing_event_data
        else:
            processed_events_by_signature[signature] = raw_event_data.copy()
            # print(f"  Aggiunto nuovo evento (firma unica): {raw_event_data.get('summary')}")

    # Crea l'oggetto Calendar finale
    final_calendar = Calendar()
    final_calendar.add('prodid', f'-//Generated Monthly Calendar ({TARGET_TIMEZONE_STR})//example.com//')
    final_calendar.add('version', '2.0')
    final_calendar.add('X-WR-CALNAME', f'Eventi San Siro - {calendar_name_suffix}')
    final_calendar.add('X-WR-TIMEZONE', TARGET_TIMEZONE_STR)

    for event_dict in processed_events_by_signature.values():
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
        location_string = f"{loc_name} - {loc_addr}" if loc_name and loc_addr else loc_name or loc_addr
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

    output_dir.mkdir(parents=True, exist_ok=True) # Crea la cartella di output se non esiste
    print(f"Cerco file di dati in: {data_source_dir}")
    print(f"I file ICS generati verranno salvati in: {output_dir}\n")

    processed_files_count = 0
    for data_file_path in data_source_dir.glob("*.py"): # Cerca tutti i file .py
        print(f"--- Processando file: {data_file_path.name} ---")
        
        raw_events = load_event_list_from_file(data_file_path)
        if not raw_events:
            print(f"  Nessun evento caricato da {data_file_path.name}. File saltato.\n")
            continue
            
        print(f"  Trovati {len(raw_events)} eventi grezzi in {data_file_path.name}.")

        # Determina il nome del calendario/file di output dal nome del file sorgente
        # Es: eventi_2025_06.py -> Eventi San Siro - 2025_06
        output_file_basename = data_file_path.stem # Es: "eventi_2025_06"
        calendar_name_part = output_file_basename.replace("eventi_", "") # Es: "2025_06"
        
        monthly_calendar = process_raw_event_list(raw_events, calendar_name_part)
        
        if not monthly_calendar or not len(monthly_calendar.walk('VEVENT')):
            print(f"  Nessun evento valido da scrivere per {data_file_path.name}.\n")
            continue

        output_ics_file_path = output_dir / f"{output_file_basename}.ics"
        try:
            with open(output_ics_file_path, 'wb') as f:
                f.write(monthly_calendar.to_ical())
            print(f"  Calendario salvato in: {output_ics_file_path}")
            print(f"  Eventi totali nel calendario '{output_ics_file_path.name}': {len(monthly_calendar.walk('VEVENT'))}\n")
            processed_files_count += 1
        except Exception as e:
            print(f"  Errore nello scrivere il file ICS {output_ics_file_path}: {e}\n")
            
    if processed_files_count == 0:
        print("Nessun file di dati è stato processato con successo.")
    else:
        print(f"Processamento completato. {processed_files_count} file ICS generati.")


if __name__ == "__main__":
    main()
