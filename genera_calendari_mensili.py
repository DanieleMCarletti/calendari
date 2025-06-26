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

# --- Funzioni di Download e Parsing URL ---
def get_calendar_from_url(url):
    try:
        response = requests.get(url, timeout=20)
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
    for data_file_path in sorted(data_source_dir.glob("*.py")):
        print(f"  Processando file dati: {data_file_path.name}")
        raw_events_monthly = load_event_list_from_file(data_file_path)
        if not raw_events_monthly:
            print(f"    Nessun evento caricato da {data_file_path.name}.\n")
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
                print(f"    Calendario mensile salvato in: {output_ics_file_path}")
            except Exception as e:
                print(f"    Errore nello scrivere il file ICS mensile {output_ics_file_path}: {e}")
        print("")

    print(f"\n--- Fase 2: Processamento Calendari Partite da URL ---")
    giorni_passato_da_includere = 7 
    data_riferimento_feed = datetime.now(TARGET_TIMEZONE_OBJ) - timedelta(days=giorni_passato_da_includere)
    # Per testare con una stagione specifica, decommenta e adatta le righe seguenti e commenta quelle sopra:
    # test_stagione_inizio_anno = 2023
    # test_stagione_fine_anno_per_feed = 2024 # L'anno in cui finisce la stagione
    # data_riferimento_feed_inizio_stagione = datetime(test_stagione_inizio_anno, 7, 1, tzinfo=TARGET_TIMEZONE_OBJ) 
    # data_riferimento_feed_fine_stagione = datetime(test_stagione_fine_anno_per_feed, 6, 30, tzinfo=TARGET_TIMEZONE_OBJ)
    # print(f"    Considerando partite dai feed ICS per la stagione {test_stagione_inizio_anno}/{test_stagione_fine_anno_per_feed}")


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

            is_truly_relevant_match = False
            if is_home_match_candidate:
                if location_text:
                    normalized_feed_location = normalize_location_for_signature(location_text)
                    if normalized_feed_location in LOCATION_ALIASES:
                        is_truly_relevant_match = True
                    else:
                        # print(f"    INFO: Evento feed '{summary_text}' scartato, location '{location_text}' non è San Siro/La Maura.")
                        pass # Non aggiungere, è una trasferta con location specificata
                else: # Location non fornita, ci fidiamo del summary per "in casa"
                    is_truly_relevant_match = True
            
            if is_truly_relevant_match:
                event_dict = ical_event_component_to_dict(component)
                if event_dict.get('dtstart_str'):
                    all_events_for_aggregation.append(event_dict)
                    team_events_added_count += 1
        print(f"    Aggiunti {team_events_added_count} eventi da {club_name_for_feed} dopo filtro casa/location e data.")
    
    print(f"\n--- Fase 3: Creazione Calendario Aggregato Finale ---")
    print(f"  Numero totale di eventi prima della de-duplicazione finale: {len(all_events_for_aggregation)}")
    final_unique_event_dicts = apply_deduplication_and_merge(all_events_for_aggregation)
    display_name_aggregated = f'Eventi San Siro (Aggregato)'
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
