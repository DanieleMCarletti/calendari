# Python script to merge and filter iCalendar feeds

import requests
from icalendar import Calendar, Event
from datetime import datetime, timezone # timezone non è usato direttamente ma è buona pratica importarlo con datetime
import pytz # For timezone handling
import os # Per lavorare con i percorsi dei file
from pathlib import Path # Per una gestione più moderna dei percorsi
import uuid # Per generare UID se mancano

# --- Configurazione ---
CALENDAR_URLS = {
    "inter": "https://www.stanza.news/api/calendar/inter/all.ics",
    "milan": "https://www.stanza.news/api/calendar/milan/all.ics",
    "festivita_usa": "https://www.officeholidays.com/ics-fed/usa" # Calendario di test
}

# Nuova configurazione per i calendari locali
LOCAL_ICS_FOLDER = "calendari_custom" # Nome della sottocartella nella root del progetto

STADIO_SAN_SIRO_NAMES = [
    "san siro",
    "giuseppe meazza",
    "stadio giuseppe meazza",
    "stadio san siro"
]

OUTPUT_ICS_FILE = "eventi_san_siro_merged.ics"
TARGET_TIMEZONE = 'Europe/Rome'

# --- Funzioni ---
def get_calendar_from_url(url):
    """Scarica e parsa un calendario da un URL."""
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()  # Solleva un errore per status HTTP non validi
        calendar = Calendar.from_ical(response.text)
        return calendar
    except requests.exceptions.RequestException as e:
        print(f"Errore nel scaricare il calendario da {url}: {e}")
        return None
    except Exception as e:
        print(f"Errore nel parsare il calendario da {url}: {e}")
        return None

def is_home_game(event):
    """Verifica se un evento è una partita in casa a San Siro."""
    location = event.get('location')
    if not location:
        return False
    
    location_lower = str(location).lower()
    for name in STADIO_SAN_SIRO_NAMES:
        if name in location_lower:
            return True
    return False

def normalize_dt(dt_value):
    """Normalizza un datetime object a un timezone specifico o lo rende timezone-aware."""
    if isinstance(dt_value, datetime):
        # Se il datetime è naive, localizzalo prima a UTC (assumendo che i feed usino UTC o forniscano già info di tz)
        # Poi convertilo al TARGET_TIMEZONE.
        # Se il datetime ha già un timezone, verrà semplicemente convertito.
        if dt_value.tzinfo is None or dt_value.tzinfo.utcoffset(dt_value) is None:
            aware_dt = pytz.utc.localize(dt_value)
        else:
            aware_dt = dt_value
        return aware_dt.astimezone(pytz.timezone(TARGET_TIMEZONE))
    return dt_value # Lascia invariato se non è un datetime (es. date object, che non ha tz)

def get_calendar_from_local_file(file_path):
    """Legge e parsa un calendario da un file .ics locale."""
    try:
        with open(file_path, 'rb') as f: # Apri in modalità binaria 'rb'
            calendar_data = f.read()
        calendar = Calendar.from_ical(calendar_data)
        return calendar
    except FileNotFoundError:
        print(f"Errore: File calendario locale non trovato: {file_path}")
        return None
    except Exception as e:
        print(f"Errore nel parsare il calendario locale {file_path}: {e}")
        return None

# --- Script Principale ---
def main():
    merged_calendar = Calendar()
    merged_calendar.add('prodid', '-//Combined San Siro Events Calendar//example.com//')
    merged_calendar.add('version', '2.0')
    merged_calendar.add('X-WR-CALNAME', 'Eventi San Siro (Filtrati e Test)') # Aggiornato nome per chiarezza
    merged_calendar.add('X-WR-TIMEZONE', TARGET_TIMEZONE)

    all_event_uids = set()

    # 1. Processa calendari da URL
    for calendar_key, url in CALENDAR_URLS.items():
        print(f"Processando calendario da URL per: {calendar_key} da {url}")
        calendar = get_calendar_from_url(url)
        if not calendar:
            continue

        events_added_from_this_feed = 0
        for component in calendar.walk():
            if component.name == "VEVENT":
                event_uid = component.get('uid')
                if event_uid and event_uid in all_event_uids : # Controlla che event_uid non sia None
                    # print(f"  Evento duplicato (da URL) saltato (UID): {component.get('summary')}")
                    continue

                # Logica per decidere se l'evento è rilevante
                event_is_relevant = False
                if calendar_key in ["inter", "milan"]: # Applica filtro San Siro solo a Inter e Milan
                    if is_home_game(component):
                        event_is_relevant = True
                elif calendar_key == "festivita_usa": # Per il calendario di test, includi tutti gli eventi
                    event_is_relevant = True
                # Aggiungi altri 'elif' qui se hai altri URL con logiche di filtro diverse

                if event_is_relevant:
                    new_event = Event()
                    for prop_key, prop_value_encoded in component.items():
                        try:
                            # Gestione della decodifica e dei tipi di dati
                            if isinstance(prop_value_encoded, bytes):
                                value_decoded = prop_value_encoded.decode('utf-8', errors='replace')
                            elif hasattr(prop_value_encoded, 'dt'): # Per oggetti vDDDTypes (date/datetime)
                                value_decoded = prop_value_encoded.dt 
                            else:
                                value_decoded = prop_value_encoded
                            
                            # Normalizzazione delle date/ore
                            if prop_key.upper() in ['DTSTART', 'DTEND', 'DTSTAMP', 'CREATED', 'LAST-MODIFIED', 'RECURRENCE-ID']:
                                new_event.add(prop_key, normalize_dt(value_decoded))
                            else:
                                new_event.add(prop_key, value_decoded)
                        except Exception as e:
                            # print(f"    Attenzione: errore nel processare la proprietà '{prop_key}': {e} - Valore: {prop_value_encoded}")
                            if prop_key.upper() not in ['UID', 'SUMMARY', 'DTSTART']:
                                 new_event.add(prop_key, str(prop_value_encoded)) # Prova ad aggiungerla come stringa
                            else:
                                print(f"    Errore critico con la proprietà '{prop_key}', l'evento potrebbe essere incompleto.")
                    
                    current_event_uid = new_event.get('uid')
                    if not current_event_uid:
                        current_event_uid = str(uuid.uuid4())
                        new_event.add('uid', current_event_uid)
                    
                    # Controllo finale duplicati UID prima di aggiungere
                    if current_event_uid not in all_event_uids:
                        merged_calendar.add_component(new_event)
                        all_event_uids.add(current_event_uid)
                        events_added_from_this_feed += 1
                    # else:
                        # print(f"  Evento duplicato (da URL, post-elaborazione UID) saltato (UID): {new_event.get('summary')}")

        print(f"  Aggiunti {events_added_from_this_feed} eventi da URL {calendar_key}")

    # 2. Processa calendari locali dalla sottocartella 'calendari_custom'
    script_dir = Path(__file__).resolve().parent
    custom_calendars_path = script_dir / LOCAL_ICS_FOLDER

    if custom_calendars_path.is_dir():
        print(f"\nProcessando calendari locali da: {custom_calendars_path}")
        for filename in os.listdir(custom_calendars_path):
            if filename.lower().endswith(".ics"):
                file_path = custom_calendars_path / filename
                print(f"  Processando file locale: {filename}")
                local_calendar = get_calendar_from_local_file(file_path)
                
                if not local_calendar:
                    continue

                events_added_from_this_file = 0
                for component in local_calendar.walk():
                    if component.name == "VEVENT":
                        event_uid = component.get('uid')
                        if event_uid and event_uid in all_event_uids:
                            # print(f"    Evento duplicato (da file locale) saltato (UID): {component.get('summary')}")
                            continue
                        
                        # Per i file locali, assumiamo che gli eventi siano già rilevanti
                        # e non applichiamo il filtro is_home_game(), a meno che tu non voglia.
                        
                        new_event = Event()
                        for prop_key, prop_value_encoded in component.items():
                            try:
                                if isinstance(prop_value_encoded, bytes):
                                    value_decoded = prop_value_encoded.decode('utf-8', errors='replace')
                                elif hasattr(prop_value_encoded, 'dt'): 
                                    value_decoded = prop_value_encoded.dt
                                else:
                                    value_decoded = prop_value_encoded

                                if prop_key.upper() in ['DTSTART', 'DTEND', 'DTSTAMP', 'CREATED', 'LAST-MODIFIED', 'RECURRENCE-ID']:
                                    new_event.add(prop_key, normalize_dt(value_decoded))
                                else:
                                    new_event.add(prop_key, value_decoded)
                            except Exception as e:
                                # print(f"    Attenzione: errore nel processare la proprietà '{prop_key}' (locale): {e} - Valore: {prop_value_encoded}")
                                if prop_key.upper() not in ['UID', 'SUMMARY', 'DTSTART']:
                                     new_event.add(prop_key, str(prop_value_encoded))
                                else:
                                    print(f"    Errore critico con la proprietà '{prop_key}' (locale), l'evento potrebbe essere incompleto.")

                        current_event_uid = new_event.get('uid')
                        if not current_event_uid:
                            current_event_uid = str(uuid.uuid4())
                            new_event.add('uid', current_event_uid)
                        
                        if current_event_uid not in all_event_uids:
                            merged_calendar.add_component(new_event)
                            all_event_uids.add(current_event_uid)
                            events_added_from_this_file += 1
                        # else:
                            # print(f"  Evento duplicato (da file locale, post-elaborazione UID) saltato (UID): {new_event.get('summary')}")

                print(f"    Aggiunti {events_added_from_this_file} eventi da file {filename}")
    else:
        print(f"\nLa cartella dei calendari custom '{custom_calendars_path}' non esiste. Saltando i file locali.")

    # Scrivi il calendario unito in un file .ics
    try:
        output_file_path = script_dir / OUTPUT_ICS_FILE
        with open(output_file_path, 'wb') as f:
            f.write(merged_calendar.to_ical())
        print(f"\nCalendario unito e filtrato salvato in: {output_file_path}")
        print(f"Eventi totali nel calendario generato: {len(all_event_uids)}")
    except Exception as e:
        print(f"Errore nello scrivere il file ICS: {e}")

if __name__ == "__main__":
    main()
