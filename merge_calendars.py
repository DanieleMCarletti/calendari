# Python script to merge and filter iCalendar feeds

import requests
from icalendar import Calendar, Event
from datetime import datetime, timezone
import pytz # For timezone handling
import os # Per lavorare con i percorsi dei file
from pathlib import Path # Per una gestione più moderna dei percorsi
import uuid # Per generare UID se mancano

# --- Configurazione ---
CALENDAR_URLS = {
    "inter": "https://www.stanza.news/api/calendar/inter/all.ics",
    "milan": "https://www.stanza.news/api/calendar/milan/all.ics",
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
        if dt_value.tzinfo is None or dt_value.tzinfo.utcoffset(dt_value) is None:
            # Se è naive, lo localizziamo a UTC e poi lo convertiamo
            # Google Calendar preferisce UTC o timezone specifiche.
            # Per i feed di stanza.news, DTSTART/DTEND sembrano essere UTC.
            aware_dt = pytz.utc.localize(dt_value)
        else:
            aware_dt = dt_value
        return aware_dt.astimezone(pytz.timezone(TARGET_TIMEZONE))
    return dt_value # Lascia invariato se non è un datetime (es. date object)

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
    merged_calendar.add('X-WR-CALNAME', 'Eventi San Siro (Partite e Custom)')
    merged_calendar.add('X-WR-TIMEZONE', TARGET_TIMEZONE)

    all_event_uids = set()

    # 1. Processa calendari da URL
    for team_name, url in CALENDAR_URLS.items():
        print(f"Processando calendario da URL per: {team_name} da {url}")
        calendar = get_calendar_from_url(url)
        if not calendar:
            continue

        events_added_from_this_feed = 0
        for component in calendar.walk():
            if component.name == "VEVENT":
                event_uid = component.get('uid')
                if event_uid in all_event_uids:
                    # print(f"  Evento duplicato (da URL) saltato (UID): {component.get('summary')}")
                    continue

                if is_home_game(component): # Applica il filtro San Siro per i feed URL
                    new_event = Event()
                    for key, value_encoded in component.items(): # Rinomino value in value_encoded
                        # Decodifica solo se necessario e gestisci diversi tipi di dati
                        try:
                            if isinstance(value_encoded, bytes):
                                value_decoded = value_encoded.decode('utf-8')
                            elif hasattr(value_encoded, 'dt'): # Per oggetti vDDDTypes (date/datetime)
                                value_decoded = value_encoded.dt 
                            else:
                                value_decoded = value_encoded

                            if key.upper() in ['DTSTART', 'DTEND', 'DTSTAMP', 'CREATED', 'LAST-MODIFIED', 'RECURRENCE-ID']:
                                new_event.add(key, normalize_dt(value_decoded))
                            else:
                                new_event.add(key, value_decoded)
                        except Exception as e:
                            # print(f"    Attenzione: errore nel processare la proprietà '{key}': {e} - Valore: {value_encoded}")
                            # Se una proprietà non critica causa problemi, potresti volerla saltare o loggare
                            if key.upper() not in ['UID', 'SUMMARY', 'DTSTART']: # Salta se non critica
                                 new_event.add(key, str(value_encoded)) # Prova ad aggiungerla come stringa
                            else:
                                print(f"    Errore critico con la proprietà '{key}', l'evento potrebbe essere incompleto.")


                    if not new_event.get('uid'):
                        new_event.add('uid', str(uuid.uuid4()))

                    merged_calendar.add_component(new_event)
                    all_event_uids.add(new_event.get('uid'))
                    events_added_from_this_feed += 1
        print(f"  Aggiunti {events_added_from_this_feed} eventi da URL {team_name}")

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
                        if event_uid in all_event_uids:
                            # print(f"    Evento duplicato (da file locale) saltato (UID): {component.get('summary')}")
                            continue
                        
                        new_event = Event()
                        for key, value_encoded in component.items():
                            try:
                                if isinstance(value_encoded, bytes):
                                    value_decoded = value_encoded.decode('utf-8')
                                elif hasattr(value_encoded, 'dt'): 
                                    value_decoded = value_encoded.dt
                                else:
                                    value_decoded = value_encoded

                                if key.upper() in ['DTSTART', 'DTEND', 'DTSTAMP', 'CREATED', 'LAST-MODIFIED', 'RECURRENCE-ID']:
                                    new_event.add(key, normalize_dt(value_decoded))
                                else:
                                    new_event.add(key, value_decoded)
                            except Exception as e:
                                # print(f"    Attenzione: errore nel processare la proprietà '{key}' (locale): {e} - Valore: {value_encoded}")
                                if key.upper() not in ['UID', 'SUMMARY', 'DTSTART']:
                                     new_event.add(key, str(value_encoded))
                                else:
                                    print(f"    Errore critico con la proprietà '{key}' (locale), l'evento potrebbe essere incompleto.")


                        if not new_event.get('uid'):
                            new_event.add('uid', str(uuid.uuid4()))

                        merged_calendar.add_component(new_event)
                        all_event_uids.add(new_event.get('uid'))
                        events_added_from_this_file += 1
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
