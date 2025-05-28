# Python script to merge and filter iCalendar feeds

import requests
from icalendar import Calendar, Event
from datetime import datetime, timezone
import pytz # For timezone handling
import os # Per lavorare con i percorsi dei file
from pathlib import Path # Per una gestione più moderna dei percorsi

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

# --- Funzioni (get_calendar_from_url, is_home_game, normalize_dt rimangono invariate) ---
def get_calendar_from_url(url):
    # ... (codice esistente) ...
# ... (codice esistente per get_calendar_from_url) ...

def is_home_game(event):
    # ... (codice esistente) ...
# ... (codice esistente per is_home_game) ...

def normalize_dt(dt_value):
    # ... (codice esistente) ...
# ... (codice esistente per normalize_dt) ...

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
                    for key, value in component.items():
                        if key.upper() in ['DTSTART', 'DTEND', 'DTSTAMP', 'CREATED', 'LAST-MODIFIED', 'RECURRENCE-ID']:
                            new_event.add(key, normalize_dt(component.decoded(key)))
                        else:
                            new_event.add(key, component.decoded(key) if isinstance(value, bytes) else value)
                    
                    if not new_event.get('uid'):
                        import uuid
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
                        
                        # Per i file locali, assumiamo che gli eventi siano già rilevanti
                        # e non applichiamo il filtro is_home_game(), a meno che tu non voglia.
                        # Se vuoi filtrare anche questi, aggiungi:
                        # if not is_home_game(component):
                        #     continue
                        
                        new_event = Event()
                        for key, value in component.items():
                            if key.upper() in ['DTSTART', 'DTEND', 'DTSTAMP', 'CREATED', 'LAST-MODIFIED', 'RECURRENCE-ID']:
                                new_event.add(key, normalize_dt(component.decoded(key)))
                            else:
                                new_event.add(key, component.decoded(key) if isinstance(value, bytes) else value)

                        if not new_event.get('uid'):
                            import uuid
                            new_event.add('uid', str(uuid.uuid4())) # Aggiungi UID se mancante

                        merged_calendar.add_component(new_event)
                        all_event_uids.add(new_event.get('uid'))
                        events_added_from_this_file += 1
                print(f"    Aggiunti {events_added_from_this_file} eventi da file {filename}")
    else:
        print(f"\nLa cartella dei calendari custom '{custom_calendars_path}' non esiste. Saltando i file locali.")

    # Scrivi il calendario unito in un file .ics
    try:
        # Assicurati che il percorso di output sia relativo alla directory dello script
        output_file_path = script_dir / OUTPUT_ICS_FILE
        with open(output_file_path, 'wb') as f:
            f.write(merged_calendar.to_ical())
        print(f"\nCalendario unito e filtrato salvato in: {output_file_path}")
        print(f"Eventi totali nel calendario generato: {len(all_event_uids)}")
    except Exception as e:
        print(f"Errore nello scrivere il file ICS: {e}")

if __name__ == "__main__":
    main()
