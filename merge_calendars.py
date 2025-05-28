# Python script to merge and filter iCalendar feeds

import requests
from icalendar import Calendar, Event
from datetime import datetime, timezone
import pytz # For timezone handling, as Google Calendar can be picky

# --- Configurazione ---
CALENDAR_URLS = {
    "inter": "https://www.stanza.news/api/calendar/inter/all.ics",
    "milan": "https://www.stanza.news/api/calendar/milan/all.ics",
    # Potresti aggiungere qui altri URL .ics se ne trovi di affidabili per concerti/eventi
    # "concerti_sansiro": "URL_EVENTUALI_CONCERTI_SANSIRO.ics"
}

# Nomi con cui identificare lo stadio di San Siro nel campo LOCATION degli eventi .ics
# Questi potrebbero aver bisogno di aggiustamenti a seconda di come sono formattati nei feed
STADIO_SAN_SIRO_NAMES = [
    "san siro",
    "giuseppe meazza",
    "stadio giuseppe meazza",
    "stadio san siro"
]

OUTPUT_ICS_FILE = "eventi_san_siro_merged.ics"
TARGET_TIMEZONE = 'Europe/Rome' # Timezone per normalizzare gli orari

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
        # Se non c'è location, assumiamo per sicurezza che non sia a San Siro
        # o potresti decidere di includerlo se il feed è MOLTO specifico
        return False
    
    location_lower = str(location).lower()
    for name in STADIO_SAN_SIRO_NAMES:
        if name in location_lower:
            # Ulteriore controllo per squadre che giocano entrambe a San Siro:
            # Assicuriamoci che il nome della squadra di casa (se presente nel summary)
            # corrisponda alla squadra del feed da cui stiamo leggendo,
            # oppure che lo stadio sia esplicitamente menzionato.
            # Questo aiuta se un feed di una squadra riporta per errore una partita dell'altra
            # squadra giocata a San Siro ma come trasferta per la squadra del feed.
            # Per ora, la semplice presenza del nome dello stadio è il nostro filtro principale.
            return True
    return False

def normalize_dt(dt_value):
    """Normalizza un datetime object a un timezone specifico o lo rende timezone-aware."""
    if isinstance(dt_value, datetime):
        if dt_value.tzinfo is None or dt_value.tzinfo.utcoffset(dt_value) is None:
            # Se è naive, lo localizziamo a UTC e poi lo convertiamo
            # Google Calendar preferisce UTC o timezone specifiche.
            # Assumere UTC se naive è una scelta, potrebbe essere necessario un aggiustamento
            # se i feed originali hanno orari locali naive ma non sono UTC.
            # Per eventi sportivi, sono spesso in UTC o con timezone.
            # Se i feed fossero in ora locale naive, dovremmo localizzarli a Europe/Rome prima.
            # I feed di stanza.news sembrano essere UTC per DTSTART/DTEND
            aware_dt = pytz.utc.localize(dt_value)
        else:
            aware_dt = dt_value
        return aware_dt.astimezone(pytz.timezone(TARGET_TIMEZONE))
    return dt_value # Lascia invariato se non è un datetime (es. date object)

# --- Script Principale ---
def main():
    merged_calendar = Calendar()
    # Proprietà standard del calendario
    merged_calendar.add('prodid', '-//Combined San Siro Events Calendar//example.com//')
    merged_calendar.add('version', '2.0')
    merged_calendar.add('X-WR-CALNAME', 'Eventi San Siro (Partite)')
    merged_calendar.add('X-WR-TIMEZONE', TARGET_TIMEZONE)


    all_event_uids = set() # Per evitare duplicati se lo stesso evento è in più feed

    for team_name, url in CALENDAR_URLS.items():
        print(f"Processando calendario per: {team_name} da {url}")
        calendar = get_calendar_from_url(url)
        if not calendar:
            continue

        events_added_from_this_feed = 0
        for component in calendar.walk():
            if component.name == "VEVENT":
                event_uid = component.get('uid')
                if event_uid in all_event_uids:
                    print(f"  Evento duplicato saltato (UID): {component.get('summary')}")
                    continue

                # Filtra per partite in casa se è un calendario di una squadra
                # Per ora, questo filtro si applica a tutti. Se aggiungi feed di concerti
                # che sono *già* specifici per San Siro, potresti voler bypassare is_home_game
                if is_home_game(component):
                    new_event = Event()
                    # Copia tutte le proprietà, normalizzando le date/ore
                    for key, value in component.items():
                        if key.upper() in ['DTSTART', 'DTEND', 'DTSTAMP', 'CREATED', 'LAST-MODIFIED', 'RECURRENCE-ID']:
                            new_event.add(key, normalize_dt(component.decoded(key)))
                        else:
                            new_event.add(key, component.decoded(key) if isinstance(value, bytes) else value)
                    
                    # Assicurati che l'evento abbia un UID
                    if not new_event.get('uid'):
                        import uuid
                        new_event.add('uid', str(uuid.uuid4()))

                    merged_calendar.add_component(new_event)
                    all_event_uids.add(new_event.get('uid'))
                    events_added_from_this_feed += 1
                    # print(f"  Aggiunto evento: {new_event.get('summary')} @ {new_event.get('dtstart').dt}")
                # else:
                #     print(f"  Evento saltato (non a San Siro o location non chiara): {component.get('summary')} - Location: {component.get('location')}")
        print(f"  Aggiunti {events_added_from_this_feed} eventi da {team_name}")


    # Scrivi il calendario unito in un file .ics
    try:
        with open(OUTPUT_ICS_FILE, 'wb') as f:
            f.write(merged_calendar.to_ical())
        print(f"\nCalendario unito e filtrato salvato in: {OUTPUT_ICS_FILE}")
        print(f"Eventi totali nel calendario generato: {len(all_event_uids)}")
    except Exception as e:
        print(f"Errore nello scrivere il file ICS: {e}")

if __name__ == "__main__":
    main()
