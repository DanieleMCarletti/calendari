# calendari — Eventi zona San Siro / Lampugnano

Calendari ICS pubblici con **partite casalinghe di Milan e Inter** (Stadio San Siro/Meazza) più **concerti ed eventi** all'Ippodromo SNAI San Siro e all'Ippodromo SNAI La Maura (Lampugnano), aggiornati automaticamente ogni notte.

Utile per chi abita o si muove nella zona e vuole anticipare il traffico.

## URL per la sottoscrizione

Apri questi link sul telefono / aggiungili come "Other calendar → From URL" su Google Calendar.

### Calendario completo (San Siro + La Maura)

```
https://raw.githubusercontent.com/DanieleMCarletti/calendari/main/calendari_output/eventi_san_siro_aggregato.ics
```

Oppure (mantenuto per retrocompatibilità, stesso contenuto):

```
https://raw.githubusercontent.com/DanieleMCarletti/calendari/main/eventi_san_siro_merged.ics
```

### Solo Lampugnano (Ippodromo SNAI La Maura)

```
https://raw.githubusercontent.com/DanieleMCarletti/calendari/main/calendari_output/eventi_lampugnano.ics
```

### Per iOS / app che richiedono `webcal://`

Sostituisci `https://` con `webcal://`:
```
webcal://raw.githubusercontent.com/DanieleMCarletti/calendari/main/eventi_san_siro_merged.ics
```

## Come si aggiorna

- **Partite Milan/Inter**: scaricate ogni giorno alle 03:00 UTC dai feed pubblici `https://ics.fixtur.es/v2/inter.ics` e `https://ics.fixtur.es/v2/ac-milan.ics`. Solo le partite **casalinghe** vengono inserite (filtro su squadra di casa + location).
- **Concerti / eventi non sportivi**: vanno aggiunti a mano (vedi sotto).
- Il workflow GitHub Actions [`generate_monthly_calendars.yml`](.github/workflows/generate_monthly_calendars.yml) committa automaticamente i file `.ics` rigenerati.

## AI discovery (concerti automatici)

Per ridurre il lavoro di curatura manuale dei concerti, c'è un secondo workflow [`discover_events.yml`](.github/workflows/discover_events.yml) che:

1. Gira **ogni domenica notte (04:00 UTC)** e su `workflow_dispatch`.
2. Scarica alcune pagine pubbliche di eventi a Milano (vedi `SOURCES` in [`discover_eventi.py`](discover_eventi.py)).
3. Passa il testo a **GitHub Models** (`openai/gpt-4o-mini`, gratis su Actions con `permissions: models: read`) per estrarre eventi a San Siro / Ippodromo La Maura / Ippodromo SNAI San Siro.
4. Valida l'output contro [`discovered/SCHEMA.json`](discovered/SCHEMA.json).
5. Fa **dedup** contro tutto quello che è già in `dati_grezzi/*.py` e `discovered/*.json` su `main`.
6. Se ci sono eventi nuovi, apre/aggiorna una **PR rolling** sul branch fisso `bot/discovered-events`.

Tu (Daniele) revisioni la PR: cancelli gli eventi spazzatura, modifichi quelli imprecisi, merge quando soddisfatto. Al run successivo di `Generate Monthly Calendars`, gli eventi entrano nel calendario pubblico.

I file `discovered/eventi_YYYY_MM.json` sono **dati**, non codice: un errore in un JSON è rilevato dal workflow [`validate_json.yml`](.github/workflows/validate_json.yml) prima del merge.

## Aggiungere un evento manuale

Gli eventi manuali (concerti e simili) vivono in file Python per mese sotto [`dati_grezzi/`](dati_grezzi/), uno per mese, con nome `eventi_YYYY_MM.py`.

Ogni file espone una lista `event_list` di dizionari:

```python
event_list = [
    {
        'summary': 'Cesare Cremonini - LIVE25',
        'dtstart_str': '2025-06-15T21:00:00',   # ISO senza timezone, sarà localizzato a Europe/Rome
        'dtend_str':   '2025-06-15T23:30:00',
        'location_name': 'Stadio San Siro',
        'location_address': 'Piazzale Angelo Moratti, 20151 Milano MI',
        'description': 'Concerto. Traffico previsto da inizio pomeriggio.',
        'google_maps_url_str': 'https://maps.google.com/?q=...',
    },
    # altri eventi...
]
```

Per aggiungere un mese nuovo: crea il file `dati_grezzi/eventi_YYYY_MM.py` con la stessa struttura. Al prossimo run del workflow (o triggerando manualmente da Actions → "Generate Monthly Calendars" → "Run workflow") sarà incluso.

## Esecuzione locale

```bash
pip install -r requirements.txt
python genera_calendari_mensili.py
```

Output in `calendari_output/`.

## Sicurezza e idempotenza

- **UID stabili**: ogni evento ha un UID deterministico (hash sha256 di `summary+dtstart+location` normalizzati) → run successivi senza modifiche **non** producono diff git, Google Calendar non duplica gli eventi.
- **Fail-safe sui feed**: se i feed pubblici sono giù o restituiscono dati anomali (< 5 eventi totali, o < 50% del run precedente), lo script esce con errore **senza sovrascrivere** i file `.ics`. Niente calendario svuotato.
- **Detection casa stretta**: una partita viene inclusa solo se il club è primo nel summary **E** la location del feed è una delle conosciute (San Siro / La Maura). Protegge da cambi di formato del feed.

## Troubleshooting

- **Workflow rosso**: apri Actions → l'ultima run di "Generate Monthly Calendars" → leggi i log. Le righe iniziano con `[YYYY-MM-DDTHH:MM:SS]`. Se vedi `ERRORE FATALE: tutti i N feed sono falliti`, è temporaneo, di solito rientra al run successivo.
- **Calendario sul telefono non aggiornato**: forza il refresh (Google Calendar → menu → Aggiorna). Se hai appena fatto switch dei vecchi UID `uuid4` → `sha256`, gli eventi possono "lampeggiare" una volta sola.
- **Voglio testare le modifiche prima di mergiare**: gira lo script in locale, ispeziona `calendari_output/*.ics`, oppure usa workflow_dispatch su un branch.

## Struttura del repository

```
genera_calendari_mensili.py   # script principale
requirements.txt               # dipendenze pip
dati_grezzi/eventi_YYYY_MM.py  # eventi manuali, uno per mese
calendari_output/              # file .ics generati (committati automaticamente)
eventi_san_siro_merged.ics     # copia in root dell'aggregato (compat URL storici)
eventi_lampugnano.ics          # copia in root del calendario Lampugnano
.github/workflows/             # workflow GitHub Actions
```
