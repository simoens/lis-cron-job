import requests
from bs4 import BeautifulSoup
import logging
from datetime import datetime, timedelta
import re
import os
import json 
from collections import deque, Counter
import pytz
from flask import Flask, render_template, request, abort, redirect, url_for, jsonify
import threading
import time
from flask_sqlalchemy import SQLAlchemy 
import psycopg2 

# --- CONFIGURATIE ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- GLOBALE STATE DICTIONARY (voor de webpagina) ---
# Dit is nu alleen nog een 'in-memory cache' voor de allereerste laadbeurt
app_state = {
    "latest_snapshot": {"timestamp": "Nog niet uitgevoerd", "content_data": {"INKOMEND": [], "UITGAAND": []}}, 
    "change_history": deque(maxlen=10)
}
data_lock = threading.Lock() 

# --- FLASK APPLICATIE & DATABASE OPZET ---
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

# --- DATABASE MODELLEN (NIEUWE STRUCTUUR) ---
# We gebruiken niet langer KeyValueStore.
# We slaan nu elke snapshot en elke wijziging op als een aparte rij.

class Snapshot(db.Model):
    """Slaat elke gegenereerde snapshot op in de geschiedenis."""
    id = db.Column(db.Integer, primary_key=True)
    timestamp = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    content_data = db.Column(db.JSON)

class DetectedChange(db.Model):
    """Slaar elke gedetecteerde wijziging op in de geschiedenis."""
    id = db.Column(db.Integer, primary_key=True)
    timestamp = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    onderwerp = db.Column(db.String(200))
    content = db.Column(db.Text)

class KeyValueStore(db.Model):
    """Blijft bestaan voor 'last_bestellingen' en 'last_report_key'."""
    key = db.Column(db.String(50), primary_key=True)
    value = db.Column(db.JSON)

# Zorg dat de tabellen bestaan wanneer de app start
with app.app_context():
    db.create_all()

# --- AANGEPASTE DATABASE FUNCTIES ---

def load_state_for_comparison():
    """Laadt alleen de data die nodig is voor de *volgende* run."""
    logging.info("Oude staat (voor vergelijking) wordt geladen uit PostgreSQL...")
    try:
        # --- HIER IS DE FIX ---
        # Het was 'last_bestellingen', het moet 'bestellingen' zijn.
        bestellingen_obj = KeyValueStore.query.get('bestellingen')
        # --- EINDE FIX ---
        
        report_key_obj = KeyValueStore.query.get('last_report_key')

        vorige_staat = {
            "bestellingen": bestellingen_obj.value if bestellingen_obj else [],
            "last_report_key": report_key_obj.value if report_key_obj else ""
        }
        return vorige_staat
    except Exception as e:
        logging.error(f"Error loading state from KeyValueStore: {e}")
        return {"bestellingen": [], "last_report_key": ""}

def save_state_for_comparison(state):
    """Slaat alleen de data op die nodig is voor de *volgende* run."""
    logging.info("Nieuwe staat (voor vergelijking) wordt opgeslagen in PostgreSQL...")
    try:
        # We slaan alleen 'bestellingen' en 'last_report_key' op
        for key in ['bestellingen', 'last_report_key']:
            value = state.get(key)
            if value is None:
                continue

            obj = KeyValueStore.query.get(key)
            if obj:
                obj.value = value
            else:
                obj = KeyValueStore(key=key, value=value)
                db.session.add(obj)
        
        # NB: We doen hier GEEN db.session.commit()
        # De 'main' functie doet dit aan het einde.
    except Exception as e:
        logging.error(f"Error saving state to KeyValueStore: {e}")
        db.session.rollback()

# --- HELPER FUNCTIE VOOR ACHTERGROND TAKEN ---
def main_task():
    """Wrapper functie om main() in een background thread te draaien."""
    try:
        logging.info("--- Background task started ---")
        with app.app_context():
            main()
        logging.info("--- Background task finished ---")
    except Exception as e:
        logging.critical(f"FATAL ERROR in background thread: {e}", exc_info=True)

@app.route('/statistieken')
def statistieken():
    """Toont een overzichtspagina met statistieken van de afgelopen 7 dagen."""
    
    # Bepaal de datumgrens (7 dagen geleden)
    seven_days_ago_utc = datetime.utcnow() - timedelta(days=7)
    
    # --- Stat 1: Totaal aantal wijzigingen (eenvoudige query) ---
    total_changes = DetectedChange.query.filter(
        DetectedChange.timestamp >= seven_days_ago_utc
    ).count()

    # --- Stat 2: Totaal aantal schepen in snapshots ---
    snapshots = Snapshot.query.filter(
        Snapshot.timestamp >= seven_days_ago_utc
    ).all()
    
    total_incoming = 0
    total_outgoing = 0
    if snapshots:
        # We pakken de data van de *meest recente* snapshot als representatief
        # (Elke snapshot tellen zou schepen dubbel tellen)
        latest_snapshot_data = snapshots[-1].content_data
        total_incoming = len(latest_snapshot_data.get('INKOMEND', []))
        total_outgoing = len(latest_snapshot_data.get('UITGAAND', []))
        
    # --- Stat 3: Meest gewijzigde schepen ---
    changes = DetectedChange.query.filter(
        DetectedChange.timestamp >= seven_days_ago_utc
    ).all()
    
    ship_counter = Counter()
    # Zoek naar 'NIEUW SCHIP: 'Scheepsnaam'', 'GEWIJZIGD: 'Scheepsnaam'', etc.
    ship_regex = re.compile(r"(?:NIEUW SCHIP|GEWIJZIGD|VERWIJDERD): '([^']+)'")
    
    for change in changes:
        if change.content:
            ship_names = ship_regex.findall(change.content)
            for name in ship_names:
                ship_counter[name] += 1
                
    top_ships = ship_counter.most_common(5) # Pak de top 5

    # Bundel alle statistieken
    stats = {
        "total_changes": total_changes,
        "total_incoming": total_incoming,
        "total_outgoing": total_outgoing,
        "top_ships": top_ships
    }
    
    return render_template('statistieken.html', 
                            stats=stats,
                            secret_key=os.environ.get('SECRET_KEY'))

@app.route('/logboek')
def logboek():
    """Toont een doorzoekbare geschiedenis van alle gedetecteerde wijzigingen."""
    
    # Haal de zoekterm uit de URL (bv. /logboek?q=MSC)
    search_term = request.args.get('q', '')
    
    query = DetectedChange.query.order_by(DetectedChange.timestamp.desc())
    
    if search_term:
        # Filter de 'content' kolom. 'ilike' betekent 'case-insensitive'
        query = query.filter(DetectedChange.content.ilike(f'%{search_term}%'))
    
    # Haal alle resultaten op (max 100 voor de veiligheid)
    changes_db_objects = query.limit(100).all()
    
    # Converteer de DB objecten naar dictionaries, net als op de homepage
    formatted_changes = []
    for change in changes_db_objects:
        formatted_changes.append({
            "timestamp": pytz.utc.localize(change.timestamp).astimezone(pytz.timezone('Europe/Brussels')).strftime('%d-%m-%Y %H:%M:%S'),
            "onderwerp": change.onderwerp,
            "content": change.content
        })

    return render_template('logboek.html', 
                            changes=formatted_changes, 
                            search_term=search_term,
                            secret_key=os.environ.get('SECRET_KEY'))

@app.route('/')
def home():
    """Rendert de homepage, toont de laatste snapshot en wijzigingsgeschiedenis."""
    
    # --- AANGEPASTE LAAD-LOGICA ---
    # Haal de ALLERLAATSTE snapshot uit de nieuwe tabel
    latest_snapshot_obj = Snapshot.query.order_by(Snapshot.timestamp.desc()).first()
    
    # Haal de LAATSTE 10 wijzigingen uit de nieuwe tabel
    recent_changes_list = DetectedChange.query.order_by(DetectedChange.timestamp.desc()).limit(10).all()
    # --- EINDE AANPASSING ---
    
    with data_lock:
        if latest_snapshot_obj:
            # Converteer het DB-object naar de dictionary die de template verwacht
            app_state["latest_snapshot"] = {
                "timestamp": pytz.utc.localize(latest_snapshot_obj.timestamp).astimezone(pytz.timezone('Europe/Brussels')).strftime('%d-%m-%Y %H:%M:%S'),
                "content_data": latest_snapshot_obj.content_data
            }
        
        if recent_changes_list:
            app_state["change_history"].clear()
            # Converteer de DB-objecten naar dictionaries
            for change in recent_changes_list:
                app_state["change_history"].append({
                    "timestamp": pytz.utc.localize(change.timestamp).astimezone(pytz.timezone('Europe/Brussels')).strftime('%d-%m-%Y %H:%M:%S'),
                    "onderwerp": change.onderwerp,
                    "content": change.content
                })
    
    with data_lock:
        return render_template('index.html',
                                snapshot=app_state["latest_snapshot"],
                                changes=list(app_state["change_history"]),
                                secret_key=os.environ.get('SECRET_KEY'))

@app.route('/trigger-run')
def trigger_run():
    """Een geheime URL endpoint die door een externe cron job wordt aangeroepen."""
    secret = request.args.get('secret') # Leest uit URL (voor curl)
    if secret != os.environ.get('SECRET_KEY'):
        logging.warning("Failed attempt to call /trigger-run with an invalid secret key.")
        abort(403)
    
    logging.info("================== External Trigger Received: Starting Background Run ==================")
    threading.Thread(target=main_task).start()
    return "OK: Scraper run triggered in background.", 200

@app.route('/force-snapshot', methods=['POST'])
def force_snapshot_route():
    """Endpoint aangeroepen door de knop om een background run te triggeren."""
    secret = request.form.get('secret') # Leest uit het formulier (voor de knop)
    if secret != os.environ.get('SECRET_KEY'):
        logging.warning("Failed attempt to call /force-snapshot with an invalid secret key.")
        abort(403)
    
    logging.info("================== Manual Background Run Triggered via Button ==================")
    threading.Thread(target=main_task).start()
    return redirect(url_for('home'))

@app.route('/status')
def status_check():
    """Een lichtgewicht endpoint die de client-side JS kan pollen."""
    # --- AANGEPASTE LAAD-LOGICA ---
    # Haal alleen de timestamp van de laatste snapshot op
    latest_snapshot_obj = Snapshot.query.order_by(Snapshot.timestamp.desc()).first()
    
    if latest_snapshot_obj:
        # Converteer naar Brusselse tijd voor de vergelijking
        brussels_time = pytz.utc.localize(latest_snapshot_obj.timestamp).astimezone(pytz.timezone('Europe/Brussels'))
        return jsonify({"timestamp": brussels_time.strftime('%d-%m-%Y %H:%M:%S')})
    else:
        with data_lock:
            return jsonify({"timestamp": app_state["latest_snapshot"].get("timestamp", "N/A")})

# --- ENVIRONMENT VARIABLES ---
USER = os.environ.get('LIS_USER')
PASS = os.environ.get('LIS_PASS')
# DATABASE_URL wordt automatisch gebruikt door SQLAlchemy

# --- HELPER: ROBUUSTE DATUM PARSER ---
def parse_besteltijd(besteltijd_str):
    """Probeert een besteltijd-string te parsen. 
    Geeft een geldig datetime-object terug, of epoch (1970) bij een fout."""
    DEFAULT_TIME = datetime(1970, 1, 1) 
    
    if not besteltijd_str:
        return DEFAULT_TIME
        
    try:
        cleaned_str = re.sub(r'\s+', ' ', besteltijd_str.strip())
        return datetime.strptime(cleaned_str, "%d/%m/%y %H:%M")
    except ValueError:
        logging.warning(f"Kon besteltijd '{besteltijd_str}' niet parsen (bv. 'Sluisplanning'). Wordt genegeerd in sortering.")
        return DEFAULT_TIME

# --- SCRAPER FUNCTIES ---
# (login, haal_bestellingen_op, haal_pta_van_reisplan blijven ONGEWIJZIGD)
def login(session):
    try:
        logging.info("Login attempt started...")
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"}
        get_response = session.get("https://lis.loodswezen.be/Lis/Login.aspx", headers=headers)
        get_response.raise_for_status()
        soup = BeautifulSoup(get_response.text, 'lxml')
        viewstate = soup.find('input', {'name': '__VIEWSTATE'})
        if not viewstate:
            logging.error("Could not find __VIEWSTATE on login page.")
            return False
        form_data = {'__VIEWSTATE': viewstate['value'],'ctl00$ContentPlaceHolder1$login$uname': USER,'ctl00$ContentPlaceHolder1$login$password': PASS,'ctl00$ContentPlaceHolder1$login$btnInloggen': 'Inloggen'}
        login_response = session.post("https://lis.loodswezen.be/Lis/Login.aspx", data=form_data, headers=headers)
        login_response.raise_for_status()
        if "Login.aspx" not in login_response.url:
            logging.info("LOGIN SUCCESSFUL!")
            return True
        logging.error("Login failed (returned to login page).")
        return False
    except Exception as e:
        logging.error(f"Error during login: {e}")
        return False

def haal_bestellingen_op(session):
    logging.info("--- Running haal_bestellingen_op (v8, Zeebrugge-Fix) ---")
    try:
        base_page_url = "https://lis.loodswezen.be/Lis/Loodsbestellingen.aspx"
        response = session.get(base_page_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'lxml')
        table = soup.find('table', id='ctl00_ContentPlaceHolder1_ctl01_list_gv')
        if table is None: 
            logging.warning("De bestellingentabel werd niet gevonden.")
            return []
        kolom_indices = {"Type": 0, "Besteltijd": 5, "ETA/ETD": 6, "RTA": 7, "Loods": 10, "Schip": 11, "Entry Point": 20, "Exit Point": 21}
        bestellingen = []
        for row in table.find_all('tr')[1:]: 
            kolom_data = row.find_all('td')
            if not kolom_data: continue
            bestelling = {}
            for k, i in kolom_indices.items():
                if i < len(kolom_data):
                    value = kolom_data[i].get_text(strip=True)
                    if k == "Loods" and "[Loods]" in value:
                        value = "Loods werd toegewezen"
                    bestelling[k] = value
            if 11 < len(kolom_data):
                schip_cel = kolom_data[11]
                link_tag = schip_cel.find('a', href=re.compile(r'Reisplan\.aspx\?ReisId='))
                if link_tag:
                    match = re.search(r'ReisId=(\d+)', link_tag['href'])
                    if match:
                        bestelling['ReisId'] = match.group(1)
            bestellingen.append(bestelling)
        logging.info(f"haal_bestellingen_op klaar. {len(bestellingen)} bestellingen gevonden.")
        return bestellingen
    except Exception as e:
        logging.error(f"Error in haal_bestellingen_op: {e}", exc_info=True)
        return []

def haal_pta_van_reisplan(session, reis_id):
    if not reis_id:
        return None
    try:
        url = f"https://lis.loodswezen.be/Lis/Reisplan.aspx?ReisId={reis_id}"
        response = session.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'lxml')
        laatste_pta_gevonden = None
        pta_pattern = re.compile(r'\d{2}-\d{2} \d{2}:\d{2}')
        for row in soup.find_all('tr'):
            if row.find(text=re.compile("Saeftinghe - Zandvliet")):
                for cell in row.find_all('td'):
                    match = pta_pattern.search(cell.get_text())
                    if match:
                        laatste_pta_gevonden = match.group(0)
        if laatste_pta_gevonden:
            try:
                pta_schoon = re.sub(r'\s+', ' ', laatste_pta_gevonden.strip())
                dt_obj = datetime.strptime(pta_schoon, '%d-%m %H:%M')
                now = datetime.now()
                dt_obj = dt_obj.replace(year=now.year)
                if dt_obj < now - timedelta(days=180):
                    dt_obj = dt_obj.replace(year=now.year + 1)
                return dt_obj.strftime("%d/%m/%y %H:%M")
            except ValueError:
                logging.warning(f"Could not parse PTA format '{laatste_pta_gevonden}' for ReisId {reis_id}.")
                return laatste_pta_gevonden
        return None
    except Exception as e:
        logging.error(f"Error fetching voyage plan for ReisId {reis_id}: {e}")
        return None

def filter_snapshot_schepen(bestellingen, session, nu): 
    gefilterd = {"INKOMEND": [], "UITGAAND": []}
    brussels_tz = pytz.timezone('Europe/Brussels') 
    grens_uit_toekomst = nu + timedelta(hours=16)
    grens_in_verleden = nu - timedelta(hours=8)
    grens_in_toekomst = nu + timedelta(hours=8)
    bestellingen.sort(key=lambda x: parse_besteltijd(x.get('Besteltijd')), reverse=True)
    for b in bestellingen:
        try:
            entry_point = b.get('Entry Point', '').lower()
            exit_point = b.get('Exit Point', '').lower()
            if 'zeebrugge' in entry_point or 'zeebrugge' in exit_point:
                continue 
            besteltijd_str_raw = b.get("Besteltijd")
            besteltijd_naive = parse_besteltijd(besteltijd_str_raw)
            if besteltijd_naive.year == 1970:
                continue
            besteltijd = brussels_tz.localize(besteltijd_naive)
            if b.get("Type") == "U": 
                if nu <= besteltijd <= grens_uit_toekomst:
                    gefilterd["UITGAAND"].append(b)
            elif b.get("Type") == "I": 
                if grens_in_verleden <= besteltijd <= grens_in_toekomst:
                    pta_saeftinghe = haal_pta_van_reisplan(session, b.get('ReisId'))
                    b['berekende_eta'] = pta_saeftinghe if pta_saeftinghe else 'N/A'
                    if b['berekende_eta'] == 'N/A':
                        eta_dt = None
                        if "wandelaar" in entry_point: eta_dt = besteltijd + timedelta(hours=6)
                        elif "steenbank" in entry_point: eta_dt = besteltijd + timedelta(hours=7)
                        if eta_dt:
                            b['berekende_eta'] = eta_dt.strftime("%d/%m/%y %H:%M")
                    gefilterd["INKOMEND"].append(b)
        except (ValueError, TypeError) as e:
            logging.warning(f"Fout bij verwerken van '{besteltijd_str_raw}' (Fout: {e}). Schip wordt overgeslagen.", exc_info=False)
            continue
    gefilterd["INKOMEND"].sort(key=lambda x: parse_besteltijd(x.get('Besteltijd')))
    gefilterd["UITGAAND"].sort(key=lambda x: parse_besteltijd(x.get('Besteltijd')))
    return gefilterd

def filter_dubbele_schepen(bestellingen):
    unieke_schepen = {}
    for b in bestellingen:
        schip_naam_raw = b.get('Schip')
        if not schip_naam_raw:
            continue
        schip_naam_gekuist = re.sub(r'\s*\(d\)\s*$', '', schip_naam_raw).strip()
        if schip_naam_gekuist in unieke_schepen:
            try:
                huidige_tijd = parse_besteltijd(b.get("Besteltijd"))
                opgeslagen_tijd = parse_besteltijd(unieke_schepen[schip_naam_gekuist].get("Besteltijd"))
                if huidige_tijd > opgeslagen_tijd:
                    unieke_schepen[schip_naam_gekuist] = b
            except (ValueError, TypeError):
                unieke_schepen[schip_naam_gekuist] = b
        else:
            unieke_schepen[schip_naam_gekuist] = b
    return list(unieke_schepen.values())

def vergelijk_bestellingen(oude, nieuwe):
    oude_dict = {re.sub(r'\s*\(d\)\s*$', '', b.get('Schip', '')).strip(): b 
            for b in filter_dubbele_schepen(oude) if b.get('Schip')}
    nieuwe_dict = {re.sub(r'\s*\(d\)\s*$', '', b.get('Schip', '')).strip(): b 
                   for b in filter_dubbele_schepen(nieuwe) if b.get('Schip')}
    oude_schepen_namen = set(oude_dict.keys())
    nieuwe_schepen_namen = set(nieuwe_dict.keys())
    wijzigingen = []
    nu_brussels = datetime.now(pytz.timezone('Europe/Brussels')) 
    brussels_tz = pytz.timezone('Europe/Brussels')
    def moet_rapporteren(bestelling):
        besteltijd_str_raw = bestelling.get('Besteltijd', '').strip()
        besteltijd_naive = parse_besteltijd(besteltijd_str_raw)
        if besteltijd_naive.year == 1970: 
             return False
        besteltijd = brussels_tz.localize(besteltijd_naive)
        rapporteer = True
        type_schip = bestelling.get('Type')
        try:
            if type_schip == 'I':
                if not (nu_brussels - timedelta(hours=8) <= besteltijd <= nu_brussels + timedelta(hours=8)):
                    rapporteer = False
            elif type_schip == 'U':
                if not (nu_brussels <= besteltijd <= nu_brussels + timedelta(hours=16)):
                    rapporteer = False
        except (ValueError, TypeError) as e:
            logging.warning(f"Datum/tijd parsefout bij filteren van '{bestelling.get('Schip')}': {e}")
            return False
        entry_point = bestelling.get('Entry Point', '').lower()
        exit_point = bestelling.get('Exit Point', '').lower()
        if 'zeebrugge' in entry_point or 'zeebrugge' in exit_point:
            rapporteer = False
        return rapporteer
    for schip_naam in (nieuwe_schepen_namen - oude_schepen_namen):
        n_best = nieuwe_dict[schip_naam] 
        if moet_rapporteren(n_best):
            wijzigingen.append({'Schip': n_best.get('Schip'), 'status': 'NIEUW', 'details': n_best })
    for schip_naam in (oude_schepen_namen - nieuwe_schepen_namen):
        o_best = oude_dict[schip_naam]
        if moet_rapporteren(o_best): 
            wijzigingen.append({'Schip': o_best.get('Schip'), 'status': 'VERWIJDERD', 'details': o_best})
    for schip_naam in (nieuwe_schepen_namen.intersection(oude_schepen_namen)):
        n_best = nieuwe_dict[schip_naam] 
        o_best = oude_dict[schip_naam]
        if n_best.get('Type') != o_best.get('Type'):
            continue
        diff = {k: {'oud': o_best.get(k, ''), 'nieuw': v} for k, v in n_best.items() if v != o_best.get(k, '')}
        if diff:
            gewijzigde_sleutels = set(diff.keys())
            if n_best.get('Type') == 'I' and gewijzigde_sleutels == {'ETA/ETD'}:
                logging.info(f"Onderdrukt: Alleen ETA/ETD gewijzigd voor inkomend schip {schip_naam}")
                continue 
            relevante = {'Besteltijd', 'ETA/ETD', 'Loods'}
            if relevante.intersection(gewijzigde_sleutels): 
                if moet_rapporteren(n_best) or moet_rapporteren(o_best):
                    wijzigingen.append({'Schip': n_best.get('Schip'), 'status': 'GEWIJZIGD', 'wijzigingen': diff})
    return wijzigingen

def format_wijzigingen_email(wijzigingen):
    body = []
    wijzigingen.sort(key=lambda x: x.get('status', ''))
    for w in wijzigingen:
        s_naam = re.sub(r'\s*\(d\)\s*$', '', w.get('Schip', '')).strip()
        status = w.get('status')
        if status == 'NIEUW':
            details = w.get('details', {})
            tekst = f"+++ NIEUW SCHIP: '{s_naam}'\n"
            tekst += f" - Type: {details.get('Type', 'N/A')}\n"
            tekst += f" - Besteltijd: {details.get('Besteltijd', 'N/A')}\n"
            tekst += f" - ETA/ETD: {details.get('ETA/ETD', 'N/A')}\n"
            tekst += f" - Loods: {details.get('Loods', 'N/A')}"
        elif status == 'GEWIJZIGD':
            tekst = f"*** GEWIJZIGD: '{s_naam}'\n"
            tekst += "\n".join([f" - {k}: '{v['oud']}' -> '{v['nieuw']}'" 
                                for k, v in w['wijzigingen'].items() 
                                if k in {'Besteltijd', 'ETA/ETD', 'Loods'}])
        elif status == 'VERWIJDERD':
            details = w.get('details', {})
            tekst = f"--- VERWIJDERD: '{s_naam}'\n"
            tekst += f" - (Was type {details.get('Type', 'N/A')}, Besteltijd {details.get('Besteltijd', 'N/A')})"
        body.append(tekst)
    return "\n\n".join(body)

def main():
    """De hoofdfunctie van de scraper, uitgevoerd door de cron job trigger."""
    if not all([USER, PASS]):
        logging.critical("FATAL ERROR: LIS_USER en LIS_PASS zijn niet ingesteld!")
        return
    
    # Gebruik de nieuwe DB-laadfunctie
    vorige_staat = load_state_for_comparison()
    
    oude_bestellingen = vorige_staat.get("bestellingen", [])
    last_report_key = vorige_staat.get("last_report_key", "")
    
    session = requests.Session()
    if not login(session):
        logging.error("Login failed during main() run.")
        return
    
    nieuwe_bestellingen = haal_bestellingen_op(session) 
    if not nieuwe_bestellingen:
        logging.error("Fetching orders failed during main() run.")
        return

    # --- Genereer ALTIJD een snapshot ---
    logging.info("Generating new snapshot for webpage (every run).")
    brussels_tz = pytz.timezone('Europe/Brussels')
    nu_brussels = datetime.now(brussels_tz) 
    
    snapshot_data = filter_snapshot_schepen(nieuwe_bestellingen, session, nu_brussels)
    
    # --- NIEUWE OPSLAG LOGICA ---
    # 1. Sla de nieuwe snapshot op in de 'Snapshot' historietabel
    # We slaan de 'aware' Brusselse tijd op als een 'naive' UTC tijd
    new_snapshot_entry = Snapshot(
        timestamp=nu_brussels.astimezone(pytz.utc).replace(tzinfo=None), 
        content_data=snapshot_data
    )
    db.session.add(new_snapshot_entry)
    logging.info("Nieuwe snapshot rij toegevoegd aan 'snapshot' tabel.")
    # --- EINDE AANPASSING ---

    # --- Change Detection ---
    if oude_bestellingen:
        wijzigingen = vergelijk_bestellingen(oude_bestellingen, nieuwe_bestellingen)
        if wijzigingen:
            inhoud = format_wijzigingen_email(wijzigingen)
            onderwerp = f"LIS Update: {len(wijzigingen)} wijziging(en)"
            logging.info(f"Found {len(wijzigingen)} changes, logging them to web history.")
            
            # --- NIEUWE OPSLAG LOGICA ---
            # 2. Sla de nieuwe wijziging op in de 'DetectedChange' historietabel
            new_change_entry = DetectedChange(
                timestamp=nu_brussels.astimezone(pytz.utc).replace(tzinfo=None),
                onderwerp=onderwerp,
                content=inhoud
            )
            db.session.add(new_change_entry)
            logging.info("Nieuwe wijziging rij toegevoegd aan 'detected_change' tabel.")
            # --- EINDE AANPASSING ---
            
            # (We updaten de in-memory cache niet meer, dat gebeurt bij de volgende 'home' laadbeurt)
        else:
            logging.info("No relevant changes found.")
    else:
        logging.info("First run, establishing baseline.")
    
    # --- Scheduled Reporting (Logica behouden, actie verwijderd) ---
    report_times = [(1,0), (4, 0), (5, 30), (9,0), (12, 0), (13, 30), (17,0), (20, 0), (21, 30)]
    tijdstip_voor_rapport = None
    
    for report_hour, report_minute in report_times:
        rapport_tijd_vandaag = nu_brussels.replace(hour=report_hour, minute=report_minute, second=0, microsecond=0)
        if nu_brussels >= rapport_tijd_vandaag:
            tijdstip_voor_rapport = rapport_tijd_vandaag
        
    if tijdstip_voor_rapport:
        current_key = tijdstip_voor_rapport.strftime('%Y-%m-%d-%H:%M')
        if current_key != last_report_key:
            logging.info(f"Time for scheduled report of {tijdstip_voor_rapport.strftime('%H:%M')}. (No email will be sent).")
            last_report_key = current_key
        
    # --- Save State for Next Run ---
    # Sla alleen de data op die nodig is voor de *volgende* vergelijking
    state_for_comparison = {
        "bestellingen": nieuwe_bestellingen, 
        "last_report_key": last_report_key
    }
    save_state_for_comparison(state_for_comparison)
    
    # --- FINALE COMMIT ---
    # Sla alle wijzigingen (Snapshot, DetectedChange, KeyValueStore) in één keer op
    try:
        db.session.commit()
        logging.info("--- Run Completed, state saved to DB. ---")
    except Exception as e:
        logging.error(f"FATAL ERROR during final DB commit: {e}")
        db.session.rollback()

# if __name__ == '__main__':
#     app.run(debug=True, port=5001)
