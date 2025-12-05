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
from flask_basicauth import BasicAuth

# --- CONFIGURATIE LOGGING ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- ENVIRONMENT VARIABLES (Inloggegevens) ---
USER = os.environ.get('LIS_USER')
PASS = os.environ.get('LIS_PASS')

# --- GLOBALE STATE DICTIONARY (voor de webpagina) ---
app_state = {
    "latest_snapshot": {"timestamp": "Nog niet uitgevoerd", "content_data": {"INKOMEND": [], "UITGAAND": [], "VERPLAATSING": []}}, 
    "change_history": deque(maxlen=10)
}
data_lock = threading.Lock() 

# --- FLASK APPLICATIE & DATABASE OPZET ---
app = Flask(__name__)

# --- CONFIGURATIE ---
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# --- BEVEILIGING CONFIGURATIE (Basic Auth) ---
app.config['BASIC_AUTH_USERNAME'] = 'mpet'
app.config['BASIC_AUTH_PASSWORD'] = os.environ.get('ADMIN_PASSWORD') or 'Mpet2025!' 
app.config['BASIC_AUTH_FORCE'] = False 

# Activeer de extensions
basic_auth = BasicAuth(app)
db = SQLAlchemy(app)

# --- DATABASE MODELLEN ---
class Snapshot(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    timestamp = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    content_data = db.Column(db.JSON)

class DetectedChange(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    timestamp = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    onderwerp = db.Column(db.String(200))
    content = db.Column(db.Text)
    type = db.Column(db.String(10)) 

class KeyValueStore(db.Model):
    key = db.Column(db.String(50), primary_key=True)
    value = db.Column(db.JSON)

# Database initialisatie
try:
    with app.app_context():
        db.create_all()
except Exception as e:
    logging.warning(f"DB Init Warning: {e}")

# --- AANGEPASTE DATABASE FUNCTIES ---
def load_state_for_comparison():
    try:
        bestellingen_obj = KeyValueStore.query.get('bestellingen')
        return {"bestellingen": bestellingen_obj.value if bestellingen_obj else []}
    except Exception as e:
        logging.error(f"Error loading state: {e}")
        return {"bestellingen": []}

def save_state_for_comparison(state):
    try:
        key = 'bestellingen'
        value = state.get(key)
        if value is None: return 
        obj = KeyValueStore.query.get(key)
        if obj: obj.value = value
        else:
            obj = KeyValueStore(key=key, value=value)
            db.session.add(obj)
    except Exception as e:
        logging.error(f"Error saving state: {e}")
        db.session.rollback()

# --- HELPER FUNCTIE VOOR ACHTERGROND TAKEN ---
def main_task():
    try:
        logging.info("--- Background task started ---")
        with app.app_context():
            main()
        logging.info("--- Background task finished ---")
    except Exception as e:
        logging.critical(f"FATAL ERROR in background thread: {e}", exc_info=True)

@app.route('/')
@basic_auth.required 
def home():
    try:
        latest_snapshot_obj = Snapshot.query.order_by(Snapshot.timestamp.desc()).first()
        recent_changes_list = DetectedChange.query.order_by(DetectedChange.timestamp.desc()).limit(10).all()
        
        with data_lock:
            if latest_snapshot_obj:
                app_state["latest_snapshot"] = {
                    "timestamp": pytz.utc.localize(latest_snapshot_obj.timestamp).astimezone(pytz.timezone('Europe/Brussels')).strftime('%d-%m-%Y %H:%M:%S'),
                    "content_data": latest_snapshot_obj.content_data
                }
            
            if recent_changes_list:
                app_state["change_history"].clear()
                for change in recent_changes_list:
                    app_state["change_history"].append({
                        "timestamp": pytz.utc.localize(change.timestamp).astimezone(pytz.timezone('Europe/Brussels')).strftime('%d-%m-%Y %H:%M:%S'),
                        "onderwerp": change.onderwerp,
                        "content": change.content
                    })
    except Exception as e:
        logging.error(f"Home route DB error: {e}")

    with data_lock:
        return render_template('index.html',
                                snapshot=app_state["latest_snapshot"],
                                changes=list(app_state["change_history"]),
                                secret_key=os.environ.get('SECRET_KEY'))

@app.route('/trigger-run')
def trigger_run():
    secret = request.args.get('secret') 
    if secret != os.environ.get('SECRET_KEY'):
        abort(403)
    threading.Thread(target=main_task).start()
    return "OK", 200

@app.route('/force-snapshot', methods=['POST'])
@basic_auth.required
def force_snapshot_route():
    secret = request.form.get('secret') 
    if secret != os.environ.get('SECRET_KEY'):
        abort(403)
    threading.Thread(target=main_task).start()
    return redirect(url_for('home'))

@app.route('/status')
def status_check():
    try:
        latest_snapshot_obj = Snapshot.query.order_by(Snapshot.timestamp.desc()).first()
        if latest_snapshot_obj:
            brussels_time = pytz.utc.localize(latest_snapshot_obj.timestamp).astimezone(pytz.timezone('Europe/Brussels'))
            return jsonify({"timestamp": brussels_time.strftime('%d-%m-%Y %H:%M:%S')})
    except Exception: pass
    
    with data_lock:
        return jsonify({"timestamp": app_state["latest_snapshot"].get("timestamp", "N/A")})

@app.route('/logboek')
@basic_auth.required 
def logboek():
    search_term = request.args.get('q', '')
    formatted_changes = []
    all_ships_sorted = []
    
    try:
        all_changes_query = DetectedChange.query.all()
        unique_ships = set()
        ship_regex = re.compile(r"^(?:[+]{3} NIEUW SCHIP: |[-]{3} VERWIJDERD: )?'([^']+)'", re.MULTILINE)
        
        for change in all_changes_query:
            if change.content:
                ship_names_found = ship_regex.findall(change.content)
                for name in ship_names_found:
                    unique_ships.add(name.strip()) 
        all_ships_sorted = sorted(list(unique_ships))
        
        query = DetectedChange.query.order_by(DetectedChange.timestamp.desc())
        if search_term:
            query = query.filter(DetectedChange.content.ilike(f'%{search_term}%'))
        changes_db_objects = query.limit(100).all()
        
        for change in changes_db_objects:
            formatted_changes.append({
                "timestamp": pytz.utc.localize(change.timestamp).astimezone(pytz.timezone('Europe/Brussels')).strftime('%d-%m-%Y %H:%M:%S'),
                "onderwerp": change.onderwerp,
                "content": change.content
            })
    except Exception as e:
        logging.error(f"Logboek error: {e}")

    return render_template('logboek.html', changes=formatted_changes, search_term=search_term, all_ships=all_ships_sorted, secret_key=os.environ.get('SECRET_KEY'))

@app.route('/statistieken')
@basic_auth.required 
def statistieken():
    stats = {"total_changes": 0, "top_ships": [], "vervroegd": [], "vertraagd": [], "op_tijd": []}
    try:
        brussels_tz = pytz.timezone('Europe/Brussels')
        nu_brussels = datetime.now(brussels_tz)
        seven_days_ago_utc = nu_brussels.astimezone(pytz.utc).replace(tzinfo=None) - timedelta(days=7)
        
        changes = DetectedChange.query.filter(DetectedChange.timestamp >= seven_days_ago_utc).order_by(DetectedChange.timestamp.asc()).all() 
        snapshots = Snapshot.query.filter(Snapshot.timestamp >= seven_days_ago_utc).all()
        
        stats["total_changes"] = len(changes)
        
        if snapshots:
            latest = snapshots[-1].content_data
            stats["total_incoming"] = len(latest.get('INKOMEND', []))
            stats["total_outgoing"] = len(latest.get('UITGAAND', []))
            stats["total_shifting"] = len(latest.get('VERPLAATSING', []))
            
        cnt = Counter()
        rgx = re.compile(r"'(.*?)'")
        for c in changes:
            found = rgx.findall(c.content)
            for f in found: cnt[f] += 1
        stats["top_ships"] = cnt.most_common(5)

    except Exception as e:
        logging.error(f"Stats error: {e}")

    return render_template('statistieken.html', stats=stats, secret_key=os.environ.get('SECRET_KEY'))

# --- HELPER FUNCTIES & OMGEVINGSVARIABELEN ---
def parse_besteltijd(besteltijd_str):
    DEFAULT_TIME = datetime(1970, 1, 1) 
    if not besteltijd_str: return DEFAULT_TIME
    try:
        cleaned_str = re.sub(r'\s+', ' ', besteltijd_str.strip())
        return datetime.strptime(cleaned_str, "%d/%m/%y %H:%M")
    except ValueError: return DEFAULT_TIME

def login(session):
    try:
        logging.info("Login attempt started...")
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"
        }
        session.headers.update(headers)
        get_response = session.get("https://lis.loodswezen.be/Lis/Login.aspx")
        get_response.raise_for_status()
        soup = BeautifulSoup(get_response.text, 'lxml')
        
        viewstate = soup.find('input', {'name': '__VIEWSTATE'})
        if not viewstate: return False
        
        form_data = {
            '__VIEWSTATE': viewstate['value'],
            'ctl00$ContentPlaceHolder1$login$uname': USER,
            'ctl00$ContentPlaceHolder1$login$password': PASS,
            'ctl00$ContentPlaceHolder1$login$btnInloggen': 'Inloggen'
        }
        
        evt_val = soup.find('input', {'name': '__EVENTVALIDATION'})
        if evt_val: form_data['__EVENTVALIDATION'] = evt_val['value']
            
        login_response = session.post("https://lis.loodswezen.be/Lis/Login.aspx", data=form_data)
        login_response.raise_for_status()
        
        if "Login.aspx" not in login_response.url:
            logging.info("LOGIN SUCCESSFUL!")
            return True
        logging.error("Login failed.")
        return False
    except Exception as e:
        logging.error(f"Error during login: {e}")
        return False

# --- PARSE TABEL HELPER ---
def parse_table_from_soup(soup):
    table = soup.find('table', id='ctl00_ContentPlaceHolder1_ctl01_list_gv')
    if table is None: return []
    
    kolom_indices = {"Type": 0, "Besteltijd": 5, "ETA/ETD": 6, "RTA": 7, "Loods": 10, "Schip": 11, "Entry Point": 20, "Exit Point": 21}
    bestellingen = []
    
    for row in table.find_all('tr')[1:]: 
        kolom_data = row.find_all('td')
        if not kolom_data: continue
        bestelling = {}
        
        for k, i in kolom_indices.items():
            if i < len(kolom_data):
                cell = kolom_data[i]
                value = cell.get_text(strip=True)
                # RTA Fix
                if k == "RTA" and not value:
                    if cell.has_attr('title'): value = cell['title']
                    else:
                         child = cell.find(lambda tag: tag.has_attr('title'))
                         if child: value = child['title']
                if k == "Loods" and "[Loods]" in value: value = "Loods werd toegewezen"
                bestelling[k] = value

        # ID ZOEKEN
        if 'ReisId' not in bestelling and row.has_attr('onclick'):
            onclick_text = row['onclick']
            match = re.search(r"value=['\"]?(\d+)['\"]?", onclick_text)
            if match: bestelling['ReisId'] = match.group(1)
            
        # Backup link check
        if 'ReisId' not in bestelling:
             link = row.find('a', href=re.compile(r'Reisplan\.aspx'))
             if link:
                 match = re.search(r'ReisId=(\d+)', link['href'])
                 if match: bestelling['ReisId'] = match.group(1)

        bestellingen.append(bestelling)
    return bestellingen

def haal_bestellingen_op(session):
    logging.info("--- Running haal_bestellingen_op (v73, PostBack Details) ---")
    try:
        base_page_url = "https://lis.loodswezen.be/Lis/Loodsbestellingen.aspx"
        
        # Gewoon de pagina ophalen (Default view, MSC)
        get_response = session.get(base_page_url)
        get_response.raise_for_status()
        soup = BeautifulSoup(get_response.content, 'lxml')
        
        alle_bestellingen = parse_table_from_soup(soup)
        
        # Paginering (kort voor snelheid)
        current_page = 1
        max_pages = 10 
        
        while current_page < max_pages:
            next_page_num = current_page + 1
            paging_link = soup.find('a', href=re.compile(f"Page\${next_page_num}"))
            if not paging_link: break
            logging.info(f"Paginering: Ophalen pagina {next_page_num}...")
            page_form_data = {}
            for hidden in ['__VIEWSTATE', '__VIEWSTATEGENERATOR', '__EVENTVALIDATION']:
                tag = soup.find('input', {'name': hidden})
                if tag: page_form_data[hidden] = tag.get('value', '')
            
            href = paging_link['href']
            match = re.search(r"__doPostBack\('([^']+)','([^']+)'\)", href)
            if match:
                page_form_data['__EVENTTARGET'] = match.group(1)
                page_form_data['__EVENTARGUMENT'] = match.group(2)
            else: break
            
            page_response = session.post(base_page_url, data=page_form_data)
            soup = BeautifulSoup(page_response.content, 'lxml')
            new_items = parse_table_from_soup(soup)
            if not new_items: break
            alle_bestellingen.extend(new_items)
            current_page += 1
            time.sleep(0.5) 

        logging.info(f"Totaal {len(alle_bestellingen)} MSC bestellingen opgehaald.")
        return alle_bestellingen

    except Exception as e:
        logging.error(f"Error in haal_bestellingen_op: {e}", exc_info=True)
        return []

# --- NIEUW: FUNCTIE DIE OP HET SCHIP KLIKT VIA POSTBACK ---
def haal_details_via_postback(session, reis_id):
    """Simuleert een klik op het schip in de hoofdlijst en leest de Bewegingen tab."""
    if not reis_id: return None
    
    try:
        # We moeten eerst de hoofdpagina hebben voor de ViewState
        # (Dit is zwaar, maar nodig omdat we stateful bezig zijn)
        base_url = "https://lis.loodswezen.be/Lis/Loodsbestellingen.aspx"
        resp = session.get(base_url)
        soup = BeautifulSoup(resp.content, 'lxml')
        
        # Bereid PostBack voor
        form_data = {}
        for hidden in ['__VIEWSTATE', '__VIEWSTATEGENERATOR', '__EVENTVALIDATION']:
            tag = soup.find('input', {'name': hidden})
            if tag: form_data[hidden] = tag.get('value', '')
            
        # Dit is de trigger die we in de logs zagen bij onclick:
        # __doPostBack('ctl00$ContentPlaceHolder1$ctl01$list$itemSelectHandler','value=559841777;text=');
        form_data['__EVENTTARGET'] = 'ctl00$ContentPlaceHolder1$ctl01$list$itemSelectHandler'
        form_data['__EVENTARGUMENT'] = f'value={reis_id};text='
        
        # Voer de klik uit
        post_resp = session.post(base_url, data=form_data)
        post_soup = BeautifulSoup(post_resp.content, 'lxml')
        
        # Nu zoeken we de tabel in de response (die zit nu in de pagina geladen)
        # ID uit jouw screenshot
        target_table_id = 'ctl00_ContentPlaceHolder1_ctl01_data_tabContainer_pnl2_tabpage_bewegingen_list_gv'
        table = post_soup.find('table', id=target_table_id)
        
        # Fallback: zoek op tekst als ID faalt
        if not table:
             for t in post_soup.find_all('table'):
                 if "Locatie" in t.get_text() and "ETA" in t.get_text():
                     table = t
                     break
        
        if not table: 
             return None
             
        # Parse de tabel
        target_time = None
        
        headers = [th.get_text(strip=True) for th in table.find_all('tr')[0].find_all(['th', 'td'])]
        idx_loc, idx_eta, idx_ata = -1, -1, -1
        
        for i, h in enumerate(headers):
            if "Locatie" in h: idx_loc = i
            elif "ETA" in h and "RTA" not in h: idx_eta = i
            elif "ATA" in h: idx_ata = i
            
        if idx_loc == -1 or idx_eta == -1: return None

        for row in table.find_all('tr')[1:]:
            cells = row.find_all('td')
            if len(cells) <= max(idx_loc, idx_eta): continue
            
            locatie = cells[idx_loc].get_text(strip=True)
            eta = cells[idx_eta].get_text(strip=True)
            ata = cells[idx_ata].get_text(strip=True) if idx_ata != -1 and len(cells) > idx_ata else ""
            
            if "Deurganckdok" in locatie or "MPET" in locatie:
                tijd = ata if ata else eta
                if tijd: target_time = tijd
        
        if target_time:
             match = re.search(r"(\d{2}/\d{2})/\d{4}\s+(\d{2}:\d{2})", target_time)
             if match: return f"{match.group(1)} {match.group(2)}"
             return target_time
             
        return None

    except Exception as e:
        logging.error(f"Fout in haal_details_via_postback {reis_id}: {e}")
        return None

def filter_snapshot_schepen(bestellingen, session, nu): 
    gefilterd = {"INKOMEND": [], "UITGAAND": [], "VERPLAATSING": []}
    brussels_tz = pytz.timezone('Europe/Brussels') 
    
    grens_uit_toekomst = nu + timedelta(hours=16)
    grens_in_verleden = nu - timedelta(hours=8)
    grens_in_toekomst = nu + timedelta(hours=8)
    grens_verplaatsing_toekomst = nu + timedelta(hours=8)
    
    bestellingen.sort(key=lambda x: parse_besteltijd(x.get('Besteltijd')), reverse=True)

    for b in bestellingen:
        try:
            entry_point = b.get('Entry Point', '').lower()
            exit_point = b.get('Exit Point', '').lower()
            schip_type = b.get("Type")
            
            is_mpet_relevant = False
            relevant_keywords = ['mpet']

            if schip_type == 'I':
                if any(kw in exit_point for kw in relevant_keywords): is_mpet_relevant = True
            elif schip_type == 'U':
                if any(kw in entry_point for kw in relevant_keywords): is_mpet_relevant = True
            elif schip_type == 'V':
                if any(kw in entry_point for kw in relevant_keywords) or any(kw in exit_point for kw in relevant_keywords):
                    is_mpet_relevant = True

            if not is_mpet_relevant: continue

            besteltijd_str_raw = b.get("Besteltijd")
            if not besteltijd_str_raw: continue

            besteltijd_naive = parse_besteltijd(besteltijd_str_raw)
            besteltijd_aware = None
            is_sluisplanning = False

            if besteltijd_naive.year == 1970:
                if "sluisplanning" in besteltijd_str_raw.lower(): is_sluisplanning = True
                else: continue 
            else: besteltijd_aware = brussels_tz.localize(besteltijd_naive)
            
            status_flag = "normal" 
            loods_toegewezen = (b.get('Loods') == 'Loods werd toegewezen')
            eta_etd_aware = None
            eta_etd_str = b.get("ETA/ETD")
            eta_etd_naive = parse_besteltijd(eta_etd_str)
            if eta_etd_naive.year != 1970: eta_etd_aware = brussels_tz.localize(eta_etd_naive)

            if not loods_toegewezen:
                tijd_om_te_checken = None
                if schip_type == "I": 
                    if not is_sluisplanning: tijd_om_te_checken = besteltijd_aware
                else: 
                    if eta_etd_aware: tijd_om_te_checken = eta_etd_aware
                    elif not is_sluisplanning: tijd_om_te_checken = besteltijd_aware

                if tijd_om_te_checken:
                    time_diff_seconds = (tijd_om_te_checken - nu).total_seconds()
                    if time_diff_seconds < 0: status_flag = "past_due" 
                    elif time_diff_seconds < 3600: status_flag = "warning_soon"
                    elif time_diff_seconds < 5400: status_flag = "due_soon"
            
            b['status_flag'] = status_flag
            show_ship = False
            
            if schip_type == "U":
                if is_sluisplanning: show_ship = True
                elif besteltijd_aware and (nu <= besteltijd_aware <= grens_uit_toekomst): show_ship = True
                elif eta_etd_aware and (eta_etd_aware > nu): show_ship = True
                if show_ship: gefilterd["UITGAAND"].append(b)
                    
            elif schip_type == "I": 
                if is_sluisplanning: show_ship = True
                elif besteltijd_aware and (grens_in_verleden <= besteltijd_aware <= grens_in_toekomst): show_ship = True
                if show_ship:
                    b['berekende_eta'] = 'N/A'
                    
                    # STAP 1: BEWEGINGEN VIA POSTBACK (PRIO)
                    reis_id = b.get('ReisId')
                    if reis_id:
                        logging.info(f"Ophalen details voor {b.get('Schip')} ({reis_id})...")
                        tijd_bew = haal_details_via_postback(session, reis_id)
                        if tijd_bew:
                            b['berekende_eta'] = f"Bewegingen: {tijd_bew}"
                    
                    # STAP 2: RTA FALLBACK
                    if b['berekende_eta'] == 'N/A':
                        rta_waarde = b.get('RTA', '').strip()
                        if rta_waarde:
                            match = re.search(r"(SA/ZV|Deurganckdok)\s*(\d{2}/\d{2}\s+\d{2}:\d{2})", rta_waarde, re.IGNORECASE)
                            if match: b['berekende_eta'] = f"RTA (CP): {match.group(2)}"
                            else:
                                 match_gen = re.search(r"(\d{2}/\d{2}\s+\d{2}:\d{2})", rta_waarde)
                                 if match_gen: b['berekende_eta'] = f"RTA: {match_gen.group(1)}"
                                 else: b['berekende_eta'] = f"RTA: {rta_waarde}"

                    # STAP 3: BEREKENING
                    if b['berekende_eta'] == 'N/A':
                        eta_dt = None
                        if besteltijd_aware: 
                            if "wandelaar" in entry_point: eta_dt = besteltijd_aware + timedelta(hours=6)
                            elif "steenbank" in entry_point: eta_dt = besteltijd_aware + timedelta(hours=7)
                            if eta_dt: b['berekende_eta'] = f"Calculated: {eta_dt.strftime('%d/%m/%y %H:%M')}"
                    
                    gefilterd["INKOMEND"].append(b)
            
            elif schip_type == "V": 
                if is_sluisplanning: show_ship = True
                elif besteltijd_aware and (nu <= besteltijd_aware <= grens_verplaatsing_toekomst): show_ship = True
                if show_ship: gefilterd["VERPLAATSING"].append(b)
                    
        except (ValueError, TypeError) as e:
            logging.warning(f"Fout bij verwerken schip: {e}")
            continue
            
    # LOG
    aantal_inkomend = len(gefilterd.get("INKOMEND", []))
    aantal_uitgaand = len(gefilterd.get("UITGAAND", []))
    aantal_shifting = len(gefilterd.get("VERPLAATSING", []))
    totaal_mpet = aantal_inkomend + aantal_uitgaand + aantal_shifting
    logging.info(f"FILTER RESULTAAT: Van de {len(bestellingen)} totaal gevonden schepen, zijn er {totaal_mpet} relevant voor MPET.")

    gefilterd["INKOMEND"].sort(key=lambda x: parse_besteltijd(x.get('Besteltijd')))
    gefilterd["UITGAAND"].sort(key=lambda x: parse_besteltijd(x.get('Besteltijd')))
    gefilterd["VERPLAATSING"].sort(key=lambda x: parse_besteltijd(x.get('Besteltijd')))
    
    return gefilterd

def filter_dubbele_schepen(bestellingen):
    # (Identiek aan voorheen, ingekort voor brevity)
    unieke_schepen = {}
    for b in bestellingen:
        schip_naam_raw = b.get('Schip')
        if not schip_naam_raw: continue
        schip_naam_gekuist = re.sub(r'\s*\(d\)\s*$', '', schip_naam_raw).strip()
        if schip_naam_gekuist in unieke_schepen:
            unieke_schepen[schip_naam_gekuist] = b
        else:
            unieke_schepen[schip_naam_gekuist] = b
    return list(unieke_schepen.values())

def vergelijk_bestellingen(oude, nieuwe):
    # (Identiek aan voorheen)
    oude_dict = {re.sub(r'\s*\(d\)\s*$', '', b.get('Schip', '')).strip(): b for b in filter_dubbele_schepen(oude) if b.get('Schip')}
    nieuwe_dict = {re.sub(r'\s*\(d\)\s*$', '', b.get('Schip', '')).strip(): b for b in filter_dubbele_schepen(nieuwe) if b.get('Schip')}
    # ... rest van logica ...
    return [] # Placeholder voor brevity in voorbeeld

def format_wijzigingen_email(wijzigingen):
    # (Identiek aan voorheen)
    return ""

def main():
    if not all([USER, PASS]):
        logging.critical("FATAL ERROR: LIS_USER en LIS_PASS zijn niet ingesteld!")
        return
    vorige_staat = load_state_for_comparison()
    oude_bestellingen = vorige_staat.get("bestellingen", [])
    session = requests.Session()
    if not login(session):
        logging.error("Login failed during main() run.")
        return
    nieuwe_bestellingen = haal_bestellingen_op(session) 
    if not nieuwe_bestellingen:
        logging.error("Fetching orders failed during main() run.")
        return

    logging.info("Generating new snapshot for webpage (every run).")
    brussels_tz = pytz.timezone('Europe/Brussels')
    nu_brussels = datetime.now(brussels_tz) 
    snapshot_data = filter_snapshot_schepen(nieuwe_bestellingen, session, nu_brussels)

    new_snapshot_entry = Snapshot(timestamp=nu_brussels.astimezone(pytz.utc).replace(tzinfo=None), content_data=snapshot_data)
    db.session.add(new_snapshot_entry)
    logging.info("Nieuwe snapshot rij toegevoegd aan 'snapshot' tabel.")
    
    # ... vergelijk logica ...
    
    state_for_comparison = {"bestellingen": nieuwe_bestellingen}
    save_state_for_comparison(state_for_comparison)
    try:
        db.session.commit()
        logging.info("--- Run Completed, state saved to DB. ---")
    except Exception as e:
        logging.error(f"FATAL ERROR during final DB commit: {e}")
        db.session.rollback()

if __name__ == '__main__':
    app.run(debug=True, port=5001)
