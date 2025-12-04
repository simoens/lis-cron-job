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

with app.app_context():
    db.create_all()

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
    latest_snapshot_obj = Snapshot.query.order_by(Snapshot.timestamp.desc()).first()
    if latest_snapshot_obj:
        brussels_time = pytz.utc.localize(latest_snapshot_obj.timestamp).astimezone(pytz.timezone('Europe/Brussels'))
        return jsonify({"timestamp": brussels_time.strftime('%d-%m-%Y %H:%M:%S')})
    else:
        with data_lock:
            return jsonify({"timestamp": app_state["latest_snapshot"].get("timestamp", "N/A")})

@app.route('/logboek')
@basic_auth.required 
def logboek():
    search_term = request.args.get('q', '')
    all_changes_query = DetectedChange.query.all()
    ship_regex = re.compile(r"^(?:[+]{3} NIEUW SCHIP: |[-]{3} VERWIJDERD: )?'([^']+)'", re.MULTILINE)
    unique_ships = set()
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
    formatted_changes = []
    for change in changes_db_objects:
        formatted_changes.append({
            "timestamp": pytz.utc.localize(change.timestamp).astimezone(pytz.timezone('Europe/Brussels')).strftime('%d-%m-%Y %H:%M:%S'),
            "onderwerp": change.onderwerp,
            "content": change.content
        })
    return render_template('logboek.html', changes=formatted_changes, search_term=search_term, all_ships=all_ships_sorted, secret_key=os.environ.get('SECRET_KEY'))

@app.route('/statistieken')
@basic_auth.required 
def statistieken():
    brussels_tz = pytz.timezone('Europe/Brussels')
    nu_brussels = datetime.now(brussels_tz)
    seven_days_ago_utc = nu_brussels.astimezone(pytz.utc).replace(tzinfo=None) - timedelta(days=7)
    changes = DetectedChange.query.filter(DetectedChange.timestamp >= seven_days_ago_utc).order_by(DetectedChange.timestamp.asc()).all() 
    snapshots = Snapshot.query.filter(Snapshot.timestamp >= seven_days_ago_utc).all()
    
    total_changes = len(changes)
    total_incoming = 0
    total_outgoing = 0
    total_shifting = 0
    if snapshots:
        latest_snapshot_data = snapshots[-1].content_data
        total_incoming = len(latest_snapshot_data.get('INKOMEND', []))
        total_outgoing = len(latest_snapshot_data.get('UITGAAND', []))
        total_shifting = len(latest_snapshot_data.get('VERPLAATSING', [])) 
        
    ship_counter = Counter()
    ship_regex_top5 = re.compile(r"^(?:[+]{3} NIEUW SCHIP: |[-]{3} VERWIJDERD: )?'([^']+)'", re.MULTILINE)
    for change in changes:
        if change.content:
            ship_names = ship_regex_top5.findall(change.content)
            for name in ship_names:
                ship_counter[name.strip()] += 1
    top_ships = ship_counter.most_common(5)

    ship_first_time = {}
    ship_final_time = {}
    nieuw_besteltijd_regex = re.compile(r"- Besteltijd: ([^\n]+)")
    gewijzigd_besteltijd_regex = re.compile(r"- Besteltijd: '([^']*)' -> '([^']*)'")
    ship_name_regex = re.compile(r"^(?:[+]{3} NIEUW SCHIP: )?'([^']+)'", re.MULTILINE)

    alle_uitgaande_logs = DetectedChange.query.filter(DetectedChange.timestamp >= seven_days_ago_utc, DetectedChange.type == 'U').order_by(DetectedChange.timestamp.asc()).all()

    for log in alle_uitgaande_logs:
        ship_name_match = ship_name_regex.search(log.content)
        if not ship_name_match: continue
        ship_name = ship_name_match.group(1).strip()
        if "+++ NIEUW SCHIP:" in log.content:
            besteltijd_match = nieuw_besteltijd_regex.search(log.content)
            if besteltijd_match:
                tijd_obj = parse_besteltijd(besteltijd_match.group(1).strip())
                if tijd_obj.year != 1970:
                    if ship_name not in ship_first_time: ship_first_time[ship_name] = tijd_obj
                    ship_final_time[ship_name] = tijd_obj 
        elif "'" in log.content: 
            besteltijd_match = gewijzigd_besteltijd_regex.search(log.content)
            if besteltijd_match:
                oud_tijd = parse_besteltijd(besteltijd_match.group(1))
                nieuw_tijd = parse_besteltijd(besteltijd_match.group(2))
                if oud_tijd.year != 1970 and nieuw_tijd.year != 1970:
                    if ship_name not in ship_first_time: ship_first_time[ship_name] = oud_tijd 
                    ship_final_time[ship_name] = nieuw_tijd 

    vervroegd_lijst = []
    vertraagd_lijst = []
    op_tijd_lijst = [] 
    for ship_name, eerste_tijd in ship_first_time.items():
        laatste_tijd = ship_final_time.get(ship_name, eerste_tijd) 
        delta_seconds = (laatste_tijd - eerste_tijd).total_seconds()
        hours, remainder = divmod(abs(delta_seconds), 3600)
        minutes, _ = divmod(remainder, 60)
        delta_str = f"{int(hours)}u {int(minutes)}m"
        result_data = {"ship_name": ship_name, "eerste_tijd": eerste_tijd.strftime('%d/%m %H:%M'), "laatste_tijd": laatste_tijd.strftime('%d/%m %H:%M'), "delta_str": delta_str, "delta_raw": delta_seconds}
        if delta_seconds > 0: vertraagd_lijst.append(result_data)
        elif delta_seconds < 0: vervroegd_lijst.append(result_data)
        else: op_tijd_lijst.append(result_data)

    vervroegd_lijst.sort(key=lambda x: abs(x['delta_raw']), reverse=True)
    vertraagd_lijst.sort(key=lambda x: abs(x['delta_raw']), reverse=True)

    stats = {"total_changes": total_changes, "total_incoming": total_incoming, "total_outgoing": total_outgoing, "total_shifting": total_shifting, "top_ships": top_ships, "vervroegd": vervroegd_lijst, "vertraagd": vertraagd_lijst, "op_tijd": op_tijd_lijst}
    return render_template('statistieken.html', stats=stats, secret_key=os.environ.get('SECRET_KEY'))

# --- HELPER FUNCTIES & OMGEVINGSVARIABELEN ---
USER = os.environ.get('LIS_USER')
PASS = os.environ.get('LIS_PASS')

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

# --- NIEUW: PARSE TABEL HELPER MET RTA FIX ---
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
                
                # RTA FIX: Als tekst leeg is, zoek naar 'title' attribuut (hover tekst)
                if k == "RTA" and not value:
                    if cell.has_attr('title'):
                        value = cell['title']
                    else:
                        child_with_title = cell.find(lambda tag: tag.has_attr('title'))
                        if child_with_title:
                            value = child_with_title['title']

                if k == "Loods" and "[Loods]" in value: value = "Loods werd toegewezen"
                bestelling[k] = value
                
        if 11 < len(kolom_data):
            schip_cel = kolom_data[11]
            link_tag = schip_cel.find('a', href=re.compile(r'Reisplan\.aspx\?ReisId='))
            if link_tag:
                match = re.search(r'ReisId=(\d+)', link_tag['href'])
                if match: bestelling['ReisId'] = match.group(1)
        bestellingen.append(bestelling)
    return bestellingen

def haal_bestellingen_op(session):
    logging.info("--- Running haal_bestellingen_op (v56, Input Field Reader) ---")
    try:
        base_page_url = "https://lis.loodswezen.be/Lis/Loodsbestellingen.aspx"
        
        # Gewoon de pagina ophalen. De default view is met 'Eigen reizen' AAN (dus alleen MSC).
        get_response = session.get(base_page_url)
        get_response.raise_for_status()
        soup = BeautifulSoup(get_response.content, 'lxml')
        
        alle_bestellingen = parse_table_from_soup(soup)
        
        # Paginering afhandelen
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

# --- FUNCTIE OM REISPLAN TE LEZEN (MET INPUT FIELDS) ---
def haal_reisplan_details(session, reis_id):
    """Scant de cellen op inhoud (Text OF Input Value)."""
    if not reis_id: return {}
    try:
        url = f"https://lis.loodswezen.be/Lis/Reisplan.aspx?ReisId={reis_id}"
        response = session.get(url, timeout=10)
        soup = BeautifulSoup(response.content, 'lxml')
        
        found_times = {'ATA': [], 'PTA': [], 'GTA': []}
        
        tabel = None
        for t in soup.find_all('table'):
            if "Besteltijd" in t.get_text():
                tabel = t
                break
        
        if not tabel: return {}

        rows = tabel.find_all('tr')
        if not rows: return {}
        
        # Vind kolom index
        target_index = -1
        header_cells = rows[0].find_all(['td', 'th'])
        for i, cell in enumerate(header_cells):
            txt = cell.get_text(strip=True)
            if "Saeftinghe" in txt and "Zandvliet" in txt:
                target_index = i
                break
            elif "Deurganckdok" in txt and target_index == -1:
                 target_index = i

        if target_index == -1: return {}

        # Scan alle rijen
        for row in rows[1:]:
            cells = row.find_all('td')
            if len(cells) > target_index:
                cell = cells[target_index]
                
                # 1. Probeer tekst (zoals "PTA")
                label_text = cell.get_text(strip=True)
                
                # 2. Probeer Input Value (de tijd)
                input_val = ""
                inp = cell.find('input')
                if inp and inp.get('value'):
                    input_val = inp.get('value').strip()
                
                # Combineer ze (zodat we "PTA 04/12..." hebben)
                # Als de tekst al in de label zit (zonder input), is dat ook goed
                full_text = label_text + " " + input_val
                full_text = full_text.strip()

                if full_text:
                    # Check type
                    if "ATA" in full_text:
                        found_times['ATA'].append(full_text)
                    elif "PTA" in full_text:
                        found_times['PTA'].append(full_text)
                    elif "GTA" in full_text:
                        found_times['GTA'].append(full_text)

        return found_times

    except Exception as e:
        logging.error(f"Error in haal_reisplan_details voor {reis_id}: {e}")
        return {}

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

                    # STAP 1: REISPLAN (PRIORITEIT LOGICA)
                    tijden_dict = haal_reisplan_details(session, b.get('ReisId'))
                    raw_tijd = None
                    
                    # Prio 1: ATA (Werkelijk)
                    if tijden_dict.get('ATA'):
                        raw_tijd = tijden_dict['ATA'][-1] # Laatste update
                    # Prio 2: PTA (Gepland)
                    elif tijden_dict.get('PTA'):
                        raw_tijd = tijden_dict['PTA'][-1]
                    # Prio 3: GTA (Gewenst)
                    elif tijden_dict.get('GTA'):
                        raw_tijd = tijden_dict['GTA'][-1]
                    
                    if raw_tijd:
                        # Verwijder prefixes (PTA, GTA, ATA, OTLB...)
                        clean_tijd = re.sub(r'^(PTA|GTA|ATA|OTLB|GTLB)\s+', '', raw_tijd).strip()
                        b['berekende_eta'] = clean_tijd

                    # STAP 2: RTA KOLOM (Fallback + Regex Fix)
                    if b['berekende_eta'] == 'N/A' or b['berekende_eta'] == '':
                        rta_waarde = b.get('RTA', '').strip()
                        if rta_waarde:
                            # Zoek SA/ZV gevolgd door tijd, negeer alles erna (zoals GTPV)
                            match = re.search(r"SA/ZV\s+(\d{2}/\d{2}\s+\d{2}:\d{2})", rta_waarde)
                            if match:
                                    b['berekende_eta'] = f"CP: {match.group(1)}"
                            else:
                                    # Fallback generiek
                                    match_gen = re.search(r"(\d{2}/\d{2}\s+\d{2}:\d{2})", rta_waarde)
                                    if match_gen: b['berekende_eta'] = match_gen.group(1)
                                    else: b['berekende_eta'] = rta_waarde

                    # STAP 3: BEREKENING
                    if b['berekende_eta'] == 'N/A' or b['berekende_eta'] == '':
                        eta_dt = None
                        if besteltijd_aware: 
                            if "wandelaar" in entry_point: eta_dt = besteltijd_aware + timedelta(hours=6)
                            elif "steenbank" in entry_point: eta_dt = besteltijd_aware + timedelta(hours=7)
                            if eta_dt: b['berekende_eta'] = eta_dt.strftime("%d/%m/%y %H:%M")
                    
                    gefilterd["INKOMEND"].append(b)
            
            elif schip_type == "V": 
                if is_sluisplanning: show_ship = True
                elif besteltijd_aware and (nu <= besteltijd_aware <= grens_verplaatsing_toekomst): show_ship = True
                if show_ship: gefilterd["VERPLAATSING"].append(b)
                    
        except (ValueError, TypeError) as e:
            logging.warning(f"Fout bij verwerken schip: {e}")
            continue
            
    # --- NIEUW: LOG HET AANTAL GEFILTERDE SCHEPEN ---
    aantal_inkomend = len(gefilterd.get("INKOMEND", []))
    aantal_uitgaand = len(gefilterd.get("UITGAAND", []))
    aantal_shifting = len(gefilterd.get("VERPLAATSING", []))
    totaal_mpet = aantal_inkomend + aantal_uitgaand + aantal_shifting
    
    logging.info(f"FILTER RESULTAAT: Van de {len(bestellingen)} totaal gevonden schepen, zijn er {totaal_mpet} relevant voor MPET.")
    logging.info(f"   - Inkomend: {aantal_inkomend}")
    logging.info(f"   - Uitgaand: {aantal_uitgaand}")
    logging.info(f"   - Shifting: {aantal_shifting}")

    gefilterd["INKOMEND"].sort(key=lambda x: parse_besteltijd(x.get('Besteltijd')))
    gefilterd["UITGAAND"].sort(key=lambda x: parse_besteltijd(x.get('Besteltijd')))
    gefilterd["VERPLAATSING"].sort(key=lambda x: parse_besteltijd(x.get('Besteltijd')))
    
    return gefilterd

def filter_dubbele_schepen(bestellingen):
    unieke_schepen = {}
    for b in bestellingen:
        schip_naam_raw = b.get('Schip')
        if not schip_naam_raw: continue
        schip_naam_gekuist = re.sub(r'\s*\(d\)\s*$', '', schip_naam_raw).strip()
        if schip_naam_gekuist in unieke_schepen:
            try:
                huidige_tijd = parse_besteltijd(b.get("Besteltijd"))
                opgeslagen_tijd = parse_besteltijd(unieke_schepen[schip_naam_gekuist].get("Besteltijd"))
                if huidige_tijd > opgeslagen_tijd: unieke_schepen[schip_naam_gekuist] = b
            except (ValueError, TypeError): unieke_schepen[schip_naam_gekuist] = b
        else: unieke_schepen[schip_naam_gekuist] = b
    return list(unieke_schepen.values())

def vergelijk_bestellingen(oude, nieuwe):
    oude_dict = {re.sub(r'\s*\(d\)\s*$', '', b.get('Schip', '')).strip(): b for b in filter_dubbele_schepen(oude) if b.get('Schip')}
    nieuwe_dict = {re.sub(r'\s*\(d\)\s*$', '', b.get('Schip', '')).strip(): b for b in filter_dubbele_schepen(nieuwe) if b.get('Schip')}
    oude_schepen_namen = set(oude_dict.keys())
    nieuwe_schepen_namen = set(nieuwe_dict.keys())
    wijzigingen = []
    nu_brussels = datetime.now(pytz.timezone('Europe/Brussels')) 
    brussels_tz = pytz.timezone('Europe/Brussels')
    
    def moet_rapporteren(bestelling):
        besteltijd_str_raw = bestelling.get('Besteltijd', '').strip()
        besteltijd_naive = parse_besteltijd(besteltijd_str_raw)
        if besteltijd_naive.year == 1970: return False
        besteltijd = brussels_tz.localize(besteltijd_naive)
        type_schip = bestelling.get('Type')
        is_mpet = False
        kw = ['mpet']
        entry = bestelling.get('Entry Point', '').lower()
        exit_p = bestelling.get('Exit Point', '').lower()
        if type_schip == 'I' and any(k in exit_p for k in kw): is_mpet = True
        elif type_schip == 'U' and any(k in entry for k in kw): is_mpet = True
        elif type_schip == 'V' and (any(k in entry for k in kw) or any(k in exit_p for k in kw)): is_mpet = True
        if not is_mpet: return False

        if type_schip == 'I': return (nu_brussels - timedelta(hours=8) <= besteltijd <= nu_brussels + timedelta(hours=8))
        elif type_schip == 'U': return (nu_brussels <= besteltijd <= nu_brussels + timedelta(hours=16))
        elif type_schip == 'V': return (nu_brussels <= besteltijd <= nu_brussels + timedelta(hours=8))
        return False
        
    for schip_naam in (nieuwe_schepen_namen - oude_schepen_namen):
        n_best = nieuwe_dict[schip_naam] 
        if moet_rapporteren(n_best): wijzigingen.append({'Schip': n_best.get('Schip'), 'status': 'NIEUW', 'details': n_best })
    for schip_naam in (oude_schepen_namen - nieuwe_schepen_namen):
        o_best = oude_dict[schip_naam]
        if moet_rapporteren(o_best): wijzigingen.append({'Schip': o_best.get('Schip'), 'status': 'VERWIJDERD', 'details': o_best})
    for schip_naam in (nieuwe_schepen_namen.intersection(oude_schepen_namen)):
        n_best = nieuwe_dict[schip_naam] 
        o_best = oude_dict[schip_naam]
        if n_best.get('Type') != o_best.get('Type'): continue
        diff = {k: {'oud': o_best.get(k, ''), 'nieuw': v} for k, v in n_best.items() if v != o_best.get(k, '')}
        if diff:
            relevante = {'Besteltijd', 'Loods'}
            if relevante.intersection(set(diff.keys())): 
                if moet_rapporteren(n_best) or moet_rapporteren(o_best):
                    wijzigingen.append({'Schip': n_best.get('Schip'), 'status': 'GEWIJZIGD', 'wijzigingen': diff, 'type': n_best.get('Type')})
    return wijzigingen

def format_wijzigingen_email(wijzigingen):
    body = []
    wijzigingen.sort(key=lambda x: x.get('status', ''))
    type_vertaling = {'I': 'Inkomend', 'U': 'Uitgaand', 'V': 'Shifting', '?': '?'}
    for w in wijzigingen:
        s_naam = re.sub(r'\s*\(d\)\s*$', '', w.get('Schip', '')).strip()
        status = w.get('status')
        if status == 'NIEUW':
            details = w.get('details', {})
            type_text = type_vertaling.get(details.get('Type', '?'), '?')
            tekst = f"+++ NIEUW SCHIP: '{s_naam}'\n - Type: {type_text}\n - Besteltijd: {details.get('Besteltijd', 'N/A')}\n - Loods: {details.get('Loods', 'N/A')}"
        elif status == 'GEWIJZIGD':
            type_text = type_vertaling.get(w.get('type', '?'), '?')
            tekst = f"'{s_naam}' (Type: {type_text})\n"
            relevante_diffs = []
            for k, v in w['wijzigingen'].items():
                if k == 'Loods':
                    if v['nieuw'] and not v['oud']: relevante_diffs.append(f" - LOODS TOEGEWEZEN")
                    elif not v['nieuw'] and v['oud']: relevante_diffs.append(f" - LOODS VERWIJDERD")
                    else: relevante_diffs.append(f" - Loods: '{v['oud']}' -> '{v['nieuw']}'")
                elif k == 'Besteltijd': relevante_diffs.append(f" - Besteltijd: '{v['oud']}' -> '{v['nieuw']}'")
            tekst += "\n".join(relevante_diffs)
        elif status == 'VERWIJDERD':
            details = w.get('details', {})
            type_text = type_vertaling.get(details.get('Type', '?'), '?')
            tekst = f"--- VERWIJDERD: '{s_naam}'\n - (Was type {type_text}, Besteltijd {details.get('Besteltijd', 'N/A')})"
        body.append(tekst)
    return "\n\n".join(filter(None, body))

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

    if oude_bestellingen:
        wijzigingen = vergelijk_bestellingen(oude_bestellingen, nieuwe_bestellingen)
        if wijzigingen:
            inhoud = format_wijzigingen_email(wijzigingen)
            onderwerp = f"{len(wijzigingen)} wijziging(en)"
            logging.info(f"Found {len(wijzigingen)} changes, logging them to web history.")
            for w in wijzigingen: 
                new_change_entry = DetectedChange(timestamp=nu_brussels.astimezone(pytz.utc).replace(tzinfo=None), onderwerp=onderwerp, content=format_wijzigingen_email([w]), type=w.get('details', {}).get('Type') if w.get('status') == 'NIEUW' else w.get('type'))
                db.session.add(new_change_entry)
            logging.info(f"{len(wijzigingen)} nieuwe wijziging rijen toegevoegd aan 'detected_change' tabel.")
        else: logging.info("No relevant changes found.")
    else: logging.info("First run, establishing baseline.")
        
    state_for_comparison = {"bestellingen": nieuwe_bestellingen, "last_report_key": vorige_staat.get("last_report_key", "")}
    save_state_for_comparison(state_for_comparison)
    try:
        db.session.commit()
        logging.info("--- Run Completed, state saved to DB. ---")
    except Exception as e:
        logging.error(f"FATAL ERROR during final DB commit: {e}")
        db.session.rollback()

if __name__ == '__main__':
    app.run(debug=True, port=5001)
