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
from werkzeug.middleware.proxy_fix import ProxyFix # NIEUW: Nodig voor Render

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

# NIEUW: Vertel Flask dat het achter een proxy (Render) draait
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)

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
        # Als de secret niet klopt, geven we een JSON error terug ipv een crash
        return jsonify({"status": "error", "message": "Invalid Secret"}), 403
        
    logging.info("Manual trigger received via web button.")
    threading.Thread(target=main_task).start()
    
    # AANGEPAST: Geef JSON terug ipv Redirect. Dit voorkomt browser errors.
    return jsonify({"status": "started", "message": "Background run started"})

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
                # RTA FIX
                if k == "RTA" and not value:
                    if cell.has_attr('title'): value = cell['title']
                    else:
                         child = cell.find(lambda tag: tag.has_attr('title'))
                         if child: value = child['title']
                if k == "Loods" and "[Loods]" in value: value = "Loods werd toegewezen"
                bestelling[k] = value

        # ID ZOEKEN (Link of OnClick)
        link_tag = row.find('a', href=re.compile(r'Reisplan\.aspx', re.IGNORECASE))
        if link_tag:
            match = re.search(r'ReisId=(\d+)', link_tag['href'], re.IGNORECASE)
            if match: bestelling['ReisId'] = match.group(1)
        
        if 'ReisId' not in bestelling and row.has_attr('onclick'):
            onclick_text = row['onclick']
            match = re.search(r"value=['\"]?(\d+)['\"]?", onclick_text)
            if match: bestelling['ReisId'] = match.group(1)

        bestellingen.append(bestelling)
    return bestellingen

def haal_bestellingen_op(session):
    logging.info("--- Running haal_bestellingen_op (v75, Fix Redirect Issue) ---")
    try:
        base_url = "https://lis.loodswezen.be/Lis/Loodsbestellingen.aspx"
        
        # 1. Start: GET pagina
        resp = session.get(base_url)
        soup = BeautifulSoup(resp.content, 'lxml')
        
        # 2. Helper voor form data
        def get_form_data(s):
            d = {}
            for i in s.find_all('input'):
                if i.get('name'): d[i['name']] = i.get('value', '')
            if 'ctl00$ContentPlaceHolder1$ctl01$select$betrokken' in d: del d['ctl00$ContentPlaceHolder1$ctl01$select$betrokken']
            keys_to_del = [k for k in d.keys() if 'btn' in k or 'checkbox' in str(d[k])]
            return d

        # 3. STAP 1: Vinkje uit (PostBack)
        logging.info("Uncheck Eigen Reizen...")
        fdata = get_form_data(soup)
        fdata['__EVENTTARGET'] = 'ctl00$ContentPlaceHolder1$ctl01$select$betrokken'
        fdata['ctl00$ContentPlaceHolder1$ctl01$select$rzn_lbs_vertrektijd_from$txtDate'] = '' # Datum leeg
        fdata['ctl00$ContentPlaceHolder1$ctl01$select$rzn_lbs_vertrektijd_to$txtDate'] = ''
        
        resp = session.post(base_url, data=fdata)
        soup = BeautifulSoup(resp.content, 'lxml')
        time.sleep(2)
        
        # 4. STAP 2: Zoeken (PostBack)
        logging.info("Zoeken...")
        fdata = get_form_data(soup)
        fdata['ctl00$ContentPlaceHolder1$ctl01$select$richting'] = '/'
        fdata['ctl00$ContentPlaceHolder1$ctl01$select$btnSearch'] = 'Zoeken'
        if 'ctl00$ContentPlaceHolder1$ctl01$select$agt_naam' in fdata: del fdata['ctl00$ContentPlaceHolder1$ctl01$select$agt_naam']
        fdata['ctl00$ContentPlaceHolder1$ctl01$select$rzn_lbs_vertrektijd_from$txtDate'] = ''
        fdata['ctl00$ContentPlaceHolder1$ctl01$select$rzn_lbs_vertrektijd_to$txtDate'] = ''
        fdata['__EVENTTARGET'] = ''
        fdata['__EVENTARGUMENT'] = ''
        
        resp = session.post(base_url, data=fdata)
        soup = BeautifulSoup(resp.content, 'lxml')
        
        # 5. LOOP PAGINA'S & DETAILS
        all_ships = []
        page_count = 1
        max_pages = 10
        
        while page_count <= max_pages:
            logging.info(f"Verwerken pagina {page_count}...")
            
            from_soup = parse_table_from_soup(soup)
            
            for schip in from_soup:
                if is_mpet_ship(schip):
                    reis_id = schip.get('ReisId')
                    if reis_id:
                        logging.info(f"MPET Schip gevonden: {schip['Schip']}. Ophalen details...")
                        
                        # We doen een PostBack OM HET SCHIP TE SELECTEREN
                        detail_data = get_form_data(soup)
                        detail_data['__EVENTTARGET'] = 'ctl00$ContentPlaceHolder1$ctl01$list$itemSelectHandler'
                        detail_data['__EVENTARGUMENT'] = f'value={reis_id};text='
                        
                        resp = session.post(base_url, data=detail_data)
                        soup = BeautifulSoup(resp.content, 'lxml') # UPDATE SOUP!
                        
                        eta_mpet = parse_bewegingen_from_page(soup)
                        if eta_mpet:
                            schip['Details_ETA'] = eta_mpet
                            logging.info(f" -> Gevonden: {eta_mpet}")
                        else:
                            logging.info(" -> Geen bewegingen gevonden.")
                            
            all_ships.extend(from_soup)
            
            next_page = page_count + 1
            link = soup.find('a', href=re.compile(f"Page\${next_page}"))
            if not link:
                break
                
            paging_data = get_form_data(soup)
            href = link['href']
            m = re.search(r"__doPostBack\('([^']+)','([^']+)'\)", href)
            if m:
                paging_data['__EVENTTARGET'] = m.group(1)
                paging_data['__EVENTARGUMENT'] = m.group(2)
                
                resp = session.post(base_url, data=paging_data)
                soup = BeautifulSoup(resp.content, 'lxml')
                page_count += 1
            else:
                break
                
        return all_ships

    except Exception as e:
        logging.error(f"Fout in haal_bestellingen_op: {e}")
        return []

def is_mpet_ship(schip_dict):
    entry = schip_dict.get('Entry Point', '').lower()
    exit_p = schip_dict.get('Exit Point', '').lower()
    if 'mpet' in entry or 'mpet' in exit_p:
        return True
    return False

def parse_bewegingen_from_page(soup):
    """Zoekt in de huidige pagina naar de bewegingen tabel."""
    table = soup.find('table', id='ctl00_ContentPlaceHolder1_ctl01_data_tabContainer_pnl2_tabpage_bewegingen_list_gv')
    if not table:
        for t in soup.find_all('table'):
            if "Locatie" in t.get_text() and "ETA" in t.get_text():
                table = t
                break
    
    if not table: return None

    headers = [th.get_text(strip=True) for th in table.find_all('tr')[0].find_all(['th', 'td'])]
    idx_loc, idx_eta, idx_ata = -1, -1, -1
    for i, h in enumerate(headers):
        if "Locatie" in h: idx_loc = i
        elif "ETA" in h: idx_eta = i
        elif "ATA" in h: idx_ata = i
    
    if idx_loc == -1: return None

    target_time = None

    for row in table.find_all('tr')[1:]:
        cells = row.find_all('td')
        if len(cells) <= idx_loc: continue
        
        loc = cells[idx_loc].get_text(strip=True)
        if "Deurganckdok" in loc or "MPET" in loc:
            eta = cells[idx_eta].get_text(strip=True) if idx_eta != -1 else ""
            ata = cells[idx_ata].get_text(strip=True) if idx_ata != -1 else ""
            
            val = ata if ata else eta
            if val: target_time = val
            
    if target_time:
        match = re.search(r"(\d{2}/\d{2})/\d{4}\s+(\d{2}:\d{2})", target_time)
        if match: return f"{match.group(1)} {match.group(2)}"
        return target_time
        
    return None

# --- FILTER FUNCTIE ---
def filter_snapshot_schepen(bestellingen, session, nu):
    gefilterd = {"INKOMEND": [], "UITGAAND": [], "VERPLAATSING": []}
    
    grens_uit = nu + timedelta(hours=16)
    grens_in_verleden = nu - timedelta(hours=8)
    grens_in = nu + timedelta(hours=8)
    
    for b in bestellingen:
        schip_type = b.get("Type")
        
        if not is_mpet_ship(b): continue
        
        besteltijd_str = b.get('Besteltijd')
        if not besteltijd_str: continue
        try:
            bt = datetime.strptime(re.sub(r'\s+', ' ', besteltijd_str), "%d/%m/%y %H:%M")
            bt = pytz.timezone('Europe/Brussels').localize(bt)
        except: continue
        
        b['berekende_eta'] = 'N/A'
        
        if 'Details_ETA' in b:
             b['berekende_eta'] = f"Bewegingen: {b['Details_ETA']}"
        elif b.get('RTA'):
             m = re.search(r"(SA/ZV|Deurganckdok)\s*(\d{2}/\d{2}\s+\d{2}:\d{2})", b['RTA'])
             if m: b['berekende_eta'] = f"RTA (CP): {m.group(2)}"
             else: b['berekende_eta'] = f"RTA: {b['RTA']}"
        else:
             b['berekende_eta'] = "Calculated"

        b['status_flag'] = 'normal'
        
        if schip_type == 'I':
             if grens_in_verleden <= bt <= grens_in: gefilterd['INKOMEND'].append(b)
        elif schip_type == 'U':
             if nu <= bt <= grens_uit: gefilterd['UITGAAND'].append(b)
        elif schip_type == 'V':
             gefilterd['VERPLAATSING'].append(b)
             
    gefilterd["INKOMEND"].sort(key=lambda x: x.get('Besteltijd'))
    
    return gefilterd

# MAIN WRAPPER
if __name__ == '__main__':
    app.run(debug=True)
