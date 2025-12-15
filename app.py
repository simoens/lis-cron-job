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
from werkzeug.middleware.proxy_fix import ProxyFix 

# --- CONFIGURATIE ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

USER = os.environ.get('LIS_USER')
PASS = os.environ.get('LIS_PASS')

app_state = {
    "latest_snapshot": {"timestamp": "Nog niet uitgevoerd", "content_data": {"INKOMEND": [], "UITGAAND": [], "VERPLAATSING": []}}, 
    "change_history": deque(maxlen=10)
}
data_lock = threading.Lock() 

app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)

# Database Fix
uri = os.environ.get('DATABASE_URL')
if uri and uri.startswith("postgres://"):
    uri = uri.replace("postgres://", "postgresql://", 1)

app.config['SQLALCHEMY_DATABASE_URI'] = uri
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['BASIC_AUTH_USERNAME'] = 'mpet'
app.config['BASIC_AUTH_PASSWORD'] = os.environ.get('ADMIN_PASSWORD') or 'Mpet2025!' 
app.config['BASIC_AUTH_FORCE'] = False 

basic_auth = BasicAuth(app)
db = SQLAlchemy(app)

# --- MODELLEN ---
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

# Init DB
try:
    with app.app_context():
        db.create_all()
except Exception as e:
    logging.warning(f"DB Init Warning: {e}")

# --- HELPER FUNCTIES ---

def parse_besteltijd(besteltijd_str):
    """Probeert datums te parsen, tolerant voor formaten."""
    DEFAULT_TIME = datetime(1970, 1, 1) 
    if not besteltijd_str: return DEFAULT_TIME
    
    clean_str = re.sub(r'\s+', ' ', besteltijd_str.strip())
    
    # Probeer verschillende formaten
    for fmt in ["%d/%m/%y %H:%M", "%d/%m/%Y %H:%M"]:
        try:
            return datetime.strptime(clean_str, fmt)
        except ValueError:
            continue
    return DEFAULT_TIME

def login(session):
    try:
        logging.info("Login attempt started...")
        headers = {"User-Agent": "Mozilla/5.0"}
        session.headers.update(headers)
        
        r = session.get("https://lis.loodswezen.be/Lis/Login.aspx", timeout=30)
        soup = BeautifulSoup(r.content, 'lxml')
        
        data = {
            '__VIEWSTATE': soup.find('input', {'name': '__VIEWSTATE'})['value'],
            'ctl00$ContentPlaceHolder1$login$uname': USER,
            'ctl00$ContentPlaceHolder1$login$password': PASS,
            'ctl00$ContentPlaceHolder1$login$btnInloggen': 'Inloggen'
        }
        val = soup.find('input', {'name': '__EVENTVALIDATION'})
        if val: data['__EVENTVALIDATION'] = val['value']
            
        r = session.post("https://lis.loodswezen.be/Lis/Login.aspx", data=data, timeout=30)
        return "Login.aspx" not in r.url
    except Exception as e:
        logging.error(f"Error during login: {e}")
        return False

# --- CORE SCRAPING LOGIC ---

def get_form_inputs(soup):
    d = {}
    for i in soup.find_all('input'):
        if i.get('name'): d[i['name']] = i.get('value', '')
    if 'ctl00$ContentPlaceHolder1$ctl01$select$betrokken' in d: 
        del d['ctl00$ContentPlaceHolder1$ctl01$select$betrokken']
    return d

def scan_text_for_eta_strict(text):
    """
    Zoekt STRIKT binnen de bewegingen-tabel naar MPET/Deurganckdok tijden.
    """
    # 1. Zoek het begin van de bewegingen tabel om ruis te filteren
    # We zoeken naar de ID string die in de HTML van de tabel zit
    table_marker = "ctl00_ContentPlaceHolder1_ctl01_data_tabContainer_pnl2_tabpage_bewegingen_list_gv"
    start_pos = text.find(table_marker)
    
    if start_pos == -1:
        # Fallback: zoek naar 'Locatie' en 'ETA' dicht bij elkaar
        match_header = re.search(r"Locatie.*?ETA.*?ATA", text)
        if match_header:
            start_pos = match_header.start()
        else:
            return None # Geen tabel, dus stop meteen (voorkomt foute matches)

    # Werk alleen met de tekst VANAF de tabel
    relevant_text = text[start_pos:]
    
    matches = []
    # Zoek naar MPET/Deurganckdok
    # Pattern: Locatie ... (binnen 150 chars) ... Datum
    for m in re.finditer(r"(Deurganckdok|MPET)", relevant_text, re.IGNORECASE):
        # Pak een kleine snippet NA de locatie (max 150 chars = genoeg voor datum, kort genoeg om op de regel te blijven)
        snippet = relevant_text[m.end():m.end()+150]
        
        # Zoek datums
        dates = re.findall(r"(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2})", snippet)
        
        if dates:
            # Als we datums vinden op deze regel, pak de laatste (ATA staat rechts van ETA)
            matches.append(dates[-1])
            
    if matches:
        # Pak de allerlaatste rij die we vonden (onderste rij in tabel)
        raw_time = matches[-1]
        
        # Format cleanen
        m = re.search(r"(\d{2}/\d{2})/\d{4}\s+(\d{2}:\d{2})", raw_time)
        if m: return f"{m.group(1)} {m.group(2)}"
        return raw_time
        
    return None

def parse_table_from_soup(soup):
    table = soup.find('table', id='ctl00_ContentPlaceHolder1_ctl01_list_gv')
    if not table: return []
    cols = {"Type": 0, "Besteltijd": 5, "ETA/ETD": 6, "RTA": 7, "Loods": 10, "Schip": 11, "Entry Point": 20, "Exit Point": 21}
    res = []
    for row in table.find_all('tr')[1:]: 
        cells = row.find_all('td')
        if not cells: continue
        d = {}
        for k, i in cols.items():
            if i < len(cells):
                val = cells[i].get_text(strip=True)
                if k == "RTA" and not val:
                    if cells[i].has_attr('title'): val = cells[i]['title']
                    else:
                        c = cells[i].find(lambda t: t.has_attr('title'))
                        if c: val = c['title']
                if k=="Loods" and "[Loods]" in val: val="Loods werd toegewezen"
                d[k] = val
        if row.has_attr('onclick'):
            m = re.search(r"value=['\"]?(\d+)['\"]?", row['onclick'])
            if m: d['ReisId'] = m.group(1)
        if 'ReisId' not in d:
             l = row.find('a', href=re.compile(r'Reisplan\.aspx'))
             if l:
                 m = re.search(r'ReisId=(\d+)', l['href'])
                 if m: d['ReisId'] = m.group(1)
        res.append(d)
    return res

def haal_bestellingen_en_details(session):
    logging.info("--- Running haal_bestellingen_en_details (v99: Strict Table Context) ---")
    
    url = "https://lis.loodswezen.be/Lis/Loodsbestellingen.aspx"
    
    # Tijdfilters voor Lazy Load
    brussels_tz = pytz.timezone('Europe/Brussels')
    nu = datetime.now(brussels_tz)
    grens_in = nu + timedelta(hours=8)
    grens_in_verleden = nu - timedelta(hours=8)
    
    try:
        # 1. GET
        resp = session.get(url, timeout=30)
        soup = BeautifulSoup(resp.content, 'lxml')
        
        # 2. Uncheck Eigen Reizen
        d = get_form_inputs(soup)
        d['__EVENTTARGET'] = 'ctl00$ContentPlaceHolder1$ctl01$select$betrokken'
        d['ctl00$ContentPlaceHolder1$ctl01$select$rzn_lbs_vertrektijd_from$txtDate'] = ''
        d['ctl00$ContentPlaceHolder1$ctl01$select$rzn_lbs_vertrektijd_to$txtDate'] = ''
        
        resp = session.post(url, data=d, timeout=30)
        soup = BeautifulSoup(resp.content, 'lxml')
        time.sleep(1)
        
        # 3. Zoeken
        d = get_form_inputs(soup)
        d['ctl00$ContentPlaceHolder1$ctl01$select$richting'] = '/'
        d['ctl00$ContentPlaceHolder1$ctl01$select$btnSearch'] = 'Zoeken'
        if 'ctl00$ContentPlaceHolder1$ctl01$select$agt_naam' in d: del d['ctl00$ContentPlaceHolder1$ctl01$select$agt_naam']
        d['__EVENTTARGET'] = ''
        
        resp = session.post(url, data=d, timeout=30)
        soup = BeautifulSoup(resp.content, 'lxml')
        
        all_ships = []
        page = 1
        max_pages = 15
        
        while page <= max_pages:
            logging.info(f"Pagina {page} verwerken...")
            
            # Lees tabel
            current_ships = parse_table_from_soup(soup)
            
            # --- HAAL DETAILS DIRECT OP ---
            for s in current_ships:
                
                # Check 1: MPET
                is_mpet = False
                e = s.get('Entry Point', '').lower()
                x = s.get('Exit Point', '').lower()
                if 'mpet' in e or 'mpet' in x: is_mpet = True
                
                # Check 2: Tijdvenster (alleen inkomend)
                stype = s.get('Type')
                in_window = False
                if stype == 'I':
                    try:
                        bt = parse_besteltijd(s.get('Besteltijd'))
                        if bt.year > 1970:
                            bt_loc = brussels_tz.localize(bt)
                            if grens_in_verleden <= bt_loc <= grens_in: in_window = True
                    except: pass
                
                # Als Relevant -> Haal details via PostBack
                if is_mpet and in_window and s.get('ReisId'):
                    logging.info(f"Relevant schip: {s['Schip']}. Details ophalen...")
                    
                    try:
                        # Prepareer PostBack voor selectie
                        det_data = get_form_inputs(soup)
                        det_data['__EVENTTARGET'] = 'ctl00$ContentPlaceHolder1$ctl01$list$itemSelectHandler'
                        det_data['__EVENTARGUMENT'] = f'value={s["ReisId"]};text='
                        
                        resp = session.post(url, data=det_data, timeout=20)
                        
                        # Scan RAW response voor ETA (MET STRICTE CHECK)
                        eta = scan_text_for_eta_strict(resp.text)
                        
                        # Als niet gevonden, probeer tab switch
                        if not eta:
                             # logging.info(" -> Tab switch proberen...")
                             soup_temp = BeautifulSoup(resp.content, 'lxml')
                             tab_data = get_form_inputs(soup_temp)
                             tab_data['__EVENTTARGET'] = 'ctl00$ContentPlaceHolder1$ctl01$data$tabContainer'
                             tab_data['__EVENTARGUMENT'] = '2' # Bewegingen tab
                             resp = session.post(url, data=tab_data, timeout=20)
                             eta = scan_text_for_eta_strict(resp.text)

                        if eta:
                            s['Details_ETA'] = eta
                            logging.info(f" -> ETA Gevonden: {eta}")
                        
                        soup = BeautifulSoup(resp.content, 'lxml')
                        
                    except Exception as ex:
                        logging.error(f"Fout bij details: {ex}")

            # Voeg toe aan totaal
            all_ships.extend(current_ships)
            
            # Volgende pagina
            next_page = page + 1
            link = soup.find('a', href=re.compile(f"Page\${next_page}"))
            if not link: break
            
            pd = get_form_inputs(soup)
            m = re.search(r"__doPostBack\('([^']+)','([^']+)'\)", link['href'])
            if m:
                pd['__EVENTTARGET'] = m.group(1)
                pd['__EVENTARGUMENT'] = m.group(2)
                resp = session.post(url, data=pd, timeout=30)
                soup = BeautifulSoup(resp.content, 'lxml')
                page += 1
            else: break
            
        return all_ships

    except Exception as e:
        logging.error(f"General error: {e}")
        return []

def filter_snapshot_schepen(bestellingen, session, nu):
    gefilterd = {"INKOMEND": [], "UITGAAND": [], "VERPLAATSING": []}
    
    grens_uit = nu + timedelta(hours=16)
    grens_in_verleden = nu - timedelta(hours=8)
    grens_in = nu + timedelta(hours=8)
    grens_verplaatsing = nu + timedelta(hours=8)
    
    for b in bestellingen:
        # 1. MPET Filter check
        e = b.get('Entry Point', '').lower()
        x = b.get('Exit Point', '').lower()
        if 'mpet' not in e and 'mpet' not in x: continue
        
        stype = b.get("Type")
        besteltijd_str = b.get('Besteltijd')
        
        try:
            bt = parse_besteltijd(besteltijd_str)
            if bt.year == 1970: continue 
            bt = pytz.timezone('Europe/Brussels').localize(bt)
        except: continue
        
        b['berekende_eta'] = 'N/A'
        
        # INKOMEND
        if stype == 'I':
            if grens_in_verleden <= bt <= grens_in:
                # 1. Details (uit scraper)
                if 'Details_ETA' in b:
                    b['berekende_eta'] = f"Bewegingen: {b['Details_ETA']}"
                
                # 2. RTA Fallback
                if b['berekende_eta'] == 'N/A':
                    rta = b.get('RTA', '').strip()
                    if rta:
                        m = re.search(r"(SA/ZV|Deurganckdok)\s*(\d{2}/\d{2}\s+\d{2}:\d{2})", rta, re.IGNORECASE)
                        if m: b['berekende_eta'] = f"RTA (CP): {m.group(2)}"
                        else: b['berekende_eta'] = f"RTA: {rta[:20]}"
                
                # 3. Calc Fallback
                if b['berekende_eta'] == 'N/A':
                     eta = bt + timedelta(hours=6)
                     b['berekende_eta'] = f"Calculated: {eta.strftime('%d/%m/%y %H:%M')}"
                
                gefilterd['INKOMEND'].append(b)
        
        elif stype == 'U':
             if nu <= bt <= grens_uit: gefilterd['UITGAAND'].append(b)
        elif stype == 'V':
             if nu <= bt <= grens_verplaatsing: gefilterd['VERPLAATSING'].append(b)

        # Status
        b['status_flag'] = 'normal'
        diff = (bt - nu).total_seconds()
        if diff < 0: b['status_flag'] = 'past_due'
        elif diff < 3600: b['status_flag'] = 'warning_soon'
        elif diff < 5400: b['status_flag'] = 'due_soon'

    gefilterd["INKOMEND"].sort(key=lambda x: parse_besteltijd(x.get('Besteltijd')))
    return gefilterd

def load_state_for_comparison(): return {}
def save_state_for_comparison(s): pass
def vergelijk_bestellingen(a, b): return []
def format_wijzigingen_email(w): return ""

# --- MAIN ---
def main():
    if not all([USER, PASS]):
        logging.critical("FATAL: Geen user/pass!")
        return
    
    session = requests.Session()
    if not login(session):
        logging.error("Login mislukt.")
        return
    
    nieuwe = haal_bestellingen_en_details(session)
    if not nieuwe:
        logging.error("Geen bestellingen.")
        return
    
    logging.info("Snapshot genereren...")
    nu = datetime.now(pytz.timezone('Europe/Brussels'))
    snapshot_data = filter_snapshot_schepen(nieuwe, session, nu)
    
    try:
        s = Snapshot(timestamp=nu.astimezone(pytz.utc).replace(tzinfo=None), content_data=snapshot_data)
        db.session.add(s)
        db.session.commit()
        with data_lock:
             app_state["latest_snapshot"] = {
                "timestamp": nu.strftime('%d-%m-%Y %H:%M:%S'),
                "content_data": snapshot_data
            }
        logging.info("Snapshot opgeslagen.")
    except Exception as e:
         logging.error(f"DB Error: {e}")
         db.session.rollback()

    save_state_for_comparison({"bestellingen": nieuwe})
    logging.info("--- Run Voltooid ---")

def main_task():
    with app.app_context(): main()

# --- ROUTES ---
@app.route('/')
@basic_auth.required 
def home():
    if app_state["latest_snapshot"]["timestamp"] == "Nog niet uitgevoerd":
        try:
            s = Snapshot.query.order_by(Snapshot.timestamp.desc()).first()
            if s:
                 app_state["latest_snapshot"] = {
                    "timestamp": pytz.utc.localize(s.timestamp).astimezone(pytz.timezone('Europe/Brussels')).strftime('%d-%m-%Y %H:%M:%S'),
                    "content_data": s.content_data
                }
        except: pass
    return render_template('index.html', snapshot=app_state["latest_snapshot"], changes=[], secret_key=os.environ.get('SECRET_KEY'))

@app.route('/force-snapshot', methods=['POST'])
@basic_auth.required
def force_snapshot_route():
    if request.form.get('secret') != os.environ.get('SECRET_KEY'): return jsonify({"status":"error"}), 403
    threading.Thread(target=main_task).start()
    return jsonify({"status": "started"})

@app.route('/trigger-run')
def trigger_run():
    if request.args.get('secret') != os.environ.get('SECRET_KEY'): abort(403)
    threading.Thread(target=main_task).start()
    return "OK", 200

@app.route('/status')
def status_check():
    return jsonify({"timestamp": app_state["latest_snapshot"].get("timestamp", "N/A")})

@app.route('/logboek')
@basic_auth.required
def logboek(): return render_template('logboek.html', changes=[], search_term="", all_ships=[], secret_key=os.environ.get('SECRET_KEY'))

@app.route('/statistieken')
@basic_auth.required
def statistieken(): return render_template('statistieken.html', stats={}, secret_key=os.environ.get('SECRET_KEY'))

if __name__ == '__main__':
    app.run(debug=True, port=5001)
