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

# --- CONFIGURATIE LOGGING ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- ENVIRONMENT VARIABLES (Inloggegevens) ---
USER = os.environ.get('LIS_USER')
PASS = os.environ.get('LIS_PASS')

# --- GLOBALE STATE ---
app_state = {
    "latest_snapshot": {"timestamp": "Nog niet uitgevoerd", "content_data": {"INKOMEND": [], "UITGAAND": [], "VERPLAATSING": []}}, 
    "change_history": deque(maxlen=10)
}
data_lock = threading.Lock() 

# --- FLASK SETUP ---
app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)

# Database Config Fix
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

# DB Init
try:
    with app.app_context():
        db.create_all()
except Exception as e:
    logging.warning(f"DB Init Warning: {e}")

# --- HELPER FUNCTIES ---
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
        
        if "Login.aspx" not in r.url:
            logging.info("LOGIN SUCCESSFUL!")
            return True
        logging.error("Login failed.")
        return False
    except Exception as e:
        logging.error(f"Error during login: {e}")
        return False

# --- PARSING & SCRAPING ---
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
                # RTA Hover fix
                if k == "RTA" and not val:
                    if cells[i].has_attr('title'): val = cells[i]['title']
                    else:
                        c = cells[i].find(lambda t: t.has_attr('title'))
                        if c: val = c['title']
                if k=="Loods" and "[Loods]" in val: val="Loods werd toegewezen"
                d[k] = val
        
        # ID FIX: Onclick + Link fallback
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

def haal_bewegingen_details(session, reis_id):
    """Haalt de tabel Bewegingen op voor MPET-tijden."""
    if not reis_id: return None
    try:
        url = f"https://lis.loodswezen.be/Lis/Bewegingen.aspx?ReisId={reis_id}"
        session.headers.update({'Referer': "https://lis.loodswezen.be/Lis/Loodsbestellingen.aspx"})
        r = session.get(url, timeout=15)
        soup = BeautifulSoup(r.content, 'lxml')
        
        # Zoek tabel
        table = soup.find('table', id='ctl00_ContentPlaceHolder1_ctl01_data_tabContainer_pnl2_tabpage_bewegingen_list_gv')
        if not table:
             # Fallback
             for t in soup.find_all('table'):
                 if "Locatie" in t.get_text() and "ETA" in t.get_text():
                     table = t; break
        
        if not table: return None
        
        # Indexen
        headers = [c.get_text(strip=True) for c in table.find_all('tr')[0].find_all(['th','td'])]
        ix_loc, ix_eta, ix_ata = -1, -1, -1
        for i, h in enumerate(headers):
            if "Locatie" in h: ix_loc = i
            elif "ETA" in h: ix_eta = i
            elif "ATA" in h: ix_ata = i
            
        if ix_loc == -1: return None
        
        target = None
        for row in table.find_all('tr')[1:]:
            cells = row.find_all('td')
            if len(cells) <= ix_loc: continue
            loc = cells[ix_loc].get_text(strip=True)
            
            if "Deurganckdok" in loc or "MPET" in loc:
                eta = cells[ix_eta].get_text(strip=True) if ix_eta > -1 else ""
                ata = cells[ix_ata].get_text(strip=True) if ix_ata > -1 else ""
                val = ata if ata else eta
                if val: target = val
                
        if target:
            m = re.search(r"(\d{2}/\d{2})/\d{4}\s+(\d{2}:\d{2})", target)
            if m: return f"{m.group(1)} {m.group(2)}"
            return target
            
    except: pass
    return None

def haal_bestellingen_op(session):
    logging.info("--- Running haal_bestellingen_op (v82: Stable MSC + Time Filters) ---")
    try:
        # Gewoon de pagina ophalen (Default View = MSC Eigen reizen)
        r = session.get("https://lis.loodswezen.be/Lis/Loodsbestellingen.aspx", timeout=30)
        soup = BeautifulSoup(r.content, 'lxml')
        
        items = parse_table_from_soup(soup)
        
        # Simpele Paginering (tot 5 pagina's)
        page = 1
        while page < 5:
            page += 1
            link = soup.find('a', href=re.compile(f"Page\${page}"))
            if not link: break
            
            logging.info(f"Paginering: {page}")
            fdata = {}
            for h in ['__VIEWSTATE', '__VIEWSTATEGENERATOR', '__EVENTVALIDATION']:
                tag = soup.find('input', {'name': h})
                if tag: fdata[h] = tag.get('value', '')
            
            m = re.search(r"__doPostBack\('([^']+)','([^']+)'\)", link['href'])
            if m:
                fdata['__EVENTTARGET'] = m.group(1)
                fdata['__EVENTARGUMENT'] = m.group(2)
                r = session.post("https://lis.loodswezen.be/Lis/Loodsbestellingen.aspx", data=fdata, timeout=30)
                soup = BeautifulSoup(r.content, 'lxml')
                items.extend(parse_table_from_soup(soup))
            else: break
            
        logging.info(f"Totaal {len(items)} schepen.")
        return items
    except Exception as e:
        logging.error(f"Error: {e}")
        return []

def filter_snapshot_schepen(bestellingen, session, nu):
    gefilterd = {"INKOMEND": [], "UITGAAND": [], "VERPLAATSING": []}
    
    # Tijdsgrenzen (strikte filtering)
    grens_uit = nu + timedelta(hours=16)
    grens_in_verleden = nu - timedelta(hours=8)
    grens_in = nu + timedelta(hours=8)
    grens_verplaatsing = nu + timedelta(hours=8)
    
    for b in bestellingen:
        # MPET Filter
        e = b.get('Entry Point', '').lower()
        x = b.get('Exit Point', '').lower()
        if 'mpet' not in e and 'mpet' not in x: continue
        
        stype = b.get("Type")
        besteltijd_str = b.get('Besteltijd')
        
        # Parse Tijd
        try:
            bt = datetime.strptime(re.sub(r'\s+', ' ', besteltijd_str), "%d/%m/%y %H:%M")
            bt = pytz.timezone('Europe/Brussels').localize(bt)
        except: 
            # Als tijd niet geparst kan worden, skip (of voeg toe als 'unknown' indien gewenst)
            continue
        
        # --- ETA LOGICA (ALLEEN INDIEN NODIG) ---
        # We halen alleen details op als het schip BINNEN het tijdvenster valt,
        # dat spaart de server.
        b['berekende_eta'] = 'N/A'
        in_time_window = False
        
        if stype == 'I':
            if grens_in_verleden <= bt <= grens_in:
                in_time_window = True
                
                # 1. Probeer Bewegingen
                rid = b.get('ReisId')
                if rid:
                    tijd = haal_bewegingen_details(session, rid)
                    if tijd: b['berekende_eta'] = f"Bewegingen: {tijd}"
                
                # 2. Fallback RTA
                if b['berekende_eta'] == 'N/A':
                    rta = b.get('RTA', '').strip()
                    if rta:
                        m = re.search(r"(SA/ZV|Deurganckdok)\s*(\d{2}/\d{2}\s+\d{2}:\d{2})", rta, re.IGNORECASE)
                        if m: b['berekende_eta'] = f"RTA (CP): {m.group(2)}"
                        else: b['berekende_eta'] = f"RTA: {rta}"
                
                # 3. Calc
                if b['berekende_eta'] == 'N/A':
                    calc_eta = bt + timedelta(hours=6)
                    b['berekende_eta'] = f"Calculated: {calc_eta.strftime('%d/%m/%y %H:%M')}"

        elif stype == 'U':
            if nu <= bt <= grens_uit:
                in_time_window = True
                
        elif stype == 'V':
            if nu <= bt <= grens_verplaatsing:
                in_time_window = True

        # Status logic
        b['status_flag'] = 'normal'
        diff = (bt - nu).total_seconds()
        if diff < 0: b['status_flag'] = 'past_due'
        elif diff < 3600: b['status_flag'] = 'warning_soon'
        elif diff < 5400: b['status_flag'] = 'due_soon'

        # Toevoegen aan lijst
        if in_time_window:
            if stype == 'I': gefilterd['INKOMEND'].append(b)
            elif stype == 'U': gefilterd['UITGAAND'].append(b)
            elif stype == 'V': gefilterd['VERPLAATSING'].append(b)

    gefilterd["INKOMEND"].sort(key=lambda x: parse_besteltijd(x.get('Besteltijd')))
    return gefilterd

def vergelijk_bestellingen(a, b): return [] # Placeholder
def format_wijzigingen_email(w): return "" # Placeholder
def load_state(): return {} # Placeholder
def save_state(s): pass # Placeholder

# --- MAIN LOGICA ---
def main():
    logging.info("--- START MAIN ---")
    if not all([USER, PASS]):
        logging.error("Geen inloggegevens!")
        return
        
    session = requests.Session()
    if not login(session): return
    
    data = haal_bestellingen_op(session)
    if not data: return
    
    nu = datetime.now(pytz.timezone('Europe/Brussels'))
    snapshot_data = filter_snapshot_schepen(data, session, nu)
    
    # Save DB
    try:
        s = Snapshot(timestamp=nu.astimezone(pytz.utc).replace(tzinfo=None), content_data=snapshot_data)
        db.session.add(s)
        db.session.commit()
        
        # Update Memory
        with data_lock:
             app_state["latest_snapshot"] = {
                "timestamp": nu.strftime('%d-%m-%Y %H:%M:%S'),
                "content_data": snapshot_data
            }
        logging.info("Snapshot opgeslagen.")
    except Exception as e:
        logging.error(f"DB Error: {e}")
        db.session.rollback()

    save_state_for_comparison({"bestellingen": data})
    logging.info("--- Run Voltooid ---")

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
    if request.form.get('secret') != os.environ.get('SECRET_KEY'): return jsonify({}), 403
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

def main_task():
    with app.app_context(): main()

if __name__ == '__main__':
    app.run(debug=True, port=5001)
