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

# --- 1. CONFIGURATIE ---
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

# Database Fix voor Render
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

# --- 2. MODELLEN ---
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

# --- 3. HELPER FUNCTIES & SCRAPERS ---

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
        
        if "Login.aspx" not in r.url:
            logging.info("LOGIN SUCCESSFUL!")
            return True
        logging.error("Login failed.")
        return False
    except Exception as e:
        logging.error(f"Error during login: {e}")
        return False

# --- DE KERN: BEWEGINGEN UITLEZEN MET REGEX ---
def haal_bewegingen_direct_regex(session, reis_id):
    """
    Haalt de specifieke Bewegingen-pagina op voor één schip (Stateless GET).
    Scant de ruwe HTML met regex naar MPET tijden.
    """
    if not reis_id: return None
    try:
        # Directe URL naar de detailpagina van DIT schip
        url = f"https://lis.loodswezen.be/Lis/Bewegingen.aspx?ReisId={reis_id}"
        session.headers.update({'Referer': "https://lis.loodswezen.be/Lis/Loodsbestellingen.aspx"})
        
        r = session.get(url, timeout=20)
        html_text = r.text
        
        # Regex scanner (dezelfde krachtige logica als v99, maar nu op de unieke pagina)
        matches = []
        
        # Zoek naar Deurganckdok of MPET
        # Pak de tekst die volgt (tot 200 chars)
        for m in re.finditer(r"(Deurganckdok|MPET)", html_text, re.IGNORECASE):
            snippet = html_text[m.end():m.end()+200]
            
            # Zoek datums in snippet (dd/mm/yyyy of dd/mm/yy)
            # Tabelvolgorde is Locatie -> ETA -> RTA -> ATA
            dates = re.findall(r"(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2})", snippet)
            if not dates:
                dates = re.findall(r"(\d{2}/\d{2}/\d{2}\s+\d{2}:\d{2})", snippet)
                
            if dates:
                # Pak de laatste datum in de rij (ATA > RTA > ETA)
                matches.append(dates[-1])
        
        if matches:
            # Pak de datum van de LAATSTE rij in de tabel (meest recent)
            raw_time = matches[-1]
            
            # Clean format (dd/mm HH:MM)
            m = re.search(r"(\d{2}/\d{2})/(?:\d{4}|\d{2})\s+(\d{2}:\d{2})", raw_time)
            if m: return f"{m.group(1)} {m.group(2)}"
            return raw_time

        return None
            
    except Exception as e:
        logging.error(f"Fout bij ophalen bewegingen {reis_id}: {e}")
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
        
        # ID FIX
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

def haal_bestellingen_op(session):
    logging.info("--- Running haal_bestellingen_op (v101: Restored Routes) ---")
    try:
        url = "https://lis.loodswezen.be/Lis/Loodsbestellingen.aspx"
        
        # 1. Start: GET pagina
        resp = session.get(url, timeout=30)
        soup = BeautifulSoup(resp.content, 'lxml')
        
        # Helper voor form
        def get_inputs(s):
            d = {}
            for i in s.find_all('input'):
                if i.get('name'): d[i['name']] = i.get('value', '')
            if 'ctl00$ContentPlaceHolder1$ctl01$select$betrokken' in d: del d['ctl00$ContentPlaceHolder1$ctl01$select$betrokken']
            return d

        # 2. STAP 1: Vinkje uit (PostBack) - KRIJG ALLE SCHEPEN
        logging.info("Uncheck Eigen Reizen...")
        d = get_inputs(soup)
        d['__EVENTTARGET'] = 'ctl00$ContentPlaceHolder1$ctl01$select$betrokken'
        d['ctl00$ContentPlaceHolder1$ctl01$select$rzn_lbs_vertrektijd_from$txtDate'] = ''
        d['ctl00$ContentPlaceHolder1$ctl01$select$rzn_lbs_vertrektijd_to$txtDate'] = ''
        
        resp = session.post(url, data=d, timeout=30)
        soup = BeautifulSoup(resp.content, 'lxml')
        time.sleep(1)
        
        # 3. STAP 2: Zoeken
        logging.info("Zoeken...")
        d = get_inputs(soup)
        d['ctl00$ContentPlaceHolder1$ctl01$select$richting'] = '/'
        d['ctl00$ContentPlaceHolder1$ctl01$select$btnSearch'] = 'Zoeken'
        # Agent weg
        if 'ctl00$ContentPlaceHolder1$ctl01$select$agt_naam' in d: del d['ctl00$ContentPlaceHolder1$ctl01$select$agt_naam']
        
        d['ctl00$ContentPlaceHolder1$ctl01$select$rzn_lbs_vertrektijd_from$txtDate'] = ''
        d['ctl00$ContentPlaceHolder1$ctl01$select$rzn_lbs_vertrektijd_to$txtDate'] = ''
        d['__EVENTTARGET'] = ''
        
        resp = session.post(url, data=d, timeout=30)
        soup = BeautifulSoup(resp.content, 'lxml')
        
        # 4. LOOP PAGINA'S
        all_ships = []
        page = 1
        max_pages = 15
        
        while page <= max_pages:
            logging.info(f"Paginering: {page}")
            current_ships = parse_table_from_soup(soup)
            all_ships.extend(current_ships)
            
            page += 1
            link = soup.find('a', href=re.compile(f"Page\${page}"))
            if not link: break
            
            pd = get_inputs(soup)
            if 'ctl00$ContentPlaceHolder1$ctl01$select$agt_naam' in pd: del pd['ctl00$ContentPlaceHolder1$ctl01$select$agt_naam']
            m = re.search(r"__doPostBack\('([^']+)','([^']+)'\)", link['href'])
            if m:
                pd['__EVENTTARGET'] = m.group(1)
                pd['__EVENTARGUMENT'] = m.group(2)
                resp = session.post(url, data=pd, timeout=30)
                soup = BeautifulSoup(resp.content, 'lxml')
            else: break
            
        logging.info(f"Totaal {len(all_ships)} schepen gevonden.")
        return all_ships

    except Exception as e:
        logging.error(f"Scrape error: {e}")
        return []

def is_mpet_ship(schip_dict):
    e = schip_dict.get('Entry Point', '').lower()
    x = schip_dict.get('Exit Point', '').lower()
    return 'mpet' in e or 'mpet' in x

def filter_snapshot_schepen(bestellingen, session, nu):
    gefilterd = {"INKOMEND": [], "UITGAAND": [], "VERPLAATSING": []}
    
    grens_uit = nu + timedelta(hours=16)
    grens_in_verleden = nu - timedelta(hours=8)
    grens_in = nu + timedelta(hours=8)
    grens_verplaatsing = nu + timedelta(hours=8)
    
    details_fetched = 0

    for b in bestellingen:
        # MPET Filter
        if not is_mpet_ship(b): continue
        
        stype = b.get("Type")
        besteltijd_str = b.get('Besteltijd')
        
        try:
            bt = parse_besteltijd(besteltijd_str)
            if bt.year == 1970: continue 
            bt = pytz.timezone('Europe/Brussels').localize(bt)
        except: continue
        
        b['berekende_eta'] = 'N/A'
        
        # INKOMEND: ETA BEPALEN
        if stype == 'I':
             if grens_in_verleden <= bt <= grens_in:
                # 1. Directe Detail Fetch
                rid = b.get('ReisId')
                if rid:
                    time.sleep(0.2) 
                    t = haal_bewegingen_direct_regex(session, rid)
                    if t: 
                        b['berekende_eta'] = f"Bewegingen: {t}"
                        details_fetched += 1
                
                # 2. Fallback RTA (Nu veel losser!)
                if b['berekende_eta'] == 'N/A':
                    rta = b.get('RTA', '').strip()
                    if rta:
                        m = re.search(r"(\d{2}/\d{2}\s+\d{2}:\d{2})", rta)
                        if m: 
                            if "SA/ZV" in rta or "CP" in rta: b['berekende_eta'] = f"RTA (CP): {m.group(1)}"
                            else: b['berekende_eta'] = f"RTA: {m.group(1)}"
                        else: b['berekende_eta'] = f"RTA: {rta[:20]}"
                
                # 3. Calc
                if b['berekende_eta'] == 'N/A':
                     eta = bt + timedelta(hours=6)
                     b['berekende_eta'] = f"Calculated: {eta.strftime('%d/%m/%y %H:%M')}"
                
                gefilterd['INKOMEND'].append(b)
        
        elif stype == 'U':
             if nu <= bt <= grens_uit: gefilterd['UITGAAND'].append(b)
        elif stype == 'V':
             if nu <= bt <= grens_verplaatsing: gefilterd['VERPLAATSING'].append(b)

        b['status_flag'] = 'normal'
        diff = (bt - nu).total_seconds()
        if diff < 0: b['status_flag'] = 'past_due'
        elif diff < 3600: b['status_flag'] = 'warning_soon'
        elif diff < 5400: b['status_flag'] = 'due_soon'

    logging.info(f"Totaal details opgehaald voor {details_fetched} inkomende schepen.")

    gefilterd["INKOMEND"].sort(key=lambda x: parse_besteltijd(x.get('Besteltijd')))
    return gefilterd

def load_state_for_comparison():
    try:
        bestellingen_obj = KeyValueStore.query.get('bestellingen')
        return {"bestellingen": bestellingen_obj.value if bestellingen_obj else []}
    except Exception as e:
        return {"bestellingen": []}

def save_state_for_comparison(state):
    try:
        key = 'bestellingen'
        value = state.get(key)
        if value is None: return 
        obj = KeyValueStore.query.get(key)
        if obj: 
            obj.value = value
        else:
            obj = KeyValueStore(key=key, value=value)
            db.session.add(obj)
    except Exception as e:
        db.session.rollback()

def vergelijk_bestellingen(oude, nieuwe):
    # (Plaats hier je vergelijkingslogica als je die weer wilt activeren)
    return []

def format_wijzigingen_email(w): 
    return ""

# --- MAIN ---
def main():
    if not all([USER, PASS]):
        logging.critical("FATAL: Geen user/pass!")
        return
    
    session = requests.Session()
    if not login(session):
        logging.error("Login mislukt.")
        return
    
    nieuwe = haal_bestellingen_op(session)
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
    except: pass
    
    return render_template('logboek.html', changes=formatted_changes, search_term=search_term, all_ships=all_ships_sorted, secret_key=os.environ.get('SECRET_KEY'))

@app.route('/statistieken')
@basic_auth.required
def statistieken():
    stats = {"total_changes": 0, "top_ships": [], "vervroegd": [], "vertraagd": [], "op_tijd": []}
    try:
        brussels_tz = pytz.timezone('Europe/Brussels')
        seven_days_ago_utc = datetime.now(brussels_tz).astimezone(pytz.utc).replace(tzinfo=None) - timedelta(days=7)
        
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

    except: pass
    return render_template('statistieken.html', stats=stats, secret_key=os.environ.get('SECRET_KEY'))

if __name__ == '__main__':
    app.run(debug=True, port=5001)
