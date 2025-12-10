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

# --- 3. HELPER FUNCTIES ---

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

# --- DE NIEUWE KERN: RAW REGEX SCANNER ---
def extract_eta_from_raw_text(html_text):
    """
    Zoekt in de ruwe HTML tekst naar MPET/Deurganckdok en de bijbehorende tijden.
    Dit werkt ook als de HTML 'broken' is door partial rendering.
    """
    try:
        # We zoeken naar een tabelrij (tr) die "Deurganckdok" of "MPET" bevat
        # En dan pakken we de cellen (td) die volgen.
        
        # Regex uitleg:
        # 1. Zoek naar 'Deurganckdok' of 'MPET'
        # 2. Zoek daarna naar de volgende <td> tags (dit zijn de tijden)
        # We pakken een ruimere chunk tekst rondom de match om te analyseren
        
        # Stap 1: Vind alle locaties van "Deurganckdok" of "MPET"
        matches = [m.start() for m in re.finditer(r"(Deurganckdok|MPET)", html_text)]
        
        last_valid_time = None
        
        for pos in matches:
            # Pak een stuk tekst na de match (bijv. 500 tekens)
            snippet = html_text[pos:pos+1000]
            
            # De structuur is meestal: Locatie </td> <td> ETA </td> <td> RTA </td> <td> ATA </td>
            # We zoeken naar patronen die lijken op een datum/tijd: dd/mm/yyyy HH:MM
            
            time_matches = re.findall(r"(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2})", snippet)
            
            if time_matches:
                # We hebben tijden gevonden in deze rij!
                # De logica: ATA is meestal de laatste of 3e, ETA de 1e.
                # Maar we willen gewoon *een* tijd.
                # Als er meerdere zijn: ATA staat rechts van ETA.
                # Dus we pakken de LAATSTE tijd die we vinden in deze rij, dat is vaak de ATA (indien aanwezig) of anders de ETA.
                
                # Check of er een ATA is (3e kolom vaak). 
                # Eigenlijk, als we gewoon de laatste datum/tijd pakken die in de buurt staat, zitten we vaak goed.
                
                candidate = time_matches[-1] # Pak de laatste gevonden tijd in de snippet
                
                # Format: dd/mm/yyyy HH:MM -> dd/mm HH:MM
                m = re.search(r"(\d{2}/\d{2})/\d{4}\s+(\d{2}:\d{2})", candidate)
                if m:
                    last_valid_time = f"{m.group(1)} {m.group(2)}"
        
        return last_valid_time

    except Exception as e:
        logging.error(f"Regex scan error: {e}")
        return None

def haal_bestellingen_op(session):
    logging.info("--- Running haal_bestellingen_op (v87: Raw Regex) ---")
    try:
        url = "https://lis.loodswezen.be/Lis/Loodsbestellingen.aspx"
        
        # 1. Start
        resp = session.get(url, timeout=30)
        soup = BeautifulSoup(resp.content, 'lxml')
        
        def get_inputs(s):
            d = {}
            for i in s.find_all('input'):
                if i.get('name'): d[i['name']] = i.get('value', '')
            if 'ctl00$ContentPlaceHolder1$ctl01$select$betrokken' in d: del d['ctl00$ContentPlaceHolder1$ctl01$select$betrokken']
            return d

        # 2. Uncheck Eigen
        logging.info("Uncheck Eigen...")
        d = get_inputs(soup)
        d['__EVENTTARGET'] = 'ctl00$ContentPlaceHolder1$ctl01$select$betrokken'
        d['ctl00$ContentPlaceHolder1$ctl01$select$rzn_lbs_vertrektijd_from$txtDate'] = ''
        d['ctl00$ContentPlaceHolder1$ctl01$select$rzn_lbs_vertrektijd_to$txtDate'] = ''
        
        resp = session.post(url, data=d, timeout=30)
        soup = BeautifulSoup(resp.content, 'lxml')
        time.sleep(1)
        
        # 3. Zoeken
        logging.info("Zoeken...")
        d = get_inputs(soup)
        d['ctl00$ContentPlaceHolder1$ctl01$select$richting'] = '/'
        d['ctl00$ContentPlaceHolder1$ctl01$select$btnSearch'] = 'Zoeken'
        d['ctl00$ContentPlaceHolder1$ctl01$select$rzn_lbs_vertrektijd_from$txtDate'] = ''
        d['ctl00$ContentPlaceHolder1$ctl01$select$rzn_lbs_vertrektijd_to$txtDate'] = ''
        if 'ctl00$ContentPlaceHolder1$ctl01$select$agt_naam' in d: del d['ctl00$ContentPlaceHolder1$ctl01$select$agt_naam']
        d['__EVENTTARGET'] = ''
        
        resp = session.post(url, data=d, timeout=30)
        soup = BeautifulSoup(resp.content, 'lxml')
        
        # 4. Loop & Details
        all_ships = []
        page = 1
        max_pages = 10
        
        while page <= max_pages:
            logging.info(f"Verwerken pagina {page}...")
            current_ships = parse_table_from_soup(soup)
            
            for s in current_ships:
                if is_mpet_ship(s) and s.get('ReisId'):
                    logging.info(f"Detail check voor {s['Schip']} ({s['ReisId']})...")
                    try:
                        # STAP A: Selecteer schip
                        det_data = get_inputs(soup)
                        det_data['__EVENTTARGET'] = 'ctl00$ContentPlaceHolder1$ctl01$list$itemSelectHandler'
                        det_data['__EVENTARGUMENT'] = f'value={s["ReisId"]};text='
                        
                        resp = session.post(url, data=det_data, timeout=20)
                        # Hier gebruiken we de RAW TEXT, geen soup parsing!
                        raw_html = resp.text
                        
                        # STAP B: Check of we op tab bewegingen zitten
                        eta_mpet = extract_eta_from_raw_text(raw_html)
                        
                        if not eta_mpet:
                            logging.info(" -> Switch naar Tab 'Bewegingen'...")
                            # Moet soms expliciet naar tab 2
                            # We moeten de inputs opnieuw halen uit de 'resp' die we net kregen (omdat viewstate update)
                            temp_soup = BeautifulSoup(resp.content, 'lxml')
                            tab_data = get_inputs(temp_soup)
                            
                            tab_data['__EVENTTARGET'] = 'ctl00$ContentPlaceHolder1$ctl01$data$tabContainer'
                            tab_data['__EVENTARGUMENT'] = '2'
                            
                            resp = session.post(url, data=tab_data, timeout=20)
                            raw_html = resp.text
                            
                            eta_mpet = extract_eta_from_raw_text(raw_html)

                        if eta_mpet:
                            s['Details_ETA'] = eta_mpet
                            logging.info(f" -> Gevonden: {eta_mpet}")
                        else:
                            logging.info(" -> Geen tijd gevonden.")

                        # Update soup voor volgende iteratie in loop
                        soup = BeautifulSoup(resp.content, 'lxml')
                            
                    except Exception as ex:
                        logging.error(f"Detail error: {ex}")

            all_ships.extend(current_ships)
            
            next_page = page + 1
            link = soup.find('a', href=re.compile(f"Page\${next_page}"))
            if not link: break
            
            pd = get_inputs(soup)
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
        logging.error(f"Scrape error: {e}")
        return []

def parse_table_from_soup(soup):
    # (Zelfde als v86 - werkt goed)
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

def is_mpet_ship(schip_dict):
    e = schip_dict.get('Entry Point', '').lower()
    x = schip_dict.get('Exit Point', '').lower()
    return 'mpet' in e or 'mpet' in x

def filter_snapshot_schepen(bestellingen, session, nu):
    gefilterd = {"INKOMEND": [], "UITGAAND": [], "VERPLAATSING": []}
    
    # Tijdsgrenzen
    grens_uit = nu + timedelta(hours=16)
    grens_in_verleden = nu - timedelta(hours=8)
    grens_in = nu + timedelta(hours=8)
    grens_verplaatsing = nu + timedelta(hours=8)
    
    for b in bestellingen:
        if not is_mpet_ship(b): continue
        
        stype = b.get("Type")
        besteltijd_str = b.get('Besteltijd')
        try:
            bt = datetime.strptime(re.sub(r'\s+', ' ', besteltijd_str), "%d/%m/%y %H:%M")
            bt = pytz.timezone('Europe/Brussels').localize(bt)
        except: continue
        
        b['berekende_eta'] = 'N/A'
        
        if stype == 'I':
             if 'Details_ETA' in b:
                 b['berekende_eta'] = f"Bewegingen: {b['Details_ETA']}"
             elif b.get('RTA'):
                 m = re.search(r"(SA/ZV|Deurganckdok)\s*(\d{2}/\d{2}\s+\d{2}:\d{2})", b['RTA'], re.IGNORECASE)
                 if m: b['berekende_eta'] = f"RTA (CP): {m.group(2)}"
                 else: b['berekende_eta'] = f"RTA: {b['RTA']}"
             else:
                  eta = bt + timedelta(hours=6)
                  b['berekende_eta'] = f"Calculated: {eta.strftime('%d/%m/%y %H:%M')}"
             
             if grens_in_verleden <= bt <= grens_in: gefilterd['INKOMEND'].append(b)
                 
        elif stype == 'U':
             if nu <= bt <= grens_uit: gefilterd['UITGAAND'].append(b)
        elif stype == 'V':
             if nu <= bt <= grens_verplaatsing: gefilterd['VERPLAATSING'].append(b)
             
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
    logging.info("--- START MAIN ---")
    if not all([USER, PASS]):
        logging.critical("FATAL: Geen user/pass!")
        return
    session = requests.Session()
    if not login(session): return
    data = haal_bestellingen_op(session)
    if not data: return
    logging.info("Snapshot genereren...")
    nu = datetime.now(pytz.timezone('Europe/Brussels'))
    snapshot_data = filter_snapshot_schepen(data, session, nu)
    s = Snapshot(timestamp=nu.astimezone(pytz.utc).replace(tzinfo=None), content_data=snapshot_data)
    try:
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
