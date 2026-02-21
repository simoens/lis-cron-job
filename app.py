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
from urllib.parse import quote

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
    max_retries = 3
    for attempt in range(max_retries):
        try:
            logging.info(f"Login attempt {attempt + 1} started...")
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
            session.headers.update(headers)
            
            r = session.get("https://lis.loodswezen.be/Lis/Login.aspx", timeout=30)
            soup = BeautifulSoup(r.content, 'lxml')
            
            # Veilig zoeken naar de ASP.NET velden
            vs_field = soup.find('input', {'name': '__VIEWSTATE'})
            ev_field = soup.find('input', {'name': '__EVENTVALIDATION'})

            if not vs_field:
                logging.warning(f"Poging {attempt + 1}: Kon __VIEWSTATE niet vinden. HTTP Status: {r.status_code}")
                # Print de eerste 500 karakters van de pagina om te zien wat we wél krijgen
                snippet = r.text[:500].replace('\n', ' ') 
                logging.warning(f"Pagina inhoud (snippet): {snippet}...")
                time.sleep(5) 
                continue

            data = {
                '__VIEWSTATE': vs_field['value'],
                'ctl00$ContentPlaceHolder1$login$uname': USER,
                'ctl00$ContentPlaceHolder1$login$password': PASS,
                'ctl00$ContentPlaceHolder1$login$btnInloggen': 'Inloggen'
            }
            if ev_field:
                data['__EVENTVALIDATION'] = ev_field['value']
                
            # Kleine pauze om menselijk gedrag na te bootsen
            time.sleep(1)
            
            r = session.post("https://lis.loodswezen.be/Lis/Login.aspx", data=data, timeout=30)
            
            if "Login.aspx" not in r.url:
                logging.info("LOGIN SUCCESSFUL!")
                return True
            else:
                logging.error("Login mislukt: Check je LIS_USER en LIS_PASS.")
                return False # Bij foute credentials heeft retry geen zin

        except Exception as e:
            logging.error(f"Fout tijdens login poging {attempt + 1}: {e}")
            time.sleep(5)
            
    return False

# --- DE KERN: IMO & BEWEGINGEN ---

def haal_imo_nummer(session, soup_met_details):
    imo = None
    try:
        html_str = str(soup_met_details)
        match = re.search(r"DcDataPage\.aspx\?control=schepen.*?ID=(\d+)", html_str)
        
        if match:
            schepen_db_id = match.group(1)
            detail_url = f"https://lis.loodswezen.be/Lis/Framework/DcDataPage.aspx?control=schepen&ID={schepen_db_id}"
            
            r = session.get(detail_url, timeout=10)
            imo_soup = BeautifulSoup(r.content, 'lxml')
            
            for inp in imo_soup.find_all('input'):
                if 'imo' in inp.get('name', '').lower() or 'imo' in inp.get('id', '').lower():
                    val = inp.get('value', '').strip()
                    if val and val.isdigit() and len(val) == 7:
                        imo = val
                        break
            
            if not imo:
                text_matches = re.findall(r"IMO\s*[:\.]?\s*(\d{7})", r.text, re.IGNORECASE)
                if text_matches:
                    imo = text_matches[0]
                    
            if imo: logging.info(f" -> IMO Gevonden: {imo}")

    except Exception as e:
        logging.warning(f"Fout bij IMO scrapa: {e}")
        
    return imo

def haal_bewegingen_direct_regex(session, reis_id):
    if not reis_id: return None
    try:
        url = f"https://lis.loodswezen.be/Lis/Bewegingen.aspx?ReisId={reis_id}"
        session.headers.update({'Referer': "https://lis.loodswezen.be/Lis/Loodsbestellingen.aspx"})
        r = session.get(url, timeout=20)
        html_text = r.text
        
        matches = []
        for m in re.finditer(r"(Deurganckdok|MPET)", html_text, re.IGNORECASE):
            snippet = html_text[m.end():m.end()+200]
            dates = re.findall(r"(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2})", snippet)
            if not dates: dates = re.findall(r"(\d{2}/\d{2}/\d{2}\s+\d{2}:\d{2})", snippet)
            if dates: matches.append(dates[-1])
        
        if matches:
            raw_time = matches[-1]
            m = re.search(r"(\d{2}/\d{2})/(?:\d{4}|\d{2})\s+(\d{2}:\d{2})", raw_time)
            if m: return f"{m.group(1)} {m.group(2)}"
            return raw_time
        return None
    except: return None

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
    logging.info("--- Running haal_bestellingen_en_details (v110: Relevant Changes Only) ---")
    url = "https://lis.loodswezen.be/Lis/Loodsbestellingen.aspx"
    
    brussels_tz = pytz.timezone('Europe/Brussels')
    nu = datetime.now(brussels_tz)
    
    # Tijdsgrenzen voor fetch optimalisatie (ruimer dan display filters om randgevallen te pakken)
    grens_in = nu + timedelta(hours=8)
    grens_in_verleden = nu - timedelta(hours=8)
    
    try:
        resp = session.get(url, timeout=30)
        soup = BeautifulSoup(resp.content, 'lxml')
        
        def get_inputs(s):
            d = {}
            for i in s.find_all('input'):
                if i.get('name'): d[i['name']] = i.get('value', '')
            if 'ctl00$ContentPlaceHolder1$ctl01$select$betrokken' in d: del d['ctl00$ContentPlaceHolder1$ctl01$select$betrokken']
            return d

        # Uncheck
        d = get_inputs(soup)
        d['__EVENTTARGET'] = 'ctl00$ContentPlaceHolder1$ctl01$select$betrokken'
        d['ctl00$ContentPlaceHolder1$ctl01$select$rzn_lbs_vertrektijd_from$txtDate'] = ''
        d['ctl00$ContentPlaceHolder1$ctl01$select$rzn_lbs_vertrektijd_to$txtDate'] = ''
        resp = session.post(url, data=d, timeout=30)
        soup = BeautifulSoup(resp.content, 'lxml')
        time.sleep(1)
        
        # Zoeken
        d = get_inputs(soup)
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
            current_ships = parse_table_from_soup(soup)
            
            for s in current_ships:
                # Is het MPET?
                is_mpet = False
                e = s.get('Entry Point', '').lower()
                x = s.get('Exit Point', '').lower()
                if 'mpet' in e or 'mpet' in x: is_mpet = True
                
                # Datum check (Inkomend)
                stype = s.get('Type')
                in_window = False
                if stype == 'I':
                    try:
                        bt = parse_besteltijd(s.get('Besteltijd'))
                        if bt.year > 1970:
                            bt_loc = brussels_tz.localize(bt)
                            if grens_in_verleden <= bt_loc <= grens_in: in_window = True
                    except: pass
                
                # Als Relevant -> Haal details via PostBack EN IMO
                if is_mpet and in_window and s.get('ReisId'):
                    logging.info(f"Relevant: {s['Schip']}. Details & IMO ophalen...")
                    try:
                        det_data = get_inputs(soup)
                        det_data['__EVENTTARGET'] = 'ctl00$ContentPlaceHolder1$ctl01$list$itemSelectHandler'
                        det_data['__EVENTARGUMENT'] = f'value={s["ReisId"]};text='
                        
                        resp = session.post(url, data=det_data, timeout=20)
                        soup_with_details = BeautifulSoup(resp.content, 'lxml')
                        
                        imo = haal_imo_nummer(session, soup_with_details)
                        if imo: s['IMO'] = imo

                        eta = haal_bewegingen_direct_regex(session, s.get('ReisId'))
                        if eta: s['Details_ETA'] = eta
                        
                        soup = soup_with_details
                    except Exception as ex:
                        logging.error(f"Fout bij details: {ex}")

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
                page += 1
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

# --- RELEVANTIE CHECKER VOOR FILTERS EN LOGBOEK ---
def is_relevant_for_dashboard(schip, nu):
    """
    Bepaalt of een schip zichtbaar zou zijn op het dashboard op dit moment.
    Gebruikt de strikte tijdsfilters.
    """
    if not is_mpet_ship(schip): return False
    
    stype = schip.get("Type")
    besteltijd_str = schip.get('Besteltijd')
    
    try:
        bt = parse_besteltijd(besteltijd_str)
        if bt.year == 1970: return False
        bt = pytz.timezone('Europe/Brussels').localize(bt)
    except: return False
    
    grens_uit = nu + timedelta(hours=16)
    grens_in_verleden = nu - timedelta(hours=8)
    grens_in = nu + timedelta(hours=8)
    grens_verplaatsing = nu + timedelta(hours=8)
    
    if stype == 'I':
        return grens_in_verleden <= bt <= grens_in
    elif stype == 'U':
        return nu <= bt <= grens_uit
    elif stype == 'V':
        return nu <= bt <= grens_verplaatsing
        
    return False

def filter_snapshot_schepen(bestellingen, session, nu):
    gefilterd = {"INKOMEND": [], "UITGAAND": [], "VERPLAATSING": []}
    
    for b in bestellingen:
        # Check eerst relevantie voor we dure logica doen
        if not is_relevant_for_dashboard(b, nu): continue
        
        stype = b.get("Type")
        
        # URL Logic
        schip_naam = b.get('Schip', '')
        if b.get('IMO'):
             b['vesselfinder_url'] = f"https://www.vesselfinder.com/vessels?name={b['IMO']}"
        else:
             clean_name = re.sub(r'[\s]+', ' ', schip_naam)
             clean_name = re.sub(r'\s*\(d\)\s*', '', clean_name, flags=re.IGNORECASE).strip()
             b['vesselfinder_url'] = f"https://www.vesselfinder.com/vessels?name={quote(clean_name)}"
        
        # ETA Logic
        b['berekende_eta'] = 'N/A'
        
        if stype == 'I':
            if 'Details_ETA' in b:
                b['berekende_eta'] = f"Bewegingen: {b['Details_ETA']}"
            if b['berekende_eta'] == 'N/A':
                rta = b.get('RTA', '').strip()
                if rta:
                    m = re.search(r"(\d{2}/\d{2}\s+\d{2}:\d{2})", rta)
                    if m: 
                        if "SA/ZV" in rta or "CP" in rta: b['berekende_eta'] = f"RTA (CP): {m.group(1)}"
                        else: b['berekende_eta'] = f"RTA: {m.group(1)}"
                    else: b['berekende_eta'] = f"RTA: {rta[:20]}"
            if b['berekende_eta'] == 'N/A':
                 # Herbereken datum voor calc
                 try:
                     bt = parse_besteltijd(b.get('Besteltijd'))
                     bt = pytz.timezone('Europe/Brussels').localize(bt)
                     eta = bt + timedelta(hours=6)
                     b['berekende_eta'] = f"Calculated: {eta.strftime('%d/%m/%y %H:%M')}"
                 except: pass

        # Status Logic
        b['status_flag'] = 'normal'
        try:
            bt = parse_besteltijd(b.get('Besteltijd'))
            bt = pytz.timezone('Europe/Brussels').localize(bt)
            diff = (bt - nu).total_seconds()
            if diff < 0: b['status_flag'] = 'past_due'
            elif diff < 3600: b['status_flag'] = 'warning_soon'
            elif diff < 5400: b['status_flag'] = 'due_soon'
        except: pass

        # Sorteren in bakjes (we weten al dat ze relevant zijn)
        if stype == 'I': gefilterd['INKOMEND'].append(b)
        elif stype == 'U': gefilterd['UITGAAND'].append(b)
        elif stype == 'V': gefilterd['VERPLAATSING'].append(b)

    gefilterd["INKOMEND"].sort(key=lambda x: parse_besteltijd(x.get('Besteltijd')))
    return gefilterd

# --- 4. WIJZIGINGEN DETECTIE (AANGEPAST) ---
def filter_dubbele_schepen(bestellingen):
    unieke_schepen = {}
    for b in bestellingen:
        schip_naam_raw = b.get('Schip')
        if not schip_naam_raw: continue
        schip_naam_gekuist = re.sub(r'\s*\(d\)\s*$', '', schip_naam_raw).strip()
        unieke_schepen[schip_naam_gekuist] = b
    return list(unieke_schepen.values())

def vergelijk_bestellingen(oude, nieuwe, nu):
    """Vergelijkt oude en nieuwe lijst, FILTERT op relevantie voor dashboard."""
    oude_dict = {re.sub(r'\s*\(d\)\s*$', '', b.get('Schip', '')).strip(): b for b in filter_dubbele_schepen(oude) if b.get('Schip')}
    nieuwe_dict = {re.sub(r'\s*\(d\)\s*$', '', b.get('Schip', '')).strip(): b for b in filter_dubbele_schepen(nieuwe) if b.get('Schip')}
    
    oude_names = set(oude_dict.keys())
    nieuwe_names = set(nieuwe_dict.keys())
    
    wijzigingen = []
    
    # Nieuw: Alleen als het direct op dashboard verschijnt
    for name in (nieuwe_names - oude_names):
        schip = nieuwe_dict[name]
        if is_relevant_for_dashboard(schip, nu):
            wijzigingen.append({'Schip': name, 'status': 'NIEUW', 'details': schip})
            
    # Gewijzigd
    for name in (nieuwe_names.intersection(oude_names)):
        n_best = nieuwe_dict[name]
        o_best = oude_dict[name]
        
        # Check of schip relevant is OF was
        # (Als het relevant was en nu niet meer, is het eigenlijk 'verdwenen' van dashboard, maar dat loggen we niet als change per se, tenzij tijd verandert)
        # We loggen alleen als de NIEUWE status relevant is, of als de wijziging het relevant maakt.
        if not is_relevant_for_dashboard(n_best, nu): continue
        
        diff = {}
        for k in ['Besteltijd', 'Loods', 'RTA', 'Details_ETA']:
            # Details_ETA is nieuw, dus even checken
            val_n = n_best.get(k)
            val_o = o_best.get(k)
            if val_n != val_o:
                diff[k] = {'oud': val_o, 'nieuw': val_n}
        if diff:
            wijzigingen.append({'Schip': name, 'status': 'GEWIJZIGD', 'wijzigingen': diff})
            
    return wijzigingen

def format_wijzigingen_email(wijzigingen):
    body = []
    for w in wijzigingen:
        s_naam = w.get('Schip')
        if w['status'] == 'NIEUW':
            d = w['details']
            body.append(f"+++ NIEUW: {s_naam} ({d.get('Type')}) - {d.get('Besteltijd')}")
        elif w['status'] == 'GEWIJZIGD':
            line = f"^^^ WIJZIGING: {s_naam}"
            for k, v in w['wijzigingen'].items():
                line += f"\n  {k}: {v['oud']} -> {v['nieuw']}"
            body.append(line)
    return "\n\n".join(body)

def load_state_for_comparison():
    try:
        bestellingen_obj = KeyValueStore.query.get('bestellingen')
        if bestellingen_obj and bestellingen_obj.value:
            return bestellingen_obj.value
    except Exception as e:
        logging.error(f"DB Load error: {e}")
    return []

def save_state_for_comparison(data):
    try:
        schepen = data.get("bestellingen", [])
        obj = KeyValueStore.query.get('bestellingen')
        if not obj:
            obj = KeyValueStore(key='bestellingen', value=schepen)
            db.session.add(obj)
        else:
            obj.value = schepen
        db.session.commit()
    except Exception as e:
        logging.error(f"DB Save error: {e}")
        db.session.rollback()

# --- MAIN ---
def main():
    if not all([USER, PASS]):
        logging.critical("FATAL: Geen LIS_USER of LIS_PASS gevonden in omgevingsvariabelen!")
        return
    
    oude_bestellingen = load_state_for_comparison()
    
    session = requests.Session()
    if not login(session):
        logging.error("Login definitief mislukt na meerdere pogingen.")
        return
    
    nieuwe = haal_bestellingen_en_details(session)
    if not nieuwe:
        logging.error("Geen bestellingen gevonden na succesvolle login.")
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

    # Change Detection (MET TIJDFILTER)
    if oude_bestellingen:
        wijzigingen = vergelijk_bestellingen(oude_bestellingen, nieuwe, nu)
        if wijzigingen:
            log_text = format_wijzigingen_email(wijzigingen)
            if log_text:
                dc = DetectedChange(timestamp=nu.astimezone(pytz.utc).replace(tzinfo=None), onderwerp=f"{len(wijzigingen)} wijzigingen", content=log_text)
                db.session.add(dc)
                db.session.commit()
                logging.info(f"{len(wijzigingen)} wijzigingen opgeslagen.")
                with data_lock:
                    app_state["change_history"].appendleft({
                        "timestamp": nu.strftime('%d-%m-%Y %H:%M:%S'),
                        "onderwerp": f"{len(wijzigingen)} wijzigingen",
                        "content": log_text
                    })

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
            changes = DetectedChange.query.order_by(DetectedChange.timestamp.desc()).limit(10).all()
            app_state["change_history"] = deque()
            for c in changes:
                app_state["change_history"].append({
                    "timestamp": pytz.utc.localize(c.timestamp).astimezone(pytz.timezone('Europe/Brussels')).strftime('%d-%m-%Y %H:%M:%S'),
                    "onderwerp": c.onderwerp,
                    "content": c.content
                })
        except: pass
    return render_template('index.html', snapshot=app_state["latest_snapshot"], changes=list(app_state["change_history"]), secret_key=os.environ.get('SECRET_KEY'))

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
    query = DetectedChange.query.order_by(DetectedChange.timestamp.desc())
    if search_term:
        query = query.filter(DetectedChange.content.ilike(f'%{search_term}%'))
    
    db_changes = query.limit(50).all()
    formatted = []
    for c in db_changes:
        formatted.append({
            "timestamp": pytz.utc.localize(c.timestamp).astimezone(pytz.timezone('Europe/Brussels')).strftime('%d-%m-%Y %H:%M:%S'),
            "onderwerp": c.onderwerp,
            "content": c.content
        })
        
    all_ships_query = Snapshot.query.order_by(Snapshot.timestamp.desc()).limit(5).all()
    unique_ships = set()
    for s in all_ships_query:
        if s.content_data:
            for cat in s.content_data.values():
                for ship in cat:
                    unique_ships.add(ship.get('Schip'))
    
    return render_template('logboek.html', changes=formatted, search_term=search_term, all_ships=sorted(list(unique_ships)), secret_key=os.environ.get('SECRET_KEY'))

@app.route('/statistieken')
@basic_auth.required
def statistieken():
    stats = {"total_changes": 0, "total_incoming":0, "total_outgoing":0, "total_shifting":0, "top_ships": []}
    try:
        brussels_tz = pytz.timezone('Europe/Brussels')
        seven_days_ago_utc = datetime.now(brussels_tz).astimezone(pytz.utc).replace(tzinfo=None) - timedelta(days=7)
        
        changes = DetectedChange.query.filter(DetectedChange.timestamp >= seven_days_ago_utc).count()
        stats["total_changes"] = changes
        
        s = Snapshot.query.order_by(Snapshot.timestamp.desc()).first()
        if s and s.content_data:
            stats["total_incoming"] = len(s.content_data.get('INKOMEND', []))
            stats["total_outgoing"] = len(s.content_data.get('UITGAAND', []))
            stats["total_shifting"] = len(s.content_data.get('VERPLAATSING', []))

        # Top ships via raw SQL of python (python is makkelijker hier)
        changes_objs = DetectedChange.query.filter(DetectedChange.timestamp >= seven_days_ago_utc).all()
        cnt = Counter()
        rgx = re.compile(r"(?:NIEUW:|WIJZIGING:)\s+([^(]+?)\s+(?:\(|$)")
        for c in changes_objs:
            found = rgx.findall(c.content)
            for f in found: cnt[f.strip()] += 1
        stats["top_ships"] = cnt.most_common(5)

    except: pass
    return render_template('statistieken.html', stats=stats, secret_key=os.environ.get('SECRET_KEY'))

if __name__ == '__main__':
    app.run(debug=True, port=5001)
