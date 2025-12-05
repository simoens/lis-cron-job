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

# --- GLOBALE STATE DICTIONARY ---
app_state = {
    "latest_snapshot": {"timestamp": "Nog niet uitgevoerd", "content_data": {"INKOMEND": [], "UITGAAND": [], "VERPLAATSING": []}}, 
    "change_history": deque(maxlen=10)
}
data_lock = threading.Lock() 

# --- FLASK APPLICATIE ---
app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)

# --- CONFIGURATIE ---
# Database fix voor Render (postgres -> postgresql)
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
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        session.headers.update(headers)
        
        # 30s Timeout
        get_response = session.get("https://lis.loodswezen.be/Lis/Login.aspx", timeout=30)
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
            
        login_response = session.post("https://lis.loodswezen.be/Lis/Login.aspx", data=form_data, timeout=30)
        login_response.raise_for_status()
        
        if "Login.aspx" not in login_response.url:
            logging.info("LOGIN SUCCESSFUL!")
            return True
        logging.error("Login failed.")
        return False
    except Exception as e:
        logging.error(f"Error during login: {e}")
        return False

def parse_table_from_soup(soup):
    """Leest de tabel met schepen."""
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
                if k == "RTA" and not value:
                     if cell.has_attr('title'): value = cell['title']
                     else:
                         child = cell.find(lambda tag: tag.has_attr('title'))
                         if child: value = child['title']
                if k == "Loods" and "[Loods]" in value: value = "Loods werd toegewezen"
                bestelling[k] = value

        # ID ZOEKEN (OnClick of Link)
        if 'ReisId' not in bestelling and row.has_attr('onclick'):
            onclick_text = row['onclick']
            match = re.search(r"value=['\"]?(\d+)['\"]?", onclick_text)
            if match: bestelling['ReisId'] = match.group(1)
        
        if 'ReisId' not in bestelling:
             link = row.find('a', href=re.compile(r'Reisplan\.aspx'))
             if link:
                 match = re.search(r'ReisId=(\d+)', link['href'])
                 if match: bestelling['ReisId'] = match.group(1)

        bestellingen.append(bestelling)
    return bestellingen

def is_mpet_ship(schip_dict):
    """Is dit schip voor MPET?"""
    entry = schip_dict.get('Entry Point', '').lower()
    exit_p = schip_dict.get('Exit Point', '').lower()
    if 'mpet' in entry or 'mpet' in exit_p:
        return True
    return False

def parse_bewegingen_from_page(soup):
    """Zoekt in de huidige pagina naar de bewegingen tabel en pakt MPET tijd."""
    # Zoek de tabel (op ID of kenmerken)
    table = soup.find('table', id='ctl00_ContentPlaceHolder1_ctl01_data_tabContainer_pnl2_tabpage_bewegingen_list_gv')
    if not table:
        # Fallback: zoek op header tekst
        for t in soup.find_all('table'):
            if "Locatie" in t.get_text() and "ETA" in t.get_text():
                table = t
                break
    
    if not table: return None

    # Headers scannen
    headers = [th.get_text(strip=True) for th in table.find_all('tr')[0].find_all(['th', 'td'])]
    idx_loc, idx_eta, idx_ata = -1, -1, -1
    for i, h in enumerate(headers):
        if "Locatie" in h: idx_loc = i
        elif "ETA" in h: idx_eta = i
        elif "ATA" in h: idx_ata = i
    
    if idx_loc == -1: return None

    target_time = None

    # Rijen scannen (onderste wint)
    for row in table.find_all('tr')[1:]:
        cells = row.find_all('td')
        if len(cells) <= max(idx_loc, idx_eta): continue
        
        loc = cells[idx_loc].get_text(strip=True)
        if "Deurganckdok" in loc or "MPET" in loc:
            eta = cells[idx_eta].get_text(strip=True) if idx_eta != -1 else ""
            ata = cells[idx_ata].get_text(strip=True) if idx_ata != -1 and len(cells) > idx_ata else ""
            
            val = ata if ata else eta
            if val: target_time = val
            
    # Format
    if target_time:
        match = re.search(r"(\d{2}/\d{2})/\d{4}\s+(\d{2}:\d{2})", target_time)
        if match: return f"{match.group(1)} {match.group(2)}"
        return target_time
        
    return None

def haal_bestellingen_op(session):
    logging.info("--- Running haal_bestellingen_op (v73-Fixed, Stateful Scraping) ---")
    try:
        base_url = "https://lis.loodswezen.be/Lis/Loodsbestellingen.aspx"
        
        # 1. Start: GET pagina
        resp = session.get(base_url, timeout=30)
        soup = BeautifulSoup(resp.content, 'lxml')
        
        # Helper voor form data
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
        fdata['ctl00$ContentPlaceHolder1$ctl01$select$rzn_lbs_vertrektijd_from$txtDate'] = ''
        fdata['ctl00$ContentPlaceHolder1$ctl01$select$rzn_lbs_vertrektijd_to$txtDate'] = ''
        
        resp = session.post(base_url, data=fdata, timeout=30)
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
        
        resp = session.post(base_url, data=fdata, timeout=30)
        soup = BeautifulSoup(resp.content, 'lxml')
        
        # 5. LOOP PAGINA'S & DETAILS
        all_ships = []
        page_count = 1
        max_pages = 15
        
        while page_count <= max_pages:
            logging.info(f"Verwerken pagina {page_count}...")
            
            # Parse huidige tabel
            from_soup = parse_table_from_soup(soup)
            
            # --- DEEP DIVE: Check voor MPET schepen op DEZE pagina ---
            for schip in from_soup:
                if is_mpet_ship(schip):
                    reis_id = schip.get('ReisId')
                    if reis_id:
                        logging.info(f"MPET Schip gevonden: {schip.get('Schip')}. Ophalen details...")
                        
                        try:
                            # PostBack om te selecteren
                            detail_data = get_form_data(soup)
                            detail_data['__EVENTTARGET'] = 'ctl00$ContentPlaceHolder1$ctl01$list$itemSelectHandler'
                            detail_data['__EVENTARGUMENT'] = f'value={reis_id};text='
                            
                            resp = session.post(base_url, data=detail_data, timeout=20)
                            soup = BeautifulSoup(resp.content, 'lxml') # UPDATE SOUP met details
                            
                            # Lees bewegingen
                            eta_mpet = parse_bewegingen_from_page(soup)
                            if eta_mpet:
                                schip['Details_ETA'] = eta_mpet
                                logging.info(f" -> Gevonden: {eta_mpet}")
                        except Exception as e:
                            logging.warning(f"Fout bij ophalen details: {e}")

            # Voeg toe
            all_ships.extend(from_soup)
            
            # 6. VOLGENDE PAGINA
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
                
                resp = session.post(base_url, data=paging_data, timeout=30)
                soup = BeautifulSoup(resp.content, 'lxml')
                page_count += 1
            else:
                break
                
        return all_ships

    except Exception as e:
        logging.error(f"Fout in haal_bestellingen_op: {e}")
        return []

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
        
        # ETA BEREKENING
        b['berekende_eta'] = 'N/A'
        
        # 1. Bewezen ETA (uit scraper via postback)
        if 'Details_ETA' in b:
             b['berekende_eta'] = f"Bewegingen: {b['Details_ETA']}"
        # 2. RTA
        elif b.get('RTA'):
             m = re.search(r"(SA/ZV|Deurganckdok)\s*(\d{2}/\d{2}\s+\d{2}:\d{2})", b['RTA'], re.IGNORECASE)
             if m: b['berekende_eta'] = f"RTA (CP): {m.group(2)}"
             else: b['berekende_eta'] = f"RTA: {b['RTA']}"
        # 3. Calc
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

# --- DATABASE FUNCTIES ---
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
        if obj: obj.value = value
        else:
            obj = KeyValueStore(key=key, value=value)
            db.session.add(obj)
    except Exception as e:
        db.session.rollback()

def vergelijk_bestellingen(oude, nieuwe):
    # ... (simplified placeholder) ...
    return []

def format_wijzigingen_email(wijzigingen):
    return ""

def main():
    if not all([USER, PASS]):
        logging.critical("FATAL: Geen user/pass!")
        return
    vorige = load_state_for_comparison()
    session = requests.Session()
    if not login(session):
        logging.error("Login mislukt.")
        return
    nieuwe = haal_bestellingen_op(session)
    if not nieuwe:
        logging.error("Geen bestellingen opgehaald.")
        return
    logging.info("Snapshot genereren...")
    nu = datetime.now(pytz.timezone('Europe/Brussels'))
    snapshot_data = filter_snapshot_schepen(nieuwe, session, nu)
    
    s = Snapshot(timestamp=nu.astimezone(pytz.utc).replace(tzinfo=None), content_data=snapshot_data)
    try:
        db.session.add(s)
        db.session.commit()
        
        with data_lock:
             app_state["latest_snapshot"] = {
                "timestamp": nu.strftime('%d-%m-%Y %H:%M:%S'),
                "content_data": snapshot_data
            }
    except: db.session.rollback()
    
    save_state_for_comparison({"bestellingen": nieuwe})
    logging.info("--- Run Voltooid ---")

if __name__ == '__main__':
    app.run(debug=True, port=5001)
