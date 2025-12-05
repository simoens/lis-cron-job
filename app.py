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
import sys

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

# --- HELPER VOOR DIRECT LOGGEN ---
def log_force(msg):
    """Print direct naar stdout om buffering te omzeilen."""
    print(f"{datetime.now()} - {msg}", flush=True)
    logging.info(msg)

# --- DATABASE FUNCTIES ---
def load_state_for_comparison():
    try:
        bestellingen_obj = KeyValueStore.query.get('bestellingen')
        return {"bestellingen": bestellingen_obj.value if bestellingen_obj else []}
    except Exception as e:
        log_force(f"Error loading state: {e}")
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
        log_force(f"Error saving state: {e}")
        db.session.rollback()

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
        log_force("Login attempt started...")
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
            log_force("LOGIN SUCCESSFUL!")
            return True
        log_force("Login failed.")
        return False
    except Exception as e:
        log_force(f"Error during login: {e}")
        return False

def parse_table_from_soup(soup):
    """Leest de hoofdtabel en haalt ReisId uit OnClick."""
    table = soup.find('table', id='ctl00_ContentPlaceHolder1_ctl01_list_gv')
    if table is None: return []
    
    kolom_indices = {"Type": 0, "Besteltijd": 5, "ETA/ETD": 6, "RTA": 7, "Loods": 10, "Schip": 11, "Entry Point": 20, "Exit Point": 21}
    bestellingen = []
    
    rows = table.find_all('tr')
    if len(rows) < 2: return []

    for row in rows[1:]: 
        kolom_data = row.find_all('td')
        if not kolom_data: continue
        bestelling = {}
        
        for k, i in kolom_indices.items():
            if i < len(kolom_data):
                cell = kolom_data[i]
                value = cell.get_text(strip=True)
                if k == "RTA" and not value: # Hover RTA fix
                     if cell.has_attr('title'): value = cell['title']
                     else:
                         child = cell.find(lambda tag: tag.has_attr('title'))
                         if child: value = child['title']
                if k == "Loods" and "[Loods]" in value: value = "Loods werd toegewezen"
                bestelling[k] = value

        # ID ZOEKEN (OnClick is het meest betrouwbaar gebleken)
        if 'ReisId' not in bestelling and row.has_attr('onclick'):
            onclick_text = row['onclick']
            match = re.search(r"value=['\"]?(\d+)['\"]?", onclick_text)
            if match: bestelling['ReisId'] = match.group(1)
        
        # Fallback: Link
        if 'ReisId' not in bestelling:
             link = row.find('a', href=re.compile(r'Reisplan\.aspx'))
             if link:
                 match = re.search(r'ReisId=(\d+)', link['href'])
                 if match: bestelling['ReisId'] = match.group(1)

        bestellingen.append(bestelling)
    return bestellingen

def haal_bewegingen_details(session, reis_id):
    """Haalt ETA/ATA op van de Bewegingen pagina voor MPET."""
    if not reis_id: return None
    try:
        url = f"https://lis.loodswezen.be/Lis/Bewegingen.aspx?ReisId={reis_id}"
        # Belangrijk: Referer meesturen anders blokkeert LIS soms directe toegang
        session.headers.update({'Referer': "https://lis.loodswezen.be/Lis/Loodsbestellingen.aspx"})
        
        response = session.get(url, timeout=20)
        soup = BeautifulSoup(response.content, 'lxml')
        
        # Specifiek ID uit de broncode van de gebruiker
        target_table_id = 'ctl00_ContentPlaceHolder1_ctl01_data_tabContainer_pnl2_tabpage_bewegingen_list_gv'
        table = soup.find('table', id=target_table_id)
        
        # Fallback als ID verandert: zoek op headers
        if not table:
             for t in soup.find_all('table'):
                 if "Locatie" in t.get_text() and "ETA" in t.get_text():
                     table = t
                     break
        
        if not table: return None
        
        target_time = None
        
        # Kolom indexen bepalen
        headers = [th.get_text(strip=True) for th in table.find_all('tr')[0].find_all(['th', 'td'])]
        idx_loc, idx_eta, idx_ata = -1, -1, -1
        
        for i, h in enumerate(headers):
            if "Locatie" in h: idx_loc = i
            elif "ETA" in h: idx_eta = i 
            elif "ATA" in h: idx_ata = i
            
        if idx_loc == -1: return None

        # Rijen scannen
        for row in table.find_all('tr')[1:]:
            cells = row.find_all('td')
            if len(cells) <= max(idx_loc, idx_eta): continue
            
            loc = cells[idx_loc].get_text(strip=True)
            
            if "Deurganckdok" in loc or "MPET" in loc:
                eta = cells[idx_eta].get_text(strip=True) if idx_eta != -1 else ""
                ata = cells[idx_ata].get_text(strip=True) if idx_ata != -1 else ""
                val = ata if ata else eta
                if val: target_time = val
        
        # Formatteren: dd/mm/yyyy HH:MM -> dd/mm HH:MM
        if target_time:
             match = re.search(r"(\d{2}/\d{2})/\d{4}\s+(\d{2}:\d{2})", target_time)
             if match: return f"{match.group(1)} {match.group(2)}"
             return target_time

        return None

    except Exception as e:
        # Geen log hier om spam te voorkomen, stil falen is ok voor details
        return None

def haal_bestellingen_op(session):
    log_force("--- Running haal_bestellingen_op (v78, Timeouts & Flush) ---")
    try:
        base_page_url = "https://lis.loodswezen.be/Lis/Loodsbestellingen.aspx"
        session.headers.update({'Referer': base_page_url})
        
        # 1. GET
        resp = session.get(base_page_url, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, 'lxml')
        
        def get_form_data(s):
            d = {}
            for i in s.find_all('input'):
                if i.get('name'): d[i['name']] = i.get('value', '')
            if 'ctl00$ContentPlaceHolder1$ctl01$select$betrokken' in d: del d['ctl00$ContentPlaceHolder1$ctl01$select$betrokken']
            return d

        # STAP 1
        log_force("STAP 1: Uncheck Eigen Reizen...")
        fdata = get_form_data(soup)
        fdata['__EVENTTARGET'] = 'ctl00$ContentPlaceHolder1$ctl01$select$betrokken'
        fdata['ctl00$ContentPlaceHolder1$ctl01$select$rzn_lbs_vertrektijd_from$txtDate'] = ''
        fdata['ctl00$ContentPlaceHolder1$ctl01$select$rzn_lbs_vertrektijd_to$txtDate'] = ''
        
        resp = session.post(base_page_url, data=fdata, timeout=30)
        soup = BeautifulSoup(resp.content, 'lxml')
        time.sleep(2)
        
        # STAP 2
        log_force("STAP 2: Zoeken...")
        fdata = get_form_data(soup)
        fdata['ctl00$ContentPlaceHolder1$ctl01$select$richting'] = '/'
        fdata['ctl00$ContentPlaceHolder1$ctl01$select$btnSearch'] = 'Zoeken'
        if 'ctl00$ContentPlaceHolder1$ctl01$select$agt_naam' in fdata: del fdata['ctl00$ContentPlaceHolder1$ctl01$select$agt_naam']
        fdata['ctl00$ContentPlaceHolder1$ctl01$select$rzn_lbs_vertrektijd_from$txtDate'] = ''
        fdata['ctl00$ContentPlaceHolder1$ctl01$select$rzn_lbs_vertrektijd_to$txtDate'] = ''
        fdata['__EVENTTARGET'] = ''
        fdata['__EVENTARGUMENT'] = ''
        
        resp = session.post(base_page_url, data=fdata, timeout=30)
        soup = BeautifulSoup(resp.content, 'lxml')
        
        alle_bestellingen = parse_table_from_soup(soup)
        log_force(f"Items gevonden op pagina 1: {len(alle_bestellingen)}")
        
        # Paging
        current_page = 1
        max_pages = 15 
        
        while current_page < max_pages:
            next_page_num = current_page + 1
            paging_link = soup.find('a', href=re.compile(f"Page\${next_page_num}"))
            if not paging_link: break
            
            log_force(f"Paginering: Ophalen pagina {next_page_num}...")
            page_form_data = {}
            for hidden in ['__VIEWSTATE', '__VIEWSTATEGENERATOR', '__EVENTVALIDATION']:
                tag = soup.find('input', {'name': hidden})
                if tag: page_form_data[hidden] = tag.get('value', '')
            
            href = paging_link['href']
            match = re.search(r"__doPostBack\('([^']+)','([^']+)'\)", href)
            if match:
                page_form_data['__EVENTTARGET'] = match.group(1)
                page_form_data['__EVENTARGUMENT'] = match.group(2)
                for k, v in fdata.items():
                    if k not in page_form_data and 'btn' not in k:
                        page_form_data[k] = v
                
                page_resp = session.post(base_page_url, data=page_form_data, timeout=30)
                soup = BeautifulSoup(page_resp.content, 'lxml')
                new_items = parse_table_from_soup(soup)
                if not new_items: break
                alle_bestellingen.extend(new_items)
                current_page += 1
                time.sleep(0.5)
            else:
                break
                
        log_force(f"Totaal {len(alle_bestellingen)} bestellingen opgehaald.")
        return alle_bestellingen

    except Exception as e:
        log_force(f"Fout in haal_bestellingen_op: {e}")
        return []

def is_mpet_ship(schip_dict):
    entry = schip_dict.get('Entry Point', '').lower()
    exit_p = schip_dict.get('Exit Point', '').lower()
    if 'mpet' in entry or 'mpet' in exit_p: return True
    return False

def filter_snapshot_schepen(bestellingen, session, nu):
    gefilterd = {"INKOMEND": [], "UITGAAND": [], "VERPLAATSING": []}
    grens_uit = nu + timedelta(hours=16)
    grens_in_verleden = nu - timedelta(hours=8)
    grens_in = nu + timedelta(hours=8)
    
    for b in bestellingen:
        if not is_mpet_ship(b): continue
        
        schip_type = b.get("Type")
        besteltijd_str = b.get('Besteltijd')
        try:
            bt = datetime.strptime(re.sub(r'\s+', ' ', besteltijd_str), "%d/%m/%y %H:%M")
            bt = pytz.timezone('Europe/Brussels').localize(bt)
        except: continue
        
        b['berekende_eta'] = 'N/A'
        
        # INKOMEND LOGICA
        if schip_type == 'I':
             # 1. Bewegingen (met timeout protectie in functie)
             reis_id = b.get('ReisId')
             if reis_id:
                 tijd_bew = haal_bewegingen_details(session, reis_id)
                 if tijd_bew:
                     b['berekende_eta'] = f"Bewegingen: {tijd_bew}"
             
             # 2. RTA
             if b['berekende_eta'] == 'N/A':
                 rta = b.get('RTA', '').strip()
                 if rta:
                     m = re.search(r"(SA/ZV|Deurganckdok)\s*(\d{2}/\d{2}\s+\d{2}:\d{2})", rta, re.IGNORECASE)
                     if m: b['berekende_eta'] = f"RTA (CP): {m.group(2)}"
                     else: b['berekende_eta'] = f"RTA: {rta}"
             
             # 3. Calc
             if b['berekende_eta'] == 'N/A':
                  eta = bt + timedelta(hours=6)
                  b['berekende_eta'] = f"Calculated: {eta.strftime('%d/%m/%y %H:%M')}"
             
             if grens_in_verleden <= bt <= grens_in:
                 gefilterd['INKOMEND'].append(b)
                 
        elif schip_type == 'U':
             if nu <= bt <= grens_uit: gefilterd['UITGAAND'].append(b)
        elif schip_type == 'V':
             gefilterd['VERPLAATSING'].append(b)
             
    gefilterd["INKOMEND"].sort(key=lambda x: x.get('Besteltijd'))
    return gefilterd

def vergelijk_bestellingen(oude, nieuwe):
    return []

def format_wijzigingen_email(wijzigingen):
    return ""

# --- MAIN FUNCTIE (TERUGGEZET!) ---
def main():
    log_force("--- START MAIN ---")
    if not all([USER, PASS]):
        log_force("FATAL: Geen user/pass!")
        return
    
    # DB Test
    vorige = load_state_for_comparison()
    
    session = requests.Session()
    if not login(session):
        log_force("Login mislukt.")
        return
    
    nieuwe = haal_bestellingen_op(session)
    if not nieuwe:
        log_force("Geen bestellingen opgehaald.")
        return
    
    log_force("Snapshot genereren...")
    nu = datetime.now(pytz.timezone('Europe/Brussels'))
    snapshot_data = filter_snapshot_schepen(nieuwe, session, nu)
    
    try:
        s = Snapshot(timestamp=nu.astimezone(pytz.utc).replace(tzinfo=None), content_data=snapshot_data)
        db.session.add(s)
        db.session.commit()
        log_force("Snapshot saved.")
    except Exception as e:
         log_force(f"DB Save Error: {e}")
         db.session.rollback()

    save_state_for_comparison({"bestellingen": nieuwe})
    log_force("--- Run Voltooid ---")

# --- THREAD WRAPPER ---
def main_task():
    try:
        log_force("--- Background task started ---")
        with app.app_context():
            main()
        log_force("--- Background task finished ---")
    except Exception as e:
        log_force(f"FATAL ERROR in background thread: {e}")

if __name__ == '__main__':
    app.run(debug=True, port=5001)
