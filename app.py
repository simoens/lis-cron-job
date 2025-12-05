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

# --- GLOBALE STATE DICTIONARY ---
app_state = {
    "latest_snapshot": {"timestamp": "Nog niet uitgevoerd", "content_data": {"INKOMEND": [], "UITGAAND": [], "VERPLAATSING": []}}, 
    "change_history": deque(maxlen=10)
}
data_lock = threading.Lock() 

# --- FLASK APPLICATIE ---
app = Flask(__name__)

# --- CONFIGURATIE ---
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL')
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

with app.app_context():
    db.create_all()

# --- LOGICA ---

def is_mpet_ship(schip_dict):
    """Helper om te bepalen of we details moeten ophalen."""
    entry = schip_dict.get('Entry Point', '').lower()
    exit_p = schip_dict.get('Exit Point', '').lower()
    # Filter op MPET
    if 'mpet' in entry or 'mpet' in exit_p:
        return True
    return False

def parse_bewegingen_from_page(soup):
    """Zoekt in de huidige pagina naar de bewegingen tabel."""
    # Zoek de tabel (op ID of kenmerken)
    table = soup.find('table', id='ctl00_ContentPlaceHolder1_ctl01_data_tabContainer_pnl2_tabpage_bewegingen_list_gv')
    if not table:
        # Fallback
        for t in soup.find_all('table'):
            if "Locatie" in t.get_text() and "ETA" in t.get_text():
                table = t
                break
    
    if not table: return None

    # Headers
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
        if len(cells) <= idx_loc: continue
        
        loc = cells[idx_loc].get_text(strip=True)
        if "Deurganckdok" in loc or "MPET" in loc:
            eta = cells[idx_eta].get_text(strip=True) if idx_eta != -1 else ""
            ata = cells[idx_ata].get_text(strip=True) if idx_ata != -1 else ""
            
            val = ata if ata else eta
            if val: target_time = val
            
    # Format
    if target_time:
        match = re.search(r"(\d{2}/\d{2})/\d{4}\s+(\d{2}:\d{2})", target_time)
        if match: return f"{match.group(1)} {match.group(2)}"
        return target_time
        
    return None

def haal_bestellingen_op(session):
    logging.info("--- Running haal_bestellingen_op (v74, Stateful Scraping) ---")
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
            # Verwijder checkboxes/buttons die we niet willen
            if 'ctl00$ContentPlaceHolder1$ctl01$select$betrokken' in d: del d['ctl00$ContentPlaceHolder1$ctl01$select$betrokken']
            keys_to_del = [k for k in d.keys() if 'btn' in k or 'checkbox' in str(d[k])] # Ruw filter
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
        # Forceer filters
        fdata['ctl00$ContentPlaceHolder1$ctl01$select$richting'] = '/'
        fdata['ctl00$ContentPlaceHolder1$ctl01$select$btnSearch'] = 'Zoeken'
        if 'ctl00$ContentPlaceHolder1$ctl01$select$agt_naam' in fdata: del fdata['ctl00$ContentPlaceHolder1$ctl01$select$agt_naam']
        # Lege datums
        fdata['ctl00$ContentPlaceHolder1$ctl01$select$rzn_lbs_vertrektijd_from$txtDate'] = ''
        fdata['ctl00$ContentPlaceHolder1$ctl01$select$rzn_lbs_vertrektijd_to$txtDate'] = ''
        # Clean events
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
            
            # Parse huidige tabel
            from_soup = parse_table_from_soup(soup) # (Functie uit v73 hergebruiken we)
            
            # --- DEEP DIVE: Check voor MPET schepen op DEZE pagina ---
            for schip in from_soup:
                if is_mpet_ship(schip):
                    reis_id = schip.get('ReisId')
                    if reis_id:
                        logging.info(f"MPET Schip gevonden: {schip['Schip']}. Ophalen details...")
                        
                        # We doen een PostBack OM HET SCHIP TE SELECTEREN
                        # Dit update 'soup' naar de pagina met de details open
                        detail_data = get_form_data(soup)
                        detail_data['__EVENTTARGET'] = 'ctl00$ContentPlaceHolder1$ctl01$list$itemSelectHandler'
                        detail_data['__EVENTARGUMENT'] = f'value={reis_id};text='
                        
                        # Voer uit
                        resp = session.post(base_url, data=detail_data)
                        soup = BeautifulSoup(resp.content, 'lxml') # UPDATE SOUP!
                        
                        # Nu zit de bewegingen tabel in 'soup'. Lees hem.
                        eta_mpet = parse_bewegingen_from_page(soup)
                        if eta_mpet:
                            schip['Details_ETA'] = eta_mpet
                            logging.info(f" -> Gevonden: {eta_mpet}")
                        else:
                            logging.info(" -> Geen bewegingen gevonden.")
                            
            # Voeg toe aan totaal
            all_ships.extend(from_soup)
            
            # 6. VOLGENDE PAGINA
            next_page = page_count + 1
            link = soup.find('a', href=re.compile(f"Page\${next_page}"))
            if not link:
                break
                
            # Prepareer paging postback
            paging_data = get_form_data(soup)
            href = link['href']
            m = re.search(r"__doPostBack\('([^']+)','([^']+)'\)", href)
            if m:
                paging_data['__EVENTTARGET'] = m.group(1)
                paging_data['__EVENTARGUMENT'] = m.group(2)
                
                resp = session.post(base_url, data=paging_data)
                soup = BeautifulSoup(resp.content, 'lxml') # UPDATE SOUP!
                page_count += 1
            else:
                break
                
        return all_ships

    except Exception as e:
        logging.error(f"Fout in haal_bestellingen_op: {e}")
        return []

# --- HERBRUIKTE HELPER FUNCTIE (ingekort voor overzicht) ---
def parse_table_from_soup(soup):
    # ... zelfde logica als v73 om tabel te lezen en ReisId te vinden ...
    table = soup.find('table', id='ctl00_ContentPlaceHolder1_ctl01_list_gv')
    if not table: return []
    rows = table.find_all('tr')[1:]
    out = []
    cols = {"Type": 0, "Besteltijd": 5, "ETA/ETD": 6, "RTA": 7, "Loods": 10, "Schip": 11, "Entry Point": 20, "Exit Point": 21}
    for r in rows:
        d = {}
        cells = r.find_all('td')
        if not cells: continue
        for k, i in cols.items():
            if i < len(cells): 
                val = cells[i].get_text(strip=True)
                # RTA fix
                if k == "RTA" and not val:
                     if cells[i].has_attr('title'): val = cells[i]['title']
                     else: 
                         c = cells[i].find(lambda t: t.has_attr('title'))
                         if c: val = c['title']
                if k=="Loods" and "[Loods]" in val: val="Loods werd toegewezen"
                d[k] = val
        
        # ID
        if r.has_attr('onclick'):
            m = re.search(r"value=['\"]?(\d+)['\"]?", r['onclick'])
            if m: d['ReisId'] = m.group(1)
        out.append(d)
    return out

# --- FILTER FUNCTIE ---
def filter_snapshot_schepen(bestellingen, session, nu):
    # ... Standaard filter logica ...
    # Hier voegen we de 'Details_ETA' toe aan 'berekende_eta'
    gefilterd = {"INKOMEND": [], "UITGAAND": [], "VERPLAATSING": []}
    
    # Datum grenzen
    grens_uit = nu + timedelta(hours=16)
    grens_in_verleden = nu - timedelta(hours=8)
    grens_in = nu + timedelta(hours=8)
    
    for b in bestellingen:
        schip_type = b.get("Type")
        
        # MPET CHECK
        if not is_mpet_ship(b): continue
        
        # DATUM CHECK
        besteltijd_str = b.get('Besteltijd')
        if not besteltijd_str: continue
        try:
            bt = datetime.strptime(re.sub(r'\s+', ' ', besteltijd_str), "%d/%m/%y %H:%M")
            bt = pytz.timezone('Europe/Brussels').localize(bt)
        except: continue
        
        # CALCULATE ETA
        b['berekende_eta'] = 'N/A'
        
        # 1. Bewezen ETA (uit scraper)
        if 'Details_ETA' in b:
             b['berekende_eta'] = f"Bewegingen: {b['Details_ETA']}"
        # 2. RTA
        elif b.get('RTA'):
             # ... regex logic ...
             m = re.search(r"(SA/ZV|Deurganckdok)\s*(\d{2}/\d{2}\s+\d{2}:\d{2})", b['RTA'])
             if m: b['berekende_eta'] = f"RTA (CP): {m.group(2)}"
             else: b['berekende_eta'] = f"RTA: {b['RTA']}"
        # 3. Calc
        else:
             # ... +6u logic ...
             b['berekende_eta'] = "Calculated"

        # STATUS FLAG
        # ... (dezelfde logica als voorheen) ...
        b['status_flag'] = 'normal'
        
        # TOEVOEGEN
        if schip_type == 'I':
             if grens_in_verleden <= bt <= grens_in: gefilterd['INKOMEND'].append(b)
        elif schip_type == 'U':
             if nu <= bt <= grens_uit: gefilterd['UITGAAND'].append(b)
        elif schip_type == 'V':
             gefilterd['VERPLAATSING'].append(b)
             
    # Sort
    gefilterd["INKOMEND"].sort(key=lambda x: x.get('Besteltijd'))
    
    return gefilterd

# ... rest van de app routes (login, home, etc) ...
# Zorg dat login/main etc hierboven staan of geïmporteerd zijn.
# Voor volledigheid: login functie is nodig.
def login(session):
    # ... (standaard login) ...
    return True # Placeholder, kopieer de echte

# MAIN WRAPPER (om code compleet te maken)
if __name__ == '__main__':
    app.run(debug=True)
