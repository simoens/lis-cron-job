import requests
from bs4 import BeautifulSoup
import logging
from datetime import datetime, timedelta
import re
import os
import smtplib
from email.mime.text import MIMEText
import json
from collections import defaultdict, deque
import pytz
from flask import Flask, render_template, request, abort, redirect, url_for
import threading
import time

# --- CONFIGURATIE ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- GLOBALE STATE DICTIONARY (voor de webpagina) ---
app_state = {
    "latest_snapshot": {"timestamp": "Nog niet uitgevoerd", "content": "Wachten op de eerste run..."},
    "change_history": deque(maxlen=10)
}
data_lock = threading.Lock()

# --- FLASK APPLICATIE ---
app = Flask(__name__)

@app.route('/')
def home():
    """Toont de webpagina met de opgeslagen e-mailinhoud."""
    vorige_staat = load_state_from_jsonbin()
    if vorige_staat:
        with data_lock:
            app_state["latest_snapshot"] = vorige_staat.get("web_snapshot", app_state["latest_snapshot"])
            app_state["change_history"].clear()
            app_state["change_history"].extend(vorige_staat.get("web_changes", []))
            
    with data_lock:
        return render_template('index.html',
                               snapshot=app_state["latest_snapshot"],
                               changes=list(app_state["change_history"]),
                               secret_key=os.environ.get('SECRET_KEY'))

@app.route('/trigger-run')
def trigger_run():
    """Geheime URL die door de externe cron job wordt aangeroepen."""
    secret = request.args.get('secret')
    if secret != os.environ.get('SECRET_KEY'):
        logging.warning("Mislukte poging om /trigger-run aan te roepen met ongeldige secret key.")
        abort(403)
    
    logging.info("================== Externe Trigger Ontvangen: Run Start ==================")
    try:
        main()
        return "OK: Scraper run voltooid.", 200
    except Exception as e:
        logging.critical(f"FATALE FOUT tijdens de getriggerde run: {e}")
        return f"ERROR: Er is een fout opgetreden: {e}", 500

@app.route('/force-snapshot', methods=['POST'])
def force_snapshot_route():
    """Wordt aangeroepen door de knop op de webpagina om een overzicht te forceren."""
    secret = request.form.get('secret')
    if secret != os.environ.get('SECRET_KEY'):
        abort(403)
    
    logging.info("================== Handmatige Snapshot Geactiveerd ==================")
    try:
        force_snapshot_task()
    except Exception as e:
        logging.critical(f"FATALE FOUT tijdens geforceerde snapshot: {e}")
    
    return redirect(url_for('home'))

# --- ENVIRONMENT VARIABLES ---
USER = os.environ.get('LIS_USER')
PASS = os.environ.get('LIS_PASS')
SMTP_SERVER = os.environ.get('SMTP_SERVER')
SMTP_PORT = int(os.environ.get('SMTP_PORT', 587))
EMAIL_USER = os.environ.get('EMAIL_USER')
EMAIL_PASS = os.environ.get('EMAIL_PASS')
ONTVANGER_EMAIL = os.environ.get('ONTVANGER_EMAIL')
JSONBIN_API_KEY = os.environ.get('JSONBIN_API_KEY')
JSONBIN_BIN_ID = os.environ.get('JSONBIN_BIN_ID')

# --- JSONBIN.IO FUNCTIES ---
def load_state_from_jsonbin():
    if not all([JSONBIN_API_KEY, JSONBIN_BIN_ID]): return None
    headers = {'X-Master-Key': JSONBIN_API_KEY, 'X-Bin-Meta': 'false'}
    url = f"https://api.jsonbin.io/v3/b/{JSONBIN_BIN_ID}/latest"
    try:
        res = requests.get(url, headers=headers, timeout=15)
        res.raise_for_status()
        logging.info("Staat succesvol geladen van jsonbin.io.")
        return res.json()
    except Exception as e:
        logging.error(f"Fout bij laden van staat van jsonbin.io: {e}")
        return None

def save_state_to_jsonbin(state):
    if not all([JSONBIN_API_KEY, JSONBIN_BIN_ID]): return
    headers = {'Content-Type': 'application/json', 'X-Master-Key': JSONBIN_API_KEY}
    url = f"https://api.jsonbin.io/v3/b/{JSONBIN_BIN_ID}"
    try:
        res = requests.put(url, headers=headers, data=json.dumps(state), timeout=15)
        res.raise_for_status()
        logging.info("Nieuwe staat succesvol opgeslagen naar jsonbin.io.")
    except Exception as e:
        logging.error(f"Fout bij opslaan van staat naar jsonbin.io: {e}")

# --- SCRAPER FUNCTIES ---
def login(session):
    try:
        logging.info("Loginpoging gestart...")
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"}
        get_response = session.get("https://lis.loodswezen.be/Lis/Login.aspx", headers=headers)
        get_response.raise_for_status()
        soup = BeautifulSoup(get_response.text, 'lxml')
        viewstate = soup.find('input', {'name': '__VIEWSTATE'})
        if not viewstate:
            logging.error("Kon __VIEWSTATE niet vinden op loginpagina.")
            return False
        form_data = {
            '__VIEWSTATE': viewstate['value'],
            'ctl00$ContentPlaceHolder1$login$uname': USER,
            'ctl00$ContentPlaceHolder1$login$password': PASS,
            'ctl00$ContentPlaceHolder1$login$btnInloggen': 'Inloggen'}
        login_response = session.post("https://lis.loodswezen.be/Lis/Login.aspx", data=form_data, headers=headers)
        login_response.raise_for_status()
        if "Login.aspx" not in login_response.url:
            logging.info("LOGIN SUCCESVOL!")
            return True
        logging.error("Login mislukt (terug op loginpagina).")
        return False
    except Exception as e:
        logging.error(f"Fout tijdens login: {e}")
        return False

def haal_bestellingen_op(session):
    try:
        response = session.get("https://lis.loodswezen.be/Lis/Loodsbestellingen.aspx")
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'lxml')
        table = soup.find('table', id='ctl00_ContentPlaceHolder1_ctl01_list_gv')
        if table is None: return []
        kolom_indices = {"Type": 0, "Besteltijd": 5, "ETA/ETD": 6, "RTA": 7, "Loods": 10, "Schip": 11, "Entry Point": 20}
        bestellingen = []
        for row in table.find_all('tr')[1:]:
            kolom_data = row.find_all('td')
            if not kolom_data: continue
            bestelling = {}
            for k, i in kolom_indices.items():
                if i < len(kolom_data):
                    bestelling[k] = kolom_data[i].get_text(strip=True)
            
            if 11 < len(kolom_data):
                schip_cel = kolom_data[11]
                link_tag = schip_cel.find('a', href=re.compile(r'Reisplan\.aspx\?ReisId='))
                if link_tag:
                    match = re.search(r'ReisId=(\d+)', link_tag['href'])
                    if match:
                        bestelling['ReisId'] = match.group(1)
            bestellingen.append(bestelling)
        return bestellingen
    except Exception as e:
        logging.error(f"Error bij ophalen bestellingen: {e}")
        return []

def haal_pta_van_reisplan(session, reis_id):
    """Haalt de ONDERSTE PTA voor Saeftinghe - Zandvliet op van de reisplan detailpagina."""
    if not reis_id:
        return None
    try:
        url = f"https://lis.loodswezen.be/Lis/Reisplan.aspx?ReisId={reis_id}"
        response = session.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'lxml')
        
        reisplan_table = soup.find('table', id='ctl00_ContentPlaceHolder1_ctl00_gvReisplan')
        if not reisplan_table:
            return None

        pta_col_index = -1
        header_row = reisplan_table.find('tr', class_='grid_header')
        if header_row:
            headers = header_row.find_all('th')
            for i, header in enumerate(headers):
                if 'PTA' in header.get_text(strip=True):
                    pta_col_index = i
                    break
        
        if pta_col_index == -1:
            return None

        # --- AANGEPASTE LOGICA ---
        laatste_pta_gevonden = None
        for row in reisplan_table.find_all('tr')[1:]:
            cells = row.find_all('td')
            # Controleer of de locatie in een van de cellen van deze rij staat
            if any("Saeftinghe - Zandvliet" in cell.get_text() for cell in cells):
                if len(cells) > pta_col_index:
                    pta_raw = cells[pta_col_index].get_text(strip=True)
                    if pta_raw:
                        # Onthoud deze PTA, maar ga door met zoeken voor het geval er nog een is
                        laatste_pta_gevonden = pta_raw
        
        if laatste_pta_gevonden:
            logging.info(f"Laatste PTA gevonden voor ReisId {reis_id}: {laatste_pta_gevonden}")
            try:
                dt_obj = datetime.strptime(laatste_pta_gevonden, '%d-%m %H:%M')
                now = datetime.now()
                dt_obj = dt_obj.replace(year=now.year)
                if dt_obj < now - timedelta(days=180):
                    dt_obj = dt_obj.replace(year=now.year + 1)
                return dt_obj.strftime("%d/%m/%y %H:%M")
            except ValueError:
                logging.warning(f"Kon PTA formaat '{laatste_pta_gevonden}' niet parsen voor ReisId {reis_id}.")
                return laatste_pta_gevonden
        
        return None # Geen enkele rij gevonden
    except Exception as e:
        logging.error(f"Fout bij ophalen van reisplan voor ReisId {reis_id}: {e}")
        return None


def filter_snapshot_schepen(bestellingen, session):
    gefilterd = {"INKOMEND": [], "UITGAAND": []}
    nu = datetime.now()
    grens_uit_toekomst = nu + timedelta(hours=16)
    grens_in_verleden = nu - timedelta(hours=8)
    grens_in_toekomst = nu + timedelta(hours=8)
    for b in bestellingen:
        try:
            besteltijd_str = b.get("Besteltijd")
            if not besteltijd_str: continue
            besteltijd = datetime.strptime(besteltijd_str, "%d/%m/%y %H:%M")
            if b.get("Type") == "U":
                if nu <= besteltijd <= grens_uit_toekomst:
                    gefilterd["UITGAAND"].append(b)
            elif b.get("Type") == "I":
                if grens_in_verleden <= besteltijd <= grens_in_toekomst:
                    pta_saeftinghe = haal_pta_van_reisplan(session, b.get('ReisId'))
                    b['berekende_eta'] = pta_saeftinghe if pta_saeftinghe else 'N/A'
                    if b['berekende_eta'] == 'N/A':
                        entry_point = b.get('Entry Point', '').lower()
                        eta_dt = None
                        if "wandelaar" in entry_point: eta_dt = besteltijd + timedelta(hours=6)
                        elif "steenbank" in entry_point: eta_dt = besteltijd + timedelta(hours=7)
                        if eta_dt:
                            b['berekende_eta'] = eta_dt.strftime("%d/%m/%y %H:%M")
                    gefilterd["INKOMEND"].append(b)
        except (ValueError, TypeError): continue
    return gefilterd

def format_snapshot_email(snapshot_data):
    body = "--- BINNENKOMENDE SCHEPEN (Besteltijd tussen -8u en +8u) ---\n"
    if snapshot_data.get('INKOMEND'):
        snapshot_data['INKOMEND'].sort(key=lambda x: x.get('Besteltijd', ''))
        for schip in snapshot_data['INKOMEND']:
            naam = schip.get('Schip', 'N/A').ljust(30)
            besteltijd_str = schip.get('Besteltijd', 'N/A')
            loods = schip.get('Loods', 'N/A')
            eta_str = schip.get('berekende_eta', 'N/A')
            body += f"- {naam} | Besteltijd: {besteltijd_str.ljust(15)} | ETA Saeftinghe: {eta_str.ljust(15)} | Loods: {loods}\n"
    else:
        body += "Geen schepen die aan de criteria voldoen.\n"
    body += "\n--- UITGAANDE SCHEPEN (Besteltijd binnen 16u) ---\n"
    if snapshot_data.get('UITGAAND'):
        snapshot_data['UITGAAND'].sort(key=lambda x: x.get('Besteltijd', ''))
        for schip in snapshot_data['UITGAAND']:
            body += f"- {schip.get('Schip', 'N/A').ljust(30)} | Besteltijd: {schip.get('Besteltijd', 'N/A')} | Loods: {schip.get('Loods', 'N/A')}\n"
    else:
        body += "Geen schepen die aan de criteria voldoen.\n"
    return body

# ... (De rest van de functies: vergelijk_bestellingen, format_wijzigingen_email, etc. blijven ongewijzigd) ...

def main():
    # ... (De main functie blijft ongewijzigd) ...
    pass

