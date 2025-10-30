import requests
from bs4 import BeautifulSoup
import logging
from datetime import datetime, timedelta
import re
import os
import json 
from collections import deque
import pytz
from flask import Flask, render_template, request, abort, redirect, url_for, jsonify
import threading
import time

# --- CONFIGURATIE ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- GLOBALE STATE DICTIONARY (voor de webpagina) ---
app_state = {
    "latest_snapshot": {"timestamp": "Nog niet uitgevoerd", "content_data": {"INKOMEND": [], "UITGAAND": []}}, 
    "change_history": deque(maxlen=10)
}
data_lock = threading.Lock() 

# --- FLASK APPLICATIE ---
app = Flask(__name__)

# --- HELPER FUNCTIE VOOR ACHTERGROND TAKEN ---
def main_task():
    """Wrapper functie om main() in een background thread te draaien."""
    try:
        logging.info("--- Background task started ---")
        main()
        logging.info("--- Background task finished ---")
    except Exception as e:
        logging.critical(f"FATAL ERROR in background thread: {e}", exc_info=True)

@app.route('/')
def home():
    """Rendert de homepage, toont de laatste snapshot en wijzigingsgeschiedenis."""
    vorige_staat = load_state_from_jsonbin()
    
    recent_changes_list = []
    if vorige_staat:
        with data_lock:
            # Laad snapshot data
            if "web_snapshot" in vorige_staat:
                if "content_data" in vorige_staat["web_snapshot"]:
                    app_state["latest_snapshot"]["content_data"] = vorige_staat["web_snapshot"]["content_data"]
                elif "content" in vorige_staat["web_snapshot"]:
                    app_state["latest_snapshot"]["content"] = vorige_staat["web_snapshot"]["content"]
                app_state["latest_snapshot"]["timestamp"] = vorige_staat["web_snapshot"].get("timestamp", "N/A")

            # Laad change history
            recent_changes_list = vorige_staat.get("web_changes", [])
            app_state["change_history"].clear()
            app_state["change_history"].extend(recent_changes_list)
    
    with data_lock:
        return render_template('index.html',
                                snapshot=app_state["latest_snapshot"],
                                changes=list(app_state["change_history"]),
                                secret_key=os.environ.get('SECRET_KEY'))

@app.route('/trigger-run')
def trigger_run():
    """Een geheime URL endpoint die door een externe cron job wordt aangeroepen."""
    secret = request.args.get('secret')
    if secret != os.environ.get('SECRET_KEY'):
        logging.warning("Failed attempt to call /trigger-run with an invalid secret key.")
        abort(403)
    
    logging.info("================== External Trigger Received: Starting Background Run ==================")
    threading.Thread(target=main_task).start()
    return "OK: Scraper run triggered in background.", 200

@app.route('/force-snapshot', methods=['POST'])
def force_snapshot_route():
    """Endpoint aangeroepen door de knop om een background run te triggeren."""
    secret = request.form.get('secret')
    if secret != os.environ.get('SECRET_KEY'):
        abort(403)
    
    logging.info("================== Manual Background Run Triggered via Button ==================")
    threading.Thread(target=main_task).start()
    return redirect(url_for('home'))

@app.route('/status')
def status_check():
    """Een lichtgewicht endpoint die de client-side JS kan pollen."""
    state = load_state_from_jsonbin()
    if state and "web_snapshot" in state and "timestamp" in state["web_snapshot"]:
        return jsonify({"timestamp": state["web_snapshot"]["timestamp"]})
    else:
        # Stuur de in-memory timestamp als fallback
        with data_lock:
            return jsonify({"timestamp": app_state["latest_snapshot"].get("timestamp", "N/A")})

# --- ENVIRONMENT VARIABLES ---
USER = os.environ.get('LIS_USER')
PASS = os.environ.get('LIS_PASS')
JSONBIN_API_KEY = os.environ.get('JSONBIN_API_KEY')
JSONBIN_BIN_ID = os.environ.get('JSONBIN_BIN_ID')

# --- JSONBIN.IO FUNCTIES ---
def load_state_from_jsonbin():
    """Laadt de laatst opgeslagen state van een jsonbin.io bin."""
    if not all([JSONBIN_API_KEY, JSONBIN_BIN_ID]): return None
    headers = {'X-Master-Key': JSONBIN_API_KEY, 'X-Bin-Meta': 'false'}
    url = f"https://api.jsonbin.io/v3/b/{JSONBIN_BIN_ID}/latest"
    try:
        res = requests.get(url, headers=headers, timeout=15)
        res.raise_for_status()
        logging.info("State successfully loaded from jsonbin.io.")
        return res.json()
    except Exception as e:
        logging.error(f"Error loading state from jsonbin.io: {e}")
        return None

def save_state_to_jsonbin(state):
    """Slaat de huidige state op in een jsonbin.io bin."""
    if not all([JSONBIN_API_KEY, JSONBIN_BIN_ID]): return
    headers = {'Content-Type': 'application/json', 'X-Master-Key': JSONBIN_API_KEY}
    url = f"https://api.jsonbin.io/v3/b/{JSONBIN_BIN_ID}"
    try:
        res = requests.put(url, headers=headers, data=json.dumps(state), timeout=15)
        res.raise_for_status()
        logging.info("New state successfully saved to jsonbin.io.")
    except Exception as e:
        logging.error(f"Error saving state to jsonbin.io: {e}")

# --- HELPER: DATUM SCHOONMAKEN ---
def clean_date_string(date_str):
    """Vervangt onzichtbare spaties (U+00A0) door gewone spaties."""
    if not date_str:
        return None
    return re.sub(r'\s+', ' ', date_str.strip())

# --- SCRAPER FUNCTIES ---
def login(session):
    """Logt in op de LIS website en retourneert True bij succes."""
    try:
        logging.info("Login attempt started...")
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"}
        get_response = session.get("https://lis.loodswezen.be/Lis/Login.aspx", headers=headers)
        get_response.raise_for_status()
        soup = BeautifulSoup(get_response.text, 'lxml')
        
        viewstate = soup.find('input', {'name': '__VIEWSTATE'})
        if not viewstate:
            logging.error("Could not find __VIEWSTATE on login page.")
            return False
        
        form_data = {
            '__VIEWSTATE': viewstate['value'],
            'ctl00$ContentPlaceHolder1$login$uname': USER,
            'ctl00$ContentPlaceHolder1$login$password': PASS,
            'ctl00$ContentPlaceHolder1$login$btnInloggen': 'Inloggen'}
        
        login_response = session.post("https://lis.loodswezen.be/Lis/Login.aspx", data=form_data, headers=headers)
        login_response.raise_for_status()
        
        if "Login.aspx" not in login_response.url:
            logging.info("LOGIN SUCCESSFUL!")
            return True
        
        logging.error("Login failed (returned to login page).")
        return False
    except Exception as e:
        logging.error(f"Error during login: {e}")
        return False

def haal_bestellingen_op(session):
    """Haalt de lijst met loodsbestellingen op van de website."""
    logging.info("--- Running haal_bestellingen_op (v6, No-Link, Whitespace-Fix) ---")
    try:
        base_page_url = "https://lis.loodswezen.be/Lis/Loodsbestellingen.aspx"
        response = session.get(base_page_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'lxml')
        table = soup.find('table', id='ctl00_ContentPlaceHolder1_ctl01_list_gv')
        
        if table is None: 
            logging.warning("De bestellingentabel werd niet gevonden.")
            return []
        
        kolom_indices = {"Type": 0, "Besteltijd": 5, "ETA/ETD": 6, "RTA": 7, "Loods": 10, "Schip": 11, "Entry Point": 20}
        bestellingen = []
        
        for row in table.find_all('tr')[1:]: 
            kolom_data = row.find_all('td')
            if not kolom_data: continue
            
            bestelling = {}
            for k, i in kolom_indices.items():
                if i < len(kolom_data):
                    value = kolom_data[i].get_text(strip=True)
                    if k == "Loods" and "[Loods]" in value:
                        value = "Loods werd toegewezen"
                    bestelling[k] = value
            
            if 11 < len(kolom_data):
                schip_cel = kolom_data[11]
                link_tag = schip_cel.find('a', href=re.compile(r'Reisplan\.aspx\?ReisId='))
                if link_tag:
                    match = re.search(r'ReisId=(\d+)', link_tag['href'])
                    if match:
                        bestelling['ReisId'] = match.group(1)
            
            bestellingen.append(bestelling)
            
        logging.info(f"haal_bestellingen_op klaar. {len(bestellingen)} bestellingen gevonden.")
        return bestellingen
    except Exception as e:
        logging.error(f"Error in haal_bestellingen_op: {e}", exc_info=True)
        return []

def haal_pta_van_reisplan(session, reis_id):
    """Haalt de PTA voor 'Saeftinghe - Zandvliet' op van de reisplan detailpagina."""
    if not reis_id:
        return None
    try:
        url = f"https://lis.loodswezen.be/Lis/Reisplan.aspx?ReisId={reis_id}"
        response = session.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'lxml')
        
        laatste_pta_gevonden = None
        pta_pattern = re.compile(r'\d{2}-\d{2} \d{2}:\d{2}')

        for row in soup.find_all('tr'):
            if row.find(text=re.compile("Saeftinghe - Zandvliet")):
                for cell in row.find_all('td'):
                    match = pta_pattern.search(cell.get_text())
                    if match:
                        laatste_pta_gevonden = match.group(0)
        
        if laatste_pta_gevonden:
            try:
                pta_schoon = clean_date_string(laatste_pta_gevonden) # FIX
                dt_obj = datetime.strptime(pta_schoon, '%d-%m %H:%M')
                
                now = datetime.now()
                dt_obj = dt_obj.replace(year=now.year)
                if dt_obj < now - timedelta(days=180):
                    dt_obj = dt_obj.replace(year=now.year + 1)
                return dt_obj.strftime("%d/%m/%y %H:%M")
            except ValueError:
                logging.warning(f"Could not parse PTA format '{laatste_pta_gevonden}' for ReisId {reis_id}.")
                return laatste_pta_gevonden
        
        return None
    except Exception as e:
        logging.error(f"Error fetching voyage plan for ReisId {reis_id}: {e}")
        return None

def filter_snapshot_schepen(bestellingen, session, nu): 
    """Filtert bestellingen om een snapshot te maken voor een specifiek tijdvenster."""
    gefilterd = {"INKOMEND": [], "UITGAAND": []}
    
    grens_uit_toekomst = nu + timedelta(hours=16)
    grens_in_verleden = nu - timedelta(hours=8)
    grens_in_toekomst = nu + timedelta(hours=8)
    
    # Sorteer bestellingen op besteltijd (nieuwste eerst)
    bestellingen.sort(key=lambda x: datetime.strptime(clean_date_string(x.get('Besteltijd')) or '01/01/70 00:00', "%d/%m/%y %H:%M"), reverse=True) # FIX

    for b in bestellingen:
        try:
            entry_point = b.get('Entry Point', '').lower()
            if 'zeebrugge' in entry_point:
                continue 
            
            besteltijd_str_raw = b.get("Besteltijd")
            besteltijd_str = clean_date_string(besteltijd_str_raw) # FIX
            if not besteltijd_str: 
                continue
            
            besteltijd = datetime.strptime(besteltijd_str, "%d/%m/%y %H:%M")
            
            if b.get("Type") == "U": 
                if nu <= besteltijd <= grens_uit_toekomst:
                    gefilterd["UITGAAND"].append(b)
                    
            elif b.get("Type") == "I": 
                if grens_in_verleden <= besteltijd <= grens_in_toekomst:
                    pta_saeftinghe = haal_pta_van_reisplan(session, b.get('ReisId'))
                    b['berekende_eta'] = pta_saeftinghe if pta_saeftinghe else 'N/A'
                    if b['berekende_eta'] == 'N/A':
                        eta_dt = None
                        if "wandelaar" in entry_point: eta_dt = besteltijd + timedelta(hours=6)
                        elif "steenbank" in entry_point: eta_dt = besteltijd + timedelta(hours=7)
                        if eta_dt:
                            b['berekende_eta'] = eta_dt.strftime("%d/%m/%y %H:%M")
                    
                    # De "ETA in het verleden" filter is hier NIET AANWEZIG
                            
                    gefilterd["INKOMEND"].append(b)
                    
        except (ValueError, TypeError):
            logging.warning(f"Kon besteltijd '{besteltijd_str_raw}' niet parsen. Schip wordt overgeslagen.", exc_info=False)
            continue

    # Sorteer de gefilterde lijsten op tijd (oudste eerst, voor weergave)
    gefilterd["INKOMEND"].sort(key=lambda x: datetime.strptime(clean_date_string(x.get('Besteltijd')) or '01/01/70 00:00', "%d/%m/%y %H:%M")) # FIX
    gefilterd["UITGAAND"].sort(key=lambda x: datetime.strptime(clean_date_string(x.get('Besteltijd')) or '01/01/70 00:00', "%d/%m/%y %H:%M")) # FIX
    
    return gefilterd

def filter_dubbele_schepen(bestellingen):
    """Filtert dubbele schepen uit de lijst."""
    unieke_schepen = {}
    for b in bestellingen:
        schip_naam_raw = b.get('Schip')
        if not schip_naam_raw:
            continue
        
        schip_naam_gekuist = re.sub(r'\s*\(d\)\s*$', '', schip_naam_raw).strip()
        
        if schip_naam_gekuist in unieke_schepen:
            try:
                tijd_str_huidig = clean_date_string(b.get("Besteltijd")) or '01/01/70 00:00' # FIX
                tijd_str_opgeslagen = clean_date_string(unieke_schepen[schip_naam_gekuist].get("Besteltijd")) or '01/01/70 00:00' # FIX
                
                huidige_tijd = datetime.strptime(tijd_str_huidig, "%d/%m/%y %H:%M")
                opgeslagen_tijd = datetime.strptime(tijd_str_opgeslagen, "%d/%m/%y %H:%M")
                
                if huidige_tijd > opgeslagen_tijd:
                    unieke_schepen[schip_naam_gekuist] = b
            except (ValueError, TypeError):
                unieke_schepen[schip_naam_gekuist] = b
        else:
            unieke_schepen[schip_naam_gekuist] = b
            
    return list(unieke_schepen.values())

def vergelijk_bestellingen(oude, nieuwe):
    """Vergelijkt oude en nieuwe bestellijsten."""
    
    oude_dict = {re.sub(r'\s*\(d\)\s*$', '', b.get('Schip', '')).strip(): b 
            for b in filter_dubbele_schepen(oude) if b.get('Schip')}
    nieuwe_dict = {re.sub(r'\s*\(d\)\s*$', '', b.get('Schip', '')).strip(): b 
                   for b in filter_dubbele_schepen(nieuwe) if b.get('Schip')}
    
    oude_schepen_namen = set(oude_dict.keys())
    nieuwe_schepen_namen = set(nieuwe_dict.keys())

    wijzigingen = []
    nu = datetime.now() # UTC tijd van de server (OK voor 'moet_rapporteren')
    
    def moet_rapporteren(bestelling):
        besteltijd_str_raw = bestelling.get('Besteltijd', '').strip()
        besteltijd_str = clean_date_string(besteltijd_str_raw) # FIX
        if not besteltijd_str: return False
        
        rapporteer = True
        type_schip = bestelling.get('Type')
        
        try:
            besteltijd = datetime.strptime(besteltijd_str, "%d/%m/%y %H:%M")
            if type_schip == 'I':
                if not (nu - timedelta(hours=8) <= besteltijd <= nu + timedelta(hours=8)):
                    rapporteer = False
            elif type_schip == 'U':
                if not (nu <= besteltijd <= nu + timedelta(hours=16)):
                    rapporteer = False
        except (ValueError, TypeError) as e:
            logging.warning(f"Datum/tijd parsefout bij filteren van '{bestelling.get('Schip')}': {e}")
            return False

        if 'zeebrugge' in bestelling.get('Entry Point', '').lower():
            rapporteer = False
            
        return rapporteer

    # --- CASE 1: NIEUWE SCHEPEN ---
    for schip_naam in (nieuwe_schepen_namen - oude_schepen_namen):
        n_best = nieuwe_dict[schip_naam] 
        if moet_rapporteren(n_best):
            wijzigingen.append({
                'Schip': n_best.get('Schip'), 
                'status': 'NIEUW',
                'details': n_best 
            })

    # --- CASE 2: VERWIJDERDE SCHEPEN ---
    for schip_naam in (oude_schepen_namen - nieuwe_schepen_namen):
        o_best = oude_dict[schip_naam]
        if moet_rapporteren(o_best): 
            wijzigingen.append({
                'Schip': o_best.get('Schip'),
                'status': 'VERWIJDERD',
                'details': o_best
            })

    # --- CASE 3: GEWIJZIGDE SCHEPEN ---
    for schip_naam in (nieuwe_schepen_namen.intersection(oude_schepen_namen)):
        n_best = nieuwe_dict[schip_naam] 
        o_best = oude_dict[schip_naam]
        
        if n_best.get('Type') != o_best.get('Type'):
            continue

        diff = {k: {'oud': o_best.get(k, ''), 'nieuw': v} for k, v in n_best.items() if v != o_best.get(k, '')}
        
        if diff:
            gewijzigde_sleutels = set(diff.keys())
            
            if n_best.get('Type') == 'I' and gewijzigde_sleutels == {'ETA/ETD'}:
                logging.info(f"Onderdrukt: Alleen ETA/ETD gewijzigd voor inkomend schip {schip_naam}")
                continue 

            relevante = {'Besteltijd', 'ETA/ETD', 'Loods'}
            if relevante.intersection(gewijzigde_sleutels): 
                if moet_rapporteren(n_best) or moet_rapporteren(o_best):
                    wijzigingen.append({
                        'Schip': n_best.get('Schip'),
                        'status': 'GEWIJZIGD',
                        'wijzigingen': diff
                    })
            
    return wijzigingen


def format_wijzigingen_email(wijzigingen):
    """Formatteert de lijst met wijzigingen naar een plain text body."""
    body = []
    wijzigingen.sort(key=lambda x: x.get('status', ''))
    
    for w in wijzigingen:
        s_naam = re.sub(r'\s*\(d\)\s*$', '', w.get('Schip', '')).strip()
        status = w.get('status')
        
        if status == 'NIEUW':
            details = w.get('details', {})
            tekst = f"+++ NIEUW SCHIP: '{s_naam}'\n"
            tekst += f" - Type: {details.get('Type', 'N/A')}\n"
            tekst += f" - Besteltijd: {details.get('Besteltijd', 'N/A')}\n"
            tekst += f" - ETA/ETD: {details.get('ETA/ETD', 'N/A')}\n"
            tekst += f" - Loods: {details.get('Loods', 'N/A')}"
        
        elif status == 'GEWIJZIGD':
            tekst = f"*** GEWIJZIGD: '{s_naam}'\n"
            tekst += "\n".join([f" - {k}: '{v['oud']}' -> '{v['nieuw']}'" 
                                for k, v in w['wijzigingen'].items() 
                                if k in {'Besteltijd', 'ETA/ETD', 'Loods'}])
        
        elif status == 'VERWIJDERD':
            details = w.get('details', {})
            tekst = f"--- VERWIJDERD: '{s_naam}'\n"
            tekst += f" - (Was type {details.get('Type', 'N/A')}, Besteltijd {details.get('Besteltijd', 'N/A')})"

        body.append(tekst)
    
    return "\n\n".join(body)

def main():
    """De hoofdfunctie van de scraper, uitgevoerd door de cron job trigger."""
    if not all([USER, PASS, JSONBIN_API_KEY, JSONBIN_BIN_ID]):
        logging.critical("FATAL ERROR: Essential Environment Variables are not set!")
        return
    
    vorige_staat = load_state_from_jsonbin()
    if vorige_staat is None:
        logging.warning("No previous state found in jsonbin, starting fresh.")
        vorige_staat = {"bestellingen": [], "last_report_key": "", "web_snapshot": {}, "web_changes": []}
    
    oude_bestellingen = vorige_staat.get("bestellingen", [])
    last_report_key = vorige_staat.get("last_report_key", "")
    
    with data_lock:
        if "web_snapshot" in vorige_staat:
            if "content_data" in vorige_staat["web_snapshot"]:
                app_state["latest_snapshot"]["content_data"] = vorige_staat["web_snapshot"]["content_data"]
            elif "content" in vorige_staat["web_snapshot"]:
                app_state["latest_snapshot"]["content"] = vorige_staat["web_snapshot"]["content"]
            app_state["latest_snapshot"]["timestamp"] = vorige_staat["web_snapshot"].get("timestamp", "N/A")
        
        app_state["change_history"].clear()
        app_state["change_history"].extend(vorige_staat.get("web_changes", []))
    
    session = requests.Session()
    if not login(session):
        logging.error("Login failed during main() run.")
        return
    
    nieuwe_bestellingen = haal_bestellingen_op(session) 
    if not nieuwe_bestellingen:
        logging.error("Fetching orders failed during main() run.")
        return

    # --- Genereer ALTIJD een snapshot ---
    logging.info("Generating new snapshot for webpage (every run).")
    brussels_tz = pytz.timezone('Europe/Brussels')
    nu_brussels = datetime.now(brussels_tz) 
    
    snapshot_data = filter_snapshot_schepen(nieuwe_bestellingen, session, nu_brussels)
    
    with data_lock:
        app_state["latest_snapshot"] = {
            "timestamp": nu_brussels.strftime('%d-%m-%Y %H:%M:%S'), 
            "content_data": snapshot_data
        }
        app_state["latest_snapshot"].pop("content", None)
    
    # --- Change Detection ---
    if oude_bestellingen:
        wijzigingen = vergelijk_bestellingen(oude_bestellingen, nieuwe_bestellingen)
        if wijzigingen:
            inhoud = format_wijzigingen_email(wijzigingen)
            onderwerp = f"LIS Update: {len(wijzigingen)} wijziging(en)"
            logging.info(f"Found {len(wijzigingen)} changes, logging them to web history.")
            
            with data_lock:
                app_state["change_history"].appendleft({
                    "timestamp": nu_brussels.strftime('%d-%m-%Y %H:%M:%S'), 
                    "onderwerp": onderwerp,
                    "content": inhoud
                })
        else:
            logging.info("No relevant changes found.")
    else:
        logging.info("First run, establishing baseline.")
    
    # --- Scheduled Reporting (Logica behouden, actie verwijderd) ---
    report_times = [(1,0), (4, 0), (5, 30), (9,0), (12, 0), (13, 30), (17,0), (20, 0), (21, 30)]
    tijdstip_voor_rapport = None
    
    for report_hour, report_minute in report_times:
        rapport_tijd_vandaag = nu_brussels.replace(hour=report_hour, minute=report_minute, second=0, microsecond=0)
        if nu_brussels >= rapport_tijd_vandaag:
            tijdstip_voor_rapport = rapport_tijd_vandaag
        
    if tijdstip_voor_rapport:
        current_key = tijdstip_voor_rapport.strftime('%Y-%m-%d-%H:%M')
        if current_key != last_report_key:
            logging.info(f"Time for scheduled report of {tijdstip_voor_rapport.strftime('%H:%M')}. (No email will be sent).")
            last_report_key = current_key
        
    # --- Save State for Next Run ---
    nieuwe_staat = {
        "bestellingen": nieuwe_bestellingen, 
        "last_report_key": last_report_key,
        "web_snapshot": app_state["latest_snapshot"],
        "web_changes": list(app_state["change_history"])
    }
    save_state_to_jsonbin(nieuwe_staat)
    logging.info("--- Run Completed, state saved to jsonbin. ---")

# if __name__ == '__main__':
#     app.run(debug=True, port=5001)
