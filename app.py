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
    # Laad de meest recente staat elke keer als de pagina wordt bezocht
    # Dit zorgt ervoor dat de pagina altijd up-to-date is met wat de cron job heeft gedaan
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
        # Draai de hoofdtaak in een aparte thread om een timeout te voorkomen
        thread = threading.Thread(target=main)
        thread.start()
        return "OK: Scraper run geactiveerd.", 200
    except Exception as e:
        logging.critical(f"FATALE FOUT bij het starten van de getriggerde run: {e}")
        return f"ERROR: Er is een fout opgetreden: {e}", 500

@app.route('/force-snapshot', methods=['POST'])
def force_snapshot_route():
    """Wordt aangeroepen door de knop op de webpagina om een overzicht te forceren."""
    secret = request.form.get('secret')
    if secret != os.environ.get('SECRET_KEY'):
        abort(403)
    
    logging.info("================== Handmatige Snapshot Geactiveerd ==================")
    try:
        # Draai de taak in een aparte thread om de gebruiker niet te laten wachten
        thread = threading.Thread(target=force_snapshot_task)
        thread.start()
    except Exception as e:
        logging.critical(f"FATALE FOUT tijdens geforceerde snapshot: {e}")
    
    # Geef de gebruiker even de tijd om de taak te starten en stuur dan terug
    time.sleep(2)
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
            bestelling = {k: kolom_data[i].get('title','').strip() if k=="RTA" else kolom_data[i].text.strip() for k, i in kolom_indices.items() if i < len(kolom_data)}
            bestellingen.append(bestelling)
        return bestellingen
    except Exception as e:
        logging.error(f"Error bij ophalen bestellingen: {e}")
        return []

def filter_dubbele_schepen(bestellingen_lijst):
    schepen_gegroepeerd = defaultdict(list)
    for bestelling in bestellingen_lijst:
        schip_naam = bestelling.get('Schip')
        if schip_naam: schepen_gegroepeerd[re.sub(r'\s*\(d\)\s*$', '', schip_naam).strip()].append(bestelling)
    gefilterde_lijst = []
    nu = datetime.now()
    for schip_naam_gekuist, dubbele_bestellingen in schepen_gegroepeerd.items():
        if len(dubbele_bestellingen) == 1:
            gefilterde_lijst.append(dubbele_bestellingen[0])
            continue
        toekomstige_orders = []
        for bestelling in dubbele_bestellingen:
            try:
                if bestelling.get("Besteltijd"):
                    parsed_tijd = datetime.strptime(bestelling.get("Besteltijd"), "%d/%m/%y %H:%M")
                    if parsed_tijd >= nu: toekomstige_orders.append((parsed_tijd, bestelling))
            except (ValueError, TypeError): continue
        if toekomstige_orders:
            toekomstige_orders.sort(key=lambda x: x[0])
            gefilterde_lijst.append(toekomstige_orders[0][1])
    return gefilterde_lijst

def vergelijk_bestellingen(oude, nieuwe):
    oude_dict = {re.sub(r'\s*\(d\)\s*$', '', b.get('Schip', '')).strip(): b for b in filter_dubbele_schepen(oude) if b.get('Schip')}
    wijzigingen = []
    nu = datetime.now()
    for n_best in filter_dubbele_schepen(nieuwe):
        n_schip_raw = n_best.get('Schip')
        if not n_schip_raw: continue
        n_schip_gekuist = re.sub(r'\s*\(d\)\s*$', '', n_schip_raw).strip()
        if n_schip_gekuist not in oude_dict: continue
        o_best = oude_dict[n_schip_gekuist]
        diff = {k: {'oud': o_best.get(k, ''), 'nieuw': v} for k, v in n_best.items() if v != o_best.get(k, '')}
        if diff:
            if not n_best.get('Besteltijd', '').strip(): continue
            relevante = {'Besteltijd', 'ETA/ETD', 'Loods'}
            if not relevante.intersection(diff.keys()): continue
            rapporteer = True
            type_schip = n_best.get('Type')
            try:
                if type_schip == 'I':
                    if len(diff) == 1 and 'ETA/ETD' in diff: rapporteer = False
                    if rapporteer and o_best.get("Besteltijd") and datetime.strptime(o_best.get("Besteltijd"), "%d/%m/%y %H:%M") > (nu + timedelta(hours=8)): rapporteer = False
                elif type_schip == 'U':
                    if n_best.get("Besteltijd") and datetime.strptime(n_best.get("Besteltijd"), "%d/%m/%y %H:%M") > (nu + timedelta(hours=16)): rapporteer = False
            except (ValueError, TypeError): pass
            if rapporteer and 'zeebrugge' in n_best.get('Entry Point', '').lower(): rapporteer = False
            if rapporteer: wijzigingen.append({'Schip': n_schip_raw, 'wijzigingen': diff})
    return wijzigingen

def format_wijzigingen_email(wijzigingen):
    body = []
    for w in wijzigingen:
        s_naam = re.sub(r'\s*\(d\)\s*$', '', w.get('Schip', '')).strip()
        tekst = f"Wijziging voor '{s_naam}':\n"
        tekst += "\n".join([f"   - {k}: '{v['oud']}' -> '{v['nieuw']}'" for k, v in w['wijzigingen'].items()])
        body.append(tekst)
    return "\n\n".join(body)

def filter_snapshot_schepen(bestellingen):
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
                    entry_point = b.get('Entry Point', '')
                    eta_dt = None
                    if "KW: Wandelaar" in entry_point: eta_dt = besteltijd + timedelta(hours=6)
                    elif "KN: Steenbank" in entry_point: eta_dt = besteltijd + timedelta(hours=7)
                    b['berekende_eta'] = eta_dt.strftime("%d/%m/%y %H:%M") if eta_dt else 'N/A'
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
            body += f"- {naam} | Besteltijd: {besteltijd_str.ljust(15)} | Berekende ETA: {eta_str.ljust(15)} | Loods: {loods}\n"
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

def verstuur_email(onderwerp, inhoud):
    if not all([SMTP_SERVER, EMAIL_USER, EMAIL_PASS, ONTVANGER_EMAIL]):
        logging.error("E-mail niet verstuurd: SMTP-instellingen ontbreken.")
        return
    try:
        msg = MIMEText(inhoud, 'plain', 'utf-8')
        msg['Subject'] = onderwerp
        msg['From'] = EMAIL_USER
        msg['To'] = ONTVANGER_EMAIL
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_USER, EMAIL_PASS)
            server.sendmail(EMAIL_USER, ONTVANGER_EMAIL, msg.as_string())
        logging.info(f"E-mail succesvol verzonden: '{onderwerp}'")
    except Exception as e:
        logging.error(f"E-mail versturen mislukt: {e}")

# --- TAAK-SPECIFIEKE FUNCTIE VOOR DE KNOP ---
def force_snapshot_task():
    """Voert alleen de logica uit om een nieuw overzicht te genereren en versturen."""
    session = requests.Session()
    if not login(session): return
    nieuwe_bestellingen = haal_bestellingen_op(session)
    if not nieuwe_bestellingen: return

    brussels_tz = pytz.timezone('Europe/Brussels')
    nu_brussels = datetime.now(brussels_tz)
    
    snapshot_data = filter_snapshot_schepen(nieuwe_bestellingen)
    inhoud = format_snapshot_email(snapshot_data)
    onderwerp = f"LIS Overzicht (Geforceerd) - {nu_brussels.strftime('%d/%m/%Y %H:%M')}"
    verstuur_email(onderwerp, inhoud)
    
    with data_lock:
        app_state["latest_snapshot"] = {
            "timestamp": nu_brussels.strftime('%d-%m-%Y %H:%M:%S'),
            "content": inhoud
        }

    # Sla de bijgewerkte snapshot op in de cloud
    current_state = load_state_from_jsonbin()
    if current_state is None:
        current_state = {}
    current_state["web_snapshot"] = app_state["latest_snapshot"]
    save_state_to_jsonbin(current_state)

# --- HOOFDFUNCTIE (voor de cron job) ---
def main():
    if not all([USER, PASS, JSONBIN_API_KEY, JSONBIN_BIN_ID]):
        logging.critical("FATALE FOUT: Essentiële Environment Variables zijn niet ingesteld!")
        return
    
    vorige_staat = load_state_from_jsonbin()
    if vorige_staat is None:
        vorige_staat = {"bestellingen": [], "last_report_key": "", "web_snapshot": app_state["latest_snapshot"], "web_changes": []}
    
    oude_bestellingen = vorige_staat.get("bestellingen", [])
    last_report_key = vorige_staat.get("last_report_key", "")
    
    with data_lock:
        app_state["latest_snapshot"] = vorige_staat.get("web_snapshot", app_state["latest_snapshot"])
        app_state["change_history"].clear()
        app_state["change_history"].extend(vorige_staat.get("web_changes", []))

    session = requests.Session()
    if not login(session): return
    nieuwe_bestellingen = haal_bestellingen_op(session)
    if not nieuwe_bestellingen: return
    
    if oude_bestellingen:
        wijzigingen = vergelijk_bestellingen(oude_bestellingen, nieuwe_bestellingen)
        if wijzigingen:
            inhoud = format_wijzigingen_email(wijzigingen)
            onderwerp = f"LIS Update: {len(wijzigingen)} wijziging(en)"
            verstuur_email(onderwerp, inhoud)
            with data_lock:
                app_state["change_history"].appendleft({
                    "timestamp": datetime.now().strftime('%d-%m-%Y %H:%M:%S'),
                    "onderwerp": onderwerp,
                    "content": inhoud
                })
        else:
            logging.info("Geen relevante wijzigingen gevonden.")
    else:
        logging.info("Eerste run, basislijn wordt opgeslagen.")

    brussels_tz = pytz.timezone('Europe/Brussels')
    nu_brussels = datetime.now(brussels_tz)
    
    report_times = [(1,0), (4, 0), (5, 30), (9,0), (12, 0), (13, 30), (17,0), (20, 0), (21, 30)]
    
    tijdstip_voor_rapport = None
    for report_hour, report_minute in report_times:
        rapport_tijd_vandaag = nu_brussels.replace(hour=report_hour, minute=report_minute, second=0, microsecond=0)
        if nu_brussels >= rapport_tijd_vandaag:
            tijdstip_voor_rapport = rapport_tijd_vandaag
            
    if tijdstip_voor_rapport:
        current_key = tijdstip_voor_rapport.strftime('%Y-%m-%d-%H:%M')
        if current_key != last_report_key:
            logging.info(f"Tijd voor gepland rapport van {tijdstip_voor_rapport.strftime('%H:%M')}")
            snapshot_data = filter_snapshot_schepen(nieuwe_bestellingen)
            inhoud = format_snapshot_email(snapshot_data)
            onderwerp = f"LIS Overzicht - {nu_brussels.strftime('%d/%m/%Y %H:%M')}"
            verstuur_email(onderwerp, inhoud)
            last_report_key = current_key
            with data_lock:
                app_state["latest_snapshot"] = {
                    "timestamp": nu_brussels.strftime('%d-%m-%Y %H:%M:%S'),
                    "content": inhoud
                }
    
    nieuwe_staat = {
        "bestellingen": nieuwe_bestellingen,
        "last_report_key": last_report_key,
        "web_snapshot": app_state["latest_snapshot"],
        "web_changes": list(app_state["change_history"])
    }
    save_state_to_jsonbin(nieuwe_staat)
    logging.info("--- Cron Job Voltooid ---")

