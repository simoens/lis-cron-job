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
from flask import Flask, render_template

# --- CONFIGURATIE ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- FLASK APPLICATIE (DE ETALAGE) ---
app = Flask(__name__)

@app.route('/')
def home():
    """Toont de webpagina met de laatst bekende data uit de 'kluis'."""
    logging.info("Webpagina opgevraagd. Data wordt uit jsonbin.io geladen.")
    state = load_state_from_jsonbin()
    if state is None:
        state = {
            "web_snapshot": {"timestamp": "Fout", "content": "Kon data niet laden uit de cloud. Wacht op de volgende run van de Cron Job."},
            "web_changes": []
        }
    
    # Maak een deque van de opgeslagen lijst voor de template
    changes_deque = deque(state.get("web_changes", []), maxlen=10)

    return render_template('index.html', 
                           snapshot=state.get("web_snapshot", {}), 
                           changes=list(changes_deque))

# --- ENVIRONMENT VARIABLES & ALGEMENE CONFIG ---
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
    if not all([JSONBIN_API_KEY, JSONBIN_BIN_ID]): 
        logging.error("jsonbin.io configuratie onvolledig in Environment Variables.")
        return None
    headers = {'X-Master-Key': JSONBIN_API_KEY, 'X-Bin-Meta': 'false'}
    url = f"https://api.jsonbin.io/v3/b/{JSONBIN_BIN_ID}/latest"
    try:
        res = requests.get(url, headers=headers, timeout=15)
        res.raise_for_status()
        return res.json()
    except Exception as e:
        logging.error(f"Fout bij laden van staat van jsonbin.io: {e}")
        return None

def save_state_to_jsonbin(state):
    if not all([JSONBIN_API_KEY, JSONBIN_BIN_ID]): 
        logging.error("jsonbin.io configuratie onvolledig, staat niet opgeslagen.")
        return
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
        form_data = {'__VIEWSTATE': viewstate['value'], 'ctl00$ContentPlaceHolder1$login$uname': USER, 'ctl00$ContentPlaceHolder1$login$password': PASS, 'ctl00$ContentPlaceHolder1$login$btnInloggen': 'Inloggen'}
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
    grens_in_toekomst_eta = nu + timedelta(hours=8)
    for b in bestellingen:
        try:
            if not b.get('Loods', '').strip(): continue
            besteltijd_str = b.get("Besteltijd")
            if not besteltijd_str: continue
            besteltijd = datetime.strptime(besteltijd_str, "%d/%m/%y %H:%M")
            if b.get("Type") == "U":
                if nu <= besteltijd <= grens_uit_toekomst: gefilterd["UITGAAND"].append(b)
            elif b.get("Type") == "I":
                entry_point = b.get('Entry Point', '')
                eta_dt = None
                if "KN: Steenbank" in entry_point: eta_dt = besteltijd + timedelta(hours=7)
                elif "KW: Wandelaar" in entry_point: eta_dt = besteltijd + timedelta(hours=6)
                if eta_dt and nu <= eta_dt <= grens_in_toekomst_eta:
                    b['berekende_eta'] = eta_dt.strftime("%d/%m/%y %H:%M")
                    gefilterd["INKOMEND"].append(b)
        except (ValueError, TypeError): continue
    return gefilterd

def format_snapshot_email(snapshot_data):
    body = "--- BINNENKOMENDE SCHEPEN (Berekende ETA binnen 8u & Loods toegewezen) ---\n"
    if snapshot_data.get('INKOMEND'):
        snapshot_data['INKOMEND'].sort(key=lambda x: x.get('berekende_eta', ''))
        for schip in snapshot_data['INKOMEND']:
            body += f"- {schip.get('Schip', 'N/A').ljust(30)} | Besteltijd: {schip.get('Besteltijd', 'N/A').ljust(15)} | Berekende ETA: {schip.get('berekende_eta', 'N/A').ljust(15)} | Loods: {schip.get('Loods', 'N/A')}\n"
    else:
        body += "Geen schepen die aan de criteria voldoen.\n"
    body += "\n--- UITGAANDE SCHEPEN (Besteltijd binnen 16u & Loods toegewezen) ---\n"
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

# --- HOOFDFUNCTIE VOOR DE CRON JOB (DE WERKBIJ) ---
def run_scraper_task():
    logging.info("================== Cron Job Taak Gestart ==================")
    if not all([USER, PASS, JSONBIN_API_KEY, JSONBIN_BIN_ID]):
        logging.critical("FATALE FOUT: Essentiële Environment Variables zijn niet ingesteld!")
        return
    
    # Laad de vorige staat uit de cloud
    vorige_staat = load_state_from_jsonbin()
    if vorige_staat is None:
        vorige_staat = {"bestellingen": [], "last_report_key": "", "web_snapshot": {"timestamp": "Nog niet uitgevoerd", "content": "Wachten op eerste run..."}, "web_changes": []}
    
    oude_bestellingen = vorige_staat.get("bestellingen", [])
    last_report_key = vorige_staat.get("last_report_key", "")
    
    # Herstel de webpagina staat in het geheugen van dit proces
    web_snapshot = vorige_staat.get("web_snapshot")
    web_changes = deque(vorige_staat.get("web_changes", []), maxlen=10)

    session = requests.Session()
    if not login(session): return
    nieuwe_bestellingen = haal_bestellingen_op(session)
    if not nieuwe_bestellingen: return
    
    # Vergelijk en verwerk wijzigingen
    if oude_bestellingen:
        wijzigingen = vergelijk_bestellingen(oude_bestellingen, nieuwe_bestellingen)
        if wijzigingen:
            inhoud = format_wijzigingen_email(wijzigingen)
            onderwerp = f"LIS Update: {len(wijzigingen)} wijziging(en)"
            verstuur_email(onderwerp, inhoud)
            web_changes.appendleft({
                "timestamp": datetime.now().strftime('%d-%m-%Y %H:%M:%S'),
                "onderwerp": onderwerp,
                "content": inhoud
            })
        else:
            logging.info("Geen relevante wijzigingen gevonden.")
    else:
        logging.info("Eerste run, basislijn wordt opgeslagen.")

    # Stuur snapshot op vaste tijden
    brussels_tz = pytz.timezone('Europe/Brussels')
    nu_brussels = datetime.now(brussels_tz)
    report_times = [(1, 0), (4, 0), (5, 30), (9, 0), (12, 0), (13, 30), (17, 0), (20, 0), (21, 30)]
    current_key = f"{nu_brussels.year}-{nu_brussels.month}-{nu_brussels.day}-{nu_brussels.hour}"
    should_send_report = any(h == nu_brussels.hour and m <= nu_brussels.minute < m + 15 for h, m in report_times)
    
    if should_send_report and current_key != last_report_key:
        snapshot_data = filter_snapshot_schepen(nieuwe_bestellingen)
        inhoud = format_snapshot_email(snapshot_data)
        onderwerp = f"LIS Overzicht - {nu_brussels.strftime('%d/%m/%Y %H:%M')}"
        verstuur_email(onderwerp, inhoud)
        last_report_key = current_key
        web_snapshot = {"timestamp": nu_brussels.strftime('%d-%m-%Y %H:%M:%S'), "content": inhoud}
    
    # Sla de nieuwe staat op voor de volgende run
    nieuwe_staat = {
        "bestellingen": nieuwe_bestellingen,
        "last_report_key": last_report_key,
        "web_snapshot": web_snapshot,
        "web_changes": list(web_changes)
    }
    save_state_to_jsonbin(nieuwe_staat)
    logging.info("================== Cron Job Taak Voltooid ==================")

# Deze 'if' blok zorgt ervoor dat de scraper alleen start als je `python -m app` draait.
if __name__ == '__main__':
    run_scraper_task()

