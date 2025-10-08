import requests
from bs4 import BeautifulSoup
import logging
from datetime import datetime, timedelta
import re
import os
import smtplib
from email.mime.text import MIMEText
import json
from collections import defaultdict
import pytz

# --- CONFIGURATIE ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Inloggegevens en e-mailinstellingen uit Render Environment Variables halen
USER = os.environ.get('LIS_USER')
PASS = os.environ.get('LIS_PASS')
SMTP_SERVER = os.environ.get('SMTP_SERVER')
SMTP_PORT = int(os.environ.get('SMTP_PORT', 587))
EMAIL_USER = os.environ.get('EMAIL_USER')
EMAIL_PASS = os.environ.get('EMAIL_PASS')
ONTVANGER_EMAIL = os.environ.get('ONTVANGER_EMAIL')

# Pad naar de databestanden op Render's gratis persistent disk
DATA_FILE_PATH = '/var/data/oude_bestellingen.json'
LAST_REPORT_FILE_PATH = '/var/data/laatste_rapport.txt'

# --- FUNCTIES ---

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
        table = soup.find('table', id='ctl00_ContentPlaceHolder1$ctl01_list_gv')
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
            if b.get("Besteltijd"):
                besteltijd = datetime.strptime(b.get("Besteltijd"), "%d/%m/%y %H:%M")
                if b.get("Type") == "U" and nu <= besteltijd <= grens_uit_toekomst: gefilterd["UITGAAND"].append(b)
                elif b.get("Type") == "I" and grens_in_verleden <= besteltijd <= grens_in_toekomst: gefilterd["INKOMEND"].append(b)
        except (ValueError, TypeError): continue
    return gefilterd

def format_snapshot_email(snapshot_data):
    body = "--- BINNENKOMENDE SCHEPEN (Besteltijd tussen -8u en +8u) ---\n"
    if snapshot_data['INKOMEND']:
        for schip in snapshot_data['INKOMEND']:
            body += f"- {schip.get('Schip', 'N/A').ljust(30)} | Besteltijd: {schip.get('Besteltijd', 'N/A')}\n"
    else:
        body += "Geen schepen die aan de criteria voldoen.\n"
    
    body += "\n--- UITGAANDE SCHEPEN (Besteltijd binnen 16u) ---\n"
    if snapshot_data['UITGAAND']:
        for schip in snapshot_data['UITGAAND']:
            body += f"- {schip.get('Schip', 'N/A').ljust(30)} | Besteltijd: {schip.get('Besteltijd', 'N/A')}\n"
    else:
        body += "Geen schepen die aan de criteria voldoen.\n"
    return body

def verstuur_email(onderwerp, inhoud):
    if not all([SMTP_SERVER, EMAIL_USER, EMAIL_PASS, ONTVANGER_EMAIL]):
        logging.error("E-mail niet verstuurd: SMTP-instellingen ontbreken in Environment Variables.")
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
        logging.info(f"E-mail succesvol verzonden naar {ONTVANGER_EMAIL}")
    except Exception as e:
        logging.error(f"E-mail versturen mislukt: {e}")

def main():
    logging.info("--- Cron Job Gestart ---")
    if not all([USER, PASS]):
        logging.critical("FATALE FOUT: LIS_USER of LIS_PASS niet ingesteld!")
        return

    session = requests.Session()
    if not login(session):
        logging.error("Inloggen mislukt. Script stopt.")
        return

    nieuwe_bestellingen = haal_bestellingen_op(session)
    if not new_bestellingen:
        logging.warning("Geen nieuwe bestellingen opgehaald. Script stopt.")
        return
    logging.info(f"{len(nieuwe_bestellingen)} bestellingen opgehaald.")

    # TAAK 1: CONTROLEER OP WIJZIGINGEN
    oude_bestellingen = []
    if os.path.exists(DATA_FILE_PATH):
        try:
            with open(DATA_FILE_PATH, 'r') as f:
                oude_bestellingen = json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            logging.warning("Kon vorig databestand niet lezen.")
    
    if oude_bestellingen:
        wijzigingen = vergelijk_bestellingen(oude_bestellingen, nieuwe_bestellingen)
        if wijzigingen:
            logging.info(f"{len(wijzigingen)} wijziging(en) gevonden!")
            inhoud = format_wijzigingen_email(wijzigingen)
            verstuur_email(f"LIS Update: {len(wijzigingen)} wijziging(en)", inhoud)
        else:
            logging.info("Geen relevante wijzigingen gevonden.")
    else:
        logging.info("Eerste run, basislijn wordt opgeslagen.")

    try:
        os.makedirs(os.path.dirname(DATA_FILE_PATH), exist_ok=True)
        with open(DATA_FILE_PATH, 'w') as f:
            json.dump(nieuwe_bestellingen, f, indent=2)
        logging.info("Nieuwe basislijn opgeslagen.")
    except Exception as e:
        logging.error(f"Kon databestand niet opslaan: {e}")

    # TAAK 2: STUUR SNAPSHOT OP VASTE TIJDEN
    brussels_tz = pytz.timezone('Europe/Brussels')
    nu_brussels = datetime.now(brussels_tz)
    
    report_times = [(4, 0), (5, 30), (12, 0), (13, 30), (20, 0), (21, 30)]
    
    minute_slot = '00' if nu_brussels.minute < 30 else '30'
    current_key = f"{nu_brussels.year}-{nu_brussels.month}-{nu_brussels.day}-{nu_brussels.hour}:{minute_slot}"
    
    last_report_key = ""
    if os.path.exists(LAST_REPORT_FILE_PATH):
        with open(LAST_REPORT_FILE_PATH, 'r') as f:
            last_report_key = f.read().strip()

    should_send_report = False
    for report_hour, report_minute in report_times:
        if nu_brussels.hour == report_hour:
            if (report_minute == 0 and 0 <= nu_brussels.minute < 15) or \
               (report_minute == 30 and 30 <= nu_brussels.minute < 45):
                should_send_report = True
                break
    
    if should_send_report and current_key != last_report_key:
        logging.info(f"Tijd voor gepland rapport: {nu_brussels.strftime('%H:%M')}")
        snapshot_data = filter_snapshot_schepen(nieuwe_bestellingen)
        inhoud = format_snapshot_email(snapshot_data)
        onderwerp = f"LIS Overzicht - {nu_brussels.strftime('%d/%m/%Y %H:%M')}"
        verstuur_email(onderwerp, inhoud)
        
        with open(LAST_REPORT_FILE_PATH, 'w') as f:
            f.write(current_key)
            
    logging.info("--- Cron Job Voltooid ---")

if __name__ == "__main__":
    main()
