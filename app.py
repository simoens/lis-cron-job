import requests
from bs4 import BeautifulSoup
import logging
from datetime import datetime, timedelta
import re
import os
import smtplib
from email.mime.text import MIMEText
import json
from collections import deque
import pytz
from flask import Flask, render_template, request, abort, redirect, url_for
import threading
import time

# --- CONFIGURATIE ---
# Configure logging to provide detailed information during execution.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- GLOBALE STATE DICTIONARY (voor de webpagina) ---
# This dictionary holds the state for the Flask web application.
app_state = {
 "latest_snapshot": {"timestamp": "Nog niet uitgevoerd", "content": "Wachten op de eerste run..."},
 "change_history": deque(maxlen=10) # Stores the last 10 detected changes.
}
data_lock = threading.Lock() # Lock to ensure thread-safe access to app_state.

# --- FLASK APPLICATIE ---
app = Flask(__name__)

@app.route('/')
def home():
 """Renders the homepage, displaying the latest snapshot and change history."""
 vorige_staat = load_state_from_jsonbin()
 if vorige_staat:
  with data_lock:
   # Update the global state with data loaded from jsonbin.io
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
 """A secret URL endpoint to be called by an external cron job to trigger the scraper."""
 secret = request.args.get('secret')
 if secret != os.environ.get('SECRET_KEY'):
  logging.warning("Failed attempt to call /trigger-run with an invalid secret key.")
  abort(403)
 
 logging.info("================== External Trigger Received: Run Start ==================")
 try:
  main()
  return "OK: Scraper run completed.", 200
 except Exception as e:
  logging.critical(f"FATAL ERROR during triggered run: {e}")
  return f"ERROR: An error occurred: {e}", 500

@app.route('/force-snapshot', methods=['POST'])
def force_snapshot_route():
    """Endpoint called by the button on the webpage to force a new snapshot AND change detection."""
    secret = request.form.get('secret')
    if secret != os.environ.get('SECRET_KEY'):
        abort(403)

    logging.info("================== Manual FULL Run Triggered via Button ==================")
    try:
        # Roep de volledige main() functie aan
        main() 
    except Exception as e:
        logging.critical(f"FATAL ERROR during forced run: {e}")

    return redirect(url_for('home'))

# --- ENVIRONMENT VARIABLES ---
# Loading sensitive information and configuration from environment variables.
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
 """Loads the last saved state from a jsonbin.io bin."""
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
 """Saves the current state to a jsonbin.io bin."""
 if not all([JSONBIN_API_KEY, JSONBIN_BIN_ID]): return
 headers = {'Content-Type': 'application/json', 'X-Master-Key': JSONBIN_API_KEY}
 url = f"https://api.jsonbin.io/v3/b/{JSONBIN_BIN_ID}"
 try:
  # Use json.dumps to convert the state dictionary to a JSON string.
  res = requests.put(url, headers=headers, data=json.dumps(state), timeout=15)
  res.raise_for_status()
  logging.info("New state successfully saved to jsonbin.io.")
 except Exception as e:
  logging.error(f"Error saving state to jsonbin.io: {e}")

# --- SCRAPER FUNCTIES ---
def login(session):
 """Logs into the LIS website and returns True on success."""
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
 """Fetches the list of pilot orders from the website."""
 try:
  response = session.get("https://lis.loodswezen.be/Lis/Loodsbestellingen.aspx")
  response.raise_for_status()
  soup = BeautifulSoup(response.content, 'lxml')
  table = soup.find('table', id='ctl00_ContentPlaceHolder1_ctl01_list_gv')
  
  if table is None: return []
  
  # Mapping of desired data to their column index in the HTML table.
  kolom_indices = {"Type": 0, "Besteltijd": 5, "ETA/ETD": 6, "RTA": 7, "Loods": 10, "Schip": 11, "Entry Point": 20}
  bestellingen = []
  
  for row in table.find_all('tr')[1:]: # Skip header row
   kolom_data = row.find_all('td')
   if not kolom_data: continue
   
   bestelling = {}
   for k, i in kolom_indices.items():
    if i < len(kolom_data):
     value = kolom_data[i].get_text(strip=True)
     # Vervang "[Loods]" door een duidelijkere tekst.
     if k == "Loods" and "[Loods]" in value:
      value = "Loods werd toegewezen"
     bestelling[k] = value
   
   # Extract the 'ReisId' from the link in the 'Schip' column.
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
  logging.error(f"Error fetching orders: {e}")
  return []

def haal_pta_van_reisplan(session, reis_id):
 """Fetches the PTA for 'Saeftinghe - Zandvliet' from the voyage plan detail page."""
 if not reis_id:
  return None
 try:
  url = f"https://lis.loodswezen.be/Lis/Reisplan.aspx?ReisId={reis_id}"
  response = session.get(url, timeout=10)
  response.raise_for_status()
  soup = BeautifulSoup(response.content, 'lxml')
  
  laatste_pta_gevonden = None
  pta_pattern = re.compile(r'\d{2}-\d{2} \d{2}:\d{2}')

  # Find all table rows on the page.
  for row in soup.find_all('tr'):
   # Check if this row contains the desired location.
   if row.find(text=re.compile("Saeftinghe - Zandvliet")):
    # Search all cells in this specific row for a PTA time.
    for cell in row.find_all('td'):
     match = pta_pattern.search(cell.get_text())
     if match:
      # If a match is found, store it. The last one found will be kept.
      laatste_pta_gevonden = match.group(0)
  
  if laatste_pta_gevonden:
   logging.info(f"Last PTA found for ReisId {reis_id}: {laatste_pta_gevonden}")
   try:
    # Convert 'dd-mm HH:MM' to the desired format 'dd/mm/yy HH:MM'
    dt_obj = datetime.strptime(laatste_pta_gevonden, '%d-%m %H:%M')
    now = datetime.now()
    dt_obj = dt_obj.replace(year=now.year)
    # Correct for year change if the date is far in the past.
    if dt_obj < now - timedelta(days=180):
     dt_obj = dt_obj.replace(year=now.year + 1)
    return dt_obj.strftime("%d/%m/%y %H:%M")
   except ValueError:
    logging.warning(f"Could not parse PTA format '{laatste_pta_gevonden}' for ReisId {reis_id}.")
    return laatste_pta_gevonden # Return raw text if parsing fails.
  
  logging.warning(f"No row with a valid PTA for 'Saeftinghe - Zandvliet' found for ReisId {reis_id}")
  return None
 except Exception as e:
  logging.error(f"Error fetching voyage plan for ReisId {reis_id}: {e}")
  return None

def filter_snapshot_schepen(bestellingen, session):
 """Filters orders to create a snapshot for a specific time window."""
 gefilterd = {"INKOMEND": [], "UITGAAND": []}
 nu = datetime.now()
 # Define time windows for filtering.
 grens_uit_toekomst = nu + timedelta(hours=16)
 grens_in_verleden = nu - timedelta(hours=8)
 grens_in_toekomst = nu + timedelta(hours=8)
 
 for b in bestellingen:
  try:
   besteltijd_str = b.get("Besteltijd")
   if not besteltijd_str: continue
   besteltijd = datetime.strptime(besteltijd_str, "%d/%m/%y %H:%M")
   
   if b.get("Type") == "U": # Outgoing
    if nu <= besteltijd <= grens_uit_toekomst:
     gefilterd["UITGAAND"].append(b)
   elif b.get("Type") == "I": # Incoming
    if grens_in_verleden <= besteltijd <= grens_in_toekomst:
     pta_saeftinghe = haal_pta_van_reisplan(session, b.get('ReisId'))
     b['berekende_eta'] = pta_saeftinghe if pta_saeftinghe else 'N/A'
     # If PTA is not available, calculate an estimate based on entry point.
     if b['berekende_eta'] == 'N/A':
      entry_point = b.get('Entry Point', '').lower()
      eta_dt = None
      if "wandelaar" in entry_point: eta_dt = besteltijd + timedelta(hours=6)
      elif "steenbank" in entry_point: eta_dt = besteltijd + timedelta(hours=7)
      if eta_dt:
       b['berekende_eta'] = eta_dt.strftime("%d/%m/%y %H:%M")
     gefilterd["INKOMEND"].append(b)
  except (ValueError, TypeError):
   continue
 return gefilterd

def format_snapshot_email(snapshot_data):
 """Formats the filtered snapshot data into a plain text string."""
 body = "--- BINNENKOMENDE SCHEPEN (Besteltijd tussen -8u en +8u) ---\n"
 if snapshot_data.get('INKOMEND'):
  # Sort incoming ships by order time.
  snapshot_data['INKOMEND'].sort(key=lambda x: datetime.strptime(x.get('Besteltijd', '01/01/70 00:00'), "%d/%m/%y %H:%M"))
  for schip in snapshot_data['INKOMEND']:
   naam = schip.get('Schip', 'N/A').ljust(30)
   besteltijd_str = schip.get('Besteltijd', 'N/A')
   loods = schip.get('Loods', 'N/A')
   eta_str = schip.get('berekende_eta', 'N/A')
   body += f"- {naam} | Besteltijd: {besteltijd_str.ljust(15)} | ETA: {eta_str.ljust(15)} | Loods: {loods}\n"
 else:
  body += "Geen schepen die aan de criteria voldoen.\n"
  
 body += "\n--- UITGAANDE SCHEPEN (Besteltijd binnen 16u) ---\n"
 if snapshot_data.get('UITGAAND'):
  # Sort outgoing ships by order time.
  snapshot_data['UITGAAND'].sort(key=lambda x: datetime.strptime(x.get('Besteltijd', '01/01/70 00:00'), "%d/%m/%y %H:%M"))
  for schip in snapshot_data['UITGAAND']:
   body += f"- {schip.get('Schip', 'N/A').ljust(30)} | Besteltijd: {schip.get('Besteltijd', 'N/A')} | Loods: {schip.get('Loods', 'N/A')}\n"
 else:
  body += "Geen schepen die aan de criteria voldoen.\n"
 return body

# --- FIX: TOEGEVOEGDE FUNCTIE ---
def filter_dubbele_schepen(bestellingen):
 """
 Filters a list of orders to ensure only the most recent order for each unique ship remains.
 This prevents false change detections on outdated duplicate entries.
 """
 unieke_schepen = {}
 for b in bestellingen:
  schip_naam_raw = b.get('Schip')
  if not schip_naam_raw:
   continue
   
  # Clean ship name by removing the '(d)' suffix.
  schip_naam_gekuist = re.sub(r'\s*\(d\)\s*$', '', schip_naam_raw).strip()
  
  # If we've already seen this ship, check if the current order is newer.
  if schip_naam_gekuist in unieke_schepen:
   try:
    huidige_tijd = datetime.strptime(b.get("Besteltijd"), "%d/%m/%y %H:%M")
    opgeslagen_tijd = datetime.strptime(unieke_schepen[schip_naam_gekuist].get("Besteltijd"), "%d/%m/%y %H:%M")
    if huidige_tijd > opgeslagen_tijd:
     unieke_schepen[schip_naam_gekuist] = b # Replace with the newer one.
   except (ValueError, TypeError):
    # If dates are invalid, prefer the current one.
    unieke_schepen[schip_naam_gekuist] = b
  else:
   # First time we see this ship.
   unieke_schepen[schip_naam_gekuist] = b
   
 return list(unieke_schepen.values())

def vergelijk_bestellingen(oude, nieuwe):
 """Vergelijkt oude en nieuwe bestellijsten en rapporteert nieuwe, gewijzigde en verwijderde schepen."""
 
 # Maak dictionaries van beide lijsten voor snelle lookup
 oude_dict = {re.sub(r'\s*\(d\)\s*$', '', b.get('Schip', '')).strip(): b 
     for b in filter_dubbele_schepen(oude) if b.get('Schip')}
 nieuwe_dict = {re.sub(r'\s*\(d\)\s*$', '', b.get('Schip', '')).strip(): b 
       for b in filter_dubbele_schepen(nieuwe) if b.get('Schip')}
 
 # Haal de sets van scheepsnamen op
 oude_schepen_namen = set(oude_dict.keys())
 nieuwe_schepen_namen = set(nieuwe_dict.keys())

 wijzigingen = []
 nu = datetime.now()
 
 # --- Interne helper functie voor filtering ---
 def moet_rapporteren(bestelling):
  """Controleert of een bestelling binnen de relevante tijd en locatie valt."""
  if not bestelling.get('Besteltijd', '').strip(): return False
  
  rapporteer = True
  type_schip = bestelling.get('Type')
  
  try:
   besteltijd = datetime.strptime(bestelling.get("Besteltijd"), "%d/%m/%y %H:%M")
   if type_schip == 'I':
    # Rapporteer inkomend alleen als besteltijd binnen 8u is
    if not (nu - timedelta(hours=8) <= besteltijd <= nu + timedelta(hours=8)):
     rapporteer = False
   elif type_schip == 'U':
    # Rapporteer uitgaand alleen als besteltijd binnen 16u is
    if not (nu <= besteltijd <= nu + timedelta(hours=16)):
     rapporteer = False
  except (ValueError, TypeError) as e:
   logging.warning(f"Datum/tijd parsefout bij filteren van '{bestelling.get('Schip')}': {e}")
   return False # Niet rapporteren bij ongeldige data

  # Filter irrelevante locaties
  if 'zeebrugge' in bestelling.get('Entry Point', '').lower():
   rapporteer = False
   
  return rapporteer

 # --- CASE 1: NIEUWE SCHEPEN ---
 # Schepen die in de nieuwe lijst staan, maar niet in de oude
 for schip_naam in (nieuwe_schepen_namen - oude_schepen_namen):
  n_best = nieuwe_dict[schip_naam]
  if moet_rapporteren(n_best):
   wijzigingen.append({
    'Schip': n_best.get('Schip'), # Gebruik originele naam
    'status': 'NIEUW',
    'details': n_best 
   })

 # --- CASE 2: VERWIJDERDE SCHEPEN ---
 # Schepen die in de oude lijst stonden, maar niet meer in de newe
 for schip_naam in (oude_schepen_namen - nieuwe_schepen_namen):
  o_best = oude_dict[schip_naam]
  # Rapporteer verwijdering alleen als het een 'actieve' order was
  if moet_rapporteren(o_best): 
   wijzigingen.append({
    'Schip': o_best.get('Schip'), # Gebruik originele naam
    'status': 'VERWIJDERD',
    'details': o_best
   })

 # --- CASE 3: GEWIJZIGDE SCHEPEN ---
 # Schepen die in beide lijsten staan
 for schip_naam in (nieuwe_schepen_namen.intersection(oude_schepen_namen)):
  n_best = nieuwe_dict[schip_naam]
  o_best = oude_dict[schip_naam]
  
  diff = {k: {'oud': o_best.get(k, ''), 'nieuw': v} for k, v in n_best.items() if v != o_best.get(k, '')}
  
  if diff:
   relevante = {'Besteltijd', 'ETA/ETD', 'Loods'}
   # Rapporteer alleen als een *relevante* kolom is gewijzigd
   if relevante.intersection(diff.keys()):
    # Rapporteer alleen als het schip nog/al relevant is
    if moet_rapporteren(n_best) or moet_rapporteren(o_best):
     wijzigingen.append({
      'Schip': n_best.get('Schip'),
      'status': 'GEWIJZIGD',
      'wijzigingen': diff
     })
      
 return wijzigingen


def format_wijzigingen_email(wijzigingen):
 """Formats the list of changes into a plain text email body."""
 body = []
 
 # Sorteer op status voor een netter overzicht
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
        if k in {'Besteltijd', 'ETA/ETD', 'Loods'}]) # Toon alleen relevante wijzigingen
  
  elif status == 'VERWIJDERD':
   details = w.get('details', {})
   tekst = f"--- VERWIJDERD: '{s_naam}'\n"
   tekst += f" - (Was type {details.get('Type', 'N/A')}, Besteltijd {details.get('Besteltijd', 'N/A')})"

  body.append(tekst)
  
 return "\n\n".join(body)

def verstuur_email(onderwerp, inhoud):
 """Sends an email using SMTP configuration from environment variables."""
 if not all([SMTP_SERVER, EMAIL_USER, EMAIL_PASS, ONTVANGER_EMAIL]):
  logging.error("Email not sent: SMTP settings are missing.")
  return
 try:
  msg = MIMEText(inhoud, 'plain', 'utf-8')
  msg['Subject'] = onderwerp
  msg['From'] = EMAIL_USER
  msg['To'] = ONTVANGER_EMAIL
  
  with smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=20) as server:
   server.starttls()
   server.login(EMAIL_USER, EMAIL_PASS)
   server.sendmail(EMAIL_USER, ONTVANGER_EMAIL, msg.as_string())
  logging.info(f"Email sent successfully: '{onderwerp}'")
 except Exception as e:
  logging.error(f"Failed to send email: {e}")

def main():
    """The main function of the scraper, executed by the cron job trigger."""
    if not all([USER, PASS, JSONBIN_API_KEY, JSONBIN_BIN_ID]):
        logging.critical("FATAL ERROR: Essential Environment Variables are not set!")
        return
    
    vorige_staat = load_state_from_jsonbin()
    if vorige_staat is None:
        logging.warning("No previous state found in jsonbin, starting fresh.")
        vorige_staat = {"bestellingen": [], "last_report_key": "", "web_snapshot": app_state["latest_snapshot"], "web_changes": []}
    
    oude_bestellingen = vorige_staat.get("bestellingen", [])
    last_report_key = vorige_staat.get("last_report_key", "")
    
    with data_lock:
        app_state["latest_snapshot"] = vorige_staat.get("web_snapshot", app_state["latest_snapshot"])
        app_state["change_history"].clear()
        app_state["change_history"].extend(vorige_staat.get("web_changes", []))
    
    session = requests.Session()
    if not login(session):
        logging.error("Login failed during main() run.")
        return # Stop de uitvoering als login mislukt
    
    nieuwe_bestellingen = haal_bestellingen_op(session)
    if not nieuwe_bestellingen:
        logging.error("Fetching orders failed during main() run.")
        return # Stop de uitvoering als ophalen mislukt

    # --- Genereer ALTIJD een snapshot ---
    logging.info("Generating new snapshot for webpage (every run).")
    brussels_tz = pytz.timezone('Europe/Brussels')
    nu_brussels = datetime.now(brussels_tz)
    
    snapshot_data = filter_snapshot_schepen(nieuwe_bestellingen, session)
    snapshot_inhoud = format_snapshot_email(snapshot_data)
    
    with data_lock:
        app_state["latest_snapshot"] = {
            "timestamp": nu_brussels.strftime('%d-%m-%Y %H:%M:%S'),
            "content": snapshot_inhoud
        }
    
    # --- Change Detection ---
    if oude_bestellingen:
        wijzigingen = vergelijk_bestellingen(oude_bestellingen, nieuwe_bestellingen)
        if wijzigingen:
            inhoud = format_wijzigingen_email(wijzigingen)
            onderwerp = f"LIS Update: {len(wijzigingen)} wijziging(en)"
            logging.info(f"Found {len(wijzigingen)} changes, sending email.")
            verstuur_email(onderwerp, inhoud)
            
            with data_lock:
                app_state["change_history"].appendleft({
                    "timestamp": datetime.now().strftime('%d-%m-%Y %H:%M:%S'),
                    "onderwerp": onderwerp,
                    "content": inhoud
                })
        else:
            logging.info("No relevant changes found.")
    else:
        logging.info("First run, establishing baseline.")
    
    # --- Scheduled Reporting ---
    report_times = [(1,0), (4, 0), (5, 30), (9,0), (12, 0), (13, 30), (17,0), (20, 0), (21, 30)]
    tijdstip_voor_rapport = None
    
    for report_hour, report_minute in report_times:
        rapport_tijd_vandaag = nu_brussels.replace(hour=report_hour, minute=report_minute, second=0, microsecond=0)
        # Zoek het LAATSTE rapport-tijdstip dat al gepasseerd is VANDAAG
        if nu_brussels >= rapport_tijd_vandaag:
            tijdstip_voor_rapport = rapport_tijd_vandaag
        
    if tijdstip_voor_rapport:
        current_key = tijdstip_voor_rapport.strftime('%Y-%m-%d-%H:%M')
        if current_key != last_report_key:
            logging.info(f"Time for scheduled report of {tijdstip_voor_rapport.strftime('%H:%M')}. Key: {current_key}")
            
            onderwerp = f"LIS Overzicht - {nu_brussels.strftime('%d/%m/%Y %H:%M')}"
            verstuur_email(onderwerp, snapshot_inhoud)
            last_report_key = current_key
        else:
            logging.info(f"Scheduled report for {current_key} already sent. Skipping.")
        
    # --- Save State for Next Run ---
    nieuwe_staat = {
        "bestellingen": nieuwe_bestellingen,
        "last_report_key": last_report_key,
        "web_snapshot": app_state["latest_snapshot"], 
        "web_changes": list(app_state["change_history"])
    }
    save_state_to_jsonbin(nieuwe_staat)
    logging.info("--- Run Completed, state saved to jsonbin. ---")
  
 # --- Scheduled Reporting ---
 report_times = [(1,0), (4, 0), (5, 30), (9,0), (12, 0), (13, 30), (17,0), (20, 0), (21, 30)]
 tijdstip_voor_rapport = None
 
 for report_hour, report_minute in report_times:
  rapport_tijd_vandaag = nu_brussels.replace(hour=report_hour, minute=report_minute, second=0, microsecond=0)
  # Zoek het LAATSTE rapport-tijdstip dat al gepasseerd is VANDAAG
  if nu_brussels >= rapport_tijd_vandaag:
   tijdstip_voor_rapport = rapport_tijd_vandaag
   
 if tijdstip_voor_rapport:
  current_key = tijdstip_voor_rapport.strftime('%Y-%m-%d-%H:%M')
  if current_key != last_report_key:
   logging.info(f"Time for scheduled report of {tijdstip_voor_rapport.strftime('%H:%M')}. Key: {current_key}")
   
   onderwerp = f"LIS Overzicht - {nu_brussels.strftime('%d/%m/%Y %H:%M')}"
   verstuur_email(onderwerp, snapshot_inhoud)
   last_report_key = current_key
  else:
   logging.info(f"Scheduled report for {current_key} already sent. Skipping.")
   
 # --- Save State for Next Run ---
 nieuwe_staat = {
  "bestellingen": nieuwe_bestellingen,
  "last_report_key": last_report_key,
  "web_snapshot": app_state["latest_snapshot"], 
  "web_changes": list(app_state["change_history"])
 }
 save_state_to_jsonbin(nieuwe_staat)
 logging.info("--- Run Completed, state saved to jsonbin. ---")

# The main execution block is commented out because this script is intended
# to be run by a WSGI server like Gunicorn, not directly.
# The scraping logic is triggered via the /trigger-run endpoint.

# if __name__ == '__main__':
#  # For local testing, you can run the Flask app directly.
#  # Make sure to set the SECRET_KEY environment variable.
#  # os.environ['SECRET_KEY'] = 'your-very-secret-key-here'
#  app.run(debug=True, port=5001)
