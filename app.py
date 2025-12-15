import requests
from bs4 import BeautifulSoup
import logging
import os
import re

# --- CONFIGURATIE ---
# Vul hier je gegevens in als je lokaal test, of laat ze via ENV komen op Render
USER = os.environ.get('LIS_USER')
PASS = os.environ.get('LIS_PASS')

logging.basicConfig(level=logging.INFO, format='%(message)s')

def run_diagnosis():
    if not USER or not PASS:
        logging.error("❌ GEEN GEBRUIKERSNAAM/WACHTWOORD GEVONDEN!")
        return

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"})

    # 1. LOGIN
    logging.info("--- 1. INLOGGEN ---")
    try:
        r = session.get("https://lis.loodswezen.be/Lis/Login.aspx")
        soup = BeautifulSoup(r.content, 'lxml')
        
        payload = {
            '__VIEWSTATE': soup.find('input', {'name': '__VIEWSTATE'})['value'],
            'ctl00$ContentPlaceHolder1$login$uname': USER,
            'ctl00$ContentPlaceHolder1$login$password': PASS,
            'ctl00$ContentPlaceHolder1$login$btnInloggen': 'Inloggen'
        }
        val = soup.find('input', {'name': '__EVENTVALIDATION'})
        if val: payload['__EVENTVALIDATION'] = val['value']

        r = session.post("https://lis.loodswezen.be/Lis/Login.aspx", data=payload)
        
        if "Login.aspx" in r.url:
            logging.error("❌ Login mislukt. Check wachtwoord.")
            return
        logging.info("✅ Login succesvol.")

    except Exception as e:
        logging.error(f"❌ Crash bij login: {e}")
        return

    # 2. PAGINA OPHALEN
    logging.info("\n--- 2. TABEL OPHALEN ---")
    r = session.get("https://lis.loodswezen.be/Lis/Loodsbestellingen.aspx")
    soup = BeautifulSoup(r.content, 'lxml')
    
    table = soup.find('table', id='ctl00_ContentPlaceHolder1_ctl01_list_gv')
    
    if not table:
        logging.error("❌ Tabel 'ctl00_ContentPlaceHolder1_ctl01_list_gv' NIET gevonden in HTML.")
        # Optioneel: print een stukje HTML om te zien wat er wel is
        # logging.info(soup.prettify()[:1000])
        return

    logging.info("✅ Tabel gevonden.")

    # 3. HEADERS ANALYSEREN
    logging.info("\n--- 3. KOLOM ANALYSE ---")
    rows = table.find_all('tr')
    
    if not rows:
        logging.error("❌ Tabel is leeg (geen rijen).")
        return

    header_row = rows[0]
    headers = header_cells = header_row.find_all(['th', 'td'])
    
    logging.info(f"Aantal kolommen gevonden: {len(headers)}")
    logging.info("-" * 40)
    logging.info(f"{'INDEX':<6} | {'KOLOMNAAM (Clean text)'}")
    logging.info("-" * 40)
    
    header_map = {}
    for idx, cell in enumerate(headers):
        txt = cell.get_text(strip=True)
        header_map[idx] = txt
        logging.info(f"{idx:<6} | {txt}")
        if "Bestel" in txt:
            logging.info(f"       ^^^ HIER LIJKT DE BESTELTIJD TE ZITTEN ^^^")

    # 4. DATA ANALYSEREN (Eerste 3 schepen)
    logging.info("\n--- 4. DATA SAMPLE (Eerste 3 rijen) ---")
    
    data_rows = rows[1:4] # Pak rij 1 t/m 3 (0 is header)
    
    for i, row in enumerate(data_rows):
        cells = row.find_all('td')
        logging.info(f"\nSCHIP #{i+1}:")
        
        # Probeer scheepsnaam te vinden (vaak kolom 11, maar we checken alles)
        for idx, cell in enumerate(cells):
            waarde = cell.get_text(strip=True)
            kolom_naam = header_map.get(idx, "Onbekend")
            
            # Speciale check voor Besteltijd
            marker = ""
            if "Bestel" in kolom_naam:
                marker = " <--- DIT ZOEKEN WE"
            elif idx == 5:
                marker = " <--- HUIDIGE SCRIPT KIJKT HIER (Index 5)"
                
            logging.info(f"   [{idx}] {kolom_naam}: '{waarde}'{marker}")

    logging.info("\n--- DIAGNOSE EINDE ---")

if __name__ == "__main__":
    run_diagnosis()
