import requests
import pandas as pd
from io import StringIO
from datetime import datetime
from bs4 import BeautifulSoup
import logging
from logging.handlers import RotatingFileHandler
from send_email import send_email
from dotenv import load_dotenv
import os

# Load env variables
load_dotenv()
FILE_NAME = os.getenv('FILE_NAME')
daily_url = os.getenv('DAILY_INTERBANK_URL')
NOTI_EMAIL = os.getenv('NOTI_EMAIL')
RECEIPENTS = os.getenv('RECEIPENTS')
DATA_FOLDER = os.getenv('DATA_FOLDER')
LOG_FOLDER = os.getenv('LOG_FOLDER')
FPTS_PROXY = os.getenv('FPTS_PROXY')

# File names
file_path = os.path.join(DATA_FOLDER, FILE_NAME)
log_file = os.path.join(LOG_FOLDER, "interbank-crawler.log")

# Logging handler
logger = logging.getLogger('interbank-crawler')
logger.setLevel(logging.INFO)

handler = RotatingFileHandler(
    log_file, maxBytes=5_000_000, backupCount=5 , encoding='utf-8'
)
formatter = logging.Formatter(
    '%(asctime)s | %(levelname)s | %(message)s',
    datefmt="%Y-%m-%d %H:%M:%S"
)
handler.setFormatter(formatter)
logger.addHandler(handler)

# Read old data
old_data = pd.read_excel(file_path)
latest_date = old_data['date'].max()

# Request website

proxies = {
    "http": FPTS_PROXY,
    "https": FPTS_PROXY
}
 
r = requests.get(daily_url, proxies=proxies)
soup = BeautifulSoup(r.text, 'html.parser')

table = soup.find_all("table", class_="bi01-table")
date_str = soup.select_one('.bi01-subnote strong').get_text(strip=True)
current_date = datetime.strptime(date_str, '%d/%m/%Y') # Date on the website

# Crawler
if current_date > latest_date:

    logger.info(f"Updating data for {current_date.date()}...")
    
    try:

        today_rate = pd.read_html(StringIO(str(table[1])), decimal='.', flavor='bs4', thousands=".")[0]
        today_rate.columns = ['maturity', 'interbank_rate', 'value']
        today_rate = today_rate.dropna().assign(
            date = current_date,
            value = lambda df: df['value']\
                .str.replace(r"\(\*\)", "", regex=True).astype(str)\
                    .apply(lambda x: x.replace(",", "", x.count(",") - 1))\
                        .str.replace(",", ".").astype(float) * 1_000_000_000,
            interbank_rate = lambda df: df['interbank_rate'].str.replace(r"\(\*\)", "", regex=True)\
                .str.replace(",", '.').astype(float) / 100
        )

        updated_data = pd.concat([old_data, today_rate])
        updated_data.to_excel(file_path, index=False)

        logger.info(f"Successfully updated data for {current_date.date()}...")
        html_res = today_rate.to_html(index=False, border=1)

        # Send notification email
        send_email(
            sender=NOTI_EMAIL,
            receiver=RECEIPENTS,
            subject=f"Interbank rate report | {current_date.date()}",
            body=f"""
            <html><body>
                <h3>Crawler Report for {current_date.date()}</h3>
                {html_res}
            </body></html>
            """
        )

    except Exception as error:
        logger.exception(f"Error at {current_date.date()}")

        send_email(
            sender=NOTI_EMAIL,
            receiver=RECEIPENTS,
            subject=f"Interbank Crawler Failed | {current_date.date()}",
            body=f"""
            <html><body>
                <h3>Crawler failed on {current_date.date()}</h3>
                {error}
            </body></html>
            """
        )

else:
    logger.info(f'Data is already updated')

    send_email(
        sender=NOTI_EMAIL,
        receiver=RECEIPENTS,
        subject=f"No new Data | {current_date.date()}",
        body=f"""
        <html><body>
            <h3>Crawler Report on {current_date.date()}</h3>
            Data is either already present in the database, or it's not updated on the official website. Please review
        </body></html>
        """
    )