from functions import buy_sell_index
from bs4 import BeautifulSoup
import requests
from datetime import datetime
import os
from dotenv import load_dotenv
import pandas as pd
import re
from io import StringIO
from create_logger import workflow_logger
from send_email import send_email


load_dotenv()
DATA_FOLDER = os.getenv('DATA_FOLDER')
FILE_NAME = os.getenv('OMO_FILE')
FPTS_PROXY = os.getenv('FPTS_PROXY')
LOG_FOLDER = os.getenv('LOG_FOLDER')

NOTI_EMAIL = os.getenv('NOTI_EMAIL')
RECEIPENTS = os.getenv('RECEIPENTS')

url = os.getenv('OMO_URL')
file_path = os.path.join(DATA_FOLDER, FILE_NAME)
log_file = os.path.join(LOG_FOLDER, "omo-crawler.log")


# Logger
logger = workflow_logger(
    name='omo-crawler',
    log_file=log_file
).get_logger()


# Read old data
old_data = pd.read_excel(file_path)
last_crawl = old_data['date'].max()

if last_crawl != None:
    last_crawl = datetime.strptime(
        last_crawl, '%Y-%m-%d'
    )
# print(last_crawl)

proxies = {
    "http": FPTS_PROXY,
    "https": FPTS_PROXY
}

req = requests.get(url, proxies=proxies)
soup = BeautifulSoup(req.content, 'html.parser')
date_str = soup.find(class_='ls01-date').get_text().strip()

day, month, year = re.findall(r'\d+', date_str)
current_date = datetime(int(year), int(month), int(day)) # Current date on the website

if current_date > last_crawl:

    logger.info(f"Updating data for {current_date.date()}...")

    try:

        # print(f"Crawling data for {current_date}")
        html = requests.get(url, proxies=proxies).text
        omo_table = pd.read_html(StringIO(html), flavor='bs4', thousands=".", decimal=",")[0]
        
        # Locate buy, sell rows
        b, s = buy_sell_index(omo_table.iloc[:,0])

        omo_table.iloc[b+1:s, 0] = omo_table.iloc[b, 0] + " " + omo_table.iloc[b+1:s, 0]
        if s is not None:
            omo_table.iloc[s+1:, 0] = omo_table.iloc[s, 0] + " " + omo_table.iloc[s+1:, 0]
        omo_table.columns = ["side", "participants", "volume", "interest"]

        # Data standardizing
        omo_table[["participants", "complete"]] = omo_table["participants"].str.split("/", expand=True, n=1) # Split participant column
        omo_table[["side", "maturity"]] = omo_table["side"].str.split(' - ', expand=True, n=1)
        omo_table["maturity"] = omo_table["maturity"].apply(
            lambda x: int(re.findall(r"\d+", str(x))[0]) if re.findall(r"\d+", str(x)) else None
        ) # Split at "-"
        omo_table[omo_table.columns[1:]] = omo_table[omo_table.columns[1:]].apply(pd.to_numeric, errors='coerce')
        omo_table = omo_table.dropna(subset=list(omo_table.columns[1:]), how='all') # Drop string row
        omo_table['date'] = current_date.strftime('%Y-%m-%d')
        omo_table['interest'] = omo_table['interest'] / 100

        updated_data = pd.concat([old_data, omo_table])
        updated_data.to_excel(file_path, index=False)

        send_email(
            sender=NOTI_EMAIL,
            receiver=RECEIPENTS,
            subject=f"Interbank rate report | {current_date.date()}",
            body=f"""
            <html><body>
                <h3>Open Market Operation | {current_date.date()}</h3>
                {omo_table.to_html(index=False, border=1)}
            </body></html>
            """
        )

        logger.info(f"Finished updating data for {current_date.date()}...")

    except Exception as error:
        logger.exception(f"Error at {current_date.date()}")

        send_email(
            sender=NOTI_EMAIL,
            receiver=RECEIPENTS,
            subject=f"OMO Crawler Failed | {current_date.date()}",
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