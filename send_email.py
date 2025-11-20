from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import google.auth
import json
import os
import base64
import requests
import json
from google.oauth2.credentials import Credentials
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from dotenv import load_dotenv

load_dotenv()
FPTS_PROXY = os.getenv('FPTS_PROXY')
os.environ['HTTP_PROXY'] = FPTS_PROXY
os.environ['HTTPS_PROXY'] = FPTS_PROXY

SCOPES = ["https://www.googleapis.com/auth/gmail.send"]

def create_token(SCOPE):
    flow = InstalledAppFlow.from_client_secrets_file(
        os.path.join('creds', "credentials.json"), SCOPES
    )
    creds = flow.run_local_server(port=0)

    # Save token
    with open(os.path.join('creds', "token.json"), "w") as f:
        f.write(creds.to_json())

    print("Token saved to creds/token.json")

def send_email(sender:str, receiver, body:str, subject:str):
    if 'token.json' not in os.listdir('creds'):
        create_token(SCOPES)

    url = "https://gmail.googleapis.com/gmail/v1/users/me/messages/send"

    # Load token
    creds = Credentials.from_authorized_user_file(os.path.join('creds', "token.json"))

    if creds.expired and creds.refresh_token:
        creds.refresh(Request())

    message = MIMEMultipart('alternative')
    message["From"] = sender
    message["To"] = receiver
    message["Subject"] = subject
    message.attach(MIMEText(body, 'html'))

    raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()

    headers = {
        "Authorization": f"Bearer {creds.token}",
        "Content-Type": "application/json",
    }

    data = {"raw": raw_message}

    proxies = {
        "http": FPTS_PROXY,
        "https": FPTS_PROXY,
    }

    response = requests.post(
        url,
        headers=headers,
        data=json.dumps(data),
        proxies=proxies,
    )

    # print(response.text)


# NOTI_EMAIL = os.getenv('NOTI_EMAIL')
# RECEIPANT = os.getenv('RECEIPANT')

# # Test Email
# current_date = "20-11-2025"
# html_res = "Table Goes Here"
# send_email(
#             sender=NOTI_EMAIL,
#             receiver=RECEIPANT,
#             subject=f"Interbank rate report | {current_date}",
#             body=f"""
# <html>
#   <body>
#     <h3>Crawler Report for {current_date}</h3>
#     {html_res}
#   </body>
# </html>
# """
# )