from simple_salesforce import Salesforce
from googleapiclient.discovery import build
from google.oauth2 import service_account
from ingestion_functions import upsertContacts, upsertFormattedContacts, linkedinUpdate, updateContacts, updateContactTags
from contextlib import redirect_stdout
from email_function import getAndSend
from results_sender import sendResults
from dotenv import load_dotenv
from queries import queries
import datetime
import csv
import os


current_date = datetime.date.today()

load_dotenv("/Users/ryanhutzley/Desktop/RI Sales Automation/.env")

RI_GMAIL_PASSWORD = os.environ.get("RI_GMAIL_PASSWORD")

SF_USERNAME = os.environ.get("USERNAME")
SF_PASSWORD = os.environ.get("PASSWORD")
SF_TOKEN = os.environ.get("TOKEN")

sf = Salesforce(username=SF_USERNAME, password=SF_PASSWORD, security_token=SF_TOKEN)

SDR_OWNERS = {
	"Ellen Scott-Young": [os.environ.get("ELLEN_YARONSINGER_API"), os.environ.get("ELLEN_YARONS_API")],
	"Katherine Hess": [os.environ.get("KAT_YARON_S_API"), os.environ.get("KAT_YS_API")],
	"Sophia Serseri": [os.environ.get("SOPHIA_YSINGER_API"), os.environ.get("SOPHIA_Y_DASH_SINGER_API")],
	"Ryan Hutzley": [os.environ.get("RYAN_YARON_DASH_SINGER_API"), os.environ.get("RYAN_YSING_API")],
	"Mark Levinson": [os.environ.get("MARK_YARON_SINGER_API"), os.environ.get("MARK_Y_SINGER_API")],
}

SERVICE_ACCOUNT_FILE = '/Users/ryanhutzley/Desktop/RI Sales Automation/data_and_emails/keys.json'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

creds = None
creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

# NLP_DATABEES = '1dTr317px33-Qq4beDik0ZeBm7Q0kE3OifjBqtbcaz-A'
# BOUNCED_EMAIL_CONTACTS = '1ed0Tffluu89Xm3Om47Otc5SHR1z8G4j6xNJwZnjPbPg'
# EMAIL_TAGGING = '1iEwScd1_pLLMPrNFSW80FBYVbKDlceRL2dOhmVxgr6g'
# NEW_NLP_DATABEES_MERGE = '13owKMVnEIzS5rXKPG8xfqcxKm9C08a4Gumpn5fcDyCc'

UNDERWORKED_DATABEES = '1W1BbZTpBSGlg4nW5OZ_13SR2JBFJtlX9J6hjjo_4kIo'
ALL_INDUSTRIES_DATABEES = '1JrWhimitcDUro-N1nqiU8ZrIsxzyiWP2kOgQegdQW9w'
NO_LINKEDIN = '1HhrwsJggJr65PqYcjTw5GmW_3NfmCeVXX03sk2SilKM'
BOUNCED_EMAILS_MERGE = '1WJD0vJWokExILrHILdVVzxTyoVjGgXSo-2XERIMURvw'
CONTACT_TAGS = '11roMxtJJpZkpYRs5d1TrwI8tuuNdI6OgZcQSTB7xN2Q'

service = build('sheets', 'v4', credentials=creds)

sheet = service.spreadsheets()

########### CONTACT UPSERTS ##############

underworked_no_email = sheet.values().get(spreadsheetId=UNDERWORKED_DATABEES, range='Matched Contacts w/o Email - To Upsert!A:F').execute().get('values', [])
underworked_no_email.pop(0)

underworked_email = sheet.values().get(spreadsheetId=UNDERWORKED_DATABEES, range='Matched Contacts w/ Email - To Upsert!A:G').execute().get('values', [])
underworked_email.pop(0)

all_inds_no_email = sheet.values().get(spreadsheetId=ALL_INDUSTRIES_DATABEES, range='Matched Contacts w/o Email - To Upsert!A:Q').execute().get('values', [])

all_inds_email = sheet.values().get(spreadsheetId=ALL_INDUSTRIES_DATABEES, range='Matched Contacts w/ Email - To Upsert!A:R').execute().get('values', [])

############ CONTACT UPDATES #############

bounced_given_contacts = sheet.values().get(spreadsheetId=BOUNCED_EMAILS_MERGE, range='Given Contacts!L:S').execute().get('values', [])

############ COMPANY/CONTACT LINKEDIN UPDATES #############

company_linkedinURLs = sheet.values().get(spreadsheetId=NO_LINKEDIN, range='Accounts: Company LinkedIn URL!A:C').execute().get('values', [])

contact_linkedinURLs = sheet.values().get(spreadsheetId=NO_LINKEDIN, range='Contacts: Contact LinkedIn URL!A:C').execute().get('values', [])

################# ACCOUNT TAGS UPDATES ####################

underworked_account_tags = sheet.values().get(spreadsheetId=UNDERWORKED_DATABEES, range='Accounts to Update!A:C').execute().get('values', [])


################# CONTACT TAGS UPDATES ####################

contact_tags = sheet.values().get(spreadsheetId=CONTACT_TAGS, range='Load Sheet!A:C').execute().get('values', [])


def redirected(text, path):
    with open(path, 'a') as out:
        with redirect_stdout(out):
            print(text)

path = f'/Users/ryanhutzley/Desktop/RI Sales Automation/logs/data_log_{current_date}.txt'

try:
    upsertContacts(underworked_no_email, sf)
    redirected('underworked_no_email:   SUCCESS', path)
except IndexError as e:
    redirected('underworked_no_email:   FAILED', path)

try:
    upsertContacts(underworked_email, sf)
    redirected('underworked_email:      SUCCESS', path)
except IndexError as e:
    redirected('underworked_email:      FAILED', path)

try:
    upsertFormattedContacts(False, all_inds_no_email, sf)
    redirected('all_inds_no_email:      SUCCESS', path)
except IndexError as e:
    redirected('all_inds_no_email:      FAILED', path)

try:
    upsertFormattedContacts(True, all_inds_email, sf)
    redirected('all_inds_email:         SUCCESS', path)
except IndexError as e:
    redirected('all_inds_email:         FAILED', path)

try:
    linkedinUpdate(False, company_linkedinURLs, sf)
    redirected('company_linkedinURLs:   SUCCESS', path)
except IndexError as e:
    redirected('company_linkedinURLs:   FAILED', path)

try:
    linkedinUpdate(True, contact_linkedinURLs, sf)
    redirected('contact_linkedinURLs:   SUCCESS', path)
except IndexError as e:
    redirected('contact_linkedinURLs:   FAILED', path)

try:
    updateContacts(bounced_given_contacts, sf)
    redirected('bounced_given_contacts: SUCCESS')
except IndexError as e:
    redirected('bounced_given_contacts: FAILED', path)

try:
    updateContactTags(contact_tags, sf)
    redirected('contact tags:           SUCCESS', path)
except IndexError as e:
    redirected('contact_tags:           FAILED', path)

# 
# EMAIL OUTREACH
# 

email_data = getAndSend(sf, queries, SDR_OWNERS)

with open(f'/Users/ryanhutzley/Desktop/RI Sales Automation/logs/email_log_{current_date}.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerows(email_data)

# 
# SEND RESULTS VIA EMAIL
# 

sendResults(SF_USERNAME, RI_GMAIL_PASSWORD, current_date)
