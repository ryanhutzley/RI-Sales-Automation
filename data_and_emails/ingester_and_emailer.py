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
	"Ellen Scott-Young": [
        (os.environ.get("ELLEN_YARONSINGER_API"), 'yaronsinger'), 
        (os.environ.get("ELLEN_YARONS_API"), 'yarons')
    ],
	"Katherine Hess": [
        (os.environ.get("KAT_YARON_S_API"), 'yaron_s'),
        (os.environ.get("KAT_YS_API"), 'ys')
    ],
	"Sophia Serseri": [
        (os.environ.get("SOPHIA_YSINGER_API"), 'ysinger'), 
        (os.environ.get("SOPHIA_Y_DASH_SINGER_API"), 'y-singer')
    ],
	"Ryan Hutzley": [
        (os.environ.get("RYAN_YARON_DASH_SINGER_API"), 'yaron-singer'), 
        (os.environ.get("RYAN_YSING_API"), 'ysing')
    ],
	"Mark Levinson": [
        (os.environ.get("MARK_YARON_SINGER_API"), 'yaron_singer'),
        (os.environ.get("MARK_Y_SINGER_API"), 'y_singer')
    ],
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

################# CONTACT TAGS UPDATES ####################

contact_tags = sheet.values().get(spreadsheetId=CONTACT_TAGS, range='Load Sheet!A:C').execute().get('values', [])


with open(f'/Users/ryanhutzley/Desktop/RI Sales Automation/logs/log_{current_date}.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["Data Ingestion"])
    writer.writerow(["Data Title", "Status"])

    try:
        upsertContacts(underworked_no_email, sf)
        writer.writerow(['underworked_no_emai', 'SUCCESS'])
    except IndexError as e:
        writer.writerow(['underworked_no_emai', 'FAILED'])

    try:
        upsertContacts(underworked_email, sf)
        writer.writerow(['underworked_email', 'SUCCESS'])
    except IndexError as e:
        writer.writerow(['underworked_email', 'FAILED'])

    try:
        upsertFormattedContacts(False, all_inds_no_email, sf)
        writer.writerow(['all_inds_no_email', 'SUCCESS'])
    except IndexError as e:
        writer.writerow(['all_inds_no_email', 'FAILED'])

    try:
        upsertFormattedContacts(True, all_inds_email, sf)
        writer.writerow(['all_inds_email', 'SUCCESS'])
    except IndexError as e:
        writer.writerow(['all_inds_email', 'FAILED'])

    try:
        linkedinUpdate(False, company_linkedinURLs, sf)
        writer.writerow(['company_linkedinURL', 'SUCCESS'])
    except IndexError as e:
        writer.writerow(['company_linkedinURL', 'FAILED'])

    try:
        linkedinUpdate(True, contact_linkedinURLs, sf)
        writer.writerow(['contact_linkedinURL', 'SUCCESS'])
    except IndexError as e:
        writer.writerow(['contact_linkedinURL', 'FAILED'])

    try:
        updateContacts(bounced_given_contacts, sf)
        writer.writerow(['bounced_given_contacts', 'SUCCESS'])
    except IndexError as e:
        writer.writerow(['bounced_given_contacts', 'FAILED'])

    try:
        updateContactTags(contact_tags, sf)
        writer.writerow(['contact tags', 'SUCCESS'])
    except IndexError as e:
        writer.writerow(['contact_tags', 'FAILED'])

    writer.writerow([''])

    # 
    # EMAIL OUTREACH
    # 

    writer.writerow(["Email Outreach"])
    email_data = getAndSend(sf, queries, SDR_OWNERS, writer)

# 
# SEND RESULTS VIA EMAIL
# 

sendResults(SF_USERNAME, RI_GMAIL_PASSWORD, current_date)
