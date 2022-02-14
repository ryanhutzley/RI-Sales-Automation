from simple_salesforce import Salesforce
from googleapiclient.discovery import build
from google.oauth2 import service_account
from dotenv import load_dotenv
import os

from sf_methods import upsertContacts, upsertFormattedContacts, linkedinUpdate, updateAccountTags, updateContacts

load_dotenv("../.env")

SF_USERNAME = os.environ.get("USERNAME")
SF_PASSWORD = os.environ.get("PASSWORD")
SF_TOKEN = os.environ.get("TOKEN")

sf = Salesforce(username=SF_USERNAME, password=SF_PASSWORD, security_token=SF_TOKEN)

SERVICE_ACCOUNT_FILE = 'keys.json'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

creds = None
creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

service = build('sheets', 'v4', credentials=creds)

sheet = service.spreadsheets()

# NLP_DATABEES = ['1dTr317px33-Qq4beDik0ZeBm7Q0kE3OifjBqtbcaz-A', 'Underworked: Contacts w/ Email - To Upsert!A:J', 'Underworked: Contacts w/o Email - To Upsert!A:I']
# BOUNCED_EMAIL_CONTACTS = ['1ed0Tffluu89Xm3Om47Otc5SHR1z8G4j6xNJwZnjPbPg', 'Bounced Email Contacts!A:J']

SHEET_IDS = {
    'BOUNCED_EMAILS_MERGE': ['1WJD0vJWokExILrHILdVVzxTyoVjGgXSo-2XERIMURvw', 'Given Contacts!L:S'],
    'UNDERWORKED_DATABEES': ['1W1BbZTpBSGlg4nW5OZ_13SR2JBFJtlX9J6hjjo_4kIo', 'Matched Contacts w/o Email - To Upsert!A:F', 'Matched Contacts w/ Email - To Upsert!A:G', 'Accounts to Update!A:C'],
    'NEW_NLP_DATABEES_MERGE': ['13owKMVnEIzS5rXKPG8xfqcxKm9C08a4Gumpn5fcDyCc', 'Matched Contacts w/o Email - To Upsert!A:Q', 'Matched Contacts w/ Email - To Upsert!A:R', 'Matched Accounts - To Update!K:M'],
    'ALL_INDUSTRIES_DATABEES': ['1JrWhimitcDUro-N1nqiU8ZrIsxzyiWP2kOgQegdQW9w', 'Matched Contacts w/o Email - To Upsert!A:Q', 'Matched Contacts w/ Email - To Upsert!A:R'],
    'NO_LINKEDIN': ['1HhrwsJggJr65PqYcjTw5GmW_3NfmCeVXX03sk2SilKM', 'Accounts: Company LinkedIn URL!A:C', 'Contacts: Contact LinkedIn URL!A:C']
}

for key, sheet_info in SHEET_IDS.items():

    sheet_id = sheet_info[0]

    if key == 'NO_LINKEDIN':
        company_urls = sheet.values().get(spreadsheetId=sheet_id, range=sheet_info[1]).execute().get('values', [])
        linkedinUpdate(False, company_urls, sf)
        print('Done with Company LinkedIn Updates')
        contact_urls = sheet.values().get(spreadsheetId=sheet_id, range=sheet_info[2]).execute().get('values', [])
        linkedinUpdate(True, contact_urls, sf)
        print('Done with Contact LinkedIn Updates')
    elif key == 'BOUNCED_EMAILS_MERGE':
        # bounced_given_contacts = sheet.values().get(spreadsheetId=sheet_id, range=sheet_info[1]).execute().get('values', [])
        # updateContacts(sf, bounced_given_contacts)
        print(f'Done with {key} Given Contact updates')
    else:
        no_email = sheet.values().get(spreadsheetId=sheet_id, range=sheet_info[1]).execute().get('values', [])
        email = sheet.values().get(spreadsheetId=sheet_id, range=sheet_info[2]).execute().get('values', [])
        if key == 'UNDERWORKED_DATABEES':
            no_email.pop(0)
            upsertContacts(sf, no_email)
            print(f'Done with {key} no email upserts')
            email.pop(0)
            upsertContacts(sf, email)
            print(f'Done with {key} email upserts')
        else:
            upsertFormattedContacts(False, no_email, sf)
            print(f'Done with {key} no email upserts')
            upsertFormattedContacts(True, email, sf)
            print(f'Done with {key} email upserts')
        if len(sheet_info) > 3:
            account_tags = sheet.values().get(spreadsheetId=sheet_id, range=sheet_info[3]).execute().get('values', [])
            updateAccountTags(account_tags, sf)
            print(f'Done with {key} account tag updates')