from simple_salesforce import Salesforce
from googleapiclient.discovery import build
from google.oauth2 import service_account
from dotenv import load_dotenv
import os

from sf_methods import upsertContacts, updateContacts, upsertFormattedContacts, linkedinUpdate, updateAccountTags

load_dotenv("../.env")

SF_USERNAME = os.environ.get("USERNAME")
SF_PASSWORD = os.environ.get("PASSWORD")
SF_TOKEN = os.environ.get("TOKEN")

sf = Salesforce(username=SF_USERNAME, password=SF_PASSWORD, security_token=SF_TOKEN)

#
# DATA COLLECTION
#

SERVICE_ACCOUNT_FILE = 'keys.json'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

creds = None
creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

# NLP_DATABEES = '1dTr317px33-Qq4beDik0ZeBm7Q0kE3OifjBqtbcaz-A'
# BOUNCED_EMAIL_CONTACTS = '1ed0Tffluu89Xm3Om47Otc5SHR1z8G4j6xNJwZnjPbPg'

UNDERWORKED_DATABEES = '1W1BbZTpBSGlg4nW5OZ_13SR2JBFJtlX9J6hjjo_4kIo'
ALL_INDUSTRIES_DATABEES = '1JrWhimitcDUro-N1nqiU8ZrIsxzyiWP2kOgQegdQW9w'
NO_LINKEDIN = '1HhrwsJggJr65PqYcjTw5GmW_3NfmCeVXX03sk2SilKM'
BOUNCED_EMAILS_MERGE = '1WJD0vJWokExILrHILdVVzxTyoVjGgXSo-2XERIMURvw'
NEW_NLP_DATABEES_MERGE = '13owKMVnEIzS5rXKPG8xfqcxKm9C08a4Gumpn5fcDyCc'

service = build('sheets', 'v4', credentials=creds)

sheet = service.spreadsheets()

########### CONTACT UPSERTS ##############

underworked_no_email = sheet.values().get(spreadsheetId=UNDERWORKED_DATABEES, range='Matched Contacts w/o Email - To Upsert!A:F').execute().get('values', [])
underworked_no_email.pop(0)

underworked_email = sheet.values().get(spreadsheetId=UNDERWORKED_DATABEES, range='Matched Contacts w/ Email - To Upsert!A:G').execute().get('values', [])
underworked_email.pop(0)

all_inds_no_email = sheet.values().get(spreadsheetId=ALL_INDUSTRIES_DATABEES, range='Matched Contacts w/o Email - To Upsert!A:Q').execute().get('values', [])

all_inds_email = sheet.values().get(spreadsheetId=ALL_INDUSTRIES_DATABEES, range='Matched Contacts w/ Email - To Upsert!A:R').execute().get('values', [])

new_nlp_no_email = sheet.values().get(spreadsheetId=NEW_NLP_DATABEES_MERGE, range='Matched Contacts w/o Email - To Upsert!A:Q').execute().get('values', [])

new_nlp_email = sheet.values().get(spreadsheetId=NEW_NLP_DATABEES_MERGE, range='Matched Contacts w/ Email - To Upsert!A:R').execute().get('values', [])

############ CONTACT UPDATES #############

bounced_given_contacts = sheet.values().get(spreadsheetId=BOUNCED_EMAILS_MERGE, range='Given Contacts!L:S').execute().get('values', [])

############ COMPANY/CONTACT LINKEDIN UPDATES #############

company_linkedinURLs = sheet.values().get(spreadsheetId=NO_LINKEDIN, range='Accounts: Company LinkedIn URL!A:C').execute().get('values', [])

contact_linkedinURLs = sheet.values().get(spreadsheetId=NO_LINKEDIN, range='Contacts: Contact LinkedIn URL!A:C').execute().get('values', [])

################# ACCOUNT TAGS UPDATES ####################

underworked_account_tags = sheet.values().get(spreadsheetId=UNDERWORKED_DATABEES, range='Accounts to Update!A:C').execute().get('values', [])

new_nlp_account_tags = sheet.values().get(spreadsheetId=NEW_NLP_DATABEES_MERGE, range='Matched Accounts - To Update!K:M').execute().get('values', [])

#
# FUNCTION INVOCATIONS 
#

upsertContacts(underworked_no_email, sf)
print('Done with underworked_no_email upserts')

upsertContacts(underworked_email, sf)
print('Done with underworked_email upserts')

upsertFormattedContacts(False, all_inds_no_email, sf)
print('Done with all_inds_no_email upserts')

upsertFormattedContacts(True, all_inds_email, sf)
print('Done with all_inds_email upserts')

upsertFormattedContacts(False, new_nlp_no_email, sf)
print('Done with all_inds_no_email upserts')

upsertFormattedContacts(True, new_nlp_email, sf)
print('Done with all_inds_email upserts')

updateContacts(bounced_given_contacts, sf)
print('Done with bounced_given_contacts updates')

linkedinUpdate(False, company_linkedinURLs, sf)
print('Done with Account LinkedInURL updates')

linkedinUpdate(True, contact_linkedinURLs, sf)
print('Done with Contact LinkedInURL updates')

updateAccountTags(underworked_account_tags, sf)
print('Done with underworked account tag updates')

updateAccountTags(new_nlp_account_tags, sf)
print('Done with new nlp account tag updates')