from simple_salesforce import Salesforce
from googleapiclient.discovery import build
from google.oauth2 import service_account
from dotenv import load_dotenv
import os

from sf_upsert_methods import upsert_contacts

# Underworked Account - Automated Databees Merge Sheet ID: 1W1BbZTpBSGlg4nW5OZ_13SR2JBFJtlX9J6hjjo_4kIo
# W/O Email Sheet Name: Matched Contacts w/o Email - To Upsert
# W/ Email Sheet Name: Matched Contacts w/ Email - To Upsert
# Account Tags Sheet Name: Accounts to Update

# All Industries - Automated Databees Merge Sheet ID: 1JrWhimitcDUro-N1nqiU8ZrIsxzyiWP2kOgQegdQW9w
# Full Contact Sheet Name: ???

# Accounts and Contacts w/o LinkedIn URL ID: 1HhrwsJggJr65PqYcjTw5GmW_3NfmCeVXX03sk2SilKM
# Account Sheet Name: Accounts: Company LinkedIn URL
# Contacts: Contacts: Contact LinkedIn URL

load_dotenv("../.env")

SF_USERNAME = os.environ.get("USERNAME")
SF_PASSWORD = os.environ.get("PASSWORD")
SF_TOKEN = os.environ.get("TOKEN")

sf = Salesforce(username=SF_USERNAME, password=SF_PASSWORD, security_token=SF_TOKEN)

############## GOOGLE SHEETS API ##################

SERVICE_ACCOUNT_FILE = 'keys.json'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

creds = None
creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

UNDERWORKED_DATABEES = '1W1BbZTpBSGlg4nW5OZ_13SR2JBFJtlX9J6hjjo_4kIo'
ALL_INDUSTRIES_DATABEES = '1JrWhimitcDUro-N1nqiU8ZrIsxzyiWP2kOgQegdQW9w'
NO_LINKEDIN = '1HhrwsJggJr65PqYcjTw5GmW_3NfmCeVXX03sk2SilKM'
NLP_DATABEES = '1dTr317px33-Qq4beDik0ZeBm7Q0kE3OifjBqtbcaz-A'
BOUNCED_EMAIL_CONTACTS = '1ed0Tffluu89Xm3Om47Otc5SHR1z8G4j6xNJwZnjPbPg'

service = build('sheets', 'v4', credentials=creds)

sheet = service.spreadsheets()

########### CONTACT UPSERTS ##############

contacts_no_email = sheet.values().get(spreadsheetId=UNDERWORKED_DATABEES, range='Matched Contacts w/o Email - To Upsert!A:F').execute()
contacts_to_upsert_no_email = contacts_no_email.get('values', [])
contacts_to_upsert_no_email.pop(0)
upsert_contacts(sf, contacts_to_upsert_no_email)
print('Done with contacts_to_upsert_no_email')

contacts_email = sheet.values().get(spreadsheetId=UNDERWORKED_DATABEES, range='Matched Contacts w/ Email - To Upsert!A:G').execute()
contacts_to_upsert_email = contacts_email.get('values', [])
contacts_to_upsert_email.pop(0)
upsert_contacts(sf, contacts_to_upsert_email)
print('Done with contacts_to_upsert_email')

additional_contacts_no_email = sheet.values().get(spreadsheetId=ALL_INDUSTRIES_DATABEES, range='Matched Contacts w/o Email - To Upsert!A:Q').execute()
added_contacts_no_email = additional_contacts_no_email.get('values', [])
added_contacts_no_email.pop(0)
formatted_added_no_email = [[row[1].strip(), row[6].strip(), row[12].strip(), row[13].strip(), row[15].strip(), row[14].strip()] for row in added_contacts_no_email]
upsert_contacts(sf, formatted_added_no_email)
print('Done with formatted_added_no_email')

additional_contacts_email = sheet.values().get(spreadsheetId=ALL_INDUSTRIES_DATABEES, range='Matched Contacts w/ Email - To Upsert!A:R').execute()
added_contacts_email = additional_contacts_email.get('values', [])
added_contacts_email.pop(0)
formatted_added_email = [[row[1].strip(), row[6].strip(), row[12].strip(), row[13].strip(), row[15].strip(), row[14].strip(), row[16].strip()] for row in added_contacts_email]
upsert_contacts(sf, formatted_added_email)
print('Done with formatted_added_email')

# nlp_contacts_email = sheet.values().get(spreadsheetId=NLP_DATABEES, range='Underworked: Contacts w/ Email - To Upsert!A:J').execute()
# nlp_email = nlp_contacts_email.get('values', [])
# nlp_email.pop(0)
# formatted_nlp_email = [[row[0].strip(), row[3].strip(), row[4].strip(), row[5].strip(), row[7].strip(), row[6].strip(), row[8].strip()] for row in nlp_email]
# upsert_contacts(sf, formatted_nlp_email)
# print('Done with formatted_nlp_email')

nlp_contacts_no_email = sheet.values().get(spreadsheetId=NLP_DATABEES, range='Underworked: Contacts w/o Email - To Upsert!A:I').execute()
nlp_no_email = nlp_contacts_no_email.get('values', [])
nlp_no_email.pop(0)
formatted_nlp_no_email = [[row[0].strip(), row[3].strip(), row[4].strip(), row[5].strip(), row[7].strip(), row[6].strip()] for row in nlp_no_email]
upsert_contacts(sf, formatted_nlp_no_email)
print('Done with formatted_nlp_no_email')

############ COMPANY/CONTACT LINKEDIN UPDATES #############

company_linkedinURLs = sheet.values().get(spreadsheetId=NO_LINKEDIN, range='Accounts: Company LinkedIn URL!A:C').execute()
company_URLs = company_linkedinURLs.get('values', [])
company_URLs.pop(0)
formatted_company_URLs = [{'Id': row[0], 'Company_LinkedIn_URL__c': row[1]} for row in company_URLs]
sf.bulk.Account.update(formatted_company_URLs, batch_size=10000, use_serial=False)
print('Done with Account LinkedInURL updates')

contact_linkedinURLs = sheet.values().get(spreadsheetId=NO_LINKEDIN, range='Contacts: Contact LinkedIn URL!A:C').execute()
contact_URLs = contact_linkedinURLs.get('values', [])
contact_URLs.pop(0)
formatted_contact_URLs = [{'Id': row[0], 'Contact_LinkedIn_URL__c': row[1]} for row in contact_URLs]
sf.bulk.Contact.update(formatted_contact_URLs, batch_size=10000, use_serial=False)
print('Done with Contact LinkedInURL updates')

################# ACCOUNT TAGS UPDATES ####################

accounts_with_tags = sheet.values().get(spreadsheetId=UNDERWORKED_DATABEES, range='Accounts to Update!A:C').execute()
accounts_with_matching_tags = accounts_with_tags.get('values', [])
accounts_with_matching_tags.pop(0)
formatted_accounts_with_matching_tags = [{'Id': row[0], 'Account_Hashtag__c': row[1]} for row in accounts_with_matching_tags]
sf.bulk.Account.update(formatted_accounts_with_matching_tags, batch_size=10000, use_serial=False)
print('Done with Account tag updates')

# nlp_account_tags_update = sheet.values().get(spreadsheetId=NLP_DATABEES, range='Underworked: Accounts to Update!A:C').execute()
# nlp_tags_update = nlp_account_tags_update.get('values', [])
# nlp_tags_update.pop(0)
# formatted_nlp_tags_update = [{'Id': row[0], 'Account_Hashtag__c': row[1]} for row in nlp_tags_update]
# sf.bulk.Account.update(formatted_nlp_tags_update, batch_size=10000, use_serial=False)
# print('Done with NLP Account tag updates')

############# BOUNCED EMAIL CLEANSING ################## 

# bounced_contacts_from_sf = sheet.values().get(spreadsheetId=BOUNCED_EMAIL_CONTACTS, range='Bounced Email Contacts!A:J').execute()
# bounced_contacts_sf = bounced_contacts_from_sf.get('values', [])
# bounced_contacts_sf.pop(0)
# formatted_bounced_contacts_sf = [{'Id': row[0], 'Email': '', 'Contact_Notes__c': f'Bounced Email: {row[1]}'} for row in bounced_contacts_sf]

# databees_sheet_update = sheet.values().append(spreadsheetId=BOUNCED_EMAIL_CONTACTS, range='Bounced Email Contacts Databees', valueInputOption='USER_ENTERED', body={'values': bounced_contacts_sf}).execute()

# sf.bulk.Contact.update(formatted_bounced_contacts_sf, batch_size=10000, use_serial=False)