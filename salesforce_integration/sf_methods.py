from simple_salesforce.exceptions import SalesforceMalformedRequest, SalesforceResourceNotFound

def upsertContacts(contacts, sf):

    for row in contacts:

        account_id = row[0].strip()
        company_linkedin = row[1].strip()
        first_name = row[2].strip()
        last_name = row[3].strip()
        contact_linkedin = row[4].strip()
        title = row[5].strip()
        email = ''
        try:
            email = row[6].strip()
        except:
            email = ''

        if account_id == '':
            print('No more contacts')
            break

        try:
            sf.Contact.create(
                {
                    'AccountId': f'{account_id}',
                    'Company_LinkedIn_URL__c': f'{company_linkedin}',
                    'FirstName': f'{first_name}',
                    'LastName': f'{last_name}', 
                    'Contact_LinkedIn_URL__c': f'{contact_linkedin}', 
                    'Title': f'{title}',
                    'Email': f'{email}'
                }
            )
            print(f'{account_id} {first_name} {last_name} created')
        except SalesforceMalformedRequest as e:
            contact_id = e.content[0]['duplicateResut']['matchResults'][0]['matchRecords'][0]['record']['Id']
            try:
                sf.Contact.update(
                    f'{contact_id}', 
                    {
                        'Company_LinkedIn_URL__c': f'{company_linkedin}', 
                        'Contact_LinkedIn_URL__c': f'{contact_linkedin}', 
                        'Title': f'{title}',
                        'Email': f'{email}'
                    }
                )
                print(f'{contact_id} {first_name} {last_name} updated')
            except SalesforceResourceNotFound as e:
                print(f'COULD NOT BE UPDATED: {contact_id} {first_name} {last_name}')


def updateContacts(contacts, sf):

    contacts.pop(0)
    formatted_contact_info = [{'Id': row[0], 'FirstName': row[1], 'LastName': row[2], 'Email': row[3], 'Title': row[5], 'Contact_LinkedIn_URL__c': row[6]} for row in contacts]
    sf.bulk.Contact.update(formatted_contact_info, batch_size=10000, use_serial=False)


def upsertFormattedContacts(has_email, data, sf):

    data.pop(0)
    if has_email: 
        formatted_data_email = [[row[1].strip(), row[6].strip(), row[12].strip(), row[13].strip(), row[15].strip(), row[14].strip(), row[16].strip()] for row in data]
        upsertContacts(formatted_data_email, sf)
    else:
        formatted_data_no_email = [[row[1].strip(), row[6].strip(), row[12].strip(), row[13].strip(), row[15].strip(), row[14].strip()] for row in data]
        upsertContacts(formatted_data_no_email, sf)


def linkedinUpdate(is_contact, data, sf):
    
    data.pop(0)
    if is_contact:
        formatted_contact_linkedins = [{'Id': row[0], 'Contact_LinkedIn_URL__c': row[1]} for row in data]
        sf.bulk.Contact.update(formatted_contact_linkedins, batch_size=10000, use_serial=False)
    else:
        formatted_company_linkedins = [{'Id': row[0], 'Company_LinkedIn_URL__c': row[1]} for row in data]
        sf.bulk.Account.update(formatted_company_linkedins, batch_size=10000, use_serial=False)
    

def updateAccountTags(data, sf):

    data.pop(0)
    tags = [{'Id': row[0], 'Account_Hashtag__c': row[1]} for row in data]
    sf.bulk.Account.update(tags, batch_size=10000, use_serial=False)