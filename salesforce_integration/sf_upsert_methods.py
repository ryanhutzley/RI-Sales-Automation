from simple_salesforce.exceptions import SalesforceMalformedRequest, SalesforceResourceNotFound

def upsert_contacts(sf, contacts):

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