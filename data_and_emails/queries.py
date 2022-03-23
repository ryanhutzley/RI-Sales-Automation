queries = [
    {
        # LOW LEVEL QUERY
        'seniority': '''
            SELECT FirstName, Email, Account.Name, Account.SDR_Owner__c, Title, Account.Account_Score_Tier__c
            FROM CONTACT
            WHERE LastActivityDate != LAST_N_DAYS:60
            AND (EMAIL != null)
            AND (
                Job_Function__c = 'Data'
                OR Job_Function__c = 'AI'
                OR Job_Function__c = 'ML'
                OR Job_Function__c = 'Artificial Intelligence'
                OR Job_Function__c = 'Machine Learning'
                OR Job_Function__c = 'Data Science'
                OR Job_Function__c = 'Engineering'
                OR Job_Function__c = 'Artificial Intelligence / Machine Learning'
            )
            AND (
                Management_Level__c = 'Director'
                OR Management_Level__c = 'Manager'
                OR Management_Level__c = 'Non-Manager'
            )
            AND (
                Contact_Status__c != 'Do Not Contact'
                OR Contact_Status__c != 'Responded - Circle Back'
                OR Contact_Status__c != 'Responded - No Interest'
            )
            AND AccountId != null
            AND Account.Priority_Account__c = FALSE
            AND Account.SDR_Owner__c != null
            AND Account.Account_Status__c
            NOT IN ('Bad Fit', 'Competitor', 'Active Opportunity', 'Closed Lost Opportunity', 'MLOps Company', 'Customer')
            ''',
        'limit': 45
    },
    {
        # HIGH LEVEL QUERY
        'seniority': '''
            SELECT FirstName, Email, Account.Name, Account.SDR_Owner__c, Title, Account.Account_Score_Tier__c
            FROM CONTACT
            WHERE LastActivityDate != LAST_N_DAYS:60
            AND (EMAIL != null)
            AND (
                Job_Function__c = 'Data'
                OR Job_Function__c = 'AI'
                OR Job_Function__c = 'ML'
                OR Job_Function__c = 'Artificial Intelligence'
                OR Job_Function__c = 'Machine Learning'
                OR Job_Function__c = 'Data Science'
                OR Job_Function__c = 'Engineering'
                OR Job_Function__c = 'Product'
                OR Job_Function__c = 'Artificial Intelligence / Machine Learning'
            )
            AND (
                Management_Level__c = 'VP-Level'
                OR Management_Level__c = 'C-Level'
            )
            AND (
                Contact_Status__c != 'Do Not Contact'
                OR Contact_Status__c != 'Responded - Circle Back'
                OR Contact_Status__c != 'Responded - No Interest'
            )
            AND AccountId != null
            AND Account.Priority_Account__c = FALSE
            AND Account.SDR_Owner__c != null
            AND Account.Account_Status__c
            NOT IN ('Bad Fit', 'Competitor', 'Active Opportunity', 'Closed Lost Opportunity', 'MLOps Company', 'Customer')
            ''',
        'limit': 9
    }
]