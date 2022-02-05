const path = require('path').resolve(__dirname + '/..')
require('dotenv').config({ path: path + '/.env' })
const MixmaxAPI = require('mixmax-api');
const jsforce = require('jsforce');

SENIORITY_LEVEL = {}

SDR_OWNERS = {
    'Ellen Scott-Young': [process.env.ELLEN_YARON_SINGER_API, process.env.ELLEN_YARON_S_API],
    'Katherine Hess': [process.env.KAT_YS_API],
    'Sophia Serseri': [process.env.SOPHIA_YSINGER_API, process.env.SOPHIA_Y_DASH_SINGER_API],
    'Ryan Hutzley': [process.env.RYAN_YARON_SINGER_API, RYAN_YSING_API]
}

const contacts = []

const conn = new jsforce.Connection();
conn.login(process.env.USERNAME, process.env.PASSWORD + process.env.TOKEN, function (err, res) {
    if (err) { return console.error(err); }
    conn.query(`
        SELECT FirstName, Email, Account.Name, Account.SDR_Owner__c, Title
        FROM CONTACT
        WHERE LastActivityDate != LAST_N_DAYS:60
        AND (EMAIL != null)
        AND (NOT Title LIKE '%vp%')
        AND (NOT Title LIKE '%CTO%')
        AND (NOT Title LIKE '%CEO%')
        AND (NOT Title LIKE '%CPO%')
        AND (NOT Title LIKE '%CDO%')
        AND (NOT Title LIKE '%chief%')
        AND (NOT Title LIKE '%vice president%')
        AND AccountId != null
        AND Account.SDR_Owner__c != null
        AND Account.Account_Status__c
        NOT IN ('Bad Fit', 'Competitor', 'Active Opportunity', 'Closed Lost Opportunity', 'MLOps Company', 'Customer')
    `, function (err, res) {
        if (err) { return console.error(err) }
        contacts.push(...res.records)
        if (!res.done) {
            getMore(res.nextRecordsUrl)
        }
    });
});

function getMore(nextRecordsUrl) {
    conn.queryMore(nextRecordsUrl, function (err, res) {
        if (err) { return console.error(err) }
        contacts.push(...res.records)
        if (!res.done) {
            getMore(res.nextRecordsUrl)
        } else if (res.done) {
            sortAndSend(contacts)
        }
    })
}

function sortAndSend(contacts) {
    let formatted_contacts = contacts.map(contact => {
        const newObj = {
            'SDR Owner': contact['Account']['SDR_Owner__c'],
            'email': contact['Email'],
            'variables': {
                'SFDC Account: Account Name': contact['Account']['Name'],
                'First Name': contact['FirstName']
            }
        }
        return newObj
    })

    for (const key in SDR_OWNERS) {
        const contactsPerSDR = formatted_contacts.filter(contact => contact['SDR Owner'] === key)
        const recipients = contactsPerSDR.map(contact => {
            delete contact['SDR Owner']
            return contact
        })
        if (recipients.length > 0 && recipients.length <= 50) {
            addUserToMixmax(recipients, SDR_OWNERS[key][0])
        }
        else if (recipients.length > 100 && SDR_OWNERS[key][1]) {
            addUserToMixmax(recipients.slice(0, 50), SDR_OWNERS[key][0])
            addUserToMixmax(recipients.slice(50, 100), SDR_OWNERS[key][1])
        }
        else if (recipients.length > 50 && SDR_OWNERS[key][1]) {
            addUserToMixmax(recipients.slice(0, 50), SDR_OWNERS[key][0])
            addUserToMixmax(recipients.slice(50), SDR_OWNERS[key][1])
        }
        else if (recipients.length > 50) {
            addUserToMixmax(recipients.slice(0, 50), SDR_OWNERS[key][0])
        }
    }
}

async function addUserToMixmax(recipients, API) {
    const api = new MixmaxAPI(API);
    const sequenceId = '61f2ed040915b49be51a23c8';
    const sequence = api.sequences.sequence(sequenceId);
    const promise = await sequence.addRecipients(recipients);
    console.log(promise)
}