import requests

SEQ_ID = "6201c6e51fe985075676ae95"

def getAndSend(sf, queries, sdrs):
    is_low_level = True

    email_results = []

    for query in queries:
        if is_low_level:
            email_results.append(['Low Level Outreach', '', '', ''])
            email_results.append(['SDR', 'Successes', 'Errors', 'Duplicates'])
        else:
            email_results.append(['', '', '', ''])
            email_results.append(['High Level Outreach', '', '', ''])
            email_results.append(['SDR','Successes', 'Errors', 'Duplicates'])

        contacts = []

        res = sf.query(query["seniority"])
        contacts.extend(res["records"])

        is_done = not res["done"]
        updating_res = res

        while is_done:
            next_res = sf.query_more(f'{updating_res["nextRecordsUrl"]}', True)
            contacts.extend(next_res["records"])
            is_done = not next_res["done"]
            updating_res = next_res

        # print(len(contacts))
        # print(query["limit"])

        def checkAccountScoreTier(e):
            tier = e["Account"]["Account_Score_Tier__c"]
            if tier == None: 
                return (tier is None, tier)
            else:
                return (tier is None, int(tier[-1]))

        contacts.sort(key=checkAccountScoreTier)

        def formatContacts(c):
            return {
                "SDR Owner": c["Account"]["SDR_Owner__c"],
                "email": c["Email"],
                "variables": {
                    "SFDC Account: Account Name": c["Account"]["Name"],
                    "First Name": c["FirstName"],
                },
            }

        formatted_contacts = list(map(formatContacts, contacts))

        min_emails = query["limit"]
        max_emails = min_emails * 2

        for sdr,tokens in sdrs.items():
            recipients = []

            for count, contact in enumerate(formatted_contacts):
                if contact["SDR Owner"] == sdr:
                    del formatted_contacts[count]
                    del contact["SDR Owner"]
                    recipients.append(contact)

            if len(recipients) == 0:
                email_results.append([sdr, 0, 0, 0])
                continue

            if len(recipients) <= min_emails:
                # pass
                acct_1 = mixmax_send(recipients, sdr, tokens[0])
                email_results.append(acct_1)
            elif len(recipients) > max_emails and len(tokens) > 1:
                # pass
                acct_1 = mixmax_send(recipients[0:min_emails], sdr, tokens[0])
                acct_2 = mixmax_send(recipients[min_emails:max_emails], sdr, tokens[1])
                total = condense(acct_1, acct_2, sdr)
                email_results.append(total)
            elif len(recipients) > min_emails and len(tokens) > 1:
                # pass
                acct_1 = mixmax_send(recipients[0:min_emails], sdr, tokens[0])
                acct_2 = mixmax_send(recipients[min_emails:], sdr, tokens[1])
                total = condense(acct_1, acct_2, sdr)
                email_results.append(total)
            else:
                # pass
                acct_1 = mixmax_send(sdr, tokens[0])
                email_results.append(acct_1)

        is_low_level = False

    return email_results


def mixmax_send(recipients, sdr, token):
    url = f"https://api.mixmax.com/v1/sequences/{SEQ_ID}/recipients/"

    payload = {
        "recipients": recipients,
    }
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "X-API-Token": token
    }

    res = requests.request("POST", url, json=payload, headers=headers)
    res_data = res.json()
    successes = len(list(filter(lambda r:r["status"] == "success", res_data["recipients"])))
    errors = len(list(filter(lambda r:r["status"] == "error", res_data["recipients"])))
    duplicates = len(list(filter(lambda r:r["status"] == "duplicated", res_data["recipients"])))

    return [sdr, successes, errors, duplicates]


def condense(acct_1, acct_2, sdr):
    zipped_sends = list(zip(acct_1, acct_2))
    sum = list(map(lambda r:r[0] + r[1], zipped_sends[1:]))
    sum.insert(0, sdr)
    return sum