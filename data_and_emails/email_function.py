import requests

SEQUENCES = ["6201c6e51fe985075676ae95", "6234ee26694544d77314b991"]


def getAndSend(sf, queries, sdrs, writer):

    seq_id = SEQUENCES[0]

    for count, query in enumerate(queries):

        if count == 2:
            seq_id = SEQUENCES[1]
        
        writer.writerow([query["title"]])
        writer.writerow(["SDR + Account", "Successes", "Errors", "Duplicates"])

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
        original_format_contacts = formatted_contacts[:]

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
                writer.writerow([sdr, 0, 0, 0])
                continue

            if len(recipients) <= min_emails:
                # pass
                mixmax_send(seq_id, recipients, sdr, tokens[0][0], tokens[0][1], original_format_contacts, writer)
            elif len(recipients) > max_emails and len(tokens) > 1:
                # pass
                mixmax_send(seq_id, recipients[0:min_emails], sdr, tokens[0][0], tokens[0][1], original_format_contacts, writer)
                mixmax_send(seq_id, recipients[min_emails:max_emails], sdr, tokens[1][0], tokens[1][1], original_format_contacts, writer)
            elif len(recipients) > min_emails and len(tokens) > 1:
                # pass
                mixmax_send(seq_id, recipients[0:min_emails], sdr, tokens[0][0], tokens[0][1], original_format_contacts, writer)
                mixmax_send(seq_id, recipients[min_emails:], sdr, tokens[1][0], tokens[1][1], original_format_contacts, writer)
            else:
                # pass
                mixmax_send(seq_id, recipients[0:min_emails], sdr, tokens[0][0], tokens[0][1], original_format_contacts, writer)

        writer.writerow([''])


def mixmax_send(seq_id, recipients, sdr, token, account, original_format_contacts, writer):
    url = f"https://api.mixmax.com/v1/sequences/{seq_id}/recipients/"

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
    duplicates = list(filter(lambda r:r["status"] == "duplicated", res_data["recipients"]))

    print(f'{sdr}: {successes}')

    writer.writerow([f'{sdr}: {account}', successes, errors, len(duplicates)])
    
    if len(duplicates) > 0:
        writer.writerow(["Duplicated Contacts"])
        writer.writerow(["Email", "SFDC Account: Account Name", "First Name"])
        dupes = getDupes(duplicates, original_format_contacts)
        writer.writerows(dupes)
    

def getDupes(duplicates, original_format_contacts):
    dupes = []
    for d in duplicates:
        for c in original_format_contacts:
            if c["email"] == d["email"]:
                dupes.append([c["email"], c["variables"]["SFDC Account: Account Name"], c["variables"]["First Name"]])
                break
    return dupes