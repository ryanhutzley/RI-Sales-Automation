from email.message import EmailMessage
import mimetypes
import smtplib
import os


def sendResults(sender, password, date):
    msg = EmailMessage()
    msg.set_content("Good Morning!\n\nAttached are the results from the bot.\n\nBest,\nBot")
    msg["Subject"] = f'Bot Results -- {date}'
    msg["From"] = sender
    recipients = [sender, 'dominic@robustintelligence.com', 'ellen@robustintelligence.com', 'sophia@robustintelligence.com', 'mark@robustintelligence.com', 'katherine@robustintelligence.com']
    msg["To"] = ", ".join(recipients)

    file_path = f'/Users/ryanhutzley/Desktop/RI Sales Automation/logs/log_{date}.csv'
    filename = os.path.basename(file_path)
    mime_type, _ = mimetypes.guess_type(file_path)
    mime_type, mime_subtype = mime_type.split('/', 1)

    with open(file_path, 'rb') as ap:
        msg.add_attachment(ap.read(), maintype=mime_type, subtype=mime_subtype, filename=filename)

    server = smtplib.SMTP_SSL("smtp.gmail.com", 465)
    server.login(sender, password)
    server.send_message(msg)
    server.quit()