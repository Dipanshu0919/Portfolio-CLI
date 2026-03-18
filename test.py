api_token = "mlsn.9619c1f868b96226f249f5505ec56f6dcf3073687547c7a5ed320ace257cf6e9"
domain = "sahyogsutra.run.place"

from mailersend import MailerSendClient, EmailBuilder

ms = MailerSendClient(api_key=api_token)

email = (EmailBuilder()
    .from_email(f"donotreply@{domain}", "dipanshu")
         .to_many([{"email": "dipanshu0919@gmail.com", "name": "Sahyogi"}])
         .subject("Hello from SahyogSutra!")
         .html("<b>This is a test mail!</b>")
         .build())

response = ms.emails.send(email)
print(response)
