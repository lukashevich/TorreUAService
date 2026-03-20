import os
import requests
from dotenv import load_dotenv

load_dotenv()

token = os.getenv("BOT_TOKEN")
chat_id = os.getenv("CHAT_ID")

print("TOKEN prefix:", token[:10] if token else None)
print("CHAT_ID:", repr(chat_id))

r = requests.post(
    f"https://api.telegram.org/bot{token}/sendMessage",
    data={
        "chat_id": chat_id,
        "text": "🚀 TorreUA bot works!"
    },
    timeout=30
)

print(r.status_code)
print(r.text)
