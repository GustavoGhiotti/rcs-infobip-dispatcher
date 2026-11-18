import os
from dotenv import load_dotenv

load_dotenv()


DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASSWORD")

DB_66 = os.getenv("DB_66")
DB_ENCRYPT = (os.getenv("DB_ENCRYPT") or "no").strip().lower()
DB_TRUST_SERVER_CERTIFICATE = (os.getenv("DB_TRUST_SERVER_CERTIFICATE") or "yes").strip().lower()

EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASS = os.getenv("EMAIL_PASSWORD")
EMAIL_TO = os.getenv("EMAIL_TO")
EMAIL_HOST = os.getenv("EMAIL_HOST") or "smtp.gmail.com"
EMAIL_PORT = int(os.getenv("EMAIL_PORT") or "465")
EMAIL_USE_SSL = (os.getenv("EMAIL_USE_SSL") or "true").lower() in ("1", "true", "yes", "y")

def dados_email():
    if not EMAIL_USER or not EMAIL_PASS:
        raise Exception("Email config incompleta no .env")

    destinatarios = []
    if EMAIL_TO:
        destinatarios = [e.strip() for e in EMAIL_TO.split(",") if e.strip()]

    return {
        "email": EMAIL_USER,
        "senha": EMAIL_PASS,
        "host": EMAIL_HOST,
        "port": EMAIL_PORT,
        "use_ssl": EMAIL_USE_SSL,
        "to": destinatarios,
    }
