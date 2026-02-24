"""Configuración general del proyecto."""
import os
from dotenv import load_dotenv
from pathlib import Path

# Cargar variables de entorno
load_dotenv()

# Directorios
import sys

# Si el programa se empaqueta con PyInstaller, `sys.frozen` es True.
# En ese caso usamos la carpeta del ejecutable como `BASE_DIR` para que
# la carpeta `data` y `agente_politico.db` puedan vivir junto al .exe.
if getattr(sys, "frozen", False):
    BASE_DIR = Path(sys.executable).resolve().parent
else:
    # config.py ahora vive en backend/, subimos un nivel para obtener la raíz
    BASE_DIR = Path(__file__).resolve().parent.parent
EXPORTS_DIR = BASE_DIR / "exports"
EXPORTS_DIR.mkdir(exist_ok=True)

# Directorio para DataFrames (modo local)
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

# Modo de almacenamiento: "local" (pandas) o "cloud" (SQLAlchemy)
ENV = os.getenv("ENV", "local").lower()

# Detectar si estamos en producción (hosting web) o local
IS_WEB_ENV = ENV != "local"

# Google Cloud Platform
# Opción 1: Google AI Studio con API Key (Free Tier - RECOMENDADO)
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

# Opción 2: Vertex AI con Service Account (Enterprise)
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", None)
GCP_LOCATION = os.getenv("GCP_LOCATION", "us-central1")
#GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# Modelo a usar
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash-lite")

# Meta/Facebook
META_APP_ID = os.getenv("META_APP_ID")
META_APP_SECRET = os.getenv("META_APP_SECRET")
META_ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN")
INSTAGRAM_ACCESS_TOKEN = os.getenv("INSTAGRAM_ACCESS_TOKEN")
META_VERIFY_TOKEN = os.getenv("META_VERIFY_TOKEN", "agente_politico_token_secreto")

# OAuth Redirect URI (para Facebook Login for Business)
if IS_WEB_ENV:
    # En producción web: usar dominio definitivo
    OAUTH_REDIRECT_URI = os.getenv("OAUTH_REDIRECT_URI", "https://app.retarget.cl/auth/facebook/callback")
else:
    # En local: usar localhost
    OAUTH_REDIRECT_URI = os.getenv("OAUTH_REDIRECT_URI", "http://localhost:8000/auth/facebook/callback")

# Control de Acceso: Lista blanca de usuarios
# True = Solo usuarios autorizados pueden acceder (PRODUCCIÓN)
# False = Acceso abierto para todos (LOCAL/TESTING)
if IS_WEB_ENV:
    # En producción: validar usuarios (lista blanca)
    VALIDAR_USUARIOS = os.getenv("VALIDAR_USUARIOS", "True").lower() in ("true", "1", "yes")
else:
    # En local: permitir acceso abierto para desarrollo
    VALIDAR_USUARIOS = os.getenv("VALIDAR_USUARIOS", "False").lower() in ("true", "1", "yes")

# WhatsApp Business API
WHATSAPP_PHONE_NUMBER_ID = os.getenv("WHATSAPP_PHONE_NUMBER_ID")
WHATSAPP_BUSINESS_ACCOUNT_ID = os.getenv("WHATSAPP_BUSINESS_ACCOUNT_ID")
WHATSAPP_VERIFY_TOKEN = os.getenv("WHATSAPP_VERIFY_TOKEN", "whatsapp_verify_token_secreto")

# Base de datos
DATABASE_PATH = BASE_DIR / "agente_politico.db"
# Usar ruta absoluta para evitar problemas si se ejecuta desde subdirectorios
if str(DATABASE_PATH).startswith("\\\\"):
   # Fix para windows UNC paths si fuera necesario, aunque pathlib suele manejarlo
   pass
DATABASE_URL = os.getenv("DATABASE_URL", f"sqlite:///{DATABASE_PATH.as_posix()}")

# Servidor
if IS_WEB_ENV:
    # En producción web: escuchar en todas las interfaces
    BACKEND_HOST = os.getenv("BACKEND_HOST", "0.0.0.0")
    FRONTEND_HOST = os.getenv("FRONTEND_HOST", "0.0.0.0")
else:
    # En local: localhost
    BACKEND_HOST = os.getenv("BACKEND_HOST", "127.0.0.1")
    FRONTEND_HOST = os.getenv("FRONTEND_HOST", "127.0.0.1")
BACKEND_PORT = int(os.getenv("PORT", os.getenv("BACKEND_PORT", 8000)))
FRONTEND_PORT = int(os.getenv("PORT", os.getenv("FRONTEND_PORT", 8050)))

# URL del backend para el frontend (puede ser diferente en producción)
# En Railway: usar variable de entorno BACKEND_URL (ej: https://crm-politics-backend.railway.app)
# En local: usar http://localhost:8000
BACKEND_URL = os.getenv("BACKEND_URL", f"http://{BACKEND_HOST}:{BACKEND_PORT}")

# Debug
DEBUG = os.getenv("DEBUG", "False").lower() == "true"

# Categorías de intereses
CATEGORIAS_INTERES = [
    "Deportes",
    "Inversión",
    "Seguridad",
    "Salud",
    "Educación"
]

# Géneros
GENEROS = ["Masculino", "Femenino", "Otro", "No especificado"]
SYNC_PASSWORD = os.getenv("SYNC_PASSWORD", "")