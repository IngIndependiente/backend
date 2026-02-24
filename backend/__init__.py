"""Backend - API FastAPI para Agente Político."""
import os
import sys

# Asegurar que el root del proyecto esté en sys.path
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)
