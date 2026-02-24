"""Almacenamiento usando Pandas DataFrames en lugar de SQLAlchemy."""
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional
import json
from backend import config
import os
import shutil
import time
import tempfile
import threading
import logging

# Rutas de archivos
PERSONAS_FILE = config.DATA_DIR / "personas.parquet"
CONVERSACIONES_FILE = config.DATA_DIR / "conversaciones.parquet"
ANALISIS_FILE = config.DATA_DIR / "analisis.parquet"
INTERESES_FILE = config.DATA_DIR / "intereses.parquet"
EVENTOS_FILE = config.DATA_DIR / "eventos.parquet"
PERSONA_INTERES_FILE = config.DATA_DIR / "persona_interes.parquet"
CANDIDATOS_FILE = config.DATA_DIR / "candidatos.parquet"


class DataFrameStorage:
    """Gestor de almacenamiento con DataFrames."""
    
    def __init__(self):
        """Inicializar y cargar DataFrames."""
        # inicializar lock y logger antes de cualquier I/O
        self._io_lock = threading.RLock()
        self.logger = logging.getLogger("agente.dataframe_storage")
        if not self.logger.handlers:
            self.logger.addHandler(logging.NullHandler())

        with self._io_lock:
            self.logger.debug("Cargando dataframes desde %s", config.DATA_DIR)
            self.candidatos_df = self._load_or_create_df(CANDIDATOS_FILE, self._get_candidatos_schema())
            self.personas_df = self._load_or_create_df(PERSONAS_FILE, self._get_personas_schema())
            self.conversaciones_df = self._load_or_create_df(CONVERSACIONES_FILE, self._get_conversaciones_schema())
            self.analisis_df = self._load_or_create_df(ANALISIS_FILE, self._get_analisis_schema())
            self.intereses_df = self._load_or_create_df(INTERESES_FILE, self._get_intereses_schema())
            self.eventos_df = self._load_or_create_df(EVENTOS_FILE, self._get_eventos_schema())
            self.persona_interes_df = self._load_or_create_df(PERSONA_INTERES_FILE, self._get_persona_interes_schema())
        
        # Inicializar intereses predefinidos
        self._init_intereses()
        # Inicializar eventos predefinidos
        self._init_eventos()
    
    def _load_or_create_df(self, file_path: Path, schema: Dict) -> pd.DataFrame:
        """Cargar DataFrame desde archivo o crear uno nuevo."""
        if file_path.exists():
            try:
                return pd.read_parquet(file_path)
            except Exception as e:
                # usar logger si está inicializado, si no fallback a print
                try:
                    self.logger.warning("Error cargando %s: %s. Creando nuevo DataFrame.", file_path.name, e)
                except Exception:
                    print(f"Warning: Error cargando {file_path.name}: {e}. Creando nuevo DataFrame.")
        
        return pd.DataFrame(schema)
    
    def _get_candidatos_schema(self) -> Dict:
        """Schema para candidatos (multi-tenant)."""
        return {
            'id': pd.Series(dtype='int64'),
            'nombre': pd.Series(dtype='object'),
            'email': pd.Series(dtype='object'),
            'partido': pd.Series(dtype='object'),
            'cargo': pd.Series(dtype='object'),
            'facebook_page_id': pd.Series(dtype='object'),
            'facebook_page_name': pd.Series(dtype='object'),
            'facebook_page_access_token': pd.Series(dtype='object'),
            'facebook_token_expiration': pd.Series(dtype='datetime64[ns]'),
            'instagram_business_account_id': pd.Series(dtype='object'),
            'instagram_username': pd.Series(dtype='object'),
            'whatsapp_phone_number_id': pd.Series(dtype='object'),
            'whatsapp_business_account_id': pd.Series(dtype='object'),
            'whatsapp_phone_number': pd.Series(dtype='object'),
            'estado': pd.Series(dtype='object'),
            'password_hash': pd.Series(dtype='object'),
            'fecha_registro': pd.Series(dtype='datetime64[ns]'),
            'fecha_ultimo_login': pd.Series(dtype='datetime64[ns]'),
            'fecha_actualizacion': pd.Series(dtype='datetime64[ns]')
        }
    
    def _get_personas_schema(self) -> Dict:
        """Schema para personas."""
        return {
            'id': pd.Series(dtype='int64'),
            'nombre_completo': pd.Series(dtype='object'),
            'edad': pd.Series(dtype='Int64'),  # Nullable integer
            'genero': pd.Series(dtype='object'),
            'telefono': pd.Series(dtype='object'),
            'email': pd.Series(dtype='object'),
            'facebook_id': pd.Series(dtype='object'),
            'instagram_id': pd.Series(dtype='object'),
            'facebook_username': pd.Series(dtype='object'),
            'instagram_username': pd.Series(dtype='object'),
            'ocupacion': pd.Series(dtype='object'),
            'ubicacion': pd.Series(dtype='object'),
            'resumen': pd.Series(dtype='object'),
            'fecha_primer_contacto': pd.Series(dtype='datetime64[ns]'),
            'fecha_ultimo_contacto': pd.Series(dtype='datetime64[ns]'),
            'fecha_creacion': pd.Series(dtype='datetime64[ns]')
        }
    
    def _get_conversaciones_schema(self) -> Dict:
        """Schema para conversaciones."""
        return {
            'id': pd.Series(dtype='int64'),
            'persona_id': pd.Series(dtype='int64'),
            'plataforma': pd.Series(dtype='object'),
            'conversacion_id': pd.Series(dtype='object'),
            'mensaje': pd.Series(dtype='object'),
            'es_enviado': pd.Series(dtype='int64'),
            'datos_extraidos': pd.Series(dtype='object'),
            'fecha_mensaje': pd.Series(dtype='datetime64[ns]'),
            'fecha_procesado': pd.Series(dtype='datetime64[ns]')
        }
    
    def _get_analisis_schema(self) -> Dict:
        """Schema para análisis."""
        return {
            'id': pd.Series(dtype='int64'),
            'persona_id': pd.Series(dtype='int64'),
            'evento_id': pd.Series(dtype='Int64'),  # Nullable
            'resumen': pd.Series(dtype='object'),
            'categorias': pd.Series(dtype='object'),
            'start_conversation': pd.Series(dtype='datetime64[ns]'),
            'fecha_analisis': pd.Series(dtype='datetime64[ns]'),
            'contenido_completo': pd.Series(dtype='object')
        }
    
    def _get_intereses_schema(self) -> Dict:
        """Schema para intereses."""
        return {
            'id': pd.Series(dtype='int64'),
            'categoria': pd.Series(dtype='object'),
            'descripcion': pd.Series(dtype='object')
        }
    
    def _get_eventos_schema(self) -> Dict:
        """Schema para eventos."""
        return {
            'id': pd.Series(dtype='int64'),
            'nombre': pd.Series(dtype='object'),
            'descripcion': pd.Series(dtype='object'),
            'fecha_creacion': pd.Series(dtype='datetime64[ns]')
        }
    
    def _get_persona_interes_schema(self) -> Dict:
        """Schema para relación persona-interés."""
        return {
            'persona_id': pd.Series(dtype='int64'),
            'interes_id': pd.Series(dtype='int64')
        }
    
    def _init_intereses(self):
        """Inicializar intereses predefinidos si no existen."""
        categorias_predefinidas = config.CATEGORIAS_INTERES
        
        for categoria in categorias_predefinidas:
            if categoria not in self.intereses_df['categoria'].values:
                nuevo_id = self.intereses_df['id'].max() + 1 if len(self.intereses_df) > 0 else 1
                nuevo_interes = pd.DataFrame([{
                    'id': nuevo_id,
                    'categoria': categoria,
                    'descripcion': f'Interés en {categoria}'
                }])
                self.intereses_df = pd.concat([self.intereses_df, nuevo_interes], ignore_index=True)
        
        self.save_intereses()
    
    def _init_eventos(self):
        """Inicializar eventos predefinidos si no existen."""
        eventos_predefinidos = [
            {"nombre": "Festival del Deporte", "descripcion": "Festival anual de deportes y recreación"},
            {"nombre": "Cabildos Ciudadanos", "descripcion": "Sesiones de encuentro con la comunidad"},
            {"nombre": "Charla de Seguridad", "descripcion": "Charlas sobre seguridad ciudadana"},
            {"nombre": "Encuentro de Salud", "descripcion": "Jornadas de salud pública y prevención"},
            {"nombre": "Foro de Educación", "descripcion": "Foros sobre mejoras en educación"},
            {"nombre": "Evento de Inversión", "descripcion": "Presentaciones sobre desarrollo económico"},
            {"nombre": "Reunión Vecinal", "descripcion": "Reuniones con vecinos del sector"},
            {"nombre": "Campaña Puerta a Puerta", "descripcion": "Recorrido por los barrios"},
            {"nombre": "Otros", "descripcion": "Otros eventos no especificados"},
        ]
        
        for evento_data in eventos_predefinidos:
            if evento_data["nombre"] not in self.eventos_df['nombre'].values:
                nuevo_id = self.eventos_df['id'].max() + 1 if len(self.eventos_df) > 0 else 1
                nuevo_evento = pd.DataFrame([{
                    'id': nuevo_id,
                    'nombre': evento_data["nombre"],
                    'descripcion': evento_data["descripcion"],
                    'fecha_creacion': datetime.now()
                }])
                self.eventos_df = pd.concat([self.eventos_df, nuevo_evento], ignore_index=True)
        
        self.save_eventos()
    
    def save_personas(self):
        """Guardar personas a disco."""
        self._atomic_save(self.personas_df, PERSONAS_FILE)
    
    def save_candidatos(self):
        """Guardar candidatos a disco."""
        self._atomic_save(self.candidatos_df, CANDIDATOS_FILE)
    
    def save_conversaciones(self):
        """Guardar conversaciones a disco."""
        self._atomic_save(self.conversaciones_df, CONVERSACIONES_FILE)
    
    def save_analisis(self):
        """Guardar análisis a disco."""
        self._atomic_save(self.analisis_df, ANALISIS_FILE)
    
    def save_intereses(self):
        """Guardar intereses a disco."""
        self._atomic_save(self.intereses_df, INTERESES_FILE)
    
    def save_eventos(self):
        """Guardar eventos a disco."""
        self._atomic_save(self.eventos_df, EVENTOS_FILE)
    
    def save_persona_interes(self):
        """Guardar relación persona-interés a disco."""
        self._atomic_save(self.persona_interes_df, PERSONA_INTERES_FILE)
    
    def save_all(self):
        """Guardar todos los DataFrames."""
        with self._io_lock:
            self.save_candidatos()
            self.save_personas()
            self.save_conversaciones()
            self.save_analisis()
            self.save_intereses()
            self.save_eventos()
            self.save_persona_interes()

    def _atomic_save(self, df: pd.DataFrame, path: Path):
        """Guardar DataFrame de forma atómica: escribir en un archivo temporal y reemplazar.

        Esto minimiza problemas de archivos parcial o locks en Windows y permite
        hacer swaps seguros desde procesos externos.
        """
        try:
            with self._io_lock:
                dir_path = path.parent
                dir_path.mkdir(parents=True, exist_ok=True)
                # Crear archivo temporal en el mismo directorio de forma segura
                fd, tmp_path_str = tempfile.mkstemp(prefix=f"{path.name}.tmp.", dir=str(dir_path))
                # Cerrar el descriptor retornado por mkstemp; pandas escribirá en la ruta
                try:
                    os.close(fd)
                except Exception:
                    pass
                tmp_path = Path(tmp_path_str)
                # escribir parquet al tmp
                df.to_parquet(tmp_path, index=False)
                # Verificar tmp existe
                if not tmp_path.exists():
                    raise FileNotFoundError(f"Temp parquet not found after write: {tmp_path}")

                # Intentar reemplazar de forma atómica — en Windows puede fallar temporalmente si otro proceso
                # tiene handle al archivo destino; reintentamos con backoff y registramos cada intento.
                max_attempts = 6
                for attempt in range(1, max_attempts + 1):
                    try:
                        self.logger.debug("Intento %d: reemplazando %s con %s", attempt, path, tmp_path)
                        os.replace(str(tmp_path), str(path))
                        self.logger.debug("Reemplazo exitoso: %s", path)
                        break
                    except (PermissionError, OSError) as ex:
                        self.logger.debug("Fallo intento %d reemplazo %s: %s", attempt, path, ex)
                        # Esperar un poco y reintentar
                        wait = 0.05 * attempt
                        time.sleep(wait)
                        if attempt == max_attempts:
                            self.logger.error("No se pudo reemplazar %s tras %d intentos", path, max_attempts)
                            raise
        except Exception as e:
            try:
                self.logger.exception("Error guardando %s: %s", path, e)
            except Exception:
                print(f"Error guardando {path}: {e}")
            # Intentar limpiar el tmp si existe
            try:
                if 'tmp_path' in locals() and tmp_path.exists():
                    tmp_path.unlink()
                    try:
                        self.logger.debug("Tmp eliminado %s", tmp_path)
                    except Exception:
                        pass
            except Exception:
                try:
                    self.logger.exception("Error limpiando tmp %s", locals().get('tmp_path', None))
                except Exception:
                    pass

    def reload_from_disk(self):
        """Recargar todos los DataFrames desde los ficheros en disco.

        Útil después de realizar un swap atómico de archivos para que la instancia
        en memoria vuelva a apuntar a los datos más recientes.
        """
        with self._io_lock:
            try:
                self.candidatos_df = self._load_or_create_df(CANDIDATOS_FILE, self._get_candidatos_schema())
                self.logger.debug("Comenzando reload_from_disk desde %s", config.DATA_DIR)
                self.personas_df = self._load_or_create_df(PERSONAS_FILE, self._get_personas_schema())
                self.conversaciones_df = self._load_or_create_df(CONVERSACIONES_FILE, self._get_conversaciones_schema())
                self.analisis_df = self._load_or_create_df(ANALISIS_FILE, self._get_analisis_schema())
                self.intereses_df = self._load_or_create_df(INTERESES_FILE, self._get_intereses_schema())
                self.eventos_df = self._load_or_create_df(EVENTOS_FILE, self._get_eventos_schema())
                self.persona_interes_df = self._load_or_create_df(PERSONA_INTERES_FILE, self._get_persona_interes_schema())
                self.logger.debug("reload_from_disk terminado correctamente")
            except Exception as e:
                try:
                    self.logger.exception("Error en reload_from_disk: %s", e)
                except Exception:
                    print(f"Error en reload_from_disk: {e}")

    def backup_all(self, backup_dir: Path):
        """Hacer backup de los archivos parquet actuales en `backup_dir`.

        Los archivos se copian con timestamp para permitir rollback manual si hace falta.
        """
        backup_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        files = [PERSONAS_FILE, CONVERSACIONES_FILE, ANALISIS_FILE, INTERESES_FILE, EVENTOS_FILE, PERSONA_INTERES_FILE]
        for f in files:
            try:
                if f.exists():
                    dest = backup_dir / f"{f.name}.{ts}"
                    shutil.copy2(f, dest)
            except Exception as e:
                try:
                    self.logger.warning("Error haciendo backup de %s: %s", f, e)
                except Exception:
                    print(f"Warning: error haciendo backup de {f}: {e}")


# Instancia global
_storage = None

def get_storage() -> DataFrameStorage:
    """Obtener instancia global de storage."""
    global _storage
    if _storage is None:
        _storage = DataFrameStorage()
    return _storage
