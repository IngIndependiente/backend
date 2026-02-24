"""Capa de abstracción que selecciona automáticamente entre SQLAlchemy y DataFrames."""
from backend import config

# Determinar qué backend usar
USE_DATAFRAMES = config.ENV == "local"

if USE_DATAFRAMES:
    print(f"[Modo LOCAL] Usando Pandas DataFrames (almacenamiento: {config.DATA_DIR})")
    from backend.database.dataframe_services import (
        DataFramePersonaService as PersonaService,
        DataFrameConversacionService as ConversacionService,
        DataFrameAnalisisService as AnalisisService,
        DataFrameEventoService as EventoService
    )
    from backend.database.dataframe_storage import get_storage
    
    # IMPORTANTE: Para funciones de gestión de usuarios, necesitamos SQLAlchemy
    # Importar SessionLocal desde database.__init__ incluso en modo LOCAL
    from backend.database import SessionLocal as _SessionLocal, init_db as _init_db_sqlite
    
    # Funciones de compatibilidad
    def get_db():
        """Contexto manager dummy para compatibilidad."""
        class DummyContext:
            def __enter__(self):
                return None
            def __exit__(self, *args):
                # Guardar todos los cambios al salir
                get_storage().save_all()
        return DummyContext()
    
    def init_db():
        """Inicializar storage (DataFrames + SQLite para usuarios)."""
        get_storage()
        _init_db_sqlite()  # Inicializar SQLite para tabla usuarios_autorizados
        print("OK - DataFrames inicializados + SQLite para usuarios")
    
    # Exportar SessionLocal para scripts de gestión de usuarios
    SessionLocal = _SessionLocal
    
    def get_db_session():
        """Dependency para FastAPI."""
        db = SessionLocal()
        try:
            yield db
        finally:
            db.close()
    
else:
    print("[Modo CLOUD] Usando SQLAlchemy")
    from backend.database.services import (
        PersonaService,
        ConversacionService,
        AnalisisService
    )
    # Importar desde database.__init__ que tiene SessionLocal
    from backend.database import get_db, init_db, SessionLocal, get_db_session
    
    # EventoService no existe en services.py, lo creamos aquí
    class EventoService:
        @staticmethod
        def obtener_todos():
            from backend.database.models import Evento
            from backend.database import get_db_session
            with get_db_session() as db:
                eventos = db.query(Evento).order_by(Evento.nombre).all()
                return [{'id': e.id, 'nombre': e.nombre, 'descripcion': e.descripcion} for e in eventos]
        
        @staticmethod
        def obtener_por_id(evento_id: int):
            from backend.database.models import Evento
            from backend.database import get_db_session
            with get_db_session() as db:
                evento = db.query(Evento).filter(Evento.id == evento_id).first()
                return {'id': evento.id, 'nombre': evento.nombre, 'descripcion': evento.descripcion} if evento else None
        
        @staticmethod
        def obtener_por_nombre(nombre: str):
            from backend.database.models import Evento
            from backend.database import get_db_session
            with get_db_session() as db:
                evento = db.query(Evento).filter(Evento.nombre == nombre).first()
                return {'id': evento.id, 'nombre': evento.nombre, 'descripcion': evento.descripcion} if evento else None
        
        @staticmethod
        def crear_evento(nombre: str, descripcion: str = None):
            from backend.database.models import Evento
            from backend.database import get_db_session
            from datetime import datetime
            with get_db_session() as db:
                evento = Evento(nombre=nombre, descripcion=descripcion or f'Evento: {nombre}', fecha_creacion=datetime.now())
                db.add(evento)
                db.commit()
                db.refresh(evento)
                return {'id': evento.id, 'nombre': evento.nombre, 'descripcion': evento.descripcion}


__all__ = [
    'PersonaService',
    'ConversacionService',
    'AnalisisService',
    'EventoService',
    'get_db',
    'init_db',
    'USE_DATAFRAMES',
    'SessionLocal',
    'get_db_session'
]
