"""Configuración y conexión a la base de datos."""
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from contextlib import contextmanager
from backend import config
from backend.database.models import Base, Interes

# Crear engine
engine = create_engine(
    config.DATABASE_URL,
    connect_args={"check_same_thread": False} if "sqlite" in config.DATABASE_URL else {},
    echo=config.DEBUG
)

# Crear SessionLocal
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def init_db():
    """Inicializar la base de datos creando todas las tablas."""
    Base.metadata.create_all(bind=engine)
    
    # Migración: agregar columna facebook_user_id si no existe
    try:
        with engine.connect() as conn:
            if "sqlite" in config.DATABASE_URL:
                result = conn.execute(text("PRAGMA table_info(usuarios_autorizados)"))
                columns = [row[1] for row in result]
                if "facebook_user_id" not in columns:
                    conn.execute(text("ALTER TABLE usuarios_autorizados ADD COLUMN facebook_user_id VARCHAR(50) UNIQUE"))
                    conn.commit()
                    print("✓ Columna facebook_user_id agregada a usuarios_autorizados")
            else:
                # PostgreSQL
                conn.execute(text("""
                    ALTER TABLE usuarios_autorizados
                    ADD COLUMN IF NOT EXISTS facebook_user_id VARCHAR(50) UNIQUE
                """))
                conn.commit()
                print("✓ Columna facebook_user_id agregada a usuarios_autorizados")
    except Exception as e:
        print(f"Migración facebook_user_id (puede ser normal si ya existe): {e}")

    # Migración: agregar columna plataforma a analisis si no existe
    try:
        with engine.connect() as conn:
            if "sqlite" not in config.DATABASE_URL:
                conn.execute(text("""
                    ALTER TABLE analisis
                    ADD COLUMN IF NOT EXISTS plataforma VARCHAR(50)
                """))
                conn.commit()
                print("✓ Columna plataforma agregada a analisis")
    except Exception as e:
        print(f"Migración analisis.plataforma (puede ser normal si ya existe): {e}")

    # Migración: agregar columna owner_facebook_user_id a candidatos si no existe
    try:
        with engine.connect() as conn:
            if "sqlite" not in config.DATABASE_URL:
                conn.execute(text("""
                    ALTER TABLE candidatos
                    ADD COLUMN IF NOT EXISTS owner_facebook_user_id VARCHAR(200)
                """))
                conn.commit()
                print("✓ Columna owner_facebook_user_id agregada a candidatos")
    except Exception as e:
        print(f"Migración candidatos.owner_facebook_user_id (puede ser normal si ya existe): {e}")

    # Migración: agregar columnas plataforma y candidato_id a personas si no existen
    try:
        with engine.connect() as conn:
            if "sqlite" not in config.DATABASE_URL:
                conn.execute(text("""
                    ALTER TABLE personas
                    ADD COLUMN IF NOT EXISTS plataforma VARCHAR(50)
                """))
                conn.execute(text("""
                    ALTER TABLE personas
                    ADD COLUMN IF NOT EXISTS candidato_id INTEGER REFERENCES candidatos(id)
                """))
                conn.commit()
                print("✓ Columnas plataforma y candidato_id agregadas a personas")
    except Exception as e:
        print(f"Migración personas.plataforma/candidato_id (puede ser normal si ya existe): {e}")

    # Migración: agregar columna instagram_access_token a candidatos si no existe
    try:
        with engine.connect() as conn:
            if "sqlite" not in config.DATABASE_URL:
                conn.execute(text("""
                    ALTER TABLE candidatos
                    ADD COLUMN IF NOT EXISTS instagram_access_token VARCHAR(1000)
                """))
                conn.commit()
                print("✓ Columna instagram_access_token agregada a candidatos")
    except Exception as e:
        print(f"Migración candidatos.instagram_access_token (puede ser normal si ya existe): {e}")

    # Crear categorías de intereses predeterminadas
    session = SessionLocal()
    try:
        # Verificar si ya existen intereses
        if session.query(Interes).count() == 0:
            for categoria in config.CATEGORIAS_INTERES:
                interes = Interes(categoria=categoria)
                session.add(interes)
            session.commit()
            print(f"✓ Categorías de intereses inicializadas: {', '.join(config.CATEGORIAS_INTERES)}")
    except Exception as e:
        print(f"Error al inicializar intereses: {e}")
        session.rollback()
    finally:
        session.close()


@contextmanager
def get_db() -> Session:
    """
    Context manager para obtener una sesión de base de datos.
    
    Uso:
        with get_db() as db:
            # Usar db aquí
            pass
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_db_session():
    """
    Dependency para FastAPI.
    
    Uso en FastAPI:
        @app.get("/endpoint")
        def endpoint(db: Session = Depends(get_db_session)):
            # Usar db aquí
            pass
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
