"""Modelos de base de datos para el agente político."""
from sqlalchemy import Column, Integer, String, DateTime, Float, ForeignKey, Table, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime

Base = declarative_base()

# Tabla intermedia para la relación many-to-many entre personas e intereses
persona_interes = Table(
    'persona_interes',
    Base.metadata,
    Column('persona_id', Integer, ForeignKey('personas.id'), primary_key=True),
    Column('interes_id', Integer, ForeignKey('intereses.id'), primary_key=True)
)


class Persona(Base):
    """Modelo para almacenar información de personas contactadas."""
    __tablename__ = 'personas'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    nombre_completo = Column(String(200))
    edad = Column(Integer, nullable=True)
    genero = Column(String(50), nullable=True)
    
    # Datos de contacto
    telefono = Column(String(50), nullable=True)
    email = Column(String(200), nullable=True)
    
    # Datos de redes sociales
    facebook_id = Column(String(200), nullable=True)
    instagram_id = Column(String(200), nullable=True)
    facebook_username = Column(String(200), nullable=True)
    instagram_username = Column(String(200), nullable=True)
    
    # Datos adicionales
    ocupacion = Column(String(200), nullable=True)
    ubicacion = Column(String(200), nullable=True)
    
    # Multi-tenant: asociar persona con candidato
    candidato_id = Column(Integer, ForeignKey('candidatos.id'), nullable=True)
    
    # Fechas
    fecha_primer_contacto = Column(DateTime, default=datetime.utcnow)
    fecha_ultimo_contacto = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    fecha_creacion = Column(DateTime, default=datetime.utcnow)
    
    # Resumen
    resumen = Column(Text, nullable=True)
    
    # Relaciones
    candidato = relationship("Candidato", back_populates="personas")
    intereses = relationship("Interes", secondary=persona_interes, back_populates="personas")
    conversaciones = relationship("Conversacion", back_populates="persona", cascade="all, delete-orphan")
    analisis = relationship("Analisis", back_populates="persona", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<Persona(id={self.id}, nombre='{self.nombre_completo}')>"


class Interes(Base):
    """Modelo para categorías de intereses."""
    __tablename__ = 'intereses'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    categoria = Column(String(100), unique=True, nullable=False)
    descripcion = Column(Text, nullable=True)
    
    # Relaciones
    personas = relationship("Persona", secondary=persona_interes, back_populates="intereses")
    
    def __repr__(self):
        return f"<Interes(id={self.id}, categoria='{self.categoria}')>"


class Conversacion(Base):
    """Modelo para almacenar conversaciones y mensajes."""
    __tablename__ = 'conversaciones'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    persona_id = Column(Integer, ForeignKey('personas.id'), nullable=False)
    
    # Origen de la conversación
    plataforma = Column(String(50), nullable=False)  # 'facebook' o 'instagram'
    conversacion_id = Column(String(200), nullable=True)  # ID de la conversación en la plataforma
    
    # Contenido
    mensaje = Column(Text, nullable=False)
    es_enviado = Column(Integer, default=0)  # 0: recibido, 1: enviado
    
    # Análisis
    datos_extraidos = Column(Text, nullable=True)  # JSON con datos estructurados extraídos
    
    # Fechas
    fecha_mensaje = Column(DateTime, default=datetime.utcnow)
    fecha_procesado = Column(DateTime, nullable=True)
    
    # Relaciones
    persona = relationship("Persona", back_populates="conversaciones")
    
    def __repr__(self):
        return f"<Conversacion(id={self.id}, persona_id={self.persona_id}, plataforma='{self.plataforma}')>"


class Evento(Base):
    """Modelo para eventos o actividades."""
    __tablename__ = 'eventos'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    nombre = Column(String(200), nullable=False, unique=True)
    descripcion = Column(Text, nullable=True)
    fecha_creacion = Column(DateTime, default=datetime.utcnow)
    
    # Relaciones
    analisis = relationship("Analisis", back_populates="evento")
    
    def __repr__(self):
        return f"<Evento(id={self.id}, nombre='{self.nombre}')>"


class Analisis(Base):
    """Modelo para almacenar análisis de conversaciones (sesiones)."""
    __tablename__ = 'analisis'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    persona_id = Column(Integer, ForeignKey('personas.id'), nullable=False)
    evento_id = Column(Integer, ForeignKey('eventos.id'), nullable=True)
    
    # Datos del análisis
    resumen = Column(Text, nullable=True)
    categorias = Column(String(200), nullable=True)  # JSON list of categories
    
    # Metadatos temporalidad
    start_conversation = Column(DateTime, nullable=True)  # Cuándo inició esta sesión de conversación
    fecha_analisis = Column(DateTime, default=datetime.utcnow)  # Cuándo se analizó
    
    # Contenido completo de la sesión analizada (concatenación de mensajes)
    contenido_completo = Column(Text, nullable=True)
    
    # Relaciones
    persona = relationship("Persona", back_populates="analisis")
    evento = relationship("Evento", back_populates="analisis")
    
    def __repr__(self):
        return f"<Analisis(id={self.id}, persona_id={self.persona_id}, fecha='{self.fecha_analisis}')>"


class Candidato(Base):
    """Modelo para almacenar candidatos (multi-tenant)."""
    __tablename__ = 'candidatos'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Información básica
    nombre = Column(String(200), nullable=False)
    email = Column(String(200), unique=True, nullable=False)
    partido = Column(String(200), nullable=True)
    cargo = Column(String(200), nullable=True)  # "Alcalde de Santiago", "Diputado Distrito 10"
    
    # Facebook/Instagram OAuth tokens
    facebook_page_id = Column(String(200), nullable=True)
    facebook_page_name = Column(String(200), nullable=True)
    facebook_page_access_token = Column(String(500), nullable=True)
    facebook_token_expiration = Column(DateTime, nullable=True)
    
    instagram_business_account_id = Column(String(200), nullable=True)
    instagram_username = Column(String(200), nullable=True)
    
    # WhatsApp
    whatsapp_phone_number_id = Column(String(200), nullable=True)
    whatsapp_business_account_id = Column(String(200), nullable=True)
    whatsapp_phone_number = Column(String(50), nullable=True)
    
    # Estado
    estado = Column(String(50), default='activo')  # 'activo', 'inactivo', 'suspendido'
    
    # Autenticación de usuario (opcional si no usas Facebook Login para entrar al dashboard)
    password_hash = Column(String(500), nullable=True)
    
    # Metadatos
    fecha_registro = Column(DateTime, default=datetime.utcnow)
    fecha_ultimo_login = Column(DateTime, nullable=True)
    fecha_actualizacion = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relaciones
    personas = relationship("Persona", back_populates="candidato")
    
    def __repr__(self):
        return f"<Candidato(id={self.id}, nombre='{self.nombre}', email='{self.email}')>"


class MetaConfig(Base):
    """Configuración y estado de las conexiones a APIs de Meta."""
    __tablename__ = 'meta_config'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    plataforma = Column(String(50), nullable=False)  # 'facebook' o 'instagram'
    
    # Tokens
    access_token = Column(String(500), nullable=True)
    token_expiracion = Column(DateTime, nullable=True)
    
    # Estado
    ultima_sincronizacion = Column(DateTime, nullable=True)
    estado = Column(String(50), default='inactivo')  # 'activo', 'inactivo', 'error'
    
    # Metadatos
    fecha_creacion = Column(DateTime, default=datetime.utcnow)
    fecha_actualizacion = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def __repr__(self):
        return f"<MetaConfig(id={self.id}, plataforma='{self.plataforma}', estado='{self.estado}')>"


class UsuarioAutorizado(Base):
    """Lista blanca de usuarios autorizados a acceder a la app."""
    __tablename__ = 'usuarios_autorizados'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    email = Column(String(200), unique=True, nullable=False)
    nombre = Column(String(200), nullable=False)
    rol = Column(String(50), default='candidato')  # 'candidato', 'admin', 'equipo'
    activo = Column(Integer, default=1)  # 1=activo, 0=inactivo (usar Integer por compatibilidad SQLite)
    
    # Auditoría
    fecha_registro = Column(DateTime, default=datetime.utcnow)
    invitado_por = Column(Integer, ForeignKey('usuarios_autorizados.id'), nullable=True)
    ultimo_acceso = Column(DateTime, nullable=True)
    
    def __repr__(self):
        return f"<UsuarioAutorizado(id={self.id}, email='{self.email}', rol='{self.rol}')>"
