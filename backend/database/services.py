"""Servicios de base de datos para gestión de personas e información."""
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_
from typing import List, Dict, Any, Optional
from datetime import datetime, date
import json

from backend.database.models import Persona, Interes, Conversacion, Analisis


class PersonaService:
    """Servicio para gestionar personas en la base de datos."""
    
    @staticmethod
    def crear_o_actualizar_persona(
        db: Session,
        datos: Dict[str, Any],
        facebook_id: str = None,
        instagram_id: str = None
    ) -> Persona:
        """
        Crear o actualizar una persona con los datos extraídos.
        
        Args:
            db: Sesión de base de datos
            datos: Diccionario con datos extraídos
            facebook_id: ID de Facebook
            instagram_id: ID de Instagram
            
        Returns:
            Persona creada o actualizada
        """
        # Buscar persona existente
        persona = None
        
        if facebook_id:
            persona = db.query(Persona).filter(Persona.facebook_id == facebook_id).first()
        elif instagram_id:
            persona = db.query(Persona).filter(Persona.instagram_id == instagram_id).first()
        
        # Si no existe, crear nueva
        if not persona:
            persona = Persona(
                facebook_id=facebook_id,
                instagram_id=instagram_id,
                fecha_primer_contacto=datetime.utcnow()
            )
            db.add(persona)
        
        # Actualizar datos solo si están presentes
        if datos.get("nombre_completo"):
            persona.nombre_completo = datos["nombre_completo"]
        
        if datos.get("edad") is not None:
            persona.edad = datos["edad"]
        
        if datos.get("genero"):
            persona.genero = datos["genero"]
        
        if datos.get("telefono"):
            persona.telefono = datos["telefono"]
        
        if datos.get("email"):
            persona.email = datos["email"]
        
        if datos.get("ocupacion"):
            persona.ocupacion = datos["ocupacion"]
        
        if datos.get("ubicacion"):
            persona.ubicacion = datos["ubicacion"]
            
        if datos.get("resumen_conversacional"):
            persona.resumen = datos["resumen_conversacional"]
        
        # Actualizar fecha último contacto
        persona.fecha_ultimo_contacto = datetime.utcnow()
        
        # Gestionar intereses
        if datos.get("intereses"):
            for categoria in datos["intereses"]:
                interes = db.query(Interes).filter(Interes.categoria == categoria).first()
                if interes and interes not in persona.intereses:
                    persona.intereses.append(interes)
        
        db.commit()
        db.refresh(persona)
        
        return persona
    
    @staticmethod
    def buscar_personas(
        db: Session,
        genero: Optional[str] = None,
        edad_min: Optional[int] = None,
        edad_max: Optional[int] = None,
        intereses: Optional[List[str]] = None,
        ubicacion: Optional[str] = None
    ) -> List[Persona]:
        """
        Buscar personas según criterios.
        
        Args:
            db: Sesión de base de datos
            genero: Filtrar por género
            edad_min: Edad mínima
            edad_max: Edad máxima
            intereses: Lista de categorías de interés
            ubicacion: Filtrar por ubicación
            
        Returns:
            Lista de personas que coinciden con los criterios
        """
        query = db.query(Persona)
        
        # Aplicar filtros
        if genero:
            query = query.filter(Persona.genero == genero)
        
        if edad_min is not None:
            query = query.filter(Persona.edad >= edad_min)
        
        if edad_max is not None:
            query = query.filter(Persona.edad <= edad_max)
        
        if ubicacion:
            query = query.filter(Persona.ubicacion.ilike(f"%{ubicacion}%"))
        
        # Filtrar por intereses
        if intereses:
            for interes_cat in intereses:
                interes = db.query(Interes).filter(Interes.categoria == interes_cat).first()
                if interes:
                    query = query.filter(Persona.intereses.contains(interes))
        
        return query.all()
    
    @staticmethod
    def obtener_persona_por_id(db: Session, persona_id: int) -> Optional[Persona]:
        """Obtener una persona por su ID."""
        return db.query(Persona).filter(Persona.id == persona_id).first()
    
    @staticmethod
    def listar_todas(db: Session, limit: int = 100, offset: int = 0) -> List[Persona]:
        """Listar todas las personas."""
        return db.query(Persona).offset(offset).limit(limit).all()


class ConversacionService:
    """Servicio para gestionar conversaciones."""
    
    @staticmethod
    def guardar_conversacion(
        db: Session,
        persona_id: int,
        mensaje: str,
        plataforma: str,
        es_enviado: bool = False,
        conversacion_id: str = None,
        datos_extraidos: Dict[str, Any] = None,
        fecha_mensaje: datetime = None
    ) -> Conversacion:
        """
        Guardar una conversación en la base de datos, evitando duplicados.
        """
        # Evitar duplicados si tenemos conversacion_id
        if conversacion_id:
            existente = db.query(Conversacion).filter(
                Conversacion.conversacion_id == conversacion_id,
                Conversacion.persona_id == persona_id
            ).first()
            if existente:
                return existente

        conversacion = Conversacion(
            persona_id=persona_id,
            mensaje=mensaje,
            plataforma=plataforma,
            es_enviado=1 if es_enviado else 0,
            conversacion_id=conversacion_id,
            datos_extraidos=json.dumps(datos_extraidos) if datos_extraidos else None,
            fecha_mensaje=fecha_mensaje or datetime.utcnow(),
            fecha_procesado=datetime.utcnow() if datos_extraidos else None
        )
        
        db.add(conversacion)
        db.commit()
        db.refresh(conversacion)
        
        return conversacion
    
    @staticmethod
    def obtener_historial(
        db: Session,
        persona_id: int,
        limit: int = 50
    ) -> List[Conversacion]:
        """
        Obtener historial de conversaciones de una persona.
        
        Args:
            db: Sesión de base de datos
            persona_id: ID de la persona
            limit: Cantidad máxima de mensajes
            
        Returns:
            Lista de conversaciones ordenadas por fecha
        """
        return db.query(Conversacion)\
            .filter(Conversacion.persona_id == persona_id)\
            .order_by(Conversacion.fecha_mensaje.desc())\
            .limit(limit)\
            .all()


class AnalisisService:
    """Servicio para gestionar análisis de sesiones."""
    
    @staticmethod
    def crear_analisis(
        db: Session,
        persona_id: int,
        resumen: str,
        contenido_completo: str,
        categorias: List[str] = None,
        start_conversation: datetime = None
    ) -> Analisis:
        """Crear un nuevo análisis de sesión, evitando duplicados para el mismo inicio de sesión."""
        # Evitar duplicados si tenemos start_conversation
        if start_conversation:
            # Tolerancia de 1 minuto para el inicio de sesión
            from datetime import timedelta
            margen = timedelta(minutes=1)
            existente = db.query(Analisis).filter(
                Analisis.persona_id == persona_id,
                Analisis.start_conversation >= start_conversation - margen,
                Analisis.start_conversation <= start_conversation + margen
            ).first()
            if existente:
                return existente

        analisis = Analisis(
            persona_id=persona_id,
            resumen=resumen,
            contenido_completo=contenido_completo,
            categorias=json.dumps(categorias) if categorias else "[]",
            start_conversation=start_conversation or datetime.utcnow(),
            fecha_analisis=datetime.utcnow()
        )
        
        db.add(analisis)
        db.commit()
        db.refresh(analisis)
        
        return analisis

    @staticmethod
    def buscar_analisis(
        db: Session,
        persona_id: int = None,
        fecha_inicio: datetime | date = None,
        fecha_fin: datetime | date = None,
        limit: int = 50
    ) -> List[Analisis]:
        """Buscar análisis."""
        query = db.query(Analisis)
        if persona_id:
            query = query.filter(Analisis.persona_id == persona_id)
            
        # Filtrar por fecha - convertir date a datetime si es necesario
        if fecha_inicio:
            # Si es datetime sin hora específica (00:00:00), lo dejamos así (inicio del día)
            # Si viene como string "YYYY-MM-DD", fromisoformat lo convierte a datetime con 00:00:00
            query = query.filter(Analisis.fecha_analisis >= fecha_inicio)
        
        if fecha_fin:
            # Si es datetime, verificar si es medianoche y ajustar al final del día
            if isinstance(fecha_fin, datetime):
                # Si la hora es 00:00:00, significa que queremos todo el día
                if fecha_fin.hour == 0 and fecha_fin.minute == 0 and fecha_fin.second == 0:
                    # Ajustar al final del día (23:59:59.999999)
                    from datetime import timedelta
                    fecha_fin = fecha_fin + timedelta(days=1) - timedelta(microseconds=1)
            query = query.filter(Analisis.fecha_analisis <= fecha_fin)
            
        return query.order_by(Analisis.fecha_analisis.desc()).limit(limit).all()
