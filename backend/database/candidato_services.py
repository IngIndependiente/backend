"""Servicios para gestión de Candidatos (multi-tenant)."""
import pandas as pd
from datetime import datetime
from typing import Dict, Any, Optional
from backend import config

if config.ENV == "local":
    from backend.database.dataframe_storage import get_storage
else:
    from backend.database.storage import get_db
    from backend.database.models import Candidato


class CandidatoService:
    """Servicio para operaciones CRUD de candidatos."""
    
    @staticmethod
    def crear_candidato(
        nombre: str,
        email: str,
        partido: Optional[str] = None,
        cargo: Optional[str] = None,
        facebook_page_id: Optional[str] = None,
        facebook_page_name: Optional[str] = None,
        facebook_page_access_token: Optional[str] = None,
        facebook_token_expiration: Optional[datetime] = None,
        instagram_business_account_id: Optional[str] = None,
        instagram_username: Optional[str] = None,
        whatsapp_phone_number_id: Optional[str] = None,
        whatsapp_business_account_id: Optional[str] = None,
        whatsapp_phone_number: Optional[str] = None,
        password_hash: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Crear nuevo candidato.
        
        Returns:
            Dict con información del candidato creado
        """
        if config.ENV == "local":
            storage = get_storage()
            
            # Verificar si email ya existe
            if email in storage.candidatos_df['email'].values:
                raise ValueError(f"Ya existe un candidato con email {email}")
            
            # Generar nuevo ID
            nuevo_id = int(storage.candidatos_df['id'].max() + 1) if len(storage.candidatos_df) > 0 else 1
            
            # Crear nuevo registro
            ahora = datetime.now()
            nuevo_candidato = pd.DataFrame([{
                'id': nuevo_id,
                'nombre': nombre,
                'email': email,
                'partido': partido,
                'cargo': cargo,
                'facebook_page_id': facebook_page_id,
                'facebook_page_name': facebook_page_name,
                'facebook_page_access_token': facebook_page_access_token,
                'facebook_token_expiration': facebook_token_expiration,
                'instagram_business_account_id': instagram_business_account_id,
                'instagram_username': instagram_username,
                'whatsapp_phone_number_id': whatsapp_phone_number_id,
                'whatsapp_business_account_id': whatsapp_business_account_id,
                'whatsapp_phone_number': whatsapp_phone_number,
                'estado': 'activo',
                'password_hash': password_hash,
                'fecha_registro': ahora,
                'fecha_ultimo_login': None,
                'fecha_actualizacion': ahora
            }])
            
            storage.candidatos_df = pd.concat([storage.candidatos_df, nuevo_candidato], ignore_index=True)
            storage.save_candidatos()
            
            return nuevo_candidato.iloc[0].to_dict()
        
        else:
            # Modo SQLAlchemy
            with get_db() as db:
                # Verificar si email ya existe
                if db.query(Candidato).filter(Candidato.email == email).first():
                    raise ValueError(f"Ya existe un candidato con email {email}")
                
                nuevo_candidato = Candidato(
                    nombre=nombre,
                    email=email,
                    partido=partido,
                    cargo=cargo,
                    facebook_page_id=facebook_page_id,
                    facebook_page_name=facebook_page_name,
                    facebook_page_access_token=facebook_page_access_token,
                    facebook_token_expiration=facebook_token_expiration,
                    instagram_business_account_id=instagram_business_account_id,
                    instagram_username=instagram_username,
                    whatsapp_phone_number_id=whatsapp_phone_number_id,
                    whatsapp_business_account_id=whatsapp_business_account_id,
                    whatsapp_phone_number=whatsapp_phone_number,
                    estado='activo',
                    password_hash=password_hash
                )
                
                db.add(nuevo_candidato)
                db.commit()
                db.refresh(nuevo_candidato)
                
                return {
                    'id': nuevo_candidato.id,
                    'nombre': nuevo_candidato.nombre,
                    'email': nuevo_candidato.email,
                    'partido': nuevo_candidato.partido,
                    'cargo': nuevo_candidato.cargo,
                    'facebook_page_id': nuevo_candidato.facebook_page_id,
                    'facebook_page_name': nuevo_candidato.facebook_page_name,
                    'estado': nuevo_candidato.estado,
                    'fecha_registro': nuevo_candidato.fecha_registro
                }
    
    @staticmethod
    def obtener_candidato_por_email(email: str) -> Optional[Dict[str, Any]]:
        """Obtener candidato por email."""
        if config.ENV == "local":
            storage = get_storage()
            mask = storage.candidatos_df['email'] == email
            if mask.any():
                return storage.candidatos_df[mask].iloc[0].to_dict()
            return None
        else:
            with get_db() as db:
                candidato = db.query(Candidato).filter(Candidato.email == email).first()
                if candidato:
                    return {
                        'id': candidato.id,
                        'nombre': candidato.nombre,
                        'email': candidato.email,
                        'partido': candidato.partido,
                        'cargo': candidato.cargo,
                        'facebook_page_id': candidato.facebook_page_id,
                        'facebook_page_name': candidato.facebook_page_name,
                        'facebook_page_access_token': candidato.facebook_page_access_token,
                        'facebook_token_expiration': candidato.facebook_token_expiration,
                        'instagram_business_account_id': candidato.instagram_business_account_id,
                        'instagram_username': candidato.instagram_username,
                        'whatsapp_phone_number_id': candidato.whatsapp_phone_number_id,
                        'whatsapp_business_account_id': candidato.whatsapp_business_account_id,
                        'whatsapp_phone_number': candidato.whatsapp_phone_number,
                        'estado': candidato.estado,
                        'fecha_registro': candidato.fecha_registro,
                        'fecha_ultimo_login': candidato.fecha_ultimo_login
                    }
                return None
    
    @staticmethod
    def obtener_candidato_por_id(candidato_id: int) -> Optional[Dict[str, Any]]:
        """Obtener candidato por ID."""
        if config.ENV == "local":
            storage = get_storage()
            mask = storage.candidatos_df['id'] == candidato_id
            if mask.any():
                return storage.candidatos_df[mask].iloc[0].to_dict()
            return None
        else:
            with get_db() as db:
                candidato = db.query(Candidato).filter(Candidato.id == candidato_id).first()
                if candidato:
                    return {
                        'id': candidato.id,
                        'nombre': candidato.nombre,
                        'email': candidato.email,
                        'partido': candidato.partido,
                        'cargo': candidato.cargo,
                        'facebook_page_id': candidato.facebook_page_id,
                        'facebook_page_name': candidato.facebook_page_name,
                        'facebook_page_access_token': candidato.facebook_page_access_token,
                        'facebook_token_expiration': candidato.facebook_token_expiration,
                        'instagram_business_account_id': candidato.instagram_business_account_id,
                        'instagram_username': candidato.instagram_username,
                        'whatsapp_phone_number_id': candidato.whatsapp_phone_number_id,
                        'whatsapp_business_account_id': candidato.whatsapp_business_account_id,
                        'whatsapp_phone_number': candidato.whatsapp_phone_number,
                        'estado': candidato.estado,
                        'fecha_registro': candidato.fecha_registro,
                        'fecha_ultimo_login': candidato.fecha_ultimo_login
                    }
                return None
    
    @staticmethod
    def obtener_candidato_por_page_id(facebook_page_id: str) -> Optional[Dict[str, Any]]:
        """Obtener candidato por Facebook Page ID."""
        if config.ENV == "local":
            storage = get_storage()
            mask = storage.candidatos_df['facebook_page_id'] == facebook_page_id
            if mask.any():
                return storage.candidatos_df[mask].iloc[0].to_dict()
            return None
        else:
            with get_db() as db:
                candidato = db.query(Candidato).filter(Candidato.facebook_page_id == facebook_page_id).first()
                if candidato:
                    return {
                        'id': candidato.id,
                        'nombre': candidato.nombre,
                        'facebook_page_id': candidato.facebook_page_id,
                        'facebook_page_access_token': candidato.facebook_page_access_token,
                        'facebook_token_expiration': candidato.facebook_token_expiration,
                        'instagram_business_account_id': candidato.instagram_business_account_id
                    }
                return None
    
    @staticmethod
    def actualizar_tokens_facebook(
        candidato_id: int,
        facebook_page_id: str,
        facebook_page_name: str,
        facebook_page_access_token: str,
        facebook_token_expiration: datetime,
        instagram_business_account_id: Optional[str] = None,
        instagram_username: Optional[str] = None
    ) -> Dict[str, Any]:
        """Actualizar tokens de Facebook/Instagram para un candidato."""
        if config.ENV == "local":
            storage = get_storage()
            mask = storage.candidatos_df['id'] == candidato_id
            
            if not mask.any():
                raise ValueError(f"Candidato con ID {candidato_id} no encontrado")
            
            # Actualizar
            storage.candidatos_df.loc[mask, 'facebook_page_id'] = facebook_page_id
            storage.candidatos_df.loc[mask, 'facebook_page_name'] = facebook_page_name
            storage.candidatos_df.loc[mask, 'facebook_page_access_token'] = facebook_page_access_token
            storage.candidatos_df.loc[mask, 'facebook_token_expiration'] = facebook_token_expiration
            
            if instagram_business_account_id:
                storage.candidatos_df.loc[mask, 'instagram_business_account_id'] = instagram_business_account_id
            if instagram_username:
                storage.candidatos_df.loc[mask, 'instagram_username'] = instagram_username
            
            storage.candidatos_df.loc[mask, 'fecha_actualizacion'] = datetime.now()
            storage.save_candidatos()
            
            return storage.candidatos_df[mask].iloc[0].to_dict()
        
        else:
            with get_db() as db:
                candidato = db.query(Candidato).filter(Candidato.id == candidato_id).first()
                if not candidato:
                    raise ValueError(f"Candidato con ID {candidato_id} no encontrado")
                
                candidato.facebook_page_id = facebook_page_id
                candidato.facebook_page_name = facebook_page_name
                candidato.facebook_page_access_token = facebook_page_access_token
                candidato.facebook_token_expiration = facebook_token_expiration
                
                if instagram_business_account_id:
                    candidato.instagram_business_account_id = instagram_business_account_id
                if instagram_username:
                    candidato.instagram_username = instagram_username
                
                db.commit()
                db.refresh(candidato)
                
                return {
                    'id': candidato.id,
                    'nombre': candidato.nombre,
                    'facebook_page_id': candidato.facebook_page_id,
                    'facebook_page_name': candidato.facebook_page_name,
                    'instagram_business_account_id': candidato.instagram_business_account_id,
                    'instagram_username': candidato.instagram_username
                }
    
    @staticmethod
    def listar_candidatos() -> list:
        """Listar todos los candidatos activos."""
        if config.ENV == "local":
            storage = get_storage()
            candidatos = storage.candidatos_df[storage.candidatos_df['estado'] == 'activo']
            return candidatos.to_dict('records')
        else:
            with get_db() as db:
                candidatos = db.query(Candidato).filter(Candidato.estado == 'activo').all()
                return [{
                    'id': c.id,
                    'nombre': c.nombre,
                    'email': c.email,
                    'partido': c.partido,
                    'cargo': c.cargo,
                    'facebook_page_name': c.facebook_page_name,
                    'instagram_username': c.instagram_username,
                    'whatsapp_phone_number': c.whatsapp_phone_number,
                    'estado': c.estado
                } for c in candidatos]
    
    @staticmethod
    def actualizar_whatsapp(
        candidato_id: int,
        whatsapp_phone_number_id: str,
        whatsapp_business_account_id: str,
        whatsapp_phone_number: str,
        whatsapp_access_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """Actualizar configuración de WhatsApp para un candidato."""
        if config.ENV == "local":
            storage = get_storage()
            mask = storage.candidatos_df['id'] == candidato_id
            
            if not mask.any():
                raise ValueError(f"Candidato con ID {candidato_id} no encontrado")
            
            # Actualizar
            storage.candidatos_df.loc[mask, 'whatsapp_phone_number_id'] = whatsapp_phone_number_id
            storage.candidatos_df.loc[mask, 'whatsapp_business_account_id'] = whatsapp_business_account_id
            storage.candidatos_df.loc[mask, 'whatsapp_phone_number'] = whatsapp_phone_number
            
            # WhatsApp usa el mismo token de la página de Facebook
            if whatsapp_access_token:
                storage.candidatos_df.loc[mask, 'facebook_page_access_token'] = whatsapp_access_token
            
            storage.candidatos_df.loc[mask, 'fecha_actualizacion'] = datetime.now()
            storage.save_candidatos()
            
            return storage.candidatos_df[mask].iloc[0].to_dict()
        
        else:
            with get_db() as db:
                candidato = db.query(Candidato).filter(Candidato.id == candidato_id).first()
                if not candidato:
                    raise ValueError(f"Candidato con ID {candidato_id} no encontrado")
                
                candidato.whatsapp_phone_number_id = whatsapp_phone_number_id
                candidato.whatsapp_business_account_id = whatsapp_business_account_id
                candidato.whatsapp_phone_number = whatsapp_phone_number
                
                if whatsapp_access_token:
                    candidato.facebook_page_access_token = whatsapp_access_token
                
                db.commit()
                db.refresh(candidato)
                
                return {
                    'id': candidato.id,
                    'nombre': candidato.nombre,
                    'whatsapp_phone_number_id': candidato.whatsapp_phone_number_id,
                    'whatsapp_business_account_id': candidato.whatsapp_business_account_id,
                    'whatsapp_phone_number': candidato.whatsapp_phone_number
                }
