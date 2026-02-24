"""Servicios para gestión de datos usando Pandas DataFrames."""
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any, Optional
import json
from backend.database.dataframe_storage import get_storage



class DataFramePersonaService:
    """Servicio para gestionar personas con DataFrames."""
    
    @staticmethod
    def crear_o_actualizar_persona(
        datos: Dict[str, Any],
        facebook_id: str = None,
        instagram_id: str = None,
        telefono: str = None
    ) -> Dict[str, Any]:
        """Crear o actualizar una persona."""
        storage = get_storage()
        
        # Buscar persona existente
        persona = None
        if facebook_id:
            mask = storage.personas_df['facebook_id'] == facebook_id
            if mask.any():
                persona = storage.personas_df[mask].iloc[0].to_dict()
        elif instagram_id:
            mask = storage.personas_df['instagram_id'] == instagram_id
            if mask.any():
                persona = storage.personas_df[mask].iloc[0].to_dict()
        elif telefono:
            mask = storage.personas_df['telefono'] == telefono
            if mask.any():
                persona = storage.personas_df[mask].iloc[0].to_dict()
        
        now = datetime.now()
        
        if persona is None:
            # Crear nueva persona
            nuevo_id = storage.personas_df['id'].max() + 1 if len(storage.personas_df) > 0 else 1
            persona = {
                'id': nuevo_id,
                'facebook_id': facebook_id,
                'instagram_id': instagram_id,
                'telefono': telefono,
                'fecha_primer_contacto': now,
                'fecha_creacion': now
            }
        
        # Actualizar datos
        if datos.get("nombre_completo"):
            persona['nombre_completo'] = datos["nombre_completo"]
        if datos.get("edad") is not None:
            persona['edad'] = datos["edad"]
        if datos.get("genero"):
            persona['genero'] = datos["genero"]
        if datos.get("telefono"):
            persona['telefono'] = datos["telefono"]
        if datos.get("email"):
            persona['email'] = datos["email"]
        if datos.get("ocupacion"):
            persona['ocupacion'] = datos["ocupacion"]
        if datos.get("ubicacion"):
            persona['ubicacion'] = datos["ubicacion"]
        if datos.get("resumen_conversacional"):
            persona['resumen'] = datos["resumen_conversacional"]
        
        persona['fecha_ultimo_contacto'] = now
        
        # Guardar o actualizar en el DataFrame
        persona_id = persona['id']
        mask = storage.personas_df['id'] == persona_id
        
        if mask.any():
            # Actualizar
            for key, value in persona.items():
                storage.personas_df.loc[mask, key] = value
        else:
            # Insertar
            nueva_fila = pd.DataFrame([persona])
            storage.personas_df = pd.concat([storage.personas_df, nueva_fila], ignore_index=True)
        
        storage.save_personas()
        
        # Gestionar intereses
        if datos.get("intereses"):
            for categoria in datos["intereses"]:
                interes_mask = storage.intereses_df['categoria'] == categoria
                if interes_mask.any():
                    interes_id = storage.intereses_df[interes_mask].iloc[0]['id']
                    
                    # Verificar si ya existe la relación
                    rel_mask = (storage.persona_interes_df['persona_id'] == persona_id) & \
                               (storage.persona_interes_df['interes_id'] == interes_id)
                    
                    if not rel_mask.any():
                        nueva_rel = pd.DataFrame([{'persona_id': persona_id, 'interes_id': interes_id}])
                        storage.persona_interes_df = pd.concat([storage.persona_interes_df, nueva_rel], ignore_index=True)
            
            storage.save_persona_interes()
        
        return persona
    
    @staticmethod
    def obtener_persona_por_id(persona_id: int) -> Optional[Dict]:
        """Obtener una persona por ID."""
        storage = get_storage()
        mask = storage.personas_df['id'] == persona_id
        if mask.any():
            return storage.personas_df[mask].iloc[0].to_dict()
        return None
    
    @staticmethod
    def obtener_por_telefono(telefono: str) -> Optional[Dict]:
        """Obtener una persona por número de teléfono."""
        storage = get_storage()
        mask = storage.personas_df['telefono'] == telefono
        if mask.any():
            return storage.personas_df[mask].iloc[0].to_dict()
        return None


class DataFrameConversacionService:
    """Servicio para gestionar conversaciones con DataFrames."""
    
    @staticmethod
    def guardar_conversacion(
        persona_id: int,
        mensaje: str,
        plataforma: str,
        es_enviado: bool = False,
        conversacion_id: str = None,
        fecha_mensaje: datetime = None
    ) -> Dict[str, Any]:
        """Guardar una conversación evitando duplicados."""
        storage = get_storage()
        
        # Evitar duplicados
        if conversacion_id:
            mask = (storage.conversaciones_df['conversacion_id'] == conversacion_id) & \
                   (storage.conversaciones_df['persona_id'] == persona_id)
            if mask.any():
                return storage.conversaciones_df[mask].iloc[0].to_dict()
        
        nuevo_id = storage.conversaciones_df['id'].max() + 1 if len(storage.conversaciones_df) > 0 else 1
        
        conversacion = {
            'id': nuevo_id,
            'persona_id': persona_id,
            'plataforma': plataforma,
            'conversacion_id': conversacion_id,
            'mensaje': mensaje,
            'es_enviado': 1 if es_enviado else 0,
            'fecha_mensaje': fecha_mensaje or datetime.now(),
            'fecha_procesado': datetime.now()
        }
        
        nueva_fila = pd.DataFrame([conversacion])
        storage.conversaciones_df = pd.concat([storage.conversaciones_df, nueva_fila], ignore_index=True)
        storage.save_conversaciones()
        
        return conversacion
    
    @staticmethod
    def obtener_historial(persona_id: int, limit: int = 50) -> List[Dict]:
        """Obtener historial de conversaciones."""
        storage = get_storage()
        mask = storage.conversaciones_df['persona_id'] == persona_id
        historial = storage.conversaciones_df[mask].sort_values('fecha_mensaje', ascending=False).head(limit)
        return historial.to_dict('records')


class DataFrameAnalisisService:
    """Servicio para gestionar análisis con DataFrames."""
    
    @staticmethod
    def crear_analisis(
        persona_id: int,
        resumen: str,
        contenido_completo: str = None,
        categorias: List[str] = None,
        start_conversation: datetime = None,
        evento_id: int = None
    ) -> Dict[str, Any]:
        """Crear un análisis evitando duplicados para la misma sesión."""
        storage = get_storage()
        
        # Evitar duplicados
        if start_conversation:
            from datetime import timedelta
            margen = timedelta(minutes=1)

            # Si el input tiene timezone, lo convertimos a naive para comparar con el DF
            if start_conversation.tzinfo is not None:
                start_conversation = start_conversation.replace(tzinfo=None)
            
            # Asegurarse de que start_conversation en el DF sea datetime
            df = storage.analisis_df
            if not df.empty:
                # Convertir a datetime si no lo es (a veces pandas carga como string)
                if not pd.api.types.is_datetime64_any_dtype(df['start_conversation']):
                    df['start_conversation'] = pd.to_datetime(df['start_conversation'])
                
                # Asegurar que los datos del DF sean naive para la comparación
                # (to_datetime suele hacerlo pero por si acaso)
                df_dates = df['start_conversation']
                if hasattr(df_dates.dt, 'tz_localize') and df_dates.dt.tz is not None:
                    df_dates = df_dates.dt.tz_localize(None)

                mask = (df['persona_id'] == persona_id) & \
                       (df_dates >= start_conversation - margen) & \
                       (df_dates <= start_conversation + margen)
                
                if mask.any():
                    return df[mask].iloc[0].to_dict()
        
        nuevo_id = storage.analisis_df['id'].max() + 1 if len(storage.analisis_df) > 0 else 1
        
        analisis = {
            'id': nuevo_id,
            'persona_id': persona_id,
            'evento_id': evento_id,
            'resumen': resumen,
            'categorias': json.dumps(categorias) if categorias else None,
            'start_conversation': start_conversation or datetime.now(),
            'fecha_analisis': datetime.now(),
            'contenido_completo': contenido_completo
        }
        
        nueva_fila = pd.DataFrame([analisis])
        storage.analisis_df = pd.concat([storage.analisis_df, nueva_fila], ignore_index=True)
        storage.save_analisis()
        
        return analisis
    
    @staticmethod
    def buscar_analisis(
        fecha_inicio: datetime = None,
        fecha_fin: datetime = None,
        limit: int = 100
    ) -> List[Dict]:
        """Buscar análisis por fecha."""
        storage = get_storage()
        df = storage.analisis_df.copy()

        # Si el input tiene timezone, lo convertimos a naive para comparar con el DF
        if fecha_inicio and fecha_inicio.tzinfo is not None:
            fecha_inicio = fecha_inicio.replace(tzinfo=None)
        if fecha_fin and fecha_fin.tzinfo is not None:
            fecha_fin = fecha_fin.replace(tzinfo=None)
        
        if not df.empty:
            if not pd.api.types.is_datetime64_any_dtype(df['start_conversation']):
                df['start_conversation'] = pd.to_datetime(df['start_conversation'])

            if fecha_inicio:
                df = df[df['start_conversation'] >= fecha_inicio]
            if fecha_fin:
                df = df[df['start_conversation'] <= fecha_fin]
        
        df = df.sort_values('start_conversation', ascending=False).head(limit)
        return df.to_dict('records')
    
    @staticmethod
    def obtener_por_id(analisis_id: int) -> Optional[Dict]:
        """Obtener análisis por ID."""
        storage = get_storage()
        mask = storage.analisis_df['id'] == analisis_id
        if mask.any():
            return storage.analisis_df[mask].iloc[0].to_dict()
        return None
    
    @staticmethod
    def actualizar_evento(analisis_id: int, evento_id: Optional[int]):
        """Actualizar el evento de un análisis."""
        storage = get_storage()
        try:
            storage.logger.debug("Actualizar evento: analisis_id=%s evento_id=%s", analisis_id, evento_id)
        except Exception:
            pass
        mask = storage.analisis_df['id'] == analisis_id
        if mask.any():
            storage.analisis_df.loc[mask, 'evento_id'] = evento_id
            storage.save_analisis()
            try:
                storage.logger.debug("Evento actualizado en analisis %s -> %s", analisis_id, evento_id)
            except Exception:
                pass
            return True
        return False


class DataFrameEventoService:
    """Servicio para gestionar eventos con DataFrames."""
    
    @staticmethod
    def obtener_todos() -> List[Dict]:
        """Obtener todos los eventos."""
        storage = get_storage()
        return storage.eventos_df.sort_values('nombre').to_dict('records')
    
    @staticmethod
    def obtener_por_id(evento_id: int) -> Optional[Dict]:
        """Obtener evento por ID."""
        storage = get_storage()
        mask = storage.eventos_df['id'] == evento_id
        if mask.any():
            return storage.eventos_df[mask].iloc[0].to_dict()
        return None
    
    @staticmethod
    def obtener_por_nombre(nombre: str) -> Optional[Dict]:
        """Obtener evento por nombre."""
        storage = get_storage()
        mask = storage.eventos_df['nombre'] == nombre
        if mask.any():
            return storage.eventos_df[mask].iloc[0].to_dict()
        return None
    
    @staticmethod
    def crear_evento(nombre: str, descripcion: str = None) -> Dict:
        """Crear un nuevo evento."""
        storage = get_storage()
        
        nuevo_id = storage.eventos_df['id'].max() + 1 if len(storage.eventos_df) > 0 else 1
        
        evento = {
            'id': nuevo_id,
            'nombre': nombre,
            'descripcion': descripcion or f'Evento: {nombre}',
            'fecha_creacion': datetime.now()
        }
        
        nueva_fila = pd.DataFrame([evento])
        storage.eventos_df = pd.concat([storage.eventos_df, nueva_fila], ignore_index=True)
        storage.save_eventos()
        
        return evento
