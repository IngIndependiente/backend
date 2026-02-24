"""Script para procesar conversaciones de Meta y poblar la base de datos."""
import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Any
from sqlalchemy.orm import Session
import dateutil.parser

from backend import config
from backend.database.storage import get_db, init_db, PersonaService, ConversacionService, AnalisisService, USE_DATAFRAMES
from backend.agent.langgraph_agent import procesar_conversacion
from backend.integrations.meta_api import meta_client


def agrupar_mensajes_por_dia_cl(mensajes: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
    """Agrupa mensajes por día calendario en zona horaria UTC-3 (Chile)."""
    if not mensajes:
        return []
        
    # Ordenar por fecha
    mensajes_ord = sorted(mensajes, key=lambda x: x.get("created_time", ""))
    
    sesiones_por_dia = {}
    
    for msg in mensajes_ord:
        t_utc = dateutil.parser.isoparse(msg.get("created_time"))
        # Ajustar a UTC-3 (Chile)
        t_cl = t_utc - timedelta(hours=3)
        fecha_cl = t_cl.date()
        
        if fecha_cl not in sesiones_por_dia:
            sesiones_por_dia[fecha_cl] = []
            
        sesiones_por_dia[fecha_cl].append(msg)
        
    # Devolver como lista de listas preservando el orden cronológico de los días
    dias_ordenados = sorted(sesiones_por_dia.keys())
    return [sesiones_por_dia[dia] for dia in dias_ordenados]


def procesar_mensajes_usuario(
    db: Session,
    user_id: str,
    username: str,
    plataforma: str,
    mensajes: List[Dict[str, Any]],
    ignorar_id: str = None
):
    """Procesa todo el historial de mensajes de un usuario."""
    print(f"\n👤 Procesando historial de: {username or user_id} ({len(mensajes)} mensajes)")
    
    # 1. Crear o actualizar Persona (datos básicos)
    # Nota: No tenemos agente aquí todavía para extraer datos del texto, 
    # eso se hará en el análisis por sesión.
    facebook_id = user_id if plataforma == "facebook" else None
    instagram_id = user_id if plataforma == "instagram" else None
    
    if USE_DATAFRAMES:
        persona = PersonaService.crear_o_actualizar_persona(
            datos={"nombre_completo": username} if username else {},
            facebook_id=facebook_id,
            instagram_id=instagram_id
        )
    else:
        persona = PersonaService.crear_o_actualizar_persona(
            db,
            datos={"nombre_completo": username} if username else {},
            facebook_id=facebook_id,
            instagram_id=instagram_id
        )
    
    if username:
        if USE_DATAFRAMES:
            # persona ya es un dict en modo dataframe
            dirty = False
            if plataforma == "facebook" and persona.get('facebook_username') != username:
                persona['facebook_username'] = username
                dirty = True
            elif plataforma == "instagram" and persona.get('instagram_username') != username:
                persona['instagram_username'] = username
                dirty = True
            
            if dirty:
                # Actualizar en el storage
                from backend.database.dataframe_storage import get_storage
                storage = get_storage()
                mask = storage.personas_df['id'] == persona['id']
                if mask.any():
                    if plataforma == "facebook":
                        storage.personas_df.loc[mask, 'facebook_username'] = username
                    else:
                        storage.personas_df.loc[mask, 'instagram_username'] = username
                    storage.save_personas()
        else:
            if plataforma == "facebook":
                persona.facebook_username = username
            elif plataforma == "instagram":
                persona.instagram_username = username
            db.commit()
    
    # 2. Guardar mensajes individuales (evitando duplicados si ya existen logica simple)
    # y filtrar mensajes propios
    mensajes_validos = []
    
    for msg in mensajes:
        from_id = msg.get("from", {}).get("id")
        if ignorar_id and from_id == ignorar_id:
            continue
            
        texto = msg.get("message")
        if not texto:
            continue
            
        mensajes_validos.append(msg)
        
        # Extraer fecha del mensaje
        fecha_msg = None
        if msg.get("created_time"):
            try:
                # Quitamos timezone para evitar problemas de comparación
                fecha_msg = dateutil.parser.isoparse(msg.get("created_time")).replace(tzinfo=None)
            except:
                pass
        
        # Guardar en tabla Conversacion
        # Idealmente checkear si existe por ID de mensaje de plataforma
        # Aquí asumimos insert simple por ahora, o podríamos mejorar ConversacionService
        persona_id = persona['id'] if USE_DATAFRAMES else persona.id
        if USE_DATAFRAMES:
            ConversacionService.guardar_conversacion(
                persona_id=persona_id,
                mensaje=texto,
                plataforma=plataforma,
                es_enviado=False,
                conversacion_id=msg.get("id"),
                fecha_mensaje=fecha_msg
            )
        else:
            ConversacionService.guardar_conversacion(
                db,
                persona_id=persona_id,
                mensaje=texto,
                plataforma=plataforma,
                es_enviado=False,
                conversacion_id=msg.get("id"),
                fecha_mensaje=fecha_msg
            )

    if not mensajes_validos:
        return

    # 3. Agrupar en Sesiones por día (UTC-3)
    sesiones = agrupar_mensajes_por_dia_cl(mensajes_validos)
    
    print(f"   📅 Detectados {len(sesiones)} días con actividad")
    
    # 4. Analizar cada Sesión (cada día)
    for i, sesion in enumerate(sesiones):
        # Obtener fecha del primer mensaje ajustada a UTC-3 para el chequeo de día
        msg_time_utc = dateutil.parser.isoparse(sesion[0].get("created_time"))
        t_cl = msg_time_utc - timedelta(hours=3)
        fecha_cl = t_cl.date()
        
        # El start_time que guardaremos será el UTC original (naive) para consistencia
        start_time_utc = msg_time_utc.replace(tzinfo=None)
        
        # Definir el rango del día UTC-3 expresado en UTC para la búsqueda en la BD
        # Dia D en UTC-3 empieza a las D 03:00 UTC y termina a las D+1 02:59 UTC
        dia_inicio_utc = datetime.combine(fecha_cl, datetime.min.time()) + timedelta(hours=3)
        dia_fin_utc = dia_inicio_utc + timedelta(days=1) - timedelta(seconds=1)

        # Verificar si ya existe análisis para este día (UTC-3)
        persona_id = persona['id'] if USE_DATAFRAMES else persona.id
        
        analisis_existente = False
        if USE_DATAFRAMES:
            from backend.database.dataframe_services import DataFrameAnalisisService
            # buscar_analisis ya limpia tzinfo, comparamos en el rango UTC calculado
            analisis_list = DataFrameAnalisisService.buscar_analisis(
                fecha_inicio=dia_inicio_utc,
                fecha_fin=dia_fin_utc
            )
            analisis_existente = any(a['persona_id'] == persona_id for a in analisis_list)
        else:
            from backend.database.models import Analisis
            analisis_existente = db.query(Analisis).filter(
                Analisis.persona_id == persona_id,
                Analisis.start_conversation >= dia_inicio_utc,
                Analisis.start_conversation <= dia_fin_utc
            ).first() is not None

        if analisis_existente:
            print(f"   ⏩ Día {fecha_cl} ya analizado previamente. Saltando...")
            continue

        # Texto concatenado de la sesión
        texto_sesion = "\n".join([m.get("message", "") for m in sesion])
        
        print(f"   🔍 Analizando mensajes del día {fecha_cl} ({len(sesion)} mensajes)...")
        
        # Contexto: usamos la propia sesión como historial
        resultado = procesar_conversacion(
            mensaje=texto_sesion, # Enviamos todo el bloque
            plataforma=plataforma,
            persona_id=persona_id,
            historial=[],
            nombre_usuario=username
        )
        
        datos = resultado.get("datos_extraidos", {})
        resumen = datos.get("resumen_conversacional") or texto_sesion[:100]
        
        # Actualizar datos demográficos de la persona con lo encontrado en esta sesión
        if USE_DATAFRAMES:
            PersonaService.crear_o_actualizar_persona(
                datos=datos,
                facebook_id=facebook_id,
                instagram_id=instagram_id
            )
        else:
            PersonaService.crear_o_actualizar_persona(
                db,
                datos=datos,
                facebook_id=facebook_id,
                instagram_id=instagram_id
            )
        
        # Guardar Fila de Análisis
        if USE_DATAFRAMES:
            AnalisisService.crear_analisis(
                persona_id=persona_id,
                resumen=resumen,
                contenido_completo=texto_sesion,
                categorias=datos.get("intereses"),
                start_conversation=start_time_utc
            )
        else:
            AnalisisService.crear_analisis(
                db,
                persona_id=persona_id,
                resumen=resumen,
                contenido_completo=texto_sesion,
                categorias=datos.get("intereses"),
                start_conversation=start_time_utc
            )
        print(f"   ✅ Día registrado ({fecha_cl}): {resumen[:50]}...")


def sincronizar_facebook(page_id: str, limit: int = 10):
    """Sincronizar conversaciones de Facebook."""
    print(f"\n🔄 Sincronizando conversaciones de Facebook (página: {page_id})")
    conversaciones = meta_client.obtener_conversaciones_facebook(page_id, limit)
    
    if not conversaciones:
        print("⚠️ No se encontraron conversaciones")
        return
        
    with get_db() as db:
        for conv in conversaciones:
            conv_id = conv.get("id")
            mensajes = meta_client.obtener_mensajes_conversacion_facebook(conv_id)
            
            # Agrupar por usuario (aunque una conversación de FB suele ser 1 usuario + pagina)
            # Extraer el "otro" participante
            participants = conv.get("participants", {}).get("data", [])
            user_participant = next((p for p in participants if p["id"] != page_id), None)
            
            if not user_participant:
                continue
                
            user_id = user_participant["id"]
            username = user_participant["name"]
            
            procesar_mensajes_usuario(
                db, user_id, username, "facebook", mensajes, ignorar_id=page_id
            )


def sincronizar_instagram(account_id: str, limit: int = 10):
    """Sincronizar conversaciones de Instagram."""
    print(f"\n🔄 Sincronizando conversaciones de Instagram (cuenta: {account_id})")
    conversaciones = meta_client.obtener_conversaciones_instagram(account_id, limit)
    
    if not conversaciones:
        print("⚠️ No se encontraron conversaciones")
        return

    with get_db() as db:
        for conv in conversaciones:
            conv_id = conv.get("id")
            mensajes = meta_client.obtener_mensajes_conversacion_instagram(conv_id)
            
            if not mensajes:
                print(f"⚠️ No se encontraron mensajes para conversación {conv_id}")
                continue
            
            # Extraer información del usuario desde los participantes o mensajes
            participants = conv.get("participants", {}).get("data", [])
            user_participant = next((p for p in participants if p["id"] != account_id), None)
            
            # Si no hay participantes en la conversación, intentar extraer del primer mensaje
            if not user_participant:
                # Obtener el ID del remitente del primer mensaje que no sea nosotros
                for msg in mensajes:
                    from_id = msg.get("from", {}).get("id")
                    if from_id and from_id != account_id:
                        user_participant = {
                            "id": from_id,
                            "username": from_id  # Usamos el ID como nombre por defecto
                        }
                        break
            
            if not user_participant:
                print(f"⚠️ No se pudo identificar el usuario para conversación {conv_id}")
                continue

            user_id = user_participant["id"]
            username = user_participant.get("username") or user_participant.get("name") or user_id
            
            procesar_mensajes_usuario(
                db, user_id, username, "instagram", mensajes, ignorar_id=account_id
            )


def ejemplo_procesamiento_manual():
    """Ejemplo de procesamiento manual de mensajes para testing."""
    print("\n🧪 Modo de ejemplo - Procesamiento manual")
    
    # Usuarios de ejemplo con sus conversaciones
    usuarios_ejemplo = [
        {
            "user_id": "ejemplo_001",
            "username": "juan.perez",
            "plataforma": "facebook",
            "mensajes": [
                {
                    "id": "msg_001_1",
                    "message": "Hola, me llamo Juan Pérez, tengo 28 años",
                    "created_time": (datetime.now() - timedelta(days=5)).isoformat(),
                    "from": {"id": "ejemplo_001"}
                },
                {
                    "id": "msg_001_2",
                    "message": "Me preocupa mucho la seguridad en mi barrio. Vivo en Providencia y trabajo como ingeniero.",
                    "created_time": (datetime.now() - timedelta(days=5, hours=-1)).isoformat(),
                    "from": {"id": "ejemplo_001"}
                }
            ]
        },
        {
            "user_id": "ejemplo_002",
            "username": "maria.gonzalez",
            "plataforma": "instagram",
            "mensajes": [
                {
                    "id": "msg_002_1",
                    "message": "Buenos días, soy María González, tengo 45 años",
                    "created_time": (datetime.now() - timedelta(days=3)).isoformat(),
                    "from": {"id": "ejemplo_002"}
                },
                {
                    "id": "msg_002_2",
                    "message": "Me interesa la educación de calidad para mis hijos. Soy profesora y vivo en Santiago Centro.",
                    "created_time": (datetime.now() - timedelta(days=3, hours=-2)).isoformat(),
                    "from": {"id": "ejemplo_002"}
                }
            ]
        },
        {
            "user_id": "ejemplo_003",
            "username": "c.rodriguez",
            "plataforma": "facebook",
            "mensajes": [
                {
                    "id": "msg_003_1",
                    "message": "Hola! Mi nombre es Carlos Rodríguez, 32 años, empresario.",
                    "created_time": (datetime.now() - timedelta(days=7)).isoformat(),
                    "from": {"id": "ejemplo_003"}
                },
                {
                    "id": "msg_003_2",
                    "message": "Me preocupan los temas de inversión y desarrollo económico. Contacto: carlos@email.com",
                    "created_time": (datetime.now() - timedelta(days=7, hours=-3)).isoformat(),
                    "from": {"id": "ejemplo_003"}
                },
                {
                    "id": "msg_003_3",
                    "message": "También me interesa el tema de infraestructura y transporte público",
                    "created_time": (datetime.now() - timedelta(days=1)).isoformat(),
                    "from": {"id": "ejemplo_003"}
                }
            ]
        },
        {
            "user_id": "ejemplo_004",
            "username": "ana.silva",
            "plataforma": "instagram",
            "mensajes": [
                {
                    "id": "msg_004_1",
                    "message": "Buenas, soy Ana Silva, 26 años, estudiante de medicina.",
                    "created_time": (datetime.now() - timedelta(days=2)).isoformat(),
                    "from": {"id": "ejemplo_004"}
                },
                {
                    "id": "msg_004_2",
                    "message": "Me interesa todo lo relacionado con salud pública y deporte. Vivo en Las Condes.",
                    "created_time": (datetime.now() - timedelta(days=2, hours=-1)).isoformat(),
                    "from": {"id": "ejemplo_004"}
                }
            ]
        },
        {
            "user_id": "ejemplo_005",
            "username": "pedro.morales",
            "plataforma": "facebook",
            "mensajes": [
                {
                    "id": "msg_005_1",
                    "message": "Qué tal, Pedro Morales acá, 55 años, jubilado.",
                    "created_time": (datetime.now() - timedelta(days=10)).isoformat(),
                    "from": {"id": "ejemplo_005"}
                },
                {
                    "id": "msg_005_2",
                    "message": "Me preocupa la seguridad y la salud. Mi teléfono es +56912345678",
                    "created_time": (datetime.now() - timedelta(days=10, hours=-2)).isoformat(),
                    "from": {"id": "ejemplo_005"}
                }
            ]
        },
        {
            "user_id": "ejemplo_006",
            "username": "laura.martinez",
            "plataforma": "instagram",
            "mensajes": [
                {
                    "id": "msg_006_1",
                    "message": "Hola, soy Laura Martínez, 35 años, arquitecta",
                    "created_time": (datetime.now() - timedelta(days=4)).isoformat(),
                    "from": {"id": "ejemplo_006"}
                },
                {
                    "id": "msg_006_2",
                    "message": "Me interesan los temas de vivienda y desarrollo urbano sustentable. Email: laura.m@ejemplo.com",
                    "created_time": (datetime.now() - timedelta(days=4, hours=-1)).isoformat(),
                    "from": {"id": "ejemplo_006"}
                }
            ]
        }
    ]
    
    with get_db() as db:
        for usuario in usuarios_ejemplo:
            print(f"\n{'='*80}")
            procesar_mensajes_usuario(
                db,
                user_id=usuario["user_id"],
                username=usuario["username"],
                plataforma=usuario["plataforma"],
                mensajes=usuario["mensajes"],
                ignorar_id=None
            )
            print(f"{'='*80}")
    
    print("\n✅ Procesamiento de ejemplos completado")
    print(f"\n📊 Se procesaron {len(usuarios_ejemplo)} usuarios con sus conversaciones")



def main():
    """Función principal."""
    print("=" * 80)
    print("🤖 AGENTE POLÍTICO - Sincronizador de Conversaciones")
    print("=" * 80)
    
    # Inicializar base de datos
    init_db()
    
    # Menú de opciones
    print("\nOpciones:")
    print("1. Sincronizar Facebook")
    print("2. Sincronizar Instagram")
    print("3. Procesar mensajes de ejemplo (sin APIs)")
    print("4. Salir")
    
    opcion = input("\nSelecciona una opción (1-4): ")
    
    if opcion == "1":
        # Intentar obtener ID automáticamente
        page_info = meta_client.obtener_info_pagina()
        default_id = page_info.get("id", "")
        page_name = page_info.get("name", "Desconocido")
        
        prompt = f"Ingresa el Page ID de Facebook"
        if default_id:
            prompt += f" (Enter para usar {page_name} - {default_id})"
        prompt += ": "
        
        page_id = input(prompt) or default_id
        
        if not page_id:
            print("❌ Debes ingresar un Page ID")
            return

        limit = int(input("Número de conversaciones a procesar (default 10): ") or "10")
        sincronizar_facebook(page_id, limit)
    
    elif opcion == "2":
        # Intentar obtener ID automáticamente
        page_info = meta_client.obtener_info_pagina()
        ig_account = page_info.get("instagram_business_account", {})
        default_id = ig_account.get("id", "")
        
        prompt = f"Ingresa el Account ID de Instagram"
        if default_id:
            prompt += f" (Enter para usar ID detectado: {default_id})"
        prompt += ": "
        
        account_id = input(prompt) or default_id
        
        if not account_id:
            print("❌ Debes ingresar un Account ID")
            return
            
        limit = int(input("Número de conversaciones a procesar (default 10): ") or "10")
        sincronizar_instagram(account_id, limit)
    
    elif opcion == "3":
        ejemplo_procesamiento_manual()
    
    elif opcion == "4":
        print("👋 ¡Hasta luego!")
        return
    
    else:
        print("❌ Opción inválida")


if __name__ == "__main__":
    main()
