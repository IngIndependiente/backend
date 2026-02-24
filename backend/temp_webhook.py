
# === Webhook para Meta (Facebook/Instagram) ===

@app.get("/webhook")
async def verify_webhook(request: Request):
    """
    Endpoint de verificación para el Webhook de Meta.
    Valida el token y devuelve el challenge.
    """
    params = request.query_params
    mode = params.get("hub.mode")
    token = params.get("hub.verify_token")
    challenge = params.get("hub.challenge")

    if mode and token:
        if mode == "subscribe" and token == config.META_VERIFY_TOKEN:
            print("WEBHOOK_VERIFIED")
            return PlainTextResponse(content=challenge, status_code=200)
        else:
            raise HTTPException(status_code=403, detail="Verification failed")
    
    return {"status": "ok"}


@app.get("/webhook/whatsapp")
async def verify_whatsapp_webhook(request: Request):
    """
    Endpoint de verificación para el Webhook de WhatsApp.
    Valida el token y devuelve el challenge.
    """
    params = request.query_params
    mode = params.get("hub.mode")
    token = params.get("hub.verify_token")
    challenge = params.get("hub.challenge")

    if mode and token:
        if mode == "subscribe" and token == config.WHATSAPP_VERIFY_TOKEN:
            print("WHATSAPP_WEBHOOK_VERIFIED")
            return PlainTextResponse(content=challenge, status_code=200)
        else:
            raise HTTPException(status_code=403, detail="WhatsApp verification failed")
    
    return {"status": "ok"}


@app.post("/webhook")
async def webhook_handler(request: Request, background_tasks: BackgroundTasks, db: Session = Depends(get_db_session)):
    """
    Manejar eventos de mensajes entrantes.
    """
    try:
        body = await request.json()
        
        # Verificar si es un evento de página (Facebook)
        if body.get("object") == "page":
            for entry in body.get("entry", []):
                # Iterar sobre eventos de mensajería
                messaging_events = entry.get("messaging", [])
                for event in messaging_events:
                    sender_id = event.get("sender", {}).get("id")
                    message = event.get("message", {})
                    
                    if message and message.get("text"):
                        texto = message.get("text")
                        
                        # Procesar en background para responder rápido a Meta (200 OK)
                        background_tasks.add_task(
                            procesar_mensaje_webhook, 
                            sender_id, 
                            texto, 
                            "facebook",
                            db
                        )
                        
            return PlainTextResponse(content="EVENT_RECEIVED", status_code=200)
            
        # Verificar si es un evento de Instagram
        elif body.get("object") == "instagram":
             for entry in body.get("entry", []):
                messaging_events = entry.get("messaging", [])
                for event in messaging_events:
                    sender_id = event.get("sender", {}).get("id")
                    message = event.get("message", {})
                    
                    if message and message.get("text"):
                        texto = message.get("text")
                        
                        background_tasks.add_task(
                            procesar_mensaje_webhook, 
                            sender_id, 
                            texto, 
                            "instagram",
                            db
                        )
                        
             return PlainTextResponse(content="EVENT_RECEIVED", status_code=200)

        else:
            raise HTTPException(status_code=404, detail="Event not supported")
            
    except Exception as e:
        print(f"Error en webhook: {e}")
        # Siempre devolver 200 a Meta para evitar reintentos infinitos si falla nuestro código
        return PlainTextResponse(content="EVENT_RECEIVED", status_code=200)


@app.post("/webhook/whatsapp")
async def whatsapp_webhook_handler(request: Request, background_tasks: BackgroundTasks):
    """
    Manejar eventos de mensajes entrantes de WhatsApp.
    """
    try:
        body = await request.json()
        
        # Procesar con el cliente de WhatsApp
        data = whatsapp_client.procesar_webhook_whatsapp(body)
        
        if data and data.get("message"):
            # Es un mensaje entrante
            phone = data.get("phone")
            message = data.get("message")
            message_id = data.get("message_id")
            username = data.get("username")
            
            # Procesar en background
            background_tasks.add_task(
                procesar_mensaje_whatsapp,
                phone,
                message,
                username,
                message_id
            )
            
            return PlainTextResponse(content="EVENT_RECEIVED", status_code=200)
        
        # Si es un cambio de estado (leído, entregado, etc), simplemente aceptar
        return PlainTextResponse(content="EVENT_RECEIVED", status_code=200)
            
    except Exception as e:
        print(f"Error en webhook WhatsApp: {e}")
        import traceback
        traceback.print_exc()
        # Siempre devolver 200 para evitar reintentos
        return PlainTextResponse(content="EVENT_RECEIVED", status_code=200)

def procesar_mensaje_webhook(sender_id: str, texto: str, plataforma: str, db: Session):
    """
    Procesar mensaje del webhook (función auxiliar).
    Se ejecuta en background task.
    """
    # IMPORTANTE: Requerimos crear una nueva sesión de DB si estamos en un hilo aparte
    # pero BackgroundTasks en FastAPI se ejecuta en el mismo loop antes de cerrar la response?
    # No, se ejecuta after response. La sesión 'db' (inyectada por Depends) podría cerrarse.
    # Lo más seguro es crear una nueva sesión aquí si falla la inyectada, pero FastAPI
    # suele manejar esto si se pasa la sesion. Probemos con la inyectada pero con cautela.
    # CORRECCIÓN: BackgroundTasks se ejecutan DESPUÉS de enviar la respuesta.
    # La sesión 'db' creada con Depends(get_db_session) es un generador que se cierra ('yield').
    # Por lo tanto, no podemos usar 'db' aquí de forma segura si la respuesta ya se envió y se cerró.
    # Debemos crear una nueva sesión.
    
    from backend.database import SessionLocal
    local_db = SessionLocal()
    
    try:
        # 1. Buscar o crear persona
        persona = None
        if plataforma == "facebook":
            persona = local_db.query(Persona).filter(Persona.facebook_id == sender_id).first()
        elif plataforma == "instagram":
            persona = local_db.query(Persona).filter(Persona.instagram_id == sender_id).first()
            
        persona_id = persona.id if persona else None
        
        # 2. Obtener historial
        historial = []
        if persona_id:
            conversaciones = ConversacionService.obtener_historial(local_db, persona_id, limit=10)
            historial = [c.mensaje for c in conversaciones]
            
        # 3. Procesar con Agente
        # Podemos usar la función importada 'procesar_conversacion'
        resultado = procesar_conversacion(
            mensaje=texto,
            plataforma=plataforma,
            persona_id=persona_id,
            historial=historial
        )
        
        # 4. Guardar resultados
        if resultado.get("datos_extraidos"):
            # Update Persona
            persona = PersonaService.crear_o_actualizar_persona(
                local_db,
                datos=resultado["datos_extraidos"],
                facebook_id=sender_id if plataforma == "facebook" else None,
                instagram_id=sender_id if plataforma == "instagram" else None
            )
            
            # Guardar Conversación (Entrante)
            ConversacionService.guardar_conversacion(
                local_db,
                persona_id=persona.id,
                mensaje=texto,
                plataforma=plataforma,
                es_enviado=False,
                datos_extraidos=resultado["datos_extraidos"]
            )
            
            print(f"Mensaje procesado para {plataforma} ID {sender_id}")
            
            # (Opcional) Aquí podríamos generar y enviar una respuesta automática
            # meta_client.enviar_mensaje_facebook(sender_id, "Gracias por tu mensaje...")
            
    except Exception as e:
        print(f"Error procesando mensaje webhook: {e}")
    finally:
        local_db.close()


def procesar_mensaje_whatsapp(phone: str, texto: str, username: str, message_id: str):
    """
    Procesar mensaje de WhatsApp (función auxiliar).
    Se ejecuta en background task.
    """
    try:
        # Buscar o crear persona por teléfono
        if USE_DATAFRAMES:
            persona = PersonaService.obtener_por_telefono(phone)
            
            if not persona:
                # Crear nueva persona
                datos = {"telefono": phone}
                if username:
                    datos["nombre_completo"] = username
                persona = PersonaService.crear_o_actualizar_persona(
                    datos=datos,
                    telefono=phone
                )
            
            persona_id = persona['id']
            
            # Obtener historial
            historial = ConversacionService.obtener_historial_por_persona(persona_id, limit=10)
            historial_mensajes = [c['mensaje'] for c in historial]
            
        else:
            # Modo SQLAlchemy
            from backend.database import SessionLocal
            local_db = SessionLocal()
            
            try:
                persona = local_db.query(Persona).filter(Persona.telefono == phone).first()
                
                if not persona:
                    # Crear nueva persona
                    datos = {"telefono": phone}
                    if username:
                        datos["nombre_completo"] = username
                    persona = PersonaService.crear_o_actualizar_persona(
                        local_db,
                        datos=datos,
                        telefono=phone
                    )
                
                persona_id = persona.id
                
                # Obtener historial
                conversaciones = ConversacionService.obtener_historial(local_db, persona_id, limit=10)
                historial_mensajes = [c.mensaje for c in conversaciones]
                
            finally:
                local_db.close()
        
        # Procesar con Agente
        resultado = procesar_conversacion(
            mensaje=texto,
            plataforma="whatsapp",
            persona_id=persona_id,
            historial=historial_mensajes
        )
        
        # Guardar resultados
        if resultado.get("datos_extraidos"):
            if USE_DATAFRAMES:
                # Actualizar persona
                PersonaService.crear_o_actualizar_persona(
                    datos=resultado["datos_extraidos"],
                    telefono=phone
                )
                
                # Guardar conversación
                ConversacionService.guardar_conversacion(
                    persona_id=persona_id,
                    mensaje=texto,
                    plataforma="whatsapp",
                    es_enviado=False,
                    datos_extraidos=resultado["datos_extraidos"]
                )
            else:
                from backend.database import SessionLocal
                local_db = SessionLocal()
                
                try:
                    # Actualizar persona
                    PersonaService.crear_o_actualizar_persona(
                        local_db,
                        datos=resultado["datos_extraidos"],
                        telefono=phone
                    )
                    
                    # Buscar persona actualizada
                    persona = local_db.query(Persona).filter(Persona.telefono == phone).first()
                    
                    # Guardar conversación
                    ConversacionService.guardar_conversacion(
                        local_db,
                        persona_id=persona.id,
                        mensaje=texto,
                        plataforma="whatsapp",
                        es_enviado=False,
                        datos_extraidos=resultado["datos_extraidos"]
                    )
                finally:
                    local_db.close()
            
            print(f"✅ Mensaje WhatsApp procesado para {phone}")
            
            # Marcar mensaje como leído
            whatsapp_client.marcar_como_leido(message_id)
            
            # (Opcional) Aquí podríamos enviar una respuesta automática
            # whatsapp_client.enviar_mensaje(phone, "Gracias por tu mensaje...")
            
    except Exception as e:
        print(f"❌ Error procesando mensaje WhatsApp: {e}")
        import traceback
        traceback.print_exc()
