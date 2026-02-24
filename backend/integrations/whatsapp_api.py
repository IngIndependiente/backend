"""Cliente para WhatsApp Business API."""
import requests
from typing import Dict, Any, List, Optional
from backend import config


class WhatsAppClient:
    """Cliente para interactuar con WhatsApp Business API."""
    
    def __init__(self, phone_number_id: Optional[str] = None, access_token: Optional[str] = None, business_account_id: Optional[str] = None):
        """
        Inicializar el cliente con las credenciales.
        
        Args:
            phone_number_id: ID del número de WhatsApp (opcional, usa .env si no se provee)
            access_token: Token de acceso (opcional, usa .env si no se provee)
            business_account_id: ID de la cuenta de negocio (opcional)
        """
        self.phone_number_id = phone_number_id or config.WHATSAPP_PHONE_NUMBER_ID
        self.business_account_id = business_account_id or config.WHATSAPP_BUSINESS_ACCOUNT_ID
        self.access_token = access_token or config.META_ACCESS_TOKEN
        self.base_url = "https://graph.facebook.com/v18.0"
    
    def enviar_mensaje(self, recipient_phone: str, message: str) -> bool:
        """
        Enviar mensaje de texto a un número de WhatsApp.
        
        Args:
            recipient_phone: Número de teléfono del destinatario (formato internacional sin +)
            message: Texto del mensaje a enviar
            
        Returns:
            True si el mensaje se envió correctamente, False en caso contrario
        """
        url = f"{self.base_url}/{self.phone_number_id}/messages"
        
        payload = {
            "messaging_product": "whatsapp",
            "recipient_type": "individual",
            "to": recipient_phone,
            "type": "text",
            "text": {"body": message}
        }
        
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        
        try:
            response = requests.post(url, json=payload, headers=headers)
            response.raise_for_status()
            result = response.json()
            print(f"✅ Mensaje WhatsApp enviado a {recipient_phone}: {result}")
            return True
        except Exception as e:
            print(f"❌ Error enviando mensaje WhatsApp: {e}")
            if hasattr(e, 'response'):
                print(f"   Respuesta: {e.response.text}")
            return False
    
    def obtener_conversaciones(
        self,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Obtener conversaciones de WhatsApp Business.
        
        Args:
            limit: Número máximo de conversaciones a obtener
            
        Returns:
            Lista de conversaciones con sus mensajes
        """
        # Nota: WhatsApp Business API no tiene endpoint para obtener historial
        # Solo funciona mediante webhooks en tiempo real
        # Este método existe para mantener consistencia con la API
        print("⚠️ WhatsApp Business API solo funciona mediante webhooks en tiempo real")
        return []
    
    def procesar_webhook_whatsapp(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Procesar notificación de webhook de WhatsApp.
        
        Args:
            data: Datos del webhook de WhatsApp
            
        Returns:
            Diccionario con datos del mensaje procesado o None si no es válido
        """
        try:
            # Verificar que sea un mensaje válido
            if "entry" not in data:
                return None
                
            entry = data["entry"][0]
            changes = entry.get("changes", [])
            
            if not changes:
                return None
                
            change = changes[0]
            value = change.get("value", {})
            
            # Procesar mensaje entrante
            if "messages" in value:
                message = value["messages"][0]
                
                # Información del remitente
                phone = message.get("from", "")
                message_id = message.get("id", "")
                timestamp = message.get("timestamp", "")
                
                # Contenido del mensaje
                message_type = message.get("type", "")
                text = ""
                
                if message_type == "text":
                    text = message.get("text", {}).get("body", "")
                elif message_type == "button":
                    text = message.get("button", {}).get("text", "")
                elif message_type == "interactive":
                    interactive = message.get("interactive", {})
                    if interactive.get("type") == "button_reply":
                        text = interactive.get("button_reply", {}).get("title", "")
                    elif interactive.get("type") == "list_reply":
                        text = interactive.get("list_reply", {}).get("title", "")
                
                # Información del perfil (nombre del contacto)
                contacts = value.get("contacts", [])
                username = None
                if contacts:
                    profile = contacts[0].get("profile", {})
                    username = profile.get("name", "")
                
                # Metadata del negocio
                metadata = value.get("metadata", {})
                display_phone = metadata.get("display_phone_number", "")
                
                return {
                    "phone": phone,
                    "message": text,
                    "message_id": message_id,
                    "timestamp": timestamp,
                    "username": username,
                    "display_phone": display_phone,
                    "message_type": message_type
                }
            
            # Procesar cambios de estado (entregado, leído, etc.)
            if "statuses" in value:
                status = value["statuses"][0]
                return {
                    "type": "status",
                    "status": status.get("status", ""),
                    "message_id": status.get("id", ""),
                    "recipient_id": status.get("recipient_id", ""),
                    "timestamp": status.get("timestamp", "")
                }
                
        except Exception as e:
            print(f"❌ Error procesando webhook de WhatsApp: {e}")
            import traceback
            traceback.print_exc()
        
        return None
    
    def enviar_mensaje_con_botones(
        self,
        recipient_phone: str,
        text: str,
        buttons: List[Dict[str, str]]
    ) -> bool:
        """
        Enviar mensaje con botones interactivos en WhatsApp.
        
        Args:
            recipient_phone: Número de teléfono del destinatario
            text: Texto del mensaje
            buttons: Lista de botones, cada uno con 'id' y 'title'
                    WhatsApp permite máximo 3 botones
                    
        Returns:
            True si el mensaje se envió correctamente
        """
        # WhatsApp solo soporta hasta 3 botones
        if len(buttons) > 3:
            print(f"⚠️ WhatsApp solo permite 3 botones, truncando de {len(buttons)} a 3")
            buttons = buttons[:3]
        
        # Truncar títulos de botones a 20 caracteres
        formatted_buttons = []
        for btn in buttons:
            title = btn['title'][:20]  # WhatsApp límite de 20 chars
            formatted_buttons.append({
                "type": "reply",
                "reply": {
                    "id": btn['id'],
                    "title": title
                }
            })
        
        url = f"{self.base_url}/{self.phone_number_id}/messages"
        
        payload = {
            "messaging_product": "whatsapp",
            "recipient_type": "individual",
            "to": recipient_phone,
            "type": "interactive",
            "interactive": {
                "type": "button",
                "body": {
                    "text": text
                },
                "action": {
                    "buttons": formatted_buttons
                }
            }
        }
        
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        
        try:
            response = requests.post(url, json=payload, headers=headers)
            response.raise_for_status()
            result = response.json()
            print(f"✅ Mensaje interactivo WhatsApp enviado a {recipient_phone}")
            return True
        except Exception as e:
            print(f"❌ Error enviando mensaje interactivo WhatsApp: {e}")
            if hasattr(e, 'response'):
                print(f"   Respuesta: {e.response.text}")
            return False
    
    def marcar_como_leido(self, message_id: str) -> bool:
        """
        Marcar un mensaje como leído.
        
        Args:
            message_id: ID del mensaje a marcar como leído
            
        Returns:
            True si se marcó correctamente, False en caso contrario
        """
        url = f"{self.base_url}/{self.phone_number_id}/messages"
        
        payload = {
            "messaging_product": "whatsapp",
            "status": "read",
            "message_id": message_id
        }
        
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        
        try:
            response = requests.post(url, json=payload, headers=headers)
            response.raise_for_status()
            return True
        except Exception as e:
            print(f"❌ Error marcando mensaje como leído: {e}")
            return False


# Instancia global del cliente (usa configuración de .env)
whatsapp_client = WhatsAppClient()


def crear_cliente_whatsapp_candidato(
    phone_number_id: str,
    access_token: str,
    business_account_id: Optional[str] = None
) -> WhatsAppClient:
    """
    Crear un cliente de WhatsApp con configuración específica de un candidato.
    
    Args:
        phone_number_id: ID del número de WhatsApp del candidato
        access_token: Token de acceso del candidato
        business_account_id: ID de la cuenta de negocio (opcional)
        
    Returns:
        Instancia de WhatsAppClient configurada con datos del candidato
    """
    return WhatsAppClient(
        phone_number_id=phone_number_id,
        access_token=access_token,
        business_account_id=business_account_id
    )
