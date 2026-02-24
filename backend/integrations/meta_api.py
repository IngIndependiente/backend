"""Cliente para la API de Meta (Facebook e Instagram)."""
import requests
from typing import List, Dict, Any, Optional
from datetime import datetime
from backend import config


class MetaAPIClient:
    """Cliente para interactuar con las APIs de Meta."""
    
    def __init__(self, facebook_token: Optional[str] = None, instagram_token: Optional[str] = None):
        """
        Inicializar el cliente con las credenciales.
        
        Args:
            facebook_token: Token específico de página (opcional, usa .env si no se provee)
            instagram_token: Token específico de Instagram (opcional, usa facebook_token si no se provee)
        """
        self.facebook_token = facebook_token or config.META_ACCESS_TOKEN
        # Instagram Business API usa el mismo token de la página de Facebook
        self.instagram_token = instagram_token or self.facebook_token
        self.base_url = "https://graph.facebook.com/v18.0"
    
    def enviar_mensaje_con_quick_replies(
        self,
        recipient_id: str,
        text: str,
        quick_replies: list,
        plataforma: str = "facebook"
    ) -> dict:
        """
        Enviar mensaje con botones de respuesta rápida (Quick Replies).
        
        Args:
            recipient_id: ID del destinatario (Facebook PSID o Instagram IGSID)
            text: Texto del mensaje
            quick_replies: Lista de opciones [{"title": "Seguridad", "payload": "SEGURIDAD"}, ...]
            plataforma: "facebook" o "instagram"
            
        Returns:
            Respuesta de la API
        """
        if plataforma == "facebook":
            url = f"{self.base_url}/me/messages"
            token = self.facebook_token
        else:  # instagram
            url = f"{self.base_url}/me/messages"
            token = self.instagram_token
        
        # Convertir quick_replies a formato de Meta
        quick_reply_buttons = [
            {
                "content_type": "text",
                "title": qr["title"][:20],  # Máximo 20 caracteres
                "payload": qr.get("payload", qr["title"].upper())[:1000]
            }
            for qr in quick_replies[:13]  # Máximo 13 quick replies
        ]
        
        payload = {
            "recipient": {"id": recipient_id},
            "messaging_type": "RESPONSE",
            "message": {
                "text": text,
                "quick_replies": quick_reply_buttons
            }
        }
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        try:
            response = requests.post(url, json=payload, headers=headers)
            response.raise_for_status()
            print(f"✅ Quick replies enviadas a {recipient_id}")
            return response.json()
        except Exception as e:
            print(f"❌ Error enviando quick replies: {e}")
            if hasattr(e, 'response') and e.response:
                print(f"   Respuesta: {e.response.text}")
            return {}
    
    def enviar_mensaje_simple(
        self,
        recipient_id: str,
        text: str,
        plataforma: str = "facebook"
    ) -> dict:
        """
        Enviar mensaje de texto simple.
        
        Args:
            recipient_id: ID del destinatario
            text: Texto del mensaje
            plataforma: "facebook" o "instagram"
            
        Returns:
            Respuesta de la API
        """
        if plataforma == "facebook":
            url = f"{self.base_url}/me/messages"
            token = self.facebook_token
        else:  # instagram
            url = f"{self.base_url}/me/messages"
            token = self.instagram_token
        
        payload = {
            "recipient": {"id": recipient_id},
            "messaging_type": "RESPONSE",
            "message": {
                "text": text
            }
        }
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        try:
            response = requests.post(url, json=payload, headers=headers)
            response.raise_for_status()
            print(f"✅ Mensaje enviado a {recipient_id}")
            return response.json()
        except Exception as e:
            print(f"❌ Error enviando mensaje: {e}")
            if hasattr(e, 'response') and e.response:
                print(f"   Respuesta: {e.response.text}")
            return {}

    def obtener_info_pagina(self) -> Dict[str, Any]:
        """
        Obtener información de la página asociada al token.
        
        Returns:
            Datos de la página (id, name, instagram_business_account)
        """
        url = f"{self.base_url}/me"
        params = {
            "access_token": self.facebook_token,
            "fields": "id,name,instagram_business_account"
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Error al obtener info de la página: {e}")
            return {}

    def obtener_conversaciones_facebook(
        self,
        page_id: str,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Obtener conversaciones de una página de Facebook.
        
        Args:
            page_id: ID de la página de Facebook
            limit: Número máximo de conversaciones
            
        Returns:
            Lista de conversaciones
        """
        url = f"{self.base_url}/{page_id}/conversations"
        params = {
            "access_token": self.facebook_token,
            "fields": "participants,messages{message,from,created_time}",
            "limit": limit
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            return data.get("data", [])
        except requests.exceptions.RequestException as e:
            print(f"Error al obtener conversaciones de Facebook: {e}")
            return []
    
    def obtener_mensajes_conversacion_facebook(
        self,
        conversation_id: str,
        limit: int = 25
    ) -> List[Dict[str, Any]]:
        """
        Obtener mensajes de una conversación específica de Facebook.
        
        Args:
            conversation_id: ID de la conversación
            limit: Número máximo de mensajes
            
        Returns:
            Lista de mensajes
        """
        url = f"{self.base_url}/{conversation_id}/messages"
        params = {
            "access_token": self.facebook_token,
            "fields": "message,from,created_time,to",
            "limit": limit
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            return data.get("data", [])
        except requests.exceptions.RequestException as e:
            print(f"Error al obtener mensajes de Facebook: {e}")
            return []
    
    def obtener_conversaciones_instagram(
        self,
        instagram_account_id: str,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Obtener conversaciones de Instagram.
        
        Args:
            instagram_account_id: ID de la cuenta de Instagram Business
            limit: Número máximo de conversaciones
            
        Returns:
            Lista de conversaciones
        """
        # Para Instagram, usar el endpoint de media/conversations
        url = f"{self.base_url}/{instagram_account_id}/conversations"
        params = {
            "access_token": self.instagram_token,
            "fields": "id,updated_time,participants",
            "limit": limit
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            return data.get("data", [])
        except requests.exceptions.RequestException as e:
            error_data = {}
            if hasattr(response, 'text'):
                try:
                    error_data = response.json()
                except:
                    pass
            
            error_code = error_data.get("error", {}).get("code")
            error_message = error_data.get("error", {}).get("message", "")
            
            print(f"❌ Error al obtener conversaciones de Instagram: {e}")
            
            # Detectar errores específicos y dar soluciones
            if error_code == 3:
                print("\n" + "="*80)
                print("⚠️  TU APLICACIÓN NECESITA ACCESO AVANZADO A INSTAGRAM MESSAGING API")
                print("="*80)
                print("\nEl error (#3) indica que tu app no tiene habilitado Instagram Messaging API.")
                print("\n📋 PASOS PARA SOLUCIONAR:")
                print("\n1. Ve a: https://developers.facebook.com/apps/{}/app-review/".format(
                    self.facebook_token.split('|')[0] if '|' in self.facebook_token else 'TU_APP_ID'
                ))
                print("\n2. Solicita 'Standard Access' para estos permisos:")
                print("   - instagram_manage_messages")
                print("   - instagram_basic")
                print("\n3. Completa la Business Verification si aún no lo has hecho")
                print("\n4. En el proceso de revisión, explica:")
                print("   - Cómo usarás la mensajería de Instagram")
                print("   - Graba un video demostrando tu aplicación")
                print("   - Explica el caso de uso político/CRM")
                print("\n5. Espera la aprobación de Meta (puede tomar varios días)")
                print("\n💡 ALTERNATIVA TEMPORAL:")
                print("   Puedes agregar usuarios de prueba en:")
                print("   https://developers.facebook.com/apps/{}/roles/test-users/".format(
                    self.facebook_token.split('|')[0] if '|' in self.facebook_token else 'TU_APP_ID'
                ))
                print("   Los usuarios de prueba pueden usar la app sin aprobación.")
                print("="*80 + "\n")
            elif error_code == 190:
                print(f"❌ Token inválido o expirado. Genera un nuevo token.")
            else:
                print(f"Respuesta completa: {response.text}")
            
            return []
    
    def obtener_mensajes_conversacion_instagram(
        self,
        conversation_id: str,
        limit: int = 25
    ) -> List[Dict[str, Any]]:
        """
        Obtener mensajes de una conversación específica de Instagram.
        
        Args:
            conversation_id: ID de la conversación
            limit: Número máximo de mensajes
            
        Returns:
            Lista de mensajes
        """
        url = f"{self.base_url}/{conversation_id}"
        params = {
            "access_token": self.instagram_token,
            "fields": f"messages{{id,message,from,created_time}}.limit({limit})",
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            messages = data.get("messages", {}).get("data", [])
            return messages
        except requests.exceptions.RequestException as e:
            print(f"Error al obtener mensajes de Instagram: {e}")
            if hasattr(response, 'text'):
                print(f"Respuesta: {response.text}")
            return []
    
    def enviar_mensaje_facebook(
        self,
        recipient_id: str,
        message: str
    ) -> bool:
        """
        Enviar un mensaje a través de Facebook Messenger.
        
        Args:
            recipient_id: ID del destinatario
            message: Texto del mensaje
            
        Returns:
            True si se envió correctamente
        """
        url = f"{self.base_url}/me/messages"
        payload = {
            "recipient": {"id": recipient_id},
            "message": {"text": message},
            "messaging_type": "RESPONSE"
        }
        params = {"access_token": self.facebook_token}
        
        try:
            response = requests.post(url, json=payload, params=params)
            response.raise_for_status()
            return True
        except requests.exceptions.RequestException as e:
            print(f"Error al enviar mensaje de Facebook: {e}")
            return False
    
    def enviar_mensaje_instagram(
        self,
        recipient_id: str,
        message: str
    ) -> bool:
        """
        Enviar un mensaje a través de Instagram Direct.
        
        Args:
            recipient_id: ID del destinatario
            message: Texto del mensaje
            
        Returns:
            True si se envió correctamente
        """
        url = f"{self.base_url}/me/messages"
        payload = {
            "recipient": {"id": recipient_id},
            "message": {"text": message}
        }
        params = {"access_token": self.instagram_token}
        
        try:
            response = requests.post(url, json=payload, params=params)
            response.raise_for_status()
            return True
        except requests.exceptions.RequestException as e:
            print(f"Error al enviar mensaje de Instagram: {e}")
            return False


# Instancia global del cliente (usa token de .env)
meta_client = MetaAPIClient()


def crear_cliente_candidato(facebook_page_access_token: str) -> MetaAPIClient:
    """
    Crear un cliente de Meta API con el token de un candidato específico.
    
    Args:
        facebook_page_access_token: Token de la página del candidato
        
    Returns:
        Instancia de MetaAPIClient configurada con el token del candidato
    """
    return MetaAPIClient(facebook_token=facebook_page_access_token)
