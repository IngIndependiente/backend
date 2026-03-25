"""Agente LangGraph para análisis de conversaciones políticas."""
from typing import TypedDict, Annotated, List, Dict, Any, Optional
from langgraph.graph import StateGraph, END
from langchain_core.messages import HumanMessage, SystemMessage
import json
from datetime import datetime
import os
from backend import config


class AgentState(TypedDict):
    """Estado del agente."""
    mensaje: str
    plataforma: str
    persona_id: Optional[int]
    nombre_usuario: Optional[str]
    datos_extraidos: Dict[str, Any]
    historial_conversacion: List[str]
    necesita_mas_info: bool
    error: Optional[str]

# ... (inside AgenteExtraccionDatos)

class AgenteExtraccionDatos:
    """
    Agente LangGraph que analiza conversaciones y extrae información estructurada.
    
    El agente identifica:
    - Información personal (nombre, edad, género)
    - Intereses (categorizados)
    - Datos de contacto
    - Ocupación y ubicación
    """
    
    def __init__(self):
        """Inicializar el agente con el modelo de lenguaje."""
        self.llm = self._create_llm()
        self.available = self.llm is not None
        
        # Construir el grafo solo si hay LLM disponible
        if self.available:
            self.graph = self._build_graph()
        else:
            self.graph = None
    
    def _create_llm(self):
        """Crear el modelo de lenguaje según la configuración disponible."""
        # Opción 1: Vertex AI con GCP Project (usa ADC - gcloud auth application-default login)
        if config.GCP_PROJECT_ID:
            # Usamos ChatVertexAI para autenticación via GCP/ADC
            # Nota: Hay un warning de deprecación pero la funcionalidad es correcta
            import warnings
            warnings.filterwarnings("ignore", category=DeprecationWarning)
            from langchain_google_vertexai import ChatVertexAI
            
            print("[Vertex AI] Usando Vertex AI con ADC (Google CLI)")
            print(f"   Proyecto: {config.GCP_PROJECT_ID}")
            print(f"   Ubicacion: {config.GCP_LOCATION}")
            print(f"   Modelo: {config.GEMINI_MODEL}")
            
            # Si hay credenciales de service account, configurarlas
            if config.GOOGLE_APPLICATION_CREDENTIALS:
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config.GOOGLE_APPLICATION_CREDENTIALS
                print("   Credenciales: Service Account")
            else:
                print("   Credenciales: Application Default Credentials (gcloud auth)")
            
            return ChatVertexAI(
                model=config.GEMINI_MODEL,
                project=config.GCP_PROJECT_ID,
                location=config.GCP_LOCATION,
                temperature=0.3,
            )
        
        # Opción 2: Google AI Studio con API Key (Free Tier)
        elif config.GOOGLE_API_KEY:
            from langchain_google_genai import ChatGoogleGenerativeAI
            
            print("[API Key] Usando Google AI Studio (Free Tier)")
            print(f"   Modelo: {config.GEMINI_MODEL}")
            
            return ChatGoogleGenerativeAI(
                model=config.GEMINI_MODEL,
                temperature=0.3,
                google_api_key=config.GOOGLE_API_KEY
            )
        
        else:
            print("[ADVERTENCIA] No se encontraron credenciales de Google.")
            print("El agente funcionará en modo degradado sin capacidades de IA.")
            print("Para habilitar el agente:")
            print("  - Opción 1: Configura GOOGLE_API_KEY en tu archivo .env")
            print("  - Opción 2: Configura GCP_PROJECT_ID para usar Vertex AI")
            return None
    
    def _build_graph(self) -> StateGraph:
        """Construir el grafo de estado del agente."""
        workflow = StateGraph(AgentState)
        
        # Añadir nodos (FIX: eliminado nodo analizar_mensaje que hacía una llamada LLM innecesaria)
        workflow.add_node("extraer_datos", self._extraer_datos)
        workflow.add_node("validar_datos", self._validar_datos)
        
        # Definir el flujo
        workflow.set_entry_point("extraer_datos")
        workflow.add_edge("extraer_datos", "validar_datos")
        workflow.add_edge("validar_datos", END)
        
        return workflow.compile()
    
    def _extraer_datos(self, state: AgentState) -> AgentState:
        """Extraer datos estructurados del mensaje."""
        mensaje = state["mensaje"]
        historial = state.get("historial_conversacion", [])
        nombre_usuario = state.get("nombre_usuario", "Desconocido")
        
        # Contexto del historial
        contexto_historial = "\n".join(historial[-5:]) if historial else mensaje
        
        system_prompt = f"""Eres un experto en extracción de información de conversaciones políticas.

CONTEXTO IMPORTANTE:
Estás analizando mensajes enviados POR UN CIUDADANO a la página de un político/candidato.
- El nombre de usuario (perfil de red social) del CIUDADANO que escribe es: "{nombre_usuario}"
- Todo lo que extraigas debe ser sobre EL CIUDADANO que escribe, NO sobre el político mencionado.
- Si el ciudadano menciona o felicita a un político, NO uses el nombre del político como nombre_completo.
- La ocupación debe ser la del CIUDADANO, no la del político mencionado.

GÉNEROS DISPONIBLES:
{', '.join(config.GENEROS)}

INSTRUCCIONES:
1. Extrae información del CIUDADANO que escribe, NO del político mencionado.
2. Para nombre_completo: usa "{nombre_usuario}" como base. Solo reemplázalo si el ciudadano dice explícitamente su propio nombre.
3. Si el GÉNERO no se menciona, INFIÉRELO del nombre de usuario: "{nombre_usuario}".
4. Para intereses: genera libremente las categorías que mejor describan las preocupaciones del ciudadano.
   Ejemplos: "Seguridad", "Educación", "Salud", "Empleo", "Vivienda", "Pensiones", "Medio Ambiente", "Transporte", "Economía", "Derechos Humanos", etc.
   NO te limites a categorías predefinidas. Usa la que mejor represente la preocupación real.
5. Si el mensaje es solo un emoji, saludo breve o felicitación sin contenido sustantivo, deja intereses como lista vacía.
6. Para campos sin información, usa null (no uses strings como "None", "No especificado" o "Desconocido").

Devuelve SOLO un JSON con esta estructura:
{{
    "nombre_completo": "{nombre_usuario}" o nombre real si el ciudadano lo menciona,
    "edad": número o null,
    "genero": "uno de: Masculino, Femenino, Otro, No especificado",
    "telefono": "número o null",
    "email": "email o null",
    "ocupacion": "ocupación del CIUDADANO o null",
    "ubicacion": "ubicación del CIUDADANO o null",
    "intereses": ["lista libre de temas políticos que preocupan al ciudadano"],
    "resumen_conversacional": "resumen breve (max 100 chars) de la conversación",
    "otros_datos": {{"clave": "valor adicional relevante"}},
    "confianza": "alta/media/baja"
}}"""

        try:
            response = self.llm.invoke([
                SystemMessage(content=system_prompt),
                HumanMessage(content=f"Conversación:\n{contexto_historial}")
            ])
            
            # Parsear la respuesta JSON
            response_text = response.content
            
            # Extraer JSON de la respuesta
            if "```json" in response_text:
                response_text = response_text.split("```json")[1].split("```")[0].strip()
            elif "```" in response_text:
                response_text = response_text.split("```")[1].split("```")[0].strip()
            
            datos = json.loads(response_text)
            state["datos_extraidos"] = datos
            
        except json.JSONDecodeError as e:
            state["error"] = f"Error al parsear JSON: {str(e)}"
            state["datos_extraidos"] = {}
        except Exception as e:
            state["error"] = f"Error al extraer datos: {str(e)}"
            state["datos_extraidos"] = {}
        
        return state
    
    def _validar_datos(self, state: AgentState) -> AgentState:
        """Validar y limpiar los datos extraídos."""
        datos = state.get("datos_extraidos", {})
        
        # Valores que la IA a veces devuelve en vez de null
        _VALORES_NULOS = {"none", "null", "no especificado", "desconocido", "n/a", "no disponible", ""}
        
        def _limpiar_campo(valor):
            """Retorna None si el valor es un string nulo disfrazado."""
            if valor is None:
                return None
            if isinstance(valor, str) and valor.strip().lower() in _VALORES_NULOS:
                return None
            return valor
        
        # Limpiar campos de texto que la IA puede devolver como "None" string
        for campo in ["nombre_completo", "telefono", "email", "ocupacion", "ubicacion"]:
            if campo in datos:
                datos[campo] = _limpiar_campo(datos[campo])
        
        # Validar intereses: aceptar cualquier categoría (generación dinámica por IA)
        if "intereses" in datos and datos["intereses"]:
            # Solo limpiar: quitar vacíos y normalizar capitalización
            intereses_limpios = []
            for i in datos["intereses"]:
                if isinstance(i, str) and i.strip():
                    intereses_limpios.append(i.strip().title())
            datos["intereses"] = intereses_limpios
        
        # Validar género
        if "genero" in datos and datos["genero"]:
            if datos["genero"] not in config.GENEROS:
                datos["genero"] = "No especificado"
        
        # Validar edad
        if "edad" in datos and datos["edad"]:
            try:
                edad = int(datos["edad"])
                if edad < 0 or edad > 120:
                    datos["edad"] = None
            except (ValueError, TypeError):
                datos["edad"] = None
        
        # Usar nombre_usuario como fallback si nombre_completo quedó vacío
        nombre_usuario = state.get("nombre_usuario")
        if not datos.get("nombre_completo") and nombre_usuario:
            datos["nombre_completo"] = nombre_usuario
        
        # Determinar si necesita más información
        campos_importantes = ["nombre_completo", "intereses", "edad", "genero"]
        campos_presentes = sum(1 for campo in campos_importantes if datos.get(campo))
        
        state["necesita_mas_info"] = campos_presentes < 2
        state["datos_extraidos"] = datos
        
        return state
    
    def procesar_mensaje(
        self, 
        mensaje: str, 
        plataforma: str = "desconocida",
        persona_id: int = None,
        historial: List[str] = None,
        nombre_usuario: str = None
    ) -> Dict[str, Any]:
        """
        Procesar un mensaje y extraer información estructurada.
        
        Args:
            mensaje: Texto del mensaje a analizar
            plataforma: Plataforma de origen (facebook/instagram)
            persona_id: ID de la persona si ya existe
            historial: Historial de mensajes previos
            nombre_usuario: Nombre de usuario para inferencia
            
        Returns:
            Diccionario con los datos extraídos y metadatos
        """
        initial_state = AgentState(
            mensaje=mensaje,
            plataforma=plataforma,
            persona_id=persona_id,
            nombre_usuario=nombre_usuario,
            datos_extraidos={},
            historial_conversacion=historial or [],
            necesita_mas_info=False,
            error=None
        )
        
        # Ejecutar el grafo
        result = self.graph.invoke(initial_state)
        
        return {
            "datos_extraidos": result.get("datos_extraidos", {}),
            "necesita_mas_info": result.get("necesita_mas_info", False),
            "error": result.get("error"),
            "fecha_procesamiento": datetime.utcnow().isoformat()
        }


# Instancia global del agente (lazy initialization)
_agente_instance = None

def get_agente():
    """Obtener instancia del agente (lazy initialization)."""
    global _agente_instance
    if _agente_instance is None:
        try:
            _agente_instance = AgenteExtraccionDatos()
        except Exception as e:
            print(f"[ADVERTENCIA] No se pudo inicializar el agente: {e}")
            _agente_instance = None
    return _agente_instance

# Para compatibilidad con código existente
agente = None


def procesar_conversacion(
    mensaje: str,
    plataforma: str = "desconocida",
    persona_id: int = None,
    historial: List[str] = None,
    nombre_usuario: str = None
) -> Dict[str, Any]:
    """
    Función auxiliar para procesar una conversación.
    
    Args:
        mensaje: Texto del mensaje
        plataforma: Plataforma de origen
        persona_id: ID de la persona
        historial: Historial de conversación
        nombre_usuario: Nombre de usuario
        
    Returns:
        Datos extraídos
    """
    agente_instance = get_agente()
    if agente_instance is None:
        return {
            "datos_extraidos": {},
            "necesita_mas_info": False,
            "error": "Agente no disponible - credenciales de Google no configuradas",
            "fecha_procesamiento": datetime.utcnow().isoformat()
        }
    return agente_instance.procesar_mensaje(mensaje, plataforma, persona_id, historial, nombre_usuario)
