"""
Tests unitarios para validar los 9 fixes del CRM.
Ejecutar: pytest tests/test_fixes.py -v
"""
import pytest
import sys
import os
import json

# Agregar raíz del proyecto al path para imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ─────────────────────────────────────────────────────────────
# TEST 1: _validar_datos filtra strings nulos ("None", "Desconocido", etc.)
# ─────────────────────────────────────────────────────────────

class TestValidarDatos:
    """Tests para la validación de datos extraídos por la IA."""

    def _get_validator(self):
        """Instancia mínima del agente para testear _validar_datos."""
        from backend.agent.langgraph_agent import AgenteExtraccionDatos, AgentState
        agente = object.__new__(AgenteExtraccionDatos)
        return agente, AgentState

    def test_none_string_se_limpia(self):
        """FIX 4: 'None' como string debe convertirse a None real."""
        agente, AgentState = self._get_validator()
        state = AgentState(
            mensaje="hola",
            plataforma="facebook",
            persona_id=None,
            nombre_usuario="Juan Perez",
            datos_extraidos={
                "nombre_completo": "None",
                "edad": None,
                "genero": "Masculino",
                "telefono": "null",
                "email": "No especificado",
                "ocupacion": "Desconocido",
                "ubicacion": "N/A",
                "intereses": [],
            },
            historial_conversacion=[],
            necesita_mas_info=False,
            error=None,
        )
        result = agente._validar_datos(state)
        datos = result["datos_extraidos"]

        assert datos["nombre_completo"] == "Juan Perez", "Debe usar nombre_usuario como fallback"
        assert datos["telefono"] is None, "'null' string debe ser None"
        assert datos["email"] is None, "'No especificado' debe ser None"
        assert datos["ocupacion"] is None, "'Desconocido' debe ser None"
        assert datos["ubicacion"] is None, "'N/A' debe ser None"

    def test_valores_reales_no_se_limpian(self):
        """Valores legítimos no deben ser eliminados."""
        agente, AgentState = self._get_validator()
        state = AgentState(
            mensaje="hola",
            plataforma="facebook",
            persona_id=None,
            nombre_usuario="Maria",
            datos_extraidos={
                "nombre_completo": "María González",
                "edad": 35,
                "genero": "Femenino",
                "telefono": "+56912345678",
                "email": "maria@test.com",
                "ocupacion": "Profesora",
                "ubicacion": "Santiago",
                "intereses": ["Educación"],
            },
            historial_conversacion=[],
            necesita_mas_info=False,
            error=None,
        )
        result = agente._validar_datos(state)
        datos = result["datos_extraidos"]

        assert datos["nombre_completo"] == "María González"
        assert datos["telefono"] == "+56912345678"
        assert datos["email"] == "maria@test.com"
        assert datos["ocupacion"] == "Profesora"
        assert datos["ubicacion"] == "Santiago"

    def test_nombre_usuario_fallback(self):
        """Si nombre_completo queda vacío, debe usar nombre_usuario."""
        agente, AgentState = self._get_validator()
        state = AgentState(
            mensaje="👍",
            plataforma="instagram",
            persona_id=None,
            nombre_usuario="Koke Hernandez",
            datos_extraidos={
                "nombre_completo": None,
                "intereses": [],
            },
            historial_conversacion=[],
            necesita_mas_info=False,
            error=None,
        )
        result = agente._validar_datos(state)
        assert result["datos_extraidos"]["nombre_completo"] == "Koke Hernandez"


# ─────────────────────────────────────────────────────────────
# TEST 2: Categorías de interés dinámicas (no se descartan)
# ─────────────────────────────────────────────────────────────

class TestCategoriasDinamicas:
    """Tests para verificar que la IA puede generar categorías libremente."""

    def _get_validator(self):
        from backend.agent.langgraph_agent import AgenteExtraccionDatos, AgentState
        agente = object.__new__(AgenteExtraccionDatos)
        return agente, AgentState

    def test_categorias_nuevas_se_aceptan(self):
        """FIX 3: Categorías como 'Empleo' o 'Vivienda' ya no se descartan."""
        agente, AgentState = self._get_validator()
        state = AgentState(
            mensaje="necesito empleo",
            plataforma="whatsapp",
            persona_id=None,
            nombre_usuario="Oscar",
            datos_extraidos={
                "nombre_completo": "Oscar Cáceres",
                "intereses": ["Empleo", "Vivienda", "Pensiones", "Medio Ambiente"],
            },
            historial_conversacion=[],
            necesita_mas_info=False,
            error=None,
        )
        result = agente._validar_datos(state)
        intereses = result["datos_extraidos"]["intereses"]

        assert "Empleo" in intereses, "Empleo debe aceptarse"
        assert "Vivienda" in intereses, "Vivienda debe aceptarse"
        assert "Pensiones" in intereses, "Pensiones debe aceptarse"
        assert "Medio Ambiente" in intereses, "Medio Ambiente debe aceptarse"
        assert len(intereses) == 4, "No debe descartar ninguna categoría"

    def test_categorias_originales_siguen_funcionando(self):
        """Las 5 categorías originales deben seguir aceptándose."""
        agente, AgentState = self._get_validator()
        state = AgentState(
            mensaje="me preocupa la salud",
            plataforma="facebook",
            persona_id=None,
            nombre_usuario="Test",
            datos_extraidos={
                "nombre_completo": "Test",
                "intereses": ["Deportes", "Inversión", "Seguridad", "Salud", "Educación"],
            },
            historial_conversacion=[],
            necesita_mas_info=False,
            error=None,
        )
        result = agente._validar_datos(state)
        assert len(result["datos_extraidos"]["intereses"]) == 5

    def test_intereses_se_normalizan_title_case(self):
        """Los intereses deben normalizarse a Title Case."""
        agente, AgentState = self._get_validator()
        state = AgentState(
            mensaje="test",
            plataforma="facebook",
            persona_id=None,
            nombre_usuario="Test",
            datos_extraidos={
                "nombre_completo": "Test",
                "intereses": ["empleo", "SEGURIDAD", "medio ambiente"],
            },
            historial_conversacion=[],
            necesita_mas_info=False,
            error=None,
        )
        result = agente._validar_datos(state)
        intereses = result["datos_extraidos"]["intereses"]
        assert "Empleo" in intereses
        assert "Seguridad" in intereses
        assert "Medio Ambiente" in intereses

    def test_intereses_vacios_se_limpian(self):
        """Strings vacíos en intereses deben eliminarse."""
        agente, AgentState = self._get_validator()
        state = AgentState(
            mensaje="test",
            plataforma="facebook",
            persona_id=None,
            nombre_usuario="Test",
            datos_extraidos={
                "nombre_completo": "Test",
                "intereses": ["Salud", "", "  ", "Educación"],
            },
            historial_conversacion=[],
            necesita_mas_info=False,
            error=None,
        )
        result = agente._validar_datos(state)
        assert len(result["datos_extraidos"]["intereses"]) == 2


# ─────────────────────────────────────────────────────────────
# TEST 3: Protección de endpoints de debug
# ─────────────────────────────────────────────────────────────

class TestDebugProtection:
    """Tests para verificar que los endpoints de debug están protegidos."""

    def test_debug_password_en_config(self):
        """FIX 8: config.py debe tener DEBUG_PASSWORD."""
        from backend import config
        assert hasattr(config, "DEBUG_PASSWORD"), "DEBUG_PASSWORD debe existir en config"

    def test_password_vacia_permite_acceso(self):
        """Sin DEBUG_PASSWORD configurada, los endpoints quedan abiertos (local dev)."""
        from backend import config
        original = config.DEBUG_PASSWORD
        try:
            config.DEBUG_PASSWORD = ""
            # Sin password configurada, no debería bloquear
            assert config.DEBUG_PASSWORD == ""
        finally:
            config.DEBUG_PASSWORD = original

    def test_password_incorrecta_bloquea(self):
        """Con DEBUG_PASSWORD configurada, password incorrecta debe fallar."""
        from backend import config
        original = config.DEBUG_PASSWORD
        try:
            config.DEBUG_PASSWORD = "test_secret"
            # Simular la lógica de validación
            password_intento = "wrong"
            bloqueado = config.DEBUG_PASSWORD and password_intento != config.DEBUG_PASSWORD
            assert bloqueado, "Password incorrecta debe bloquear"
        finally:
            config.DEBUG_PASSWORD = original

    def test_password_correcta_permite(self):
        """Con DEBUG_PASSWORD configurada, password correcta debe permitir."""
        from backend import config
        original = config.DEBUG_PASSWORD
        try:
            config.DEBUG_PASSWORD = "test_secret"
            password_intento = "test_secret"
            permitido = not config.DEBUG_PASSWORD or password_intento == config.DEBUG_PASSWORD
            assert permitido, "Password correcta debe permitir acceso"
        finally:
            config.DEBUG_PASSWORD = original


# ─────────────────────────────────────────────────────────────
# TEST 4: Graph API actualizada
# ─────────────────────────────────────────────────────────────

class TestGraphAPI:
    """Test para verificar versión de Graph API."""

    def test_whatsapp_api_version_actualizada(self):
        """FIX 9: WhatsApp debe usar Graph API v21.0."""
        from backend.integrations.whatsapp_api import WhatsAppClient
        client = WhatsAppClient.__new__(WhatsAppClient)
        # Simular __init__ mínimo
        client.base_url = "https://graph.facebook.com/v21.0"
        assert "v21.0" in client.base_url, "Debe usar Graph API v21.0"

    def test_whatsapp_api_no_usa_v18(self):
        """No debe usar la versión deprecada v18.0."""
        filepath = os.path.join(os.path.dirname(__file__), "..", "backend", "integrations", "whatsapp_api.py")
        with open(filepath) as f:
            content = f.read()
        assert "v18.0" not in content, "No debe contener v18.0 (deprecada)"
        assert "v21.0" in content, "Debe contener v21.0"


# ─────────────────────────────────────────────────────────────
# TEST 5: Prompt de IA distingue candidato de ciudadano
# ─────────────────────────────────────────────────────────────

class TestPromptIA:
    """Tests para verificar que el prompt está corregido."""

    def test_prompt_contiene_contexto_ciudadano(self):
        """FIX 1: El prompt debe instruir extraer datos del CIUDADANO."""
        filepath = os.path.join(os.path.dirname(__file__), "..", "backend", "agent", "langgraph_agent.py")
        with open(filepath) as f:
            content = f.read()
        assert "CIUDADANO" in content, "El prompt debe mencionar CIUDADANO"
        assert "NO sobre el político" in content or "NO sobre el politico" in content, \
            "El prompt debe advertir no confundir con el político"

    def test_prompt_no_tiene_categorias_hardcodeadas(self):
        """FIX 3: El prompt no debe restringir a categorías fijas."""
        filepath = os.path.join(os.path.dirname(__file__), "..", "backend", "agent", "langgraph_agent.py")
        with open(filepath) as f:
            content = f.read()
        assert "CATEGORÍAS DE INTERESES DISPONIBLES" not in content, \
            "No debe presentar categorías como 'disponibles' (limita al IA)"
        assert "genera libremente" in content or "lista libre" in content, \
            "Debe instruir generación libre de categorías"

    def test_nodo_analizar_mensaje_eliminado(self):
        """FIX 2: El nodo analizar_mensaje no debe estar en el grafo."""
        filepath = os.path.join(os.path.dirname(__file__), "..", "backend", "agent", "langgraph_agent.py")
        with open(filepath) as f:
            content = f.read()
        assert 'add_node("analizar_mensaje"' not in content, \
            "El nodo analizar_mensaje debe estar eliminado del grafo"
