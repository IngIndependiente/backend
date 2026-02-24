# control.py
import threading
import time
import traceback
import uvicorn
import socket
import sys
import io
import contextlib

from backend import config
from backend import sync_conversations
from backend.database.dataframe_storage import get_storage
from pathlib import Path
from typing import Optional

_lock = threading.RLock()
_server_thread: Optional[threading.Thread] = None
_server_obj: Optional[uvicorn.Server] = None
_status = {"state": "idle", "message": "", "last": None}
_logs = []
_max_logs = 2000


def _append_log(line: str):
    with _lock:
        # normalizar y dividir por líneas
        for l in str(line).splitlines():
            ts = time.strftime("%Y-%m-%d %H:%M:%S")
            entry = f"[{ts}] {l}"
            _logs.append(entry)
            # mantener tamaño
            if len(_logs) > _max_logs:
                del _logs[0: len(_logs) - _max_logs]


def get_logs(last: int = 500):
    with _lock:
        if last is None or last <= 0:
            return list(_logs)
        return _logs[-last:]

def _set_status(state: str, message: str = ""):
    with _lock:
        _status["state"] = state
        _status["message"] = message
        _status["last"] = time.time()
    try:
        # también registrar estados en el log
        _append_log(f"STATUS: {state} - {message}")
    except Exception:
        pass

def get_status():
    with _lock:
        return dict(_status)

def _uvicorn_runner():
    global _server_obj
    try:
        cfg = uvicorn.Config(
            "backend.main:app",
            host=config.BACKEND_HOST,
            port=int(config.BACKEND_PORT),
            log_level="info",
            reload=False,
        )
        _server_obj = uvicorn.Server(cfg)
        _server_obj.run()
    except Exception:
        _set_status("error", "uvicorn error: " + traceback.format_exc())

def start_backend():
    global _server_thread, _server_obj
    with _lock:
        if _server_thread and _server_thread.is_alive():
            return True
        _set_status("starting", "arrancando backend...")
        _server_thread = threading.Thread(target=_uvicorn_runner, daemon=True)
        _server_thread.start()
    # esperar disponibilidad
    deadline = time.time() + 15
    while time.time() < deadline:
        try:
            s = socket.create_connection((config.BACKEND_HOST, int(config.BACKEND_PORT)), timeout=1)
            s.close()
            _set_status("idle", "backend listo")
            return True
        except Exception:
            time.sleep(0.2)
    _set_status("error", "timeout al arrancar backend")
    return False

def stop_backend(timeout=10):
    global _server_obj, _server_thread
    with _lock:
        if not _server_thread or not _server_thread.is_alive():
            _set_status("idle", "backend no estaba corriendo")
            return True
        _set_status("stopping", "deteniendo backend...")
        try:
            if _server_obj:
                # pedir salida al server
                _server_obj.should_exit = True
        except Exception:
            pass
    # esperar a que thread termine
    start = time.time()
    while time.time() - start < timeout:
        if not _server_thread.is_alive():
            with _lock:
                _server_obj = None
                _server_thread = None
            _set_status("stopped", "backend detenido")
            return True
        time.sleep(0.2)
    _set_status("error", "timeout deteniendo backend")
    return False

def _do_sync(limit: int = 50, include_facebook=True, include_instagram=False):
    try:
        _set_status("running_sync", "sincronizando...")
        # Capturar prints de las funciones de sincronización para enviarlos al front
        class _LogWriter(io.StringIO):
            def write(self, s):
                try:
                    if s and not s.isspace():
                        _append_log(s)
                except Exception:
                    pass
                # también escribir a stderr original para visibilidad local
                try:
                    sys.__stdout__.write(s)
                except Exception:
                    pass

        lw = _LogWriter()
        ctx_out = contextlib.redirect_stdout(lw)
        ctx_err = contextlib.redirect_stderr(lw)
        ctx = contextlib.ExitStack()
        ctx.enter_context(ctx_out)
        ctx.enter_context(ctx_err)
        # Llamadas concretas (usa los helpers existentes)
        # Intentamos obtener page/account ids desde meta_client si es posible
        try:
            if include_facebook:
                try:
                    page_info = sync_conversations.meta_client.obtener_info_pagina()
                    page_id = page_info.get("id")
                    if page_id:
                        sync_conversations.sincronizar_facebook(page_id, limit=limit)
                except Exception:
                    pass
            if include_instagram:
                try:
                    page_info = sync_conversations.meta_client.obtener_info_pagina()
                    ig_acc = page_info.get("instagram_business_account", {}) or {}
                    account_id = ig_acc.get("id")
                    if account_id:
                        sync_conversations.sincronizar_instagram(account_id, limit=limit)
                except Exception:
                    pass
        finally:
            try:
                ctx.close()
            except Exception:
                pass
        _set_status("finished", "sincronización completada")
    except Exception as e:
        _set_status("error", "sync error: " + str(e) + "\n" + traceback.format_exc())
        _append_log("ERROR: " + str(e))

def request_sync(password: str, limit: int = 50):
    """Public: iniciar sync en background si password coincide."""
    if not hasattr(config, "SYNC_PASSWORD") or not config.SYNC_PASSWORD:
        return {"ok": False, "msg": "SYNC_PASSWORD no está configurada"}
    if password != config.SYNC_PASSWORD:
        return {"ok": False, "msg": "Contraseña incorrecta"}
    # lanzar hilo que orquesta stop -> sync -> start
    def _worker():
        try:
            # En lugar de detener el backend (que causa problemas de bind en Windows),
            # ejecutamos la sincronización en background y luego recargamos el storage
            # en memoria. Los métodos de almacenamiento ahora guardan de forma atómica
            # por lo que el swap en disco es seguro.
            # Hacer backup antes de sincronizar (opcional pero recomendado)
            try:
                storage = get_storage()
                backup_dir = Path(config.DATA_DIR) / "backups"
                storage.backup_all(backup_dir)
            except Exception:
                # no crítico; continuar con sync
                pass

            _do_sync(limit=limit)

            # forzar recarga desde disco para que la instancia en memoria refleje los nuevos datos
            try:
                storage.reload_from_disk()
            except Exception:
                # no crítico: sólo reportar
                _set_status("warning", "sync finalizado, pero error recargando storage")
        except Exception as e:
            _set_status("error", "sync error: " + str(e) + "\n" + traceback.format_exc())
        else:
            _set_status("finished", "sincronización completada")
    t = threading.Thread(target=_worker, daemon=True)
    t.start()
    return {"ok": True, "msg": "Sync iniciado"}