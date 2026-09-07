from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from datetime import datetime, timezone

_CACHE_TTL = 5.0    # segundos que um resutado é aproveitado
_STATUS_TIMEOUT = 3.0   # teto de espera do probe de status

@dataclass
class _Cached:
    payload: dict
    expires_at: float

_cache: dict[int, _Cached] = {}
_locks: dict[int, asyncio.Lock] = {}

async def _probe_tcp(ip: str, port: int, timeout: float) -> tuple[bool, str]:
    """Valida apenas se a porta TCP aceita conexão."""

    writer = None
    try:
        _, writer = await asyncio.wait_for(
            asyncio.open_connection(ip, port), timeout=timeout
        )
        return True, "TCP port open."
    except asyncio.TimeoutError:
        return False, f"Timeout after {timeout}s."
    except OSError as exc:
        return False, f"TCP connection failed: {exc}"
    finally:
        if writer is not None:
            writer.close()
            try:
                await writer.wait_close()
            except Exception:
                pass

async def _probe_modbus(
    ip: str, port: int, unit_id: int, timeout: float
) -> tuple[bool, str, int | None]:
    """Conecta e lê o holding 0 - confirma que o slave responde, não só a porta."""
    try:
        from pymodbus.client import AsyncModbusTcpClient
    except ImportError:
        return False, "pymodbus is not installed", None

    client = AsyncModbusTcpClient(host=ip, port=port, timeout=timeout)
    try:
        if not await client.connect():
            return False, "Failed to estabilish TCP connection.", None

        result = await client.read_holding_registers(
            address=0, count=1, device_id=unit_id
        )
        if result.isError():
            return False, f"Connection OK, but reading failed: {result}", None

        return True, "Successful Modbus connection and reading.", result.registers[0]
    except Exception as exc:
        return False, str(exc), None
    finally:
        client.close()

async def check_plc(plc, deep: bool = False, force: bool = False) -> dict:
    now = time.monotonic()
    cached = _cache.get(plc.id)

    if cached and not force and now < cached.expires_at:
        return {**cached.payload, "cached": True}

    lock = _locks.setdefault(plc.id, asyncio.Lock())
    async with lock:
        # Recheca: outra request pode ter preenchido o cache enquanto esperávamos.
        caches = _cache.get(plc.id)

        if cached and not force and time.monotonic() < cached.expires_at:
            return {**cached.payload, "cached": True}

        timeout = min(plc.timeout, _STATUS_TIMEOUT)
        started = time.perf_counter()

        if deep:
            connected, message, value = await _probe_modbus(
                plc.ip, plc.port_modbus, plc.unit_id, timeout
            )
        else:
            connected, message = await _probe_tcp(
                plc.ip, plc.port_modbus, timeout
            )
            value = None

        payload = {
            "plc_id": plc.id,
            'name': plc.name,
            "connected": connected,
            "message": message,
            "ip": plc.ip,
            "port": plc.port_modbus,
            "unit_id": plc.unit_id,
            "latency_ms": round((time.perf_counter() - started) * 1000, 1),
            "value_reg0": value,
            "checked_at": datetime.now(timezone.utc),
        }
        _cache[plc.id] = _Cached(payload, time.monotonic() + _CACHE_TTL)
        return {**payload, "cached": False}
