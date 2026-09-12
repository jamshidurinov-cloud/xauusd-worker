"""
debug_data.py
--------------
FAQAT sinov/tekshiruv uchun: /debug/balance, /debug/positions, /debug/symbol
HTTP endpoint'lari.

MUHIM ARXITEKTURA QOIDASI (market_data.py va ctrader_client.py bilan bir
xil): bu fayl cTrader bilan TO'G'RIDAN-TO'G'RI gaplashmaydi — barcha
protobuf/tarmoq ishi `ctrader_client.py` orqali o'tadi. Bu fayl faqat:
  1) HTTP so'rovni qabul qiladi va tekshiradi (auth)
  2) `ctrader_client.py`dagi TAYYOR, faqat-o'qish metodlarni chaqiradi
  3) Natijani JSON'ga formatlaydi

MUHIM: bu yerdagi barcha endpoint'lar FAQAT GET (o'qish) — hech biri order
yubormaydi, pozitsiya yopmaydi yoki SL/TP o'zgartirmaydi. Maqsad — botni
asosiy signal/trailing oqimiga ulashdan oldin, cTrader'dan qanday
ma'lumot kelayotganini (balans, ochiq pozitsiyalar, symbol parametrlari)
tashqaridan (curl orqali) tekshirib ko'rish, ishlab turgan worker'ga
ta'sir qilmasdan.

worker.py bu faylni import qilib, Flask ilovasiga ulaydi (`init_debug_data`
orqali) — market_data.py qanday ulangan bo'lsa, xuddi shunday.
"""

from __future__ import annotations

import logging
from typing import Callable, Optional

from flask import Blueprint, jsonify, request

from ctrader_client import CTraderClient, CTraderError, SymbolInfo

logger = logging.getLogger("debug_data")

debug_data_bp = Blueprint("debug_data", __name__)

_client: Optional[CTraderClient] = None
_get_symbol_info: Optional[Callable[[], Optional[SymbolInfo]]] = None
_is_authorized: Optional[Callable[[object], bool]] = None


def init_debug_data(
    client: CTraderClient,
    get_symbol_info: Callable[[], Optional[SymbolInfo]],
    is_authorized: Callable[[object], bool],
) -> Blueprint:
    """
    worker.py ishga tushganda bir marta chaqiriladi. Parametrlar
    market_data.py'dagi bilan bir xil ma'noda — mavjud, ishlab turgan
    obyektlar shu yerga "ulanadi", yangi ulanish yaratilmaydi.
    """
    global _client, _get_symbol_info, _is_authorized
    _client = client
    _get_symbol_info = get_symbol_info
    _is_authorized = is_authorized
    return debug_data_bp


@debug_data_bp.route("/debug/balance", methods=["GET"])
def get_balance():
    if not _is_authorized(request):
        return jsonify({"status": "error", "detail": "unauthorized"}), 401

    try:
        balance = _client.get_account_balance(timeout=10)
    except CTraderError as exc:
        logger.error("Balans so'rovida xato: %s", exc)
        return jsonify({"status": "error", "detail": str(exc)}), 502

    return jsonify({"balance": balance}), 200


@debug_data_bp.route("/debug/positions", methods=["GET"])
def get_positions():
    if not _is_authorized(request):
        return jsonify({"status": "error", "detail": "unauthorized"}), 401

    try:
        positions = _client.get_open_positions_full(timeout=10)
    except CTraderError as exc:
        logger.error("Pozitsiyalar so'rovida xato: %s", exc)
        return jsonify({"status": "error", "detail": str(exc)}), 502

    return jsonify({"count": len(positions), "positions": positions}), 200


@debug_data_bp.route("/debug/symbol", methods=["GET"])
def get_symbol():
    if not _is_authorized(request):
        return jsonify({"status": "error", "detail": "unauthorized"}), 401

    symbol_info = _get_symbol_info()
    if symbol_info is None:
        return jsonify({"status": "error", "detail": "Symbol hali yuklanmagan"}), 503

    return (
        jsonify(
            {
                "symbol_id": symbol_info.symbol_id,
                "lot_size": symbol_info.lot_size,
                "digits": symbol_info.digits,
                "pip_position": symbol_info.pip_position,
            }
        ),
        200,
    )
