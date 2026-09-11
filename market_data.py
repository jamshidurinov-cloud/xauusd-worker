"""
market_data.py
---------------
FAQAT ikkita narsa uchun: /candles va /price HTTP endpoint'lari.

MUHIM ARXITEKTURA QOIDASI (ctrader_client.py'dagi bilan bir xil): bu fayl
cTrader bilan TO'G'RIDAN-TO'G'RI gaplashmaydi — barcha protobuf/tarmoq ishi
`ctrader_client.py` orqali o'tadi. Bu fayl faqat:
  1) HTTP so'rovni qabul qiladi va tekshiradi (auth, parametrlar)
  2) `ctrader_client.py`dagi tayyor metodlarni chaqiradi
  3) Natijani JSON'ga formatlaydi

worker.py bu faylni import qilib, Flask ilovasiga ulaydi (`init_market_data`
orqali) — worker.py'ning o'zi esa signal/trailing mantig'idan tashqari
narsalar bilan "shishib ketmaydi".
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Callable, Optional

from flask import Blueprint, jsonify, request

from ctrader_client import CTraderClient, CTraderError, SymbolInfo

logger = logging.getLogger("market_data")

market_data_bp = Blueprint("market_data", __name__)

# Bu modul darajasidagi o'zgaruvchilar `init_market_data()` orqali
# to'ldiriladi — worker.py o'zining mavjud obyektlarini (client,
# symbol_info, auth funksiyasi) shu yerga "ulaydi".
_client: Optional[CTraderClient] = None
_get_symbol_info: Optional[Callable[[], Optional[SymbolInfo]]] = None
_is_authorized: Optional[Callable[[object], bool]] = None
_get_latest_bid_ask: Optional[Callable[[], tuple]] = None


def init_market_data(
    client: CTraderClient,
    get_symbol_info: Callable[[], Optional[SymbolInfo]],
    is_authorized: Callable[[object], bool],
    get_latest_bid_ask: Callable[[], tuple],
) -> Blueprint:
    """
    worker.py ishga tushganda bir marta chaqiriladi. Parametrlar:
      client            — worker.py'dagi mavjud CTraderClient instansi
      get_symbol_info   — hozirgi _symbol_info'ni qaytaruvchi funksiya
                           (global o'zgaruvchi bo'lgani uchun to'g'ridan-to'g'ri
                           qiymat emas, funksiya sifatida uzatiladi — shunda
                           doim ENG YANGI qiymat o'qiladi)
      is_authorized     — worker.py'dagi mavjud _is_authorized funksiyasi
                           (WORKER_SECRET_KEY tekshiruvi qayta yozilmaydi)
      get_latest_bid_ask — (bid, ask) juftligini qaytaruvchi funksiya;
                           ikkovi ham hali kelmagan bo'lsa (None, None)
    Qaytaradi: Flask Blueprint — worker.py buni app.register_blueprint()
    orqali ulaydi.
    """
    global _client, _get_symbol_info, _is_authorized, _get_latest_bid_ask
    _client = client
    _get_symbol_info = get_symbol_info
    _is_authorized = is_authorized
    _get_latest_bid_ask = get_latest_bid_ask
    return market_data_bp


def _minutes_to_iso(utc_minutes: int) -> str:
    """cTrader trendbar vaqtini (UTC daqiqalarda) ISO 8601 satriga o'giradi."""
    dt = datetime.fromtimestamp(utc_minutes * 60, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


@market_data_bp.route("/candles", methods=["GET"])
def get_candles():
    if not _is_authorized(request):
        return jsonify({"status": "error", "detail": "unauthorized"}), 401

    symbol_info = _get_symbol_info()
    if symbol_info is None:
        return jsonify({"status": "error", "detail": "Symbol hali yuklanmagan"}), 503

    symbol_param = request.args.get("symbol", "")
    timeframe = request.args.get("timeframe", "1min")
    count_param = request.args.get("count", "450")

    # Hozircha Worker faqat BITTA symbol (SYMBOL_NAME) bilan ishlaydi —
    # boshqa symbol so'ralsa, aniq xato qaytariladi (jimgina noto'g'ri
    # ma'lumot bermaslik uchun).
    if symbol_param and symbol_param.upper() != "XAUUSD":
        return (
            jsonify(
                {
                    "status": "error",
                    "detail": f"Faqat XAUUSD qo'llab-quvvatlanadi, so'ralgan: {symbol_param}",
                }
            ),
            400,
        )

    try:
        count = int(count_param)
    except ValueError:
        return jsonify({"status": "error", "detail": "count butun son bo'lishi kerak"}), 400

    try:
        bars = _client.get_trendbars(
            symbol_id=symbol_info.symbol_id,
            digits=symbol_info.digits,
            timeframe=timeframe,
            count=count,
        )
    except ValueError as exc:
        return jsonify({"status": "error", "detail": str(exc)}), 400
    except CTraderError as exc:
        logger.error("Trendbar so'rovida xato: %s", exc)
        return jsonify({"status": "error", "detail": str(exc)}), 502

    response = [
        {
            "time": _minutes_to_iso(bar["timestamp_min"]),
            "open": round(bar["open"], symbol_info.digits),
            "high": round(bar["high"], symbol_info.digits),
            "low": round(bar["low"], symbol_info.digits),
            "close": round(bar["close"], symbol_info.digits),
            "volume": bar["volume"],
        }
        for bar in bars
    ]
    return jsonify(response), 200


@market_data_bp.route("/price", methods=["GET"])
def get_price():
    if not _is_authorized(request):
        return jsonify({"status": "error", "detail": "unauthorized"}), 401

    symbol_param = request.args.get("symbol", "")
    if symbol_param and symbol_param.upper() != "XAUUSD":
        return (
            jsonify(
                {
                    "status": "error",
                    "detail": f"Faqat XAUUSD qo'llab-quvvatlanadi, so'ralgan: {symbol_param}",
                }
            ),
            400,
        )

    bid, ask = _get_latest_bid_ask()
    if bid is None or ask is None:
        return (
            jsonify({"status": "error", "detail": "Hali narx (bid/ask) kelmagan"}),
            503,
        )

    return jsonify({"bid": bid, "ask": ask}), 200
