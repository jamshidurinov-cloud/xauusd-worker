"""
BU FAYL ALOHIDA ISHLAMAYDI — bu shunchaki main.py ga qo'shiladigan
kod bo'lagi, alohida ko'rib chiqish uchun ajratilgan.

QAYERGA QO'YISH: log_new_signal(signal, price_data, interval) funksiyasi
ichida, SL narxi (compute_sl_level orqali) va TP2/TP3/TP5/TP10/TP15
qiymatlari hisoblab bo'lingandan so'ng, o'sha funksiya oxirida chaqiring:

    send_signal_to_worker(
        event_key=event_key,
        direction="BUY" yoki "SELL",   # signal['bullish']/['bearish']dan
        entry_price=<joriy narx o'zgaruvchisi>,
        sl_price=sl_price,
        tp2=tp_levels["TP2"],
        tp3=tp_levels["TP3"],
        tp5=tp_levels["TP5"],
        tp10=tp_levels["TP10"],
        tp15=tp_levels["TP15"],
        timeframe=interval,
    )

O'zgaruvchi nomlarini (sl_price, tp_levels va h.k.) main.py'dagi HAQIQIY
nomlariga moslashtiring — bu yerda faqat funksiyaning o'zi, mustaqil va
tayyor holda beriladi.
"""

from __future__ import annotations

import logging
import os

import requests

logger = logging.getLogger(__name__)

# Render environment variables orqali beriladi — kodda hech qachon
# to'g'ridan-to'g'ri yozilmasin.
WORKER_URL = os.environ.get("WORKER_URL", "")
WORKER_SECRET_KEY = os.environ.get("WORKER_SECRET_KEY", "")


def send_signal_to_worker(
    event_key: str,
    direction: str,
    entry_price: float,
    sl_price: float,
    tp2: float,
    tp3: float,
    tp5: float,
    tp10: float,
    tp15: float,
    timeframe: str,
    symbol: str = "XAUUSD",
    strategy: str = "SMC_Sweep_FVG",
) -> None:
    """
    Worker'ga yangi signalni yuboradi. Bu funksiya HECH QACHON asosiy signal
    oqimini (Telegram, Gist) to'xtatib qo'ymasligi kerak — shuning uchun:
      - Worker sozlanmagan bo'lsa (WORKER_URL bo'sh) — jim o'tkazib yuboradi
      - Tarmoq xatosi bo'lsa — faqat log qiladi, exception ko'tarmaydi
      - timeout qisqa (5s) — cron uzoq kutib qolmasligi uchun
    """
    if not WORKER_URL or not WORKER_SECRET_KEY:
        logger.debug(
            "WORKER_URL yoki WORKER_SECRET_KEY sozlanmagan — Worker'ga "
            "yuborish o'tkazib yuborildi (bu normal, Worker hali ishga "
            "tushirilmagan bosqichda)."
        )
        return

    payload = {
        "event_key": event_key,
        "symbol": symbol,
        "timeframe": timeframe,
        "strategy": strategy,
        "direction": direction.upper(),
        "entry_price": entry_price,
        "sl_price": sl_price,
        "tp2": tp2,
        "tp3": tp3,
        "tp5": tp5,
        "tp10": tp10,
        "tp15": tp15,
    }
    headers = {
        "Authorization": f"Bearer {WORKER_SECRET_KEY}",
        "Content-Type": "application/json",
    }

    try:
        response = requests.post(
            f"{WORKER_URL.rstrip('/')}/signal",
            json=payload,
            headers=headers,
            timeout=5,
        )
        if response.status_code == 200:
            logger.info("Signal Worker'ga muvaffaqiyatli yuborildi: %s", event_key)
        else:
            logger.warning(
                "Worker signal yuborishni rad etdi (status=%s): %s",
                response.status_code,
                response.text[:300],
            )
    except requests.RequestException as exc:
        logger.warning("Worker'ga ulanib bo'lmadi (bu signalni yo'qotmaydi, "
                        "faqat avtomat execution ishlamaydi): %s", exc)
