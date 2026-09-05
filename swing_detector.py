"""
swing_detector.py
------------------
MUHIM DIZAYN ESLATMASI:

Kelishilgan TP/SL trailing qoidasida "TP3+ dan boshlab SL eng so'nggi
tasdiqlangan swing nuqtasiga ko'chiriladi" deyilgan. Bu swing tushunchasi
sizning asosiy botingizda (`main.py`, `smartmoneyconcepts` kutubxonasi va
Wyckoff `detect_spring`/`detect_upthrust` funksiyalari) allaqachon aniq
va sinovdan o'tgan mantiq bilan hisoblanadi.

Worker esa MUSTAQIL, alohida process — u main.py'ning ichki funksiyalariga
to'g'ridan-to'g'ri kira olmaydi (chunki ular alohida Render service'da
ishlaydi). Shuning uchun bu yerda ODDIY, MUSTAQIL fractal-swing aniqlash
qo'llanilgan (klassik 5-sham fraktali: o'rtadagi sham eng yuqori/past bo'lsa,
u swing nuqtasi hisoblanadi).

BU YAXSHILANISHI KERAK BO'LGAN JOY: aniqlik uchun eng to'g'ri yechim —
main.py'dagi swing aniqlash funksiyasini (`smartmoneyconcepts`dan yoki
Wyckoff modulidan) alohida umumiy kutubxonaga (masalan `shared_smc.py`)
chiqarib, ikkala process (main.py va worker.py) ham UNI ishlatishi.
Hozircha, ishga tushirish tezligi uchun, oddiy fraktal yondashuv qo'llanildi
— bu qat'iy SMC-swing bilan bir xil bo'lmasligi mumkin, lekin "narxdan
oqilona masofada himoya" vazifasini bajaradi.
"""

from __future__ import annotations

import logging
from typing import Optional

import requests

logger = logging.getLogger("swing_detector")

TWELVEDATA_BASE_URL = "https://api.twelvedata.com/time_series"


def fetch_recent_candles(
    api_key: str,
    symbol: str = "XAU/USD",
    interval: str = "1min",
    output_size: int = 50,
    timeout: float = 8.0,
) -> list[dict]:
    """TwelveData'dan so'nggi shamlarni oladi (eskisidan yangisiga tartiblangan)."""
    params = {
        "symbol": symbol,
        "interval": interval,
        "outputsize": output_size,
        "apikey": api_key,
    }
    resp = requests.get(TWELVEDATA_BASE_URL, params=params, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()

    if data.get("status") == "error":
        raise RuntimeError(f"TwelveData xatosi: {data.get('message')}")

    values = data.get("values", [])
    # TwelveData yangisidan eskisiga tartiblab beradi — teskarisiga o'giramiz
    values.reverse()
    return [
        {
            "open": float(v["open"]),
            "high": float(v["high"]),
            "low": float(v["low"]),
            "close": float(v["close"]),
        }
        for v in values
    ]


def find_last_swing(
    candles: list[dict],
    direction: str,
    fractal_width: int = 2,
) -> Optional[float]:
    """
    direction="BUY"  -> eng so'nggi tasdiqlangan SWING LOW qidiradi (SL past joylashadi)
    direction="SELL" -> eng so'nggi tasdiqlangan SWING HIGH qidiradi (SL yuqori joylashadi)

    fractal_width=2 -> klassik 5-sham fraktali (chap 2, o'ng 2, markaz 1)

    Eng so'nggi (grafikning oxiriga eng yaqin) tasdiqlangan swing qaytariladi.
    Agar topilmasa None qaytaradi (chaqiruvchi kod bu holatda SL'ni
    o'zgartirmasligi kerak — xavfsizroq default).
    """
    n = len(candles)
    if n < (2 * fractal_width + 1):
        logger.warning("Swing aniqlash uchun sham yetarli emas (%d dona)", n)
        return None

    # Oxiridan boshlab, eng yaqin tasdiqlangan fraktalni qidiramiz
    for i in range(n - fractal_width - 1, fractal_width - 1, -1):
        window = candles[i - fractal_width : i + fractal_width + 1]
        center = candles[i]

        if direction.upper() == "BUY":
            lows = [c["low"] for c in window]
            if center["low"] == min(lows):
                return center["low"]
        else:
            highs = [c["high"] for c in window]
            if center["high"] == max(highs):
                return center["high"]

    logger.warning("Tasdiqlangan swing topilmadi (%s yo'nalishi uchun)", direction)
    return None


def get_trailing_swing_sl(
    api_key: str,
    direction: str,
    symbol: str = "XAU/USD",
    interval: str = "1min",
    buffer_dollars: float = 1.0,
) -> Optional[float]:
    """
    Yuqoridagi ikki funksiyani birlashtirib, tayyor SL narxini qaytaradi
    ($1 buferi bilan, mavjud FVG-SL mantig'iga mos).
    """
    try:
        candles = fetch_recent_candles(api_key, symbol=symbol, interval=interval)
        swing = find_last_swing(candles, direction=direction)
        if swing is None:
            return None
        if direction.upper() == "BUY":
            return round(swing - buffer_dollars, 3)
        return round(swing + buffer_dollars, 3)
    except Exception:  # noqa: BLE001
        logger.exception("Swing SL hisoblashda xato")
        return None
