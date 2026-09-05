"""
trade_manager.py
-----------------
TP/SL trailing mantiqining "miya" qismi. cTrader bilan bevosita ishlamaydi —
buning uchun CTraderClient'ga bog'liq (dependency injection orqali).

KELISHILGAN QOIDA (o'zgartirilmasin, botning butun xavfsizligi shunga
tayanadi):

    Order ochilganda:      broker TP = TP5,  broker SL = boshlang'ich SL
    Narx TP2 ni o'tsa   -> SL = breakeven                (TP o'zgarmaydi)
    Narx TP3 ni o'tsa   -> SL = swing,  TP: TP5  -> TP10  (TP5'ga YETMASDAN
                                                            OLDIN suriladi)
    Narx TP5 ni o'tsa   -> SL = swing,  TP: TP10 -> TP15
    Narx TP10 ni o'tsa  -> SL = swing   (TP15 — oxirgi daraja)
    Narx TP15 ga yetsa  -> broker o'zi to'liq yopadi

Trigger har doim BIR CHECKPOINT OLDIN ishlaydi, shuning uchun broker'dagi
TP hech qachon "joriy narxga yaqin qotib qolmaydi" — bu spike/sakrash
holatlarida erta yopilib qolish xavfini kamaytiradi (butunlay yo'q qilmaydi,
chunki juda keskin harakatlarda broker baribir eski TP'da yopishi mumkin —
bu qabul qilingan tavakkal, foydasiz emas, faqat submaksimal).
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

logger = logging.getLogger("trade_manager")


class TradeSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


@dataclass
class ManagedPosition:
    """Worker xotirasida saqlanadigan, kuzatilayotgan bitta pozitsiya holati."""

    position_id: int
    event_key: str
    side: TradeSide
    entry_price: float
    initial_sl: float
    tp2: float
    tp3: float
    tp5: float
    tp10: float
    tp15: float
    volume_units: int
    risk_percent: float

    current_sl: float = field(init=False)
    current_tp: float = field(init=False)

    # Qaysi checkpoint'lar allaqachon "ishga tushirilgan" (idempotentlik
    # uchun — bir checkpoint ikki marta qayta ishlanmasligi kerak)
    tp2_triggered: bool = False
    tp3_triggered: bool = False
    tp5_triggered: bool = False
    tp10_triggered: bool = False

    def __post_init__(self):
        self.current_sl = self.initial_sl
        self.current_tp = self.tp5  # boshlang'ich TP har doim TP5


@dataclass
class TrailingAction:
    """Bitta tekshiruv siklida qaysi o'zgarish qilinishi kerakligini bildiradi."""

    position_id: int
    new_sl: Optional[float]
    new_tp: Optional[float]
    reason: str


class TradeManager:
    """
    Barcha ochiq (bot tomonidan ochilgan) pozitsiyalarni xotirada saqlaydi
    va har chaqiriqda (worker.py tomonidan 1 daqiqada bir) joriy narxga
    qarab qaysi trailing amal bajarilishi kerakligini aniqlaydi.

    Bu klass CTraderClient'ni chaqirmaydi — faqat "nima qilish kerak"ni
    hisoblab beradi (TrailingAction ro'yxati). Haqiqiy amend/close
    so'rovlarini worker.py yuboradi. Bu ajratish testlashni osonlashtiradi
    va tarmoq xatolarini biznes-mantiqdan izolyatsiya qiladi.
    """

    def __init__(self):
        self._lock = threading.RLock()
        self._positions: dict[int, ManagedPosition] = {}

    def add_position(self, position: ManagedPosition) -> None:
        with self._lock:
            self._positions[position.position_id] = position
            logger.info(
                "Pozitsiya kuzatuvga qo'shildi: id=%s side=%s entry=%s SL=%s TP=%s",
                position.position_id,
                position.side,
                position.entry_price,
                position.current_sl,
                position.current_tp,
            )

    def remove_position(self, position_id: int) -> None:
        with self._lock:
            self._positions.pop(position_id, None)
            logger.info("Pozitsiya kuzatuvdan olib tashlandi: id=%s", position_id)

    def get_all_positions(self) -> list[ManagedPosition]:
        with self._lock:
            return list(self._positions.values())

    def _price_reached(self, side: TradeSide, current_price: float, level: float) -> bool:
        """
        BUY uchun narx darajadan YUQORIGA chiqqanda, SELL uchun PASTGA
        tushganda "yetdi" deb hisoblanadi.
        """
        if side == TradeSide.BUY:
            return current_price >= level
        return current_price <= level

    def evaluate(self, position_id: int, current_price: float) -> Optional[TrailingAction]:
        """
        Bitta pozitsiya uchun joriy narxni tekshiradi va agar checkpoint
        o'tilgan bo'lsa, bajarilishi kerak bo'lgan TrailingAction'ni
        qaytaradi. Hech narsa o'zgarmasa None qaytaradi.

        MUHIM: checkpoint'lar KETMA-KET tekshiriladi (eng yuqoridan pastga),
        shunda narx bir daqiqada bir nechta checkpoint'ni "sakrab o'tgan"
        bo'lsa ham, eng oxirgi (eng uzoq) tegishli holatga to'g'ri o'tkaziladi.
        """
        with self._lock:
            pos = self._positions.get(position_id)
            if pos is None:
                logger.warning("evaluate() noma'lum position_id uchun chaqirildi: %s", position_id)
                return None

            # Eng "uzoq" checkpoint'dan boshlab tekshiramiz, shunda narx
            # bir necha checkpoint'ni birdan o'tib ketgan bo'lsa ham
            # to'g'ri (eng oxirgi) holatga sakraymiz.

            if not pos.tp10_triggered and self._price_reached(pos.side, current_price, pos.tp10):
                pos.tp10_triggered = True
                pos.current_sl = pos.current_sl  # yangi swing qiymati worker.py'dan keladi
                logger.info(
                    "Pozitsiya %s: TP10 checkpoint o'tildi. SL yangilanishi kerak (swing).",
                    position_id,
                )
                return TrailingAction(
                    position_id=position_id,
                    new_sl=None,  # worker.py joriy swing narxini hisoblab to'ldiradi
                    new_tp=None,  # TP15 — oxirgi, o'zgarmaydi
                    reason="TP10_REACHED_SL_TO_SWING",
                )

            if not pos.tp5_triggered and self._price_reached(pos.side, current_price, pos.tp5):
                pos.tp5_triggered = True
                pos.current_tp = pos.tp15
                logger.info(
                    "Pozitsiya %s: TP5 checkpoint o'tildi. TP10 -> TP15, SL -> swing.",
                    position_id,
                )
                return TrailingAction(
                    position_id=position_id,
                    new_sl=None,  # worker.py swing narxini to'ldiradi
                    new_tp=pos.tp15,
                    reason="TP5_REACHED_TP_TO_TP15",
                )

            if not pos.tp3_triggered and self._price_reached(pos.side, current_price, pos.tp3):
                pos.tp3_triggered = True
                pos.current_tp = pos.tp10
                logger.info(
                    "Pozitsiya %s: TP3 checkpoint o'tildi. TP5 -> TP10, SL -> swing.",
                    position_id,
                )
                return TrailingAction(
                    position_id=position_id,
                    new_sl=None,  # worker.py swing narxini to'ldiradi
                    new_tp=pos.tp10,
                    reason="TP3_REACHED_TP_TO_TP10",
                )

            if not pos.tp2_triggered and self._price_reached(pos.side, current_price, pos.tp2):
                pos.tp2_triggered = True
                pos.current_sl = pos.entry_price
                logger.info(
                    "Pozitsiya %s: TP2 checkpoint o'tildi. SL -> breakeven (%.4f).",
                    position_id,
                    pos.entry_price,
                )
                return TrailingAction(
                    position_id=position_id,
                    new_sl=pos.entry_price,
                    new_tp=None,
                    reason="TP2_REACHED_BREAKEVEN",
                )

            return None

    def apply_swing_sl(self, position_id: int, swing_price: float) -> None:
        """
        worker.py TP3/TP5/TP10 checkpoint'idan keyin eng so'nggi tasdiqlangan
        swing narxini hisoblab, shu funksiya orqali xotiradagi holatni
        yangilaydi (broker'ga yuborishdan keyin, muvaffaqiyatli bo'lsa).
        """
        with self._lock:
            pos = self._positions.get(position_id)
            if pos:
                pos.current_sl = swing_price
