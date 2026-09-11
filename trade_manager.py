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

    # Har bir yangi narx (tick) kelganda yangilanadigan "eng yaxshi
    # erishilgan narx" — BUY uchun ENG YUQORI, SELL uchun ENG PAST.
    # MUHIM: bu — trailing tekshiruvining 60 soniyalik oralig'i ichida
    # narx checkpoint'ga tegib, keyin QAYTIB ketgan holatlarni "ko'rmay
    # qolish"ning oldini oladi. Trailing checkpoint'lari joriy (instant)
    # narx o'rniga aynan shu qiymat bilan solishtiriladi.
    best_price: float = field(init=False)

    # True bo'lsa — pos.current_sl/current_tp broker'ga hali YUBORILMAGAN
    # (yoki yuborilgan, lekin joriy narxga nomos bo'lgani uchun rad etilishi
    # muqarrar bo'lgani sababli, ATAYLAB yuborilmagan). Worker har trailing
    # siklida shu flag True bo'lgan pozitsiyalar uchun qayta urinadi.
    pending_broker_sync: bool = False

    # Worker qayta ishga tushganda broker'dan "eng yaqin holatda" tiklangan
    # pozitsiyalar uchun False bo'ladi — chunki asl TP2-TP15 checkpoint'lari
    # (faqat signal payload'ida bo'lgan, broker'da saqlanmaydigan) yo'qolgan.
    # Noto'g'ri taxmin qilib SL'ni xato joyga surishdan ko'ra, bunday
    # pozitsiyalar uchun TP-checkpoint trailing butunlay TO'XTATILADI —
    # faqat kuzatuv (yopilishni aniqlash) va risk-hisob davom etadi.
    trailing_enabled: bool = True

    # Qaysi checkpoint'lar allaqachon "ishga tushirilgan" (idempotentlik
    # uchun — bir checkpoint ikki marta qayta ishlanmasligi kerak)
    tp2_triggered: bool = False
    tp3_triggered: bool = False
    tp5_triggered: bool = False
    tp10_triggered: bool = False

    def __post_init__(self):
        self.current_sl = self.initial_sl
        self.current_tp = self.tp5  # boshlang'ich TP har doim TP5
        self.best_price = self.entry_price  # dastlab, hali hech qayerga bormagan


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

    def update_best_price(self, current_price: float) -> None:
        """
        Har safar YANGI narx (tick) kelganda chaqiriladi — barcha ochiq
        pozitsiyalarning "eng yaxshi erishilgan narxi"ni yangilaydi.
        BUY uchun narx yuqoriga chiqqanda, SELL uchun pastga tushganda
        yangilanadi (aks holda o'zgarishsiz qoladi).
        """
        with self._lock:
            for pos in self._positions.values():
                if pos.side == TradeSide.BUY:
                    if current_price > pos.best_price:
                        pos.best_price = current_price
                else:
                    if current_price < pos.best_price:
                        pos.best_price = current_price

    def evaluate(self, position_id: int, current_price: float) -> Optional[TrailingAction]:
        """
        Bitta pozitsiya uchun joriy narxni tekshiradi va checkpoint(lar)
        o'tilgan bo'lsa, YAKUNIY (eng oxirgi to'g'ri) SL/TP holatini
        qaytaradi. Hech narsa o'zgarmasa None qaytaradi.

        MUHIM TUZATISH: agar narx bir tekshiruv oralig'ida (1 daqiqada)
        bir nechta checkpoint'ni BIRDAN o'tib ketsa (masalan to'g'ridan-to'g'ri
        TP10'gacha), BARCHA oralaridagi checkpoint'lar (TP2, TP3, TP5) ham
        shu YAGONA chaqiruvda, TO'G'RI (o'sish) TARTIBDA qayta ishlanadi —
        avvalgi versiyada checkpoint'lar teskari tartibda (TP10 birinchi)
        tekshirilib, faqat bittasi qaytarilar edi, qolganlari esa KEYINGI
        sikllarda, NOTO'G'RI TARTIBDA "qayta kashf qilinar edi" (masalan
        TP10 keyin TP5 keyin TP3 keyin TP2 — teskari), bu esa SL/TP'ni
        vaqtincha bir xil narxga tenglashtirib qo'yishi mumkin edi.

        Endi: checkpoint'lar TP2 -> TP3 -> TP5 -> TP10 tartibida, ketma-ket
        qo'llaniladi, va faqat ENG OXIRGI (eng uzoq yetilgan) checkpoint'ning
        SL/TP holati broker'ga yuboriladi — bu holat har doim ICHKI JIHATDAN
        MUVOFIQ (SL hech qachon TP bilan bir xil yoki undan uzoqroqda
        bo'lib qolmaydi).

        SL TRAILING MANTIG'I (o'zgarmadi, faqat bajarilish tartibi tuzatildi):
          TP2  o'tilsa -> SL = entry (breakeven)
          TP3  o'tilsa -> SL = TP2,  TP = TP10
          TP5  o'tilsa -> SL = TP3,  TP = TP15
          TP10 o'tilsa -> SL = TP5  (TP15 — oxirgi, o'zgarmaydi)
        """
        with self._lock:
            pos = self._positions.get(position_id)
            if pos is None:
                logger.warning("evaluate() noma'lum position_id uchun chaqirildi: %s", position_id)
                return None

            if not pos.trailing_enabled:
                # Tiklangan (orphan) pozitsiya — checkpoint'lari noma'lum,
                # shuning uchun trailing amalga oshirilmaydi (xavfsizlik).
                return None

            # Xavfsizlik uchun: agar chaqiruvchi update_best_price()ni
            # alohida chaqirmagan bo'lsa ham, shu yerdagi current_price
            # baribir best_price'ga "qo'shiladi" (faqat foydali tomonga).
            if pos.side == TradeSide.BUY:
                if current_price > pos.best_price:
                    pos.best_price = current_price
            else:
                if current_price < pos.best_price:
                    pos.best_price = current_price

            # BARCHA checkpoint tekshiruvlari endi JORIY (instant) narx
            # o'rniga ENG YAXSHI ERISHILGAN narx (best_price) bilan
            # solishtiriladi — shunda narx checkpoint'ga tegib, 60 soniyalik
            # tekshiruv oralig'ida ORQAGA qaytib ketgan bo'lsa ham,
            # checkpoint "ko'rilmay qolmaydi".
            eval_price = pos.best_price

            any_triggered = False
            last_reason = ""

            # 1) TP2 — eng yaqin checkpoint, birinchi tekshiriladi
            if not pos.tp2_triggered and self._price_reached(pos.side, eval_price, pos.tp2):
                pos.tp2_triggered = True
                pos.current_sl = pos.entry_price
                any_triggered = True
                last_reason = "TP2_REACHED_BREAKEVEN"
                logger.info(
                    "Pozitsiya %s: TP2 checkpoint o'tildi. SL -> breakeven (%.4f).",
                    position_id,
                    pos.entry_price,
                )

            # 2) TP3 — faqat TP2'dan KEYIN, o'sish tartibida
            if not pos.tp3_triggered and self._price_reached(pos.side, eval_price, pos.tp3):
                pos.tp3_triggered = True
                pos.current_sl = pos.tp2
                pos.current_tp = pos.tp10
                any_triggered = True
                last_reason = "TP3_REACHED_TP_TO_TP10"
                logger.info(
                    "Pozitsiya %s: TP3 checkpoint o'tildi. TP5 -> TP10, SL -> TP2 (%.4f).",
                    position_id,
                    pos.tp2,
                )

            # 3) TP5
            if not pos.tp5_triggered and self._price_reached(pos.side, eval_price, pos.tp5):
                pos.tp5_triggered = True
                pos.current_sl = pos.tp3
                pos.current_tp = pos.tp15
                any_triggered = True
                last_reason = "TP5_REACHED_TP_TO_TP15"
                logger.info(
                    "Pozitsiya %s: TP5 checkpoint o'tildi. TP10 -> TP15, SL -> TP3 (%.4f).",
                    position_id,
                    pos.tp3,
                )

            # 4) TP10 — eng uzoq checkpoint, oxirida tekshiriladi
            if not pos.tp10_triggered and self._price_reached(pos.side, eval_price, pos.tp10):
                pos.tp10_triggered = True
                pos.current_sl = pos.tp5
                any_triggered = True
                last_reason = "TP10_REACHED_SL_TO_TP5"
                logger.info(
                    "Pozitsiya %s: TP10 checkpoint o'tildi. SL -> TP5 (%.4f).",
                    position_id,
                    pos.tp5,
                )

            if not any_triggered:
                return None

            # Kamida bitta checkpoint o'tildi — bu holatni broker'ga
            # YUBORISH KERAK deb belgilaymiz. Haqiqiy yuborish (va joriy
            # narxga mos-nomosligini tekshirish) worker.py'da amalga
            # oshiriladi — bu yerda faqat "yetkazish kerak" belgisi qo'yiladi.
            pos.pending_broker_sync = True

            logger.info(
                "Pozitsiya %s: YAKUNIY holat (bir yoki bir nechta checkpoint "
                "birdan o'tilgan bo'lishi mumkin) — SL=%.4f TP=%.4f sabab=%s",
                position_id,
                pos.current_sl,
                pos.current_tp,
                last_reason,
            )

            return TrailingAction(
                position_id=position_id,
                new_sl=pos.current_sl,
                new_tp=pos.current_tp,
                reason=last_reason,
            )
