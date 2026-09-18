"""
trade_manager.py
-----------------
TP/SL trailing mantiqining "miya" qismi. cTrader bilan bevosita ishlamaydi —
buning uchun CTraderClient'ga bog'liq (dependency injection orqali).

KELISHILGAN QOIDA (2026-09-18'da YANGILANDI - har 1R'lik zich zanjirga
o'tildi, "smc" va "ob_fvg" profillari ENDI BIR XIL ishlaydi):

    Order ochilganda:      broker TP = 5R,  broker SL = boshlang'ich SL
    Narx 2R ni o'tsa    -> SL = breakeven                (TP o'zgarmaydi)
    Narx nR ni o'tsa (n=3..14) -> SL = (n-2)R,  TP = (n+2)R (15R'dan
                                    oshmaydi - bu qat'iy yakuniy chegara)
    Narx 15R ga yetsa   -> broker o'zi to'liq yopadi

Barcha TP darajalari (nR) ENDI faqat `entry_price` va `initial_sl`dan
(ya'ni 1R = |entry - initial_sl|) HISOBLAB CHIQARILADI - main.py'dan alohida
tp2/tp3/tp5/... qiymatlari sifatida OLINMAYDI. Bu main.py'ning haqiqiy
formulasi (tp_n = entry +/- n*R) bilan ANIQ mos (2026-09-18'da tasdiqlangan),
shuning uchun hech qanday nomuvofiqlik xavfi yo'q.

MUHIM (2026-09-18): avval "ob_fvg" profili uchun ALOHIDA, soddalashtirilgan
(TP qattiq 3R'da qotirilgan) mantiq bor edi. Gist tahlili buning OB/FVG
foydasining katta qismini (~92%i, taxminan -54R) kesib tashlaganini
ko'rsatgach, bu farq OLIB TASHLANDI - endi profildan qat'iy nazar, BARCHA
pozitsiyalar shu yagona, zich (har 1R) zanjir bo'yicha boshqariladi.
`profile` maydoni faqat MA'LUMOT sifatida saqlanadi (loglash/statistika
uchun), trailing MANTIG'IGA ENDI TA'SIR QILMAYDI.

Trigger har doim IKKI QADAM OLDIN ishlaydi (masalan nR o'tilganda TP =
(n+2)R), shuning uchun broker'dagi TP hech qachon "joriy narxga yaqin qotib
qolmaydi" - bu spike/sakrash holatlarida erta yopilib qolish xavfini
kamaytiradi (butunlay yo'q qilmaydi, chunki juda keskin harakatlarda broker
baribir eski TP'da yopishi mumkin - bu qabul qilingan tavakkal, foydasiz
emas, faqat submaksimal). Zanjir endi ZICH (har butun R) bo'lgani uchun,
avvalgi notekis oraliqlar (masalan 3R->8R kabi 5R'lik bo'shliq) endi mavjud
emas - har bir qadam aniq 1R.
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
    volume_units: int
    risk_percent: float

    # 2026-09-18: ENDI FAQAT MA'LUMOT/STATISTIKA uchun saqlanadi - trailing
    # mantig'iga (evaluate()) TA'SIR QILMAYDI (barcha profil bir xil, zich
    # har-1R zanjir bo'yicha boshqariladi). Eski, alohida "ob_fvg" (TP
    # qattiq 3R'da) mantiq OLIB TASHLANDI - Gist tahlili buni foydaning
    # katta qismini kesib tashlaganini ko'rsatgan edi.
    profile: str = "smc"

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
    # pozitsiyalar uchun False bo'ladi — chunki asl checkpoint'lari (faqat
    # signal payload'ida bo'lgan, broker'da saqlanmaydigan) yo'qolgan.
    # Noto'g'ri taxmin qilib SL'ni xato joyga surishdan ko'ra, bunday
    # pozitsiyalar uchun TP-checkpoint trailing butunlay TO'XTATILADI —
    # faqat kuzatuv (yopilishni aniqlash) va risk-hisob davom etadi.
    trailing_enabled: bool = True

    # 2026-09-18: eski 6 ta alohida "tpN_triggered" bool o'rniga - ENDI
    # zich (2,3,4,...,14) zanjir bo'lgani uchun, faqat "oxirgi o'tilgan R
    # darajasi" (butun son) saqlanadi. Bu, xuddi eskisi kabi, bir xil
    # checkpoint ikki marta ishlanmasligini (idempotentlik) kafolatlaydi.
    last_triggered_level: int = field(default=0, init=False)

    def __post_init__(self):
        self.current_sl = self.initial_sl
        # Boshlang'ich TP - ENDI BARCHA profillar uchun 5R (avvalgi
        # "ob_fvg -> 3R qattiq" farqi 2026-09-18'da olib tashlandi).
        self.current_tp = self.price_at_r(5)
        self.best_price = self.entry_price  # dastlab, hali hech qayerga bormagan

    def r_distance(self) -> float:
        """1R = entry va boshlang'ich SL orasidagi masofa (har doim musbat)."""
        return abs(self.entry_price - self.initial_sl)

    def price_at_r(self, n: float) -> float:
        """
        n-R darajasidagi narxni qaytaradi (BUY uchun entry'dan yuqoriga,
        SELL uchun pastga). n=0 -> entry_price (breakeven) bilan bir xil.
        """
        r = self.r_distance()
        if self.side == TradeSide.BUY:
            return self.entry_price + n * r
        return self.entry_price - n * r


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

        2026-09-18'da YANGILANDI - ZICH, HAR-1R ZANJIR (2R, 3R, 4R, ...,
        14R), profildan qat'iy nazar bir xil ishlaydi:

          n=2R:        SL = breakeven (entry),  TP o'zgarmaydi
          n=3R..14R:   SL = (n-2)R,              TP = (n+2)R (15R'dan
                                                   oshmaydi - qat'iy chegara)
          n=15R:       broker o'zi to'liq yopadi (bizning kodimiz aralashmaydi)

        TP hech qachon ORQAGA (kamroq foydali tomonga) surilmaydi - faqat
        oldinga (yoki joyida qoladi). Bitta chaqiruvda BARCHA o'tilgan
        checkpoint'lar ketma-ket qo'llaniladi - shunda narx bir necha
        checkpoint'ni birdan o'tib ketsa ham, hech biri "ko'rilmay qolmaydi".
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

            r = pos.r_distance()
            if r <= 0:
                # Nazariy jihatdan bo'lmasligi kerak (entry==SL), lekin
                # nolga bo'lishning oldini olish uchun xavfsizlik tekshiruvi.
                logger.warning(
                    "Pozitsiya %s: R masofasi 0 yoki manfiy (entry=%s, SL=%s) - "
                    "trailing o'tkazib yuborildi", position_id, pos.entry_price, pos.initial_sl,
                )
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

            MAX_LEVEL = 14  # 15R - yakuniy, broker o'zi yopadi
            n = max(2, pos.last_triggered_level + 1)

            while n <= MAX_LEVEL:
                level_price = pos.price_at_r(n)
                if not self._price_reached(pos.side, eval_price, level_price):
                    break  # zanjir o'sish tartibida - keyingisini tekshirish shart emas

                pos.last_triggered_level = n
                any_triggered = True

                if n == 2:
                    pos.current_sl = pos.entry_price  # breakeven
                    last_reason = "L2_REACHED_BREAKEVEN"
                    logger.info(
                        "Pozitsiya %s: 2R checkpoint o'tildi. SL -> breakeven (%.4f).",
                        position_id, pos.entry_price,
                    )
                else:
                    new_sl = pos.price_at_r(n - 2)
                    pos.current_sl = new_sl
                    last_reason = f"L{n}_REACHED"
                    logger.info(
                        "Pozitsiya %s: %dR checkpoint o'tildi. SL -> %dR (%.4f).",
                        position_id, n, n - 2, new_sl,
                    )

                target_n = min(n + 2, 15)
                candidate_tp = pos.price_at_r(target_n)
                # TP faqat OLDINGA (foydaliroq tomonga) suriladi, hech qachon orqaga.
                is_further = (
                    candidate_tp > pos.current_tp
                    if pos.side == TradeSide.BUY
                    else candidate_tp < pos.current_tp
                )
                if is_further:
                    pos.current_tp = candidate_tp

                n += 1

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
