"""
risk_manager.py
----------------
Faqat RISK bilan bog'liq hisob-kitoblar shu yerda joylashadi:
  1) Position sizing (lot hisoblash)
  2) Jami ochiq risk chegarasi
  3) Kunlik circuit breaker (zarar limiti)

Bu modul cTrader'ga yoki tarmoqqa umuman ulanmaydi — sof matematika va holat
(state) boshqaruvi. Shu tufayli osongina alohida test qilinishi mumkin.

Barcha pul miqdorlari va foizlar `float` sifatida ishlatiladi, lekin moliyaviy
hisob-kitoblarda `Decimal` ishlatish yaxshiroq amaliyot — bu yerda soddalik
uchun float qoldirilgan; agar aniqlik muhim bo'lsa (masalan brokeringiz juda
kichik farqlarga sezgir bo'lsa), Decimal'ga o'tkazish tavsiya etiladi.
"""

from __future__ import annotations

import datetime as _dt
import logging
import math
import threading
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger("risk_manager")


class SignalRejected(Exception):
    """Signal risk qoidalariga ko'ra rad etilganda ko'tariladi."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


@dataclass
class OpenPositionRisk:
    """Hozir ochiq turgan bitta pozitsiyaning risk ma'lumoti."""

    position_id: str
    risk_percent: float  # ushbu pozitsiya ochilganda hisoblangan haqiqiy risk %


@dataclass
class RiskConfig:
    risk_percent: float = 1.0
    max_risk_percent_per_trade: float = 5.0
    max_total_open_risk_percent: float = 10.0
    daily_loss_limit_percent: float = 20.0
    min_lot_size: float = 0.01
    lot_step: float = 0.01


@dataclass
class SizingResult:
    lot_size: float
    ideal_lot_size: float
    real_risk_percent: float
    risk_amount: float


class RiskManager:
    """
    Thread-safe risk holatini saqlaydi (ochiq pozitsiyalar ro'yxati, kunlik
    zarar hisobi). Worker doim ishlaydigan process bo'lgani uchun bu holat
    xotirada saqlanadi; process qayta ishga tushsa (deploy, restart) holat
    yo'qoladi — shuning uchun kunlik hisobni broker'dan ham davriy tekshirib
    turish tavsiya etiladi (masalan hisob balansi tarixi orqali), lekin bu
    boshlang'ich versiya uchun soddalashtirilgan xotira-ichi yechim.
    """

    def __init__(self, config: RiskConfig):
        self.config = config
        self._lock = threading.RLock()
        self._open_positions: dict[str, OpenPositionRisk] = {}
        # 2026-09-17 O'ZGARTIRILDI: avval faqat zararlar yig'ilardi (foyda
        # hech qachon hisobga olinmasdi) - bu shuni anglatardi: kun net
        # foydada bo'lsa ham, ketma-ket bir nechta zararli bitim kunlik
        # limitni "to'ldirib", botni to'xtatib qo'yishi mumkin edi. Endi
        # KUN BOSHIDAGI balans bilan HOZIRGI balans solishtiriladi (net) -
        # foyda va zarar bir-birini muvozanatlaydi, xuddi haqiqiy kunlik
        # drawdown kabi.
        self._daily_start_balance: Optional[float] = None
        self._daily_net_percent: float = 0.0
        self._daily_reset_date: _dt.date = _dt.datetime.utcnow().date()
        self._trading_halted_until: Optional[_dt.date] = None

    # ------------------------------------------------------------------
    # Kunlik holatni yangilash
    # ------------------------------------------------------------------
    def _maybe_reset_daily(self) -> None:
        today = _dt.datetime.utcnow().date()
        if today != self._daily_reset_date:
            logger.info(
                "Kunlik risk reset: %s -> %s (oldingi net: %.2f%%)",
                self._daily_reset_date,
                today,
                self._daily_net_percent,
            )
            self._daily_reset_date = today
            self._daily_net_percent = 0.0
            # Kun boshi balansi ham reset qilinadi - keyingi
            # register_balance_update() chaqiruvi uni yangi kun uchun
            # qayta o'rnatadi (hozirgi balans = yangi kun boshi balansi).
            self._daily_start_balance = None
            self._trading_halted_until = None

    def register_balance_update(self, current_balance: float) -> None:
        """
        Kun boshida (worker ishga tushganda) VA har pozitsiya
        yopilganidan keyin chaqiriladi. Birinchi chaqiruv - kun boshi
        balansini o'rnatadi. Keyingi chaqiruvlar - NET (foyda-zarar
        birga hisoblangan) foizni kun boshi balansiga nisbatan
        yangilaydi.
        """
        with self._lock:
            self._maybe_reset_daily()
            if self._daily_start_balance is None:
                self._daily_start_balance = current_balance
                self._daily_net_percent = 0.0
                logger.info(
                    "Kunlik boshlang'ich balans o'rnatildi: %.2f", current_balance
                )
                return
            if self._daily_start_balance <= 0:
                return
            self._daily_net_percent = (
                (current_balance - self._daily_start_balance)
                / self._daily_start_balance
                * 100.0
            )
            logger.info(
                "Kunlik net holat: %.2f%% (kun boshi=%.2f, hozir=%.2f, limit=-%.2f%%)",
                self._daily_net_percent,
                self._daily_start_balance,
                current_balance,
                self.config.daily_loss_limit_percent,
            )

    def is_trading_halted(self) -> bool:
        with self._lock:
            self._maybe_reset_daily()
            return self._daily_net_percent <= -self.config.daily_loss_limit_percent

    # ------------------------------------------------------------------
    # Ochiq pozitsiyalarni kuzatish
    # ------------------------------------------------------------------
    def register_open_position(self, position_id: str, risk_percent: float) -> None:
        with self._lock:
            self._open_positions[position_id] = OpenPositionRisk(
                position_id=position_id, risk_percent=risk_percent
            )
            logger.info(
                "Yangi ochiq pozitsiya ro'yxatga olindi: %s (risk %.2f%%), jami ochiq risk endi %.2f%%",
                position_id,
                risk_percent,
                self.total_open_risk_percent(),
            )

    def unregister_position(self, position_id: str) -> None:
        with self._lock:
            removed = self._open_positions.pop(position_id, None)
            if removed:
                logger.info(
                    "Pozitsiya ro'yxatdan o'chirildi: %s, jami ochiq risk endi %.2f%%",
                    position_id,
                    self.total_open_risk_percent(),
                )

    def update_position_risk(self, position_id: str, new_risk_percent: float) -> None:
        """
        Pozitsiyaning SL'i harakatlanganda (masalan breakeven'ga yoki
        foydaga o'tganda), uning "band qilingan" risk foizini YANGILAYDI.

        MUHIM: bu funksiya risk%ni FAQAT kamaytirish uchun ishlatiladi
        (SL har doim foyda tomon suriladi, hech qachon zararni oshirmaydi —
        bu bizning trailing qoidamizning o'zidan kelib chiqadi). Shunga
        qaramay, xavfsizlik uchun yangi qiymat manfiy bo'lmasligi
        tekshiriladi (0 dan kichik bo'lsa 0'ga tenglashtiriladi).
        """
        with self._lock:
            pos = self._open_positions.get(position_id)
            if pos is None:
                logger.warning(
                    "update_position_risk: noma'lum position_id: %s", position_id
                )
                return
            old_risk = pos.risk_percent
            pos.risk_percent = max(new_risk_percent, 0.0)
            logger.info(
                "Pozitsiya %s risk%% yangilandi: %.2f%% -> %.2f%% "
                "(jami ochiq risk endi %.2f%%)",
                position_id,
                old_risk,
                pos.risk_percent,
                self.total_open_risk_percent(),
            )

    def total_open_risk_percent(self) -> float:
        with self._lock:
            return sum(p.risk_percent for p in self._open_positions.values())

    # ------------------------------------------------------------------
    # Position sizing
    # ------------------------------------------------------------------
    def calculate_position_size(
        self,
        balance: float,
        entry_price: float,
        sl_price: float,
        pip_value_per_lot: float,
    ) -> SizingResult:
        """
        balance              -- hisobdagi joriy balans (valyutada, masalan USD)
        entry_price, sl_price -- signal kelgan narxlar
        pip_value_per_lot    -- 1.0 lot uchun, SL masofasining har $1 birligi
                                 qancha real pul zarariga teng ekanini bildiruvchi
                                 koeffitsient. XAUUSD uchun odatda 1.0 lot = 100 oz,
                                 ya'ni $1 narx harakati = $100 (aniq qiymatni broker
                                 symbol ma'lumotidan olish kerak — pastga qarang).

        Qaytaradi: SizingResult (lot_size — YAKUNIY qo'llaniladigan lot,
                                  butun broker minimumi va qadamiga moslashtirilgan)

        Agar 0.01 lot bilan ham haqiqiy risk% > max_risk_percent_per_trade
        bo'lsa, SignalRejected ko'taradi.
        """
        if balance <= 0:
            raise SignalRejected("Balans 0 yoki manfiy — hisoblash imkonsiz")

        sl_distance = abs(entry_price - sl_price)
        if sl_distance <= 0:
            raise SignalRejected("SL masofasi 0 — noto'g'ri signal")

        risk_amount_target = balance * (self.config.risk_percent / 100.0)

        # Ideal lot: SL masofasi $ da qancha zarar keltirishini hisoblab,
        # target risk summasiga mos lotni topamiz.
        loss_per_lot = sl_distance * pip_value_per_lot
        if loss_per_lot <= 0:
            raise SignalRejected("pip_value_per_lot noto'g'ri sozlangan (<=0)")

        ideal_lot = risk_amount_target / loss_per_lot

        # Broker qadamiga (lot_step) yaxlitlash, pastga qarab (haddan tashqari
        # risk olib qo'ymaslik uchun har doim PASTGA yaxlitlanadi)
        step = self.config.lot_step
        rounded_lot = math.floor(ideal_lot / step) * step
        rounded_lot = round(rounded_lot, 2)

        final_lot = max(rounded_lot, self.config.min_lot_size)

        real_risk_amount = final_lot * loss_per_lot
        real_risk_percent = (real_risk_amount / balance) * 100.0

        logger.info(
            "Position sizing: balans=%.2f, SL_masofa=%.4f, ideal_lot=%.4f, "
            "yakuniy_lot=%.2f, haqiqiy_risk=%.2f%%",
            balance,
            sl_distance,
            ideal_lot,
            final_lot,
            real_risk_percent,
        )

        if real_risk_percent > self.config.max_risk_percent_per_trade:
            raise SignalRejected(
                f"Minimal lot ({final_lot}) bilan ham haqiqiy risk "
                f"{real_risk_percent:.2f}% > ruxsat etilgan "
                f"{self.config.max_risk_percent_per_trade}%"
            )

        return SizingResult(
            lot_size=final_lot,
            ideal_lot_size=ideal_lot,
            real_risk_percent=real_risk_percent,
            risk_amount=real_risk_amount,
        )

    # ------------------------------------------------------------------
    # Yangi signalni to'liq tekshirish (position sizing + jami risk + halt)
    # ------------------------------------------------------------------
    def evaluate_new_signal(
        self,
        balance: float,
        entry_price: float,
        sl_price: float,
        pip_value_per_lot: float,
    ) -> SizingResult:
        with self._lock:
            self._maybe_reset_daily()

            if self.is_trading_halted():
                raise SignalRejected(
                    f"Kunlik zarar limiti ({self.config.daily_loss_limit_percent}%) "
                    f"allaqachon yetib bo'lgan — yangi savdo ochilmaydi"
                )

            sizing = self.calculate_position_size(
                balance=balance,
                entry_price=entry_price,
                sl_price=sl_price,
                pip_value_per_lot=pip_value_per_lot,
            )

            current_total = self.total_open_risk_percent()
            projected_total = current_total + sizing.real_risk_percent
            if projected_total > self.config.max_total_open_risk_percent:
                raise SignalRejected(
                    f"Jami ochiq risk chegarasi oshib ketadi: "
                    f"joriy {current_total:.2f}% + yangi {sizing.real_risk_percent:.2f}% "
                    f"= {projected_total:.2f}% > limit {self.config.max_total_open_risk_percent}%"
                )

            return sizing
