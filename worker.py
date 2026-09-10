"""
worker.py
---------
Butun avtomat-savdo Worker'ining bosh fayli. Ishga tushirilganda:

  1) Environment variables'ni o'qiydi va tekshiradi (xato bo'lsa darhol
     to'xtaydi — "fail fast", noto'g'ri sozlama bilan jimgina ishlashdan
     ko'ra darhol xato berish xavfsizroq).
  2) cTrader'ga ulanadi, ilova va hisob autentifikatsiyasidan o'tadi.
  3) Kerakli symbol (XAUUSD) ma'lumotini (lot_size, digits) yuklaydi.
  4) Broker'dagi mavjud ochiq pozitsiyalarni "reconcile" qiladi (restart'dan
     keyin holatni tiklash uchun — MVP versiyada faqat log qiladi, to'liq
     state-recovery keyingi bosqichda kengaytiriladi).
  5) Flask HTTP serverini ishga tushiradi — main.py'dan signal qabul qilish
     uchun (`POST /signal`, maxfiy token bilan himoyalangan).
  6) Alohida background thread'da har 1 daqiqada barcha ochiq pozitsiyalarni
     tekshiradigan trailing siklini ishga tushiradi.

XAVFSIZLIK ESLATMASI (DEMO/LIVE):
  DEMO_MODE environment variable ANIQ "true" yoki "false" bo'lishi kerak.
  Agar noaniq/bo'sh bo'lsa — dastur ataylab XATO berib to'xtaydi (default
  qiymat DEMO emas, chunki "aniq bo'lmagan holatda xavfsiz tomonga og'ish"
  printsipi shu yerda TESKARI ishlaydi: agar kimdir DEMO_MODE'ni unutib
  qoldirsa, dastur "demo deb hisoblab" jim ishlab ketishi ham, "live deb
  hisoblab" jim ishlab ketishi ham xavfli — shuning uchun unutilgan holatda
  umuman ishga tushmasligi kerak).
"""

from __future__ import annotations

import hmac
import logging
import os
import threading
import time
from typing import Optional

from flask import Flask, jsonify, request

from ctrader_client import CTraderClient, CTraderError, SymbolInfo
from ctrader_open_api.messages.OpenApiModelMessages_pb2 import ProtoOAPositionStatus
from risk_manager import RiskConfig, RiskManager, SignalRejected
from trade_manager import ManagedPosition, TradeManager, TradeSide

# ----------------------------------------------------------------------
# LOGGING
# ----------------------------------------------------------------------
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("worker")


# ----------------------------------------------------------------------
# KONFIGURATSIYANI O'QISH VA TEKSHIRISH (fail fast)
# ----------------------------------------------------------------------
def _require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(
            f"MAJBURIY environment variable sozlanmagan: {name}. "
            f"Dastur xavfsizlik nuqtai nazaridan ishga tushmaydi."
        )
    return value


def _parse_demo_mode() -> bool:
    raw = os.environ.get("DEMO_MODE", "").strip().lower()
    if raw not in ("true", "false"):
        raise RuntimeError(
            "DEMO_MODE environment variable ANIQ 'true' yoki 'false' bo'lishi "
            "shart (bo'sh yoki boshqa qiymat qabul qilinmaydi)."
        )
    return raw == "true"


WORKER_SECRET_KEY = _require_env("WORKER_SECRET_KEY")
CTRADER_CLIENT_ID = _require_env("CTRADER_CLIENT_ID")
CTRADER_CLIENT_SECRET = _require_env("CTRADER_CLIENT_SECRET")
CTRADER_ACCESS_TOKEN = _require_env("CTRADER_ACCESS_TOKEN")
CTRADER_ACCOUNT_ID = int(_require_env("CTRADER_ACCOUNT_ID"))
DEMO_MODE = _parse_demo_mode()

SYMBOL_NAME = os.environ.get("SYMBOL_NAME", "XAUUSD")

RISK_CONFIG = RiskConfig(
    risk_percent=float(os.environ.get("RISK_PERCENT", "1.0")),
    max_risk_percent_per_trade=float(os.environ.get("MAX_RISK_PERCENT_PER_TRADE", "5.0")),
    max_total_open_risk_percent=float(os.environ.get("MAX_TOTAL_OPEN_RISK_PERCENT", "10.0")),
    daily_loss_limit_percent=float(os.environ.get("DAILY_LOSS_LIMIT_PERCENT", "20.0")),
    min_lot_size=float(os.environ.get("MIN_LOT_SIZE", "0.01")),
    lot_step=float(os.environ.get("LOT_STEP", "0.01")),
)

logger.warning(
    "ISHGA TUSHYAPTI: REJIM = %s | ACCOUNT_ID = %s | SYMBOL = %s",
    "DEMO" if DEMO_MODE else "!!! LIVE — HAQIQIY PUL !!!",
    CTRADER_ACCOUNT_ID,
    SYMBOL_NAME,
)


# ----------------------------------------------------------------------
# GLOBAL HOLAT (modul darajasida, chunki Flask + background thread
# bir xil process ichida shu obyektlarni baham ko'radi)
# ----------------------------------------------------------------------
risk_manager = RiskManager(RISK_CONFIG)
trade_manager = TradeManager()
_latest_prices: dict[int, float] = {}
_latest_price_timestamps: dict[int, float] = {}
_latest_prices_lock = threading.Lock()
_symbol_info: Optional[SymbolInfo] = None
_price_feed_ever_received = False


def _on_spot_price(symbol_id: int, raw_bid: int) -> None:
    """cTrader xom narxni butun sonda yuboradi; digits asosida o'nlik qilamiz."""
    global _symbol_info, _price_feed_ever_received
    if _symbol_info is None:
        return
    real_price = raw_bid / (10 ** _symbol_info.digits)
    with _latest_prices_lock:
        _latest_prices[symbol_id] = real_price
        _latest_price_timestamps[symbol_id] = time.time()

    # MUHIM: har bir yangi tick kelganda (millisekundlarda, 60 soniyalik
    # trailing siklidan MUSTAQIL) barcha ochiq pozitsiyalarning "eng yaxshi
    # erishilgan narxi" yangilanadi — shunda narx checkpoint'ga tegib,
    # keyingi trailing tekshiruvigacha ORQAGA qaytib ketgan bo'lsa ham,
    # checkpoint o'tkazib yuborilmaydi.
    trade_manager.update_best_price(real_price)

    if not _price_feed_ever_received:
        _price_feed_ever_received = True
        logger.info(
            "Narx oqimi TASDIQLANDI — birinchi narx qabul qilindi: symbol_id=%s narx=%s",
            symbol_id,
            real_price,
        )


def _on_ctrader_error(message: str) -> None:
    logger.error("cTrader xatosi (alert yuborilishi kerak): %s", message)
    # TODO: bu yerga mavjud Telegram/ntfy alert funksiyasini ulash tavsiya
    # etiladi (masalan requests.post orqali Telegram Bot API'ga xabar).


def _on_execution_event(event) -> None:
    logger.info("Execution event qabul qilindi: %s", event)
    # 1-QATLAM: pozitsiya broker tomonidan yopilganini REAL-VAQTDA aniqlash
    # (SL/TP urilgani, qo'lda yopilgani va h.k.) — aniqlangan zahoti
    # xotiradan (trade_manager) va risk hisobidan (risk_manager) olib
    # tashlanadi, shunda keyingi signallar noto'g'ri "jami risk to'lgan"
    # deb rad etilmaydi.
    try:
        if event.HasField("position") and event.position.positionStatus == ProtoOAPositionStatus.POSITION_STATUS_CLOSED:
            pid = event.position.positionId
            still_tracked = any(p.position_id == pid for p in trade_manager.get_all_positions())
            if still_tracked:
                trade_manager.remove_position(pid)
                risk_manager.unregister_position(str(pid))
                logger.info(
                    "Pozitsiya %s YOPILGANI ANIQLANDI (execution event orqali) — "
                    "kuzatuvdan va risk hisobidan olib tashlandi",
                    pid,
                )
    except Exception:  # noqa: BLE001
        logger.exception("Execution event orqali yopiq pozitsiyani aniqlashda xato")


client = CTraderClient(
    client_id=CTRADER_CLIENT_ID,
    client_secret=CTRADER_CLIENT_SECRET,
    access_token=CTRADER_ACCESS_TOKEN,
    account_id=CTRADER_ACCOUNT_ID,
    demo_mode=DEMO_MODE,
    on_execution_event=_on_execution_event,
    on_error=_on_ctrader_error,
    on_spot_price=_on_spot_price,
)


def initialize_ctrader() -> None:
    """Ulanish, autentifikatsiya va symbol yuklashni bajaradi (bloklovchi)."""
    global _symbol_info

    client.start()
    if not client.wait_until_ready(timeout=30):
        raise RuntimeError(
            "cTrader'ga 30 soniya ichida ulanib, autentifikatsiyadan o'tib "
            "bo'lmadi. WORKER ISHGA TUSHMAYDI."
        )
    logger.info("cTrader ulanishi va autentifikatsiya muvaffaqiyatli yakunlandi")

    client.load_symbols([SYMBOL_NAME], timeout=15)
    _symbol_info = client.get_symbol_info(SYMBOL_NAME)
    logger.info("Symbol ma'lumoti tayyor: %s", _symbol_info)

    client.subscribe_spots([_symbol_info.symbol_id])
    _recover_orphan_positions()


def _recover_orphan_positions() -> None:
    """
    Worker ishga tushganda (deploy/restart'dan keyin) broker'da hali ochiq
    turgan, lekin xotirada "yo'qolgan" pozitsiyalarni topib, KUZATUVGA
    QAYTA QO'SHADI — faqat risk-hisob va yopilishni aniqlash uchun.

    MUHIM CHEKLOV (ataylab, xavfsizlik uchun): bunday pozitsiyalar uchun
    TP-checkpoint asosidagi SL trailing ISHLAMAYDI, chunki asl TP2-TP15
    darajalari (faqat signal payload'ida bo'lgan) qayta tiklab bo'lmaydi.
    Buni taxmin qilish SL'ni noto'g'ri joyga surib qo'yish xavfini
    tug'diradi — shuning uchun "hech narsa qilmaslik" (faqat kuzatish)
    "noto'g'ri taxmin qilish"dan XAVFSIZROQ deb hisoblanadi.
    """
    if _symbol_info is None:
        return

    try:
        broker_positions = client.get_open_positions_full(timeout=10)
    except CTraderError as exc:
        logger.warning(
            "Ishga tushishda broker pozitsiyalarini olib bo'lmadi (keyinroq "
            "reconcile sikli qayta urinadi): %s",
            exc,
        )
        return

    already_tracked_ids = {p.position_id for p in trade_manager.get_all_positions()}

    for bp in broker_positions:
        if bp["symbol_id"] != _symbol_info.symbol_id:
            continue  # faqat bizning symbolimiz (XAUUSD) bilan ishlaymiz
        if bp["position_id"] in already_tracked_ids:
            continue

        entry_price = bp["entry_price"]
        current_sl = bp["current_sl"]
        current_tp = bp["current_tp"]

        if current_sl is None:
            logger.warning(
                "Pozitsiya %s broker'da SL'siz topildi — xavfsizlik uchun "
                "tiklanmaydi, qo'lda tekshirish tavsiya etiladi (cTrader "
                "terminalida)",
                bp["position_id"],
            )
            continue

        side_enum = TradeSide.BUY if bp["side"] == "BUY" else TradeSide.SELL

        # tp2..tp15 uchun aniq qiymat yo'q — barchasiga joriy TP qo'yamiz.
        # Bu qiymatlar ishlatilmaydi ham (trailing_enabled=False bo'lgani
        # uchun evaluate() darhol None qaytaradi), shunchaki dataclass
        # maydonini to'ldirish uchun.
        placeholder_tp = current_tp if current_tp is not None else entry_price

        managed = ManagedPosition(
            position_id=bp["position_id"],
            event_key=f"recovered-{bp['position_id']}",
            side=side_enum,
            entry_price=entry_price,
            initial_sl=current_sl,
            tp2=placeholder_tp,
            tp3=placeholder_tp,
            tp5=placeholder_tp,
            tp10=placeholder_tp,
            tp15=placeholder_tp,
            volume_units=bp["volume_units"],
            risk_percent=0.0,  # pastda dinamik hisoblanadi
            trailing_enabled=False,
        )
        trade_manager.add_position(managed)

        # Risk% ni joriy SL masofasidan hisoblaymiz (bu — haqiqiy joriy
        # xavf, "boshlang'ich" emas, chunki boshlang'ich ma'lumot yo'qolgan).
        try:
            balance = client.get_account_balance(timeout=10)
            pip_value_per_lot = float(_symbol_info.lot_size) / 100.0
            lots = bp["volume_units"] / _symbol_info.lot_size
            sl_distance = abs(entry_price - current_sl)
            risk_amount = lots * sl_distance * pip_value_per_lot
            risk_percent = (risk_amount / balance) * 100.0 if balance > 0 else 0.0
        except Exception:  # noqa: BLE001
            logger.exception(
                "Pozitsiya %s uchun risk% hisoblab bo'lmadi — 0%% deb belgilanadi",
                bp["position_id"],
            )
            risk_percent = 0.0

        risk_manager.register_open_position(str(bp["position_id"]), risk_percent)

        logger.warning(
            "TIKLANDI (orphan): pos=%s %s entry=%s SL=%s TP=%s risk=%.2f%% "
            "— TP-CHECKPOINT TRAILING O'CHIRILGAN (faqat kuzatuv/risk-hisob ishlaydi)",
            bp["position_id"],
            bp["side"],
            entry_price,
            current_sl,
            current_tp,
            risk_percent,
        )


# ----------------------------------------------------------------------
# YANGI SIGNALNI QAYTA ISHLASH
# ----------------------------------------------------------------------
def handle_new_signal(payload: dict) -> dict:
    """
    main.py'dan kelgan signal payload'ini qabul qilib:
      1) risk_manager orqali lot hisoblaydi va tekshiradi
      2) cTrader'ga market order yuboradi (TP=TP5, SL=boshlang'ich)
      3) muvaffaqiyatli bo'lsa trade_manager'ga kuzatuvga qo'shadi

    Qaytaradi: {"status": "ok"/"rejected"/"error", "detail": "..."}
    """
    if _symbol_info is None:
        return {"status": "error", "detail": "Symbol ma'lumoti hali yuklanmagan"}

    try:
        direction = str(payload["direction"]).upper()
        entry_price = float(payload["entry_price"])
        sl_price = float(payload["sl_price"])
        tp2 = float(payload["tp2"])
        tp3 = float(payload["tp3"])
        tp5 = float(payload["tp5"])
        tp10 = float(payload["tp10"])
        tp15 = float(payload["tp15"])
        event_key = str(payload.get("event_key", ""))
    except (KeyError, ValueError, TypeError) as exc:
        logger.error("Signal payload noto'g'ri formatda: %s", exc)
        return {"status": "error", "detail": f"Noto'g'ri payload: {exc}"}

    if direction not in ("BUY", "SELL"):
        return {"status": "error", "detail": f"Noma'lum direction: {direction}"}

    # pip_value_per_lot: XAUUSD uchun 1.0 lot = 100 untsiya (broker odatiy
    # standarti). symbol_info.lot_size (masalan 10000) — bu cTrader'ning
    # VOLUME MAYDONI uchun ICHKI MASSHTABLANGAN birligi (0.01 lot->100 unit
    # yuborish uchun ishlatiladi, bu TO'G'RI), lekin haqiqiy $ hisob-kitobida
    # ishlatib bo'lmaydi — shuning uchun 100'ga bo'linadi (cTrader har doim
    # shu birlikni 100x masshtabda beradi). BU QIYMATNI HAQIQIY TEST ORDER
    # BILAN QAYTA TASDIQLASH TAVSIYA ETILADI.
    pip_value_per_lot = float(_symbol_info.lot_size) / 100.0

    # Broker faqat symbol.digits (XAUUSD uchun odatda 2) gacha o'nlik xona
    # qabul qiladi — Python hisob-kitoblaridan kelgan uzun o'nlik sonlarni
    # (masalan 4388.451850000003) yaxlitlab yuborish SHART, aks holda
    # broker "INVALID_REQUEST" bilan rad etadi.
    digits = _symbol_info.digits
    sl_price = round(sl_price, digits)
    tp2 = round(tp2, digits)
    tp3 = round(tp3, digits)
    tp5 = round(tp5, digits)
    tp10 = round(tp10, digits)
    tp15 = round(tp15, digits)

    try:
        balance = client.get_account_balance(timeout=10)
    except CTraderError as exc:
        logger.error("Balansni olishda xato: %s", exc)
        return {"status": "error", "detail": f"Balans olinmadi: {exc}"}

    try:
        sizing = risk_manager.evaluate_new_signal(
            balance=balance,
            entry_price=entry_price,
            sl_price=sl_price,
            pip_value_per_lot=pip_value_per_lot,
        )
    except SignalRejected as exc:
        logger.warning("Signal RAD ETILDI: %s", exc.reason)
        return {"status": "rejected", "detail": exc.reason}

    volume_units = round(sizing.lot_size * _symbol_info.lot_size)

    side_enum = TradeSide.BUY if direction == "BUY" else TradeSide.SELL

    deferred = client.send_market_order(
        symbol_id=_symbol_info.symbol_id,
        direction=direction,
        volume_in_units=volume_units,
        sl_price=sl_price,
        tp_price=tp5,
        label=event_key or "smc-auto",
        comment=f"risk={sizing.real_risk_percent:.2f}%",
    )

    result_holder: dict = {}
    done = threading.Event()

    def _on_order_success(response):
        try:
            from ctrader_open_api import Protobuf

            extracted = Protobuf.extract(response)
            # Turli javob tuzilmalarida positionId turli joyda bo'lishi
            # mumkin (ExecutionEvent'ning versiyasiga qarab). Barcha
            # mumkin bo'lgan joylarni tekshirib, birinchi NOL BO'LMAGAN
            # (haqiqiy) qiymatni olamiz — proto3'da 0 "o'rnatilmagan"
            # degani, shuning uchun 0'ni haqiqiy ID sifatida qabul qilmaymiz.
            candidates = []
            if hasattr(extracted, "position") and extracted.HasField("position"):
                candidates.append(getattr(extracted.position, "positionId", 0))
            if hasattr(extracted, "order") and extracted.HasField("order"):
                candidates.append(getattr(extracted.order, "positionId", 0))
            top_level = getattr(extracted, "positionId", 0)
            candidates.append(top_level)

            position_id = next((c for c in candidates if c), None)
            result_holder["position_id"] = position_id
            result_holder["raw_response"] = str(extracted)
        except Exception:  # noqa: BLE001
            logger.exception("Order javobini o'qishda xato")
        finally:
            done.set()

    def _on_order_error(failure):
        result_holder["error"] = str(failure)
        done.set()

    deferred.addCallback(_on_order_success)
    deferred.addErrback(_on_order_error)

    if not done.wait(timeout=10):
        logger.error("Order yuborilgandan keyin 10s ichida javob kelmadi")
        return {"status": "error", "detail": "Broker javob bermadi (timeout)"}

    if "error" in result_holder:
        return {"status": "error", "detail": result_holder["error"]}

    position_id = result_holder.get("position_id")
    if position_id is None:
        logger.error(
            "Order yuborildi, lekin position_id javobda topilmadi. Xom javob: %s "
            "— reconcile_open_positions() orqali qo'lda tekshirish tavsiya etiladi",
            result_holder.get("raw_response", "(yo'q)"),
        )
        return {
            "status": "error",
            "detail": "Order yuborildi, lekin position_id aniqlanmadi",
        }

    managed = ManagedPosition(
        position_id=position_id,
        event_key=event_key,
        side=side_enum,
        entry_price=entry_price,
        initial_sl=sl_price,
        tp2=tp2,
        tp3=tp3,
        tp5=tp5,
        tp10=tp10,
        tp15=tp15,
        volume_units=volume_units,
        risk_percent=sizing.real_risk_percent,
    )
    trade_manager.add_position(managed)
    risk_manager.register_open_position(str(position_id), sizing.real_risk_percent)

    logger.info(
        "POZITSIYA OCHILDI: id=%s %s lot=%.2f risk=%.2f%%",
        position_id,
        direction,
        sizing.lot_size,
        sizing.real_risk_percent,
    )

    return {
        "status": "ok",
        "position_id": position_id,
        "lot_size": sizing.lot_size,
        "real_risk_percent": sizing.real_risk_percent,
    }


# ----------------------------------------------------------------------
# TRAILING SIKLI (background thread, har 1 daqiqada)
# ----------------------------------------------------------------------
def _get_current_price(symbol_id: int) -> Optional[float]:
    with _latest_prices_lock:
        return _latest_prices.get(symbol_id)


def trailing_loop(interval_seconds: int = 60) -> None:
    logger.info("Trailing sikli ishga tushdi (har %s soniyada)", interval_seconds)
    RECONCILE_EVERY_N_CYCLES = 5  # taxminan har 5 daqiqada bir (5 x 60s)
    cycle_count = 0
    while True:
        try:
            _run_trailing_check_once()
            cycle_count += 1
            if cycle_count >= RECONCILE_EVERY_N_CYCLES:
                cycle_count = 0
                _reconcile_positions_with_broker()
        except Exception:  # noqa: BLE001
            logger.exception("Trailing tekshiruvida kutilmagan xato")
        time.sleep(interval_seconds)


def _reconcile_positions_with_broker() -> None:
    """
    2-QATLAM (xavfsizlik to'ri): execution event orqali 1-qatlam biror
    sababdan (masalan qisqa tarmoq uzilishi, Worker restart) pozitsiya
    yopilganini "eshitib qolmagan" bo'lsa ham, bu davriy tekshiruv
    xotiradagi holatni broker'ning haqiqiy holati bilan solishtirib,
    allaqachon yopilgan pozitsiyalarni kuzatuvdan tozalaydi.
    """
    try:
        broker_open_ids = client.get_open_position_ids(timeout=10)
    except CTraderError as exc:
        logger.warning("Reconcile: broker'dan ochiq pozitsiyalar ro'yxati olinmadi: %s", exc)
        return

    for pos in trade_manager.get_all_positions():
        if pos.position_id not in broker_open_ids:
            logger.warning(
                "RECONCILE: pozitsiya %s xotirada 'ochiq' turibdi, lekin broker'da "
                "endi yo'q — kuzatuvdan va risk hisobidan olib tashlanmoqda",
                pos.position_id,
            )
            trade_manager.remove_position(pos.position_id)
            risk_manager.unregister_position(str(pos.position_id))


def _compute_dynamic_risk_percent(pos, current_sl: float) -> float:
    """
    Pozitsiyaning INITIAL risk%'ini SL harakatiga qarab qayta hisoblaydi.

    Mantiq: risk% — SL masofasiga chiziqli bog'liq (lot hajmi o'zgarmagani
    uchun). Agar SL entry narxidan "xavfsiz" tomonga o'tgan bo'lsa (BUY uchun
    SL >= entry, SELL uchun SL <= entry) — bu pozitsiya ENDI ZARAR KELTIRA
    OLMAYDI, demak uning risk%i 0 bo'lishi kerak (yangi savdolar uchun to'liq
    "joy" bo'shatiladi). Aks holda, joriy SL masofasi boshlang'ich SL
    masofasiga nisbatan qanday ulushni tashkil etsa, risk% ham shunga mos
    kichraytiriladi.
    """
    if pos.side == TradeSide.BUY:
        current_distance = max(pos.entry_price - current_sl, 0.0)
    else:
        current_distance = max(current_sl - pos.entry_price, 0.0)

    initial_distance = abs(pos.entry_price - pos.initial_sl)
    if initial_distance <= 0:
        return 0.0

    dynamic_risk = pos.risk_percent * (current_distance / initial_distance)
    # Xavfsizlik: SL bizning qoidamizga ko'ra hech qachon zararni
    # oshirmasligi kerak, lekin ehtiyot uchun yuqori chegara bilan cheklaymiz.
    return min(dynamic_risk, pos.risk_percent)


_no_price_warning_count = 0


def _run_trailing_check_once() -> None:
    global _no_price_warning_count
    if _symbol_info is None:
        return

    current_price = _get_current_price(_symbol_info.symbol_id)
    if current_price is None:
        _no_price_warning_count += 1
        # Birinchi bir necha marta DEBUG (odatiy, ulanish endigina
        # boshlanayotgan bo'lishi mumkin), lekin agar bu davom etsa —
        # bu ANIQ muammo, va uni ko'rinadigan (WARNING) qilish SHART,
        # aks holda trailing "sababsiz" ishlamay qolishi mumkin (buni
        # avvalgi versiyada DEBUG darajasida yashirilgani sababli sezmay
        # qolgan edik).
        if _no_price_warning_count <= 3:
            logger.debug("Hali joriy narx kelmagan (spot subscription kutilmoqda)")
        else:
            logger.warning(
                "NARX HALI YO'Q — %s marta ketma-ket! Spot subscription "
                "ishlamayotgan bo'lishi mumkin. Ochiq pozitsiyalar TRAILING "
                "QILINMAYAPTI.",
                _no_price_warning_count,
            )
        return

    _no_price_warning_count = 0

    for pos in trade_manager.get_all_positions():
        action = trade_manager.evaluate(pos.position_id, current_price)
        if action is None:
            continue

        # SL/TP endi to'liq trade_manager.evaluate() ichida, mavjud TP
        # darajalariga asoslanib hisoblanadi (tashqi swing-hisoblash
        # kerak emas — ishonchli va oldindan aniq).
        new_sl = action.new_sl
        new_tp = action.new_tp

        if new_sl is not None or new_tp is not None:
            digits = _symbol_info.digits if _symbol_info else 2

            # MUHIM: cTrader amend so'rovi TO'LIQ holatni kutadi — agar
            # faqat o'zgargan maydon (masalan SL) yuborilib, TP jo'natilmasa,
            # broker buni "TP'ni OLIB TASHLA" deb tushunishi mumkin. Shuning
            # uchun har doim IKKALASINI HAM (joriy holat asosida) birga
            # yuboramiz — hech qachon faqat bittasini emas.
            final_sl = new_sl if new_sl is not None else pos.current_sl
            final_tp = new_tp if new_tp is not None else pos.current_tp
            final_sl = round(final_sl, digits) if final_sl is not None else None
            final_tp = round(final_tp, digits) if final_tp is not None else None

            client.amend_position_sl_tp(
                position_id=pos.position_id,
                sl_price=final_sl,
                tp_price=final_tp,
            )
            logger.info(
                "TRAILING AMALGA OSHIRILDI: pos=%s sabab=%s SL=%s TP=%s",
                pos.position_id,
                action.reason,
                final_sl,
                final_tp,
            )

            # SL o'zgargan bo'lsa (bu safar aynan shu tekshiruvda), risk-modul
            # dagi "band qilingan" risk%ni ham qayta hisoblaymiz — SL
            # foyda/breakeven tomon surilgan bo'lsa, kelasi signal(lar) uchun
            # jami risk chegarasidan avtomatik "joy bo'shaydi".
            if new_sl is not None:
                dynamic_risk = _compute_dynamic_risk_percent(pos, final_sl)
                risk_manager.update_position_risk(str(pos.position_id), dynamic_risk)


# ----------------------------------------------------------------------
# FLASK HTTP SERVER
# ----------------------------------------------------------------------
app = Flask(__name__)


def _is_authorized(req) -> bool:
    auth_header = req.headers.get("Authorization", "")
    expected = f"Bearer {WORKER_SECRET_KEY}"
    # hmac.compare_digest — timing-attack'lardan himoya qiluvchi taqqoslash
    return hmac.compare_digest(auth_header, expected)


@app.route("/signal", methods=["POST"])
def receive_signal():
    if not _is_authorized(request):
        logger.warning("Ruxsatsiz so'rov: %s", request.remote_addr)
        return jsonify({"status": "error", "detail": "unauthorized"}), 401

    payload = request.get_json(silent=True)
    if not payload:
        return jsonify({"status": "error", "detail": "JSON body kerak"}), 400

    logger.info("Yangi signal qabul qilindi: %s", payload)
    result = handle_new_signal(payload)

    status_code = 200 if result.get("status") == "ok" else 400
    return jsonify(result), status_code


@app.route("/health", methods=["GET"])
def health_check():
    """Render'ning health-check'i uchun — autentifikatsiyasiz, maxfiy ma'lumotsiz."""
    return jsonify(
        {
            "status": "ok" if client.is_ready() else "connecting",
            "demo_mode": DEMO_MODE,
            "open_positions": len(trade_manager.get_all_positions()),
        }
    )


# ----------------------------------------------------------------------
# ISHGA TUSHIRISH
# ----------------------------------------------------------------------
def main() -> None:
    initialize_ctrader()

    trailing_thread = threading.Thread(
        target=trailing_loop, name="trailing-loop", daemon=True
    )
    trailing_thread.start()

    port = int(os.environ.get("PORT", "8080"))
    logger.info("Flask server %s portda ishga tushmoqda", port)
    app.run(host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
