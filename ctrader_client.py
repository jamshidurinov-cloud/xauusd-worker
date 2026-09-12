"""
ctrader_client.py
------------------
cTrader Open API bilan bevosita ishlaydigan YAGONA modul. Boshqa hech qaysi
fayl (risk_manager.py, trade_manager.py, worker.py) to'g'ridan-to'g'ri
tarmoq/protobuf bilan ishlamaydi — hammasi shu klass orqali o'tadi.

Texnik asos: rasmiy `ctrader-open-api` (OpenApiPy) kutubxonasi, bu esa
Twisted asinxron freymvorkiga qurilgan. Ulanish TCP+SSL orqali, xabarlar
Protobuf formatida almashinadi (WebSocket emas, lekin printsip bir xil:
doimiy ochiq ulanish, ikki tomonlama xabar almashinuvi).

MUHIM: kutubxona versiyasiga qarab ba'zi Protobuf maydon nomlari va sinf
joylashuvi biroz farq qilishi mumkin. Har bir chaqiruv joyida shu narsani
paketning o'rnatilgan versiyasidagi `messages/OpenApiMessages_pb2.py`
faylidan tekshirib tasdiqlash tavsiya etiladi (`pip show ctrader-open-api`
bilan versiyani ko'rish, keyin fayl ichidan mos maydon nomini qidirish).

XAVFSIZLIK PRINSIPLARI:
  - Client ID/Secret va tokenlar FAQAT environment variables orqali keladi,
    kodda hech qachon yozilmaydi.
  - DEMO_MODE tekshiruvi bir necha joyda takrorlanadi ("defense in depth") —
    hatto konfiguratsiya xatosi bo'lsa ham, tasodifan live hisobga
    ulanib qolish ehtimoli kamaytiriladi.
  - Har bir order yuborishdan oldin qaysi hisobga (demo/live) va qaysi
    account ID'ga ulanilganini log qilib boradi — audit uchun.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass
from typing import Callable, Optional

from ctrader_open_api import Client, EndPoints, Protobuf, TcpProtocol
from ctrader_open_api.messages.OpenApiMessages_pb2 import (
    ProtoOAAccountAuthReq,
    ProtoOAAmendPositionSLTPReq,
    ProtoOAApplicationAuthReq,
    ProtoOAClosePositionReq,
    ProtoOAErrorRes,
    ProtoOAExecutionEvent,
    ProtoOANewOrderReq,
    ProtoOAReconcileReq,
    ProtoOARefreshTokenReq,
    ProtoOASpotEvent,
    ProtoOASubscribeSpotsReq,
    ProtoOASymbolByIdReq,
    ProtoOASymbolsListReq,
    ProtoOAGetTrendbarsReq,
    ProtoOATraderReq,
)
from ctrader_open_api.messages.OpenApiModelMessages_pb2 import (
    ProtoOAOrderType,
    ProtoOAPositionStatus,
    ProtoOATradeSide,
    ProtoOATrendbarPeriod,
)
from twisted.internet import reactor

logger = logging.getLogger("ctrader_client")


class CTraderError(Exception):
    """cTrader API xatoligi (masalan order rad etilganda) ko'tariladi."""


@dataclass
class SymbolInfo:
    symbol_id: int
    lot_size: int  # 1.0 lot necha "unit"ga teng (broker symbol ma'lumotidan)
    pip_position: int
    digits: int


class CTraderClient:
    """
    Yagona, doim ochiq turadigan cTrader ulanishini boshqaradi.

    Ishlatilishi (worker.py'da):
        client = CTraderClient(...)
        client.start()   # Twisted reactor'ni alohida thread'da ishga tushiradi
        client.wait_until_ready(timeout=30)
        client.send_market_order(...)
    """

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        access_token: str,
        account_id: int,
        demo_mode: bool,
        on_execution_event: Optional[Callable[[object], None]] = None,
        on_error: Optional[Callable[[str], None]] = None,
        on_spot_price: Optional[Callable[[int, float], None]] = None,
        on_spot_price_full: Optional[Callable[[int, float, float], None]] = None,
    ):
        if not client_id or not client_secret or not access_token:
            raise ValueError(
                "CTRADER_CLIENT_ID / CTRADER_CLIENT_SECRET / CTRADER_ACCESS_TOKEN "
                "bo'sh bo'lishi mumkin emas"
            )
        if not account_id:
            raise ValueError("CTRADER_ACCOUNT_ID sozlanmagan")

        self._client_id = client_id
        self._client_secret = client_secret
        self._access_token = access_token
        self._account_id = int(account_id)
        self._demo_mode = demo_mode
        self._on_execution_event = on_execution_event
        self._on_error = on_error
        self._on_spot_price = on_spot_price
        self._on_spot_price_full = on_spot_price_full

        host = EndPoints.PROTOBUF_DEMO_HOST if demo_mode else EndPoints.PROTOBUF_LIVE_HOST
        port = EndPoints.PROTOBUF_PORT

        logger.info(
            "CTraderClient sozlandi: mode=%s host=%s account_id=%s",
            "DEMO" if demo_mode else "LIVE",
            host,
            self._account_id,
        )

        # ESLATMA: avval bu yerga `timeoutForCommands=30` qo'shilgan edi
        # (ProtoOAGetTrendbarsReq'ning 5s timeout muammosini hal qilish
        # uchun), lekin bu parametr nomi NOTO'G'RI chiqdi — kutubxona buni
        # qabul qilmadi (TypeError), va bu WORKER'NI BUTUNLAY TO'XTATIB
        # QO'YDI (crash-loop). Shuning uchun DARHOL asl holatga qaytarildi.
        # Trendbar timeout muammosi hali OCHIQ — buni boshqa, xavfsizroq
        # yo'l bilan (masalan get_trendbars ichida alohida qayta urinish)
        # hal qilish kerak, ASOSIY ulanishga tegmasdan.
        self._client = Client(host, port, TcpProtocol)
        self._client.setConnectedCallback(self._on_connected)
        self._client.setDisconnectedCallback(self._on_disconnected)
        self._client.setMessageReceivedCallback(self._on_message)

        self._ready_event = threading.Event()
        self._connected_event = threading.Event()
        self._app_authenticated = False
        self._account_authenticated = False

        self._symbol_cache: dict[str, SymbolInfo] = {}
        self._reactor_thread: Optional[threading.Thread] = None

    # ------------------------------------------------------------------
    # Ishga tushirish / to'xtatish
    # ------------------------------------------------------------------
    def start(self) -> None:
        """Twisted reactor'ni alohida (daemon) thread'da ishga tushiradi."""

        def _run():
            self._client.startService()
            reactor.run(installSignalHandlers=False)

        self._reactor_thread = threading.Thread(
            target=_run, name="ctrader-reactor", daemon=True
        )
        self._reactor_thread.start()
        logger.info("cTrader reactor thread ishga tushirildi")

    def wait_until_ready(self, timeout: float = 30.0) -> bool:
        """Ulanish + ikkala autentifikatsiya tugaguncha kutadi."""
        return self._ready_event.wait(timeout=timeout)

    def is_ready(self) -> bool:
        return self._ready_event.is_set()

    # ------------------------------------------------------------------
    # Twisted callback'lari
    # ------------------------------------------------------------------
    def _on_connected(self, client) -> None:
        logger.info("cTrader serveriga TCP ulanish o'rnatildi, autentifikatsiya boshlanmoqda")
        self._connected_event.set()
        req = ProtoOAApplicationAuthReq()
        req.clientId = self._client_id
        req.clientSecret = self._client_secret
        deferred = self._client.send(req)
        deferred.addErrback(self._log_deferred_error, context="ApplicationAuth")

    def _on_disconnected(self, client, reason) -> None:
        logger.warning("cTrader ulanishi uzildi: %s. Qayta ulanishga harakat qilinadi.", reason)
        self._ready_event.clear()
        self._connected_event.clear()
        self._app_authenticated = False
        self._account_authenticated = False
        if self._on_error:
            self._on_error(f"Ulanish uzildi: {reason}")
        # ctrader-open-api Client odatda o'zi qayta ulanishga harakat qiladi
        # (ichki retry mexanizmi bilan). Qo'shimcha xavfsizlik uchun bu yerda
        # aniq monitoring/alert yuborish tavsiya etiladi (worker.py darajasida).

    def _on_message(self, client, message) -> None:
        try:
            extracted = Protobuf.extract(message)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Kelgan xabarni ochishda xato: %s", exc)
            return

        type_name = type(extracted).__name__

        if type_name == "ProtoOAApplicationAuthRes":
            logger.info("Ilova autentifikatsiyasi muvaffaqiyatli")
            self._app_authenticated = True
            self._authenticate_account()

        elif type_name == "ProtoOAAccountAuthRes":
            logger.info("Hisob autentifikatsiyasi muvaffaqiyatli: account_id=%s", self._account_id)
            self._account_authenticated = True
            self._ready_event.set()

        elif type_name == "ProtoOAErrorRes" or isinstance(extracted, ProtoOAErrorRes):
            error_code = getattr(extracted, "errorCode", "UNKNOWN")
            description = getattr(extracted, "description", "")
            logger.error("cTrader XATO: %s — %s", error_code, description)
            if self._on_error:
                self._on_error(f"{error_code}: {description}")

        elif isinstance(extracted, ProtoOAExecutionEvent) or type_name == "ProtoOAExecutionEvent":
            logger.info("Execution event keldi: %s", extracted)
            if self._on_execution_event:
                self._on_execution_event(extracted)

        elif isinstance(extracted, ProtoOASpotEvent) or type_name == "ProtoOASpotEvent":
            # MUHIM TUZATISH: extracted.HasField("bid") BU YERDA ISHLATILMAYDI —
            # ProtoOASpotEvent.bid proto3'da oddiy (optional bo'lmagan) sonli
            # maydon bo'lishi mumkin, va bunday maydonga .HasField() chaqirish
            # ValueError beradi. Bu xato ushbu funksiya try/except bilan
            # o'ralmagani uchun HAR SAFAR jimgina yutilib, natijada narx
            # HECH QACHON _on_spot_price'ga yetib bormagan — bu esa butun
            # TRAILING TIZIMINI ISHLATMAY QO'YGAN edi (joriy narx doim "yo'q"
            # deb hisoblanardi). Endi xavfsiz tekshiruv bilan almashtirildi.
            try:
                bid_value = extracted.bid
                if self._on_spot_price and bid_value:
                    self._on_spot_price(extracted.symbolId, bid_value)
                # Ixtiyoriy: agar ask ham shu xabarda kelgan bo'lsa (har doim
                # kelavermaydi — cTrader ba'zan faqat bid, ba'zan faqat ask
                # yangilanganda xabar yuboradi), /price endpoint uchun
                # alohida callback orqali uzatiladi.
                ask_value = getattr(extracted, "ask", 0)
                if self._on_spot_price_full and bid_value and ask_value:
                    self._on_spot_price_full(extracted.symbolId, bid_value, ask_value)
            except Exception:  # noqa: BLE001
                logger.exception("Spot event'ni qayta ishlashda xato")

        else:
            logger.debug("Boshqa xabar turi qabul qilindi: %s", type_name)

    def _log_deferred_error(self, failure, context: str) -> None:
        logger.error("cTrader so'rovida xato (%s): %s", context, failure)
        if self._on_error:
            self._on_error(f"{context}: {failure}")

    def _authenticate_account(self) -> None:
        req = ProtoOAAccountAuthReq()
        req.ctidTraderAccountId = self._account_id
        req.accessToken = self._access_token
        deferred = self._client.send(req)
        deferred.addErrback(self._log_deferred_error, context="AccountAuth")

    # ------------------------------------------------------------------
    # Token yangilash (access token muddati tugaganda)
    # ------------------------------------------------------------------
    def refresh_access_token(self, refresh_token: str) -> None:
        """
        Access token muddati tugaganda chaqiriladi. Yangi tokenni qaytarib,
        chaqiruvchi kod uni environment/secret storage'da yangilashi kerak
        (Render'da bu qo'lda environment variable orqali yoki alohida
        secrets-store integratsiyasi orqali amalga oshiriladi).
        """
        req = ProtoOARefreshTokenReq()
        req.refreshToken = refresh_token
        deferred = self._client.send(req)
        deferred.addErrback(self._log_deferred_error, context="RefreshToken")
        logger.info("Token yangilash so'rovi yuborildi")

    # ------------------------------------------------------------------
    # Symbol ma'lumotini olish (lot_size, digits va h.k.)
    # ------------------------------------------------------------------
    def get_symbol_info(self, symbol_name: str) -> SymbolInfo:
        """
        DIQQAT: bu funksiya soddalashtirilgan sinxron "kutish" naqshidan
        foydalanadi (threading.Event orqali). Ishlab chiqarish muhitida
        symbol ma'lumotini FAQAT ishga tushishda bir marta olib, keshda
        saqlash tavsiya etiladi (worker.py shunday qiladi).
        """
        if symbol_name in self._symbol_cache:
            return self._symbol_cache[symbol_name]
        raise CTraderError(
            f"Symbol '{symbol_name}' keshda topilmadi. "
            f"Worker ishga tushganda load_symbols() chaqirilganiga ishonch hosil qiling."
        )

    def load_symbols(self, symbol_names: list[str], timeout: float = 15.0) -> None:
        """
        Ishga tushishda bir marta chaqiriladi: barcha kerakli symbol'lar
        ro'yxatini va ularning lot_size/digits ma'lumotini oladi.

        ESLATMA: ProtoOASymbolsListReq -> ProtoOASymbolsListRes symbol nomi
        va ID'sini beradi, lekin lotSize/pipPosition kabi to'liq ma'lumot
        uchun har bir symbol ID bo'yicha ProtoOASymbolByIdReq yuborish kerak
        bo'lishi mumkin (kutubxona versiyasiga qarab ProtoOALightSymbol
        ba'zan lotSize'ni ham o'z ichiga oladi — o'rnatilgan versiyada
        tekshiring). Quyidagi kod ikkala bosqichni ham amalga oshiradi.
        """
        done = threading.Event()
        result: dict[str, SymbolInfo] = {}

        def _on_symbols_list(extracted):
            for sym in extracted.symbol:
                if sym.symbolName in symbol_names:
                    id_req = ProtoOASymbolByIdReq()
                    id_req.ctidTraderAccountId = self._account_id
                    id_req.symbolId.append(sym.symbolId)

                    def _on_detail(detail_extracted, name=sym.symbolName, sid=sym.symbolId):
                        if detail_extracted.symbol:
                            detail = detail_extracted.symbol[0]
                            result[name] = SymbolInfo(
                                symbol_id=sid,
                                lot_size=detail.lotSize,
                                pip_position=detail.pipPosition,
                                digits=detail.digits,
                            )
                            self._symbol_cache[name] = result[name]
                            logger.info(
                                "Symbol ma'lumoti yuklandi: %s -> %s", name, result[name]
                            )
                        if len(result) == len(symbol_names):
                            done.set()

                    reactor.callFromThread(self._send_and_await, id_req, _on_detail)

        list_req = ProtoOASymbolsListReq()
        list_req.ctidTraderAccountId = self._account_id
        self._send_and_await(list_req, _on_symbols_list)

        if not done.wait(timeout=timeout):
            raise CTraderError(
                f"Symbol ma'lumotlarini olish {timeout}s ichida tugallanmadi: "
                f"{symbol_names}"
            )

    def _send_and_await(self, request, callback, response_timeout: float = 5.0) -> None:
        # response_timeout — ctrader-open-api kutubxonasining ICHKI Deferred
        # timeout'i (standart holatda 5s, kutubxona ichida qattiq kodlangan).
        # Bu ulanish/auth sozlamalariga (Client konstruktoriga) UMUMAN
        # tegmaydi — faqat shu BITTA so'rovga tegishli, kutubxonaning
        # o'zi taqdim etgan rasmiy parametr orqali (`send(..., 
        # responseTimeoutInSeconds=...)`) uzatiladi. Default=5.0 — hozirgi
        # ishlab turgan barcha boshqa so'rovlar (auth, order, balance va h.k.)
        # uchun avvalgi xatti-harakat AYNAN saqlanadi, faqat chaqiruvchi
        # aniq boshqacha qiymat uzatgandagina o'zgaradi.
        deferred = self._client.send(request, responseTimeoutInSeconds=response_timeout)

        def _on_success(response):
            try:
                extracted = Protobuf.extract(response)
                callback(extracted)
            except Exception:  # noqa: BLE001
                logger.exception("Javobni qayta ishlashda xato")

        deferred.addCallback(_on_success)
        deferred.addErrback(self._log_deferred_error, context=type(request).__name__)

    # ------------------------------------------------------------------
    # Order yuborish
    # ------------------------------------------------------------------
    def send_market_order(
        self,
        symbol_id: int,
        direction: str,  # "BUY" yoki "SELL"
        volume_in_units: int,
        sl_price: Optional[float] = None,
        tp_price: Optional[float] = None,
        label: str = "",
        comment: str = "",
    ):
        """
        Market order yuboradi, ixtiyoriy SL/TP bilan.

        volume_in_units — cTrader API lotni emas, "unit" so'raydi
        (masalan 0.01 lot XAUUSD uchun symbol.lotSize ga qarab hisoblanadi).
        Bu konvertatsiya risk_manager/trade orchestration darajasida
        (worker.py) amalga oshiriladi, chunki u symbol_info'ga bog'liq.

        Qaytaradi: Twisted Deferred — chaqiruvchi kod .addCallback/.addErrback
        bilan natijani kuzatishi mumkin.
        """
        req = ProtoOANewOrderReq()
        req.ctidTraderAccountId = self._account_id
        req.symbolId = symbol_id
        req.orderType = ProtoOAOrderType.MARKET
        req.tradeSide = (
            ProtoOATradeSide.BUY if direction.upper() == "BUY" else ProtoOATradeSide.SELL
        )
        req.volume = volume_in_units

        if sl_price is not None:
            req.stopLoss = sl_price
        if tp_price is not None:
            req.takeProfit = tp_price
        if label:
            req.label = label
        if comment:
            req.comment = comment

        logger.info(
            "ORDER YUBORILMOQDA: %s symbol_id=%s volume=%s SL=%s TP=%s label=%s",
            direction,
            symbol_id,
            volume_in_units,
            sl_price,
            tp_price,
            label,
        )

        deferred = self._client.send(req)
        deferred.addErrback(self._log_deferred_error, context="NewOrder")
        return deferred

    def amend_position_sl_tp(
        self,
        position_id: int,
        sl_price: Optional[float] = None,
        tp_price: Optional[float] = None,
    ):
        """
        Ochiq pozitsiyaning SL va/yoki TP narxini yangilaydi (trailing uchun).

        MUHIM TUZATISH: avval faqat XATO bo'lsa log qilinardi (errback),
        MUVAFFAQIYAT esa "jim" edi — ya'ni so'rov yuborilgani ko'rinardi,
        lekin broker haqiqatan ham qabul qilganini TASDIQLOVCHI hech qanday
        log yo'q edi. Bu amaliyotda "so'rov yuborildi, lekin natija noma'lum"
        degan noaniq holatlarga olib kelgan (masalan javob umuman kelmasa,
        buni bilib bo'lmas edi). Endi HAR IKKALA holat ham (muvaffaqiyat va
        xato) aniq, alohida log qilinadi.
        """
        req = ProtoOAAmendPositionSLTPReq()
        req.ctidTraderAccountId = self._account_id
        req.positionId = position_id
        if sl_price is not None:
            req.stopLoss = sl_price
        if tp_price is not None:
            req.takeProfit = tp_price

        logger.info(
            "SL/TP YANGILANMOQDA: position_id=%s SL=%s TP=%s",
            position_id,
            sl_price,
            tp_price,
        )

        def _on_amend_success(response):
            try:
                extracted = Protobuf.extract(response)
                logger.info(
                    "SL/TP AMEND JAVOBI KELDI: position_id=%s javob_turi=%s tarkibi=%s",
                    position_id,
                    type(extracted).__name__,
                    extracted,
                )
            except Exception:  # noqa: BLE001
                logger.exception(
                    "SL/TP amend javobini o'qishda xato: position_id=%s", position_id
                )

        def _on_amend_error(failure):
            logger.error(
                "SL/TP AMEND RAD ETILDI yoki XATO: position_id=%s xato=%s",
                position_id,
                failure,
            )

        deferred = self._client.send(req)
        deferred.addCallback(_on_amend_success)
        deferred.addErrback(_on_amend_error)
        return deferred

    def close_position(self, position_id: int, volume_in_units: int):
        """Pozitsiyani to'liq (yoki qisman, volume orqali) yopadi."""
        req = ProtoOAClosePositionReq()
        req.ctidTraderAccountId = self._account_id
        req.positionId = position_id
        req.volume = volume_in_units

        logger.info("POZITSIYA YOPILMOQDA: position_id=%s volume=%s", position_id, volume_in_units)

        deferred = self._client.send(req)
        deferred.addErrback(self._log_deferred_error, context="ClosePosition")
        return deferred

    def get_account_balance(self, timeout: float = 10.0) -> float:
        """
        Joriy hisob balansini so'raydi va sinxron tarzda qaytaradi.
        DIQQAT: balans odatda cTrader API'da "centa/kopeyka"ga o'xshash
        eng kichik birlikda keladi (masalan moneyDigits ga qarab, ko'pincha
        real qiymat = balance / 100). Aniq bo'linuvchini trader javobidagi
        `moneyDigits` maydonidan olish tavsiya etiladi — worker.py buni
        hisobga oladi.
        """
        done = threading.Event()
        result: dict[str, float] = {}

        def _on_trader(extracted):
            trader = extracted.trader
            money_digits = getattr(trader, "moneyDigits", 2)
            divisor = 10 ** money_digits
            result["balance"] = trader.balance / divisor
            done.set()

        req = ProtoOATraderReq()
        req.ctidTraderAccountId = self._account_id
        self._send_and_await(req, _on_trader)

        if not done.wait(timeout=timeout):
            raise CTraderError("Balansni olish uchun javob kelmadi (timeout)")

        return result["balance"]

    def subscribe_spots(self, symbol_ids: list[int]) -> None:
        """
        Berilgan symbol'lar uchun jonli narx oqimiga obuna bo'ladi. Har bir
        yangi narx kelganda konstruktordagi `on_spot_price(symbol_id, bid)`
        chaqiriladi. Trailing tekshiruvi buni ishlatishi mumkin, lekin
        boshlang'ich versiyada worker.py 1-daqiqalik pollingni ustun
        qo'yadi (soddalik uchun) — bu obuna kelajakda tezroq reaktsiya
        kerak bo'lsa ishlatilishi uchun tayyorlab qo'yilgan.
        """
        req = ProtoOASubscribeSpotsReq()
        req.ctidTraderAccountId = self._account_id
        for sid in symbol_ids:
            req.symbolId.append(sid)
        deferred = self._client.send(req)
        deferred.addErrback(self._log_deferred_error, context="SubscribeSpots")
        logger.info("Narx oqimiga obuna yuborildi: %s", symbol_ids)

    def get_open_positions_full(self, timeout: float = 10.0) -> list:
        """
        Broker'dagi barcha ochiq pozitsiyalar haqida TO'LIQ ma'lumot
        (position_id, symbol_id, side, volume, entry narxi, joriy SL/TP)
        qaytaradi. Worker qayta ishga tushganda (deploy/restart), xotirada
        yo'qolgan, lekin broker'da hali ochiq turgan pozitsiyalarni
        "eng yaqin holatda" tiklash uchun ishlatiladi.
        """
        done = threading.Event()
        result: dict = {}

        def _on_reconcile(extracted):
            positions = []
            for pos in extracted.position:
                if pos.positionStatus != ProtoOAPositionStatus.POSITION_STATUS_OPEN:
                    continue
                positions.append(
                    {
                        "position_id": pos.positionId,
                        "symbol_id": pos.tradeData.symbolId,
                        "side": "BUY" if pos.tradeData.tradeSide == ProtoOATradeSide.BUY else "SELL",
                        "volume_units": pos.tradeData.volume,
                        "entry_price": pos.price,
                        "current_sl": pos.stopLoss if pos.HasField("stopLoss") else None,
                        "current_tp": pos.takeProfit if pos.HasField("takeProfit") else None,
                    }
                )
            result["positions"] = positions
            done.set()

        req = ProtoOAReconcileReq()
        req.ctidTraderAccountId = self._account_id
        self._send_and_await(req, _on_reconcile)

        if not done.wait(timeout=timeout):
            raise CTraderError("Reconcile (to'liq) javobi kelmadi (timeout)")

        return result["positions"]

    def get_open_position_ids(self, timeout: float = 10.0) -> set:
        """
        Broker'da HOZIR haqiqatan ham ochiq turgan pozitsiyalar ID'lari
        ro'yxatini so'raydi. Bu — xavfsizlik uchun ikkinchi qatlam: agar
        biror sababdan execution event orqali pozitsiya yopilgani "eshitilib
        qolmagan" bo'lsa (masalan qisqa tarmoq uzilishi), davriy reconcile
        orqali xotiradagi holat broker bilan solishtirilib tuzatiladi.
        """
        done = threading.Event()
        result: dict = {}

        def _on_reconcile(extracted):
            ids = set()
            for pos in extracted.position:
                if pos.positionStatus == ProtoOAPositionStatus.POSITION_STATUS_OPEN:
                    ids.add(pos.positionId)
            result["ids"] = ids
            done.set()

        req = ProtoOAReconcileReq()
        req.ctidTraderAccountId = self._account_id
        self._send_and_await(req, _on_reconcile)

        if not done.wait(timeout=timeout):
            raise CTraderError("Reconcile javobi kelmadi (timeout)")

        return result["ids"]

    def get_trendbars(
        self,
        symbol_id: int,
        digits: int,
        timeframe: str,
        count: int,
        timeout: float = 15.0,
    ) -> list:
        """
        So'nggi N ta shamni (candle) cTrader'dan so'raydi, ESKIDAN YANGIGA
        tartiblangan holda qaytaradi.

        timeframe: "1min" yoki "5min" (boshqa qiymat ValueError beradi).
        count: nechta sham kerakligi (1-1000 oralig'ida cheklanadi, chunki
        cTrader odatda bitta so'rovda shuncha bergacha ruxsat beradi).

        MUHIM — narx kodlash formati: cTrader trendbar'da narxlar "low"
        (asosiy, butun sonda) va undan FARQ (delta) sifatida kodlangan
        open/high/close qiymatlari bilan keladi:
            haqiqiy_low   = trendbar.low / 100000
            haqiqiy_open  = (trendbar.low + trendbar.deltaOpen) / 100000
            haqiqiy_high  = (trendbar.low + trendbar.deltaHigh) / 100000
            haqiqiy_close = (trendbar.low + trendbar.deltaClose) / 100000

        MUHIM TUZATISH (real so'rov bilan tasdiqlandi): avval bu yerda
        `10**digits` ishlatilgan edi (izohda "tasdiqlash tavsiya etiladi"
        deb ogohlantirilgan holat — aynan shu tekshiruv paytida xato
        topildi). Haqiqiy `/candles` so'rovi natijasi `/price`dagi jonli
        narx bilan solishtirilganda, narx ANIQ 1000 MARTA KATTA chiqishi
        aniqlandi (masalan close=4347810.0, aslida 4347.81 bo'lishi kerak
        edi). Sabab: XAUUSD uchun broker digits=2 qaytaradi, `10**2=100`
        bilan bo'lish esa noto'g'ri — cTrader trendbar narxlari, xuddi
        ProtoOASpotEvent (jonli narx) kabi, DOIM fiksirlangan 1e5 (100000)
        shkalada keladi, `digits`ga BOG'LIQ EMAS (bu — worker.py'da spot
        narx uchun avval topilgan va tuzatilgan xatoning xuddi shu turi,
        faqat tarixiy sham qismida). `digits` esa faqat NATIJANI
        YAXLITLASH (round) uchun ishlatiladi, masshtablash uchun emas —
        bu market_data.py'da o'zgarishsiz qoladi.
        """
        period_map = {"1min": ProtoOATrendbarPeriod.M1, "5min": ProtoOATrendbarPeriod.M5}
        if timeframe not in period_map:
            raise ValueError(f"Noma'lum timeframe: {timeframe} (faqat '1min' yoki '5min')")

        count = max(1, min(int(count), 1000))

        done = threading.Event()
        result: dict = {}

        def _on_trendbars(extracted):
            bars = []
            for tb in extracted.trendbar:
                # DIQQAT: 100000 — FIKSIRLANGAN shkala (digits emas). Yuqoridagi
                # docstring'dagi "MUHIM TUZATISH" izohiga qarang.
                low = tb.low / 100000
                open_ = (tb.low + tb.deltaOpen) / 100000
                high = (tb.low + tb.deltaHigh) / 100000
                close = (tb.low + tb.deltaClose) / 100000
                bars.append(
                    {
                        "timestamp_min": tb.utcTimestampInMinutes,
                        "open": open_,
                        "high": high,
                        "low": low,
                        "close": close,
                        "volume": tb.volume,
                    }
                )
            # cTrader odatda yangidan eskiga beradi — eskidan yangiga
            # (o'sish tartibida) qaytarish uchun teskari qilamiz.
            bars.sort(key=lambda b: b["timestamp_min"])
            result["bars"] = bars
            done.set()

        req = ProtoOAGetTrendbarsReq()
        req.ctidTraderAccountId = self._account_id
        req.symbolId = symbol_id
        req.period = period_map[timeframe]
        req.count = count
        req.toTimestamp = int(time.time() * 1000)

        # MUHIM TUZATISH (kandidat #2): avval faqat `toTimestamp` yuborilgan,
        # `fromTimestamp` UMUMAN qo'yilmagan edi (demak standart qiymat —
        # 0, ya'ni 1970-yil bilan ketgan). cTrader rasmiy hujjatida va
        # barcha ishlaydigan namunalarda IKKALASI HAM beriladi; forumda esa
        # noto'g'ri/to'liq bo'lmagan vaqt oralig'i yuborilganda serverning
        # HECH QANDAY XATO QAYTARMASDAN JIMGINA javob bermasligi alohida
        # tasdiqlangan — bu bizning "timeout, lekin ProtoOAErrorRes yo'q"
        # holatimizga mos keladi. `count` so'ralgan bo'lsa `fromTimestamp`
        # texnik jihatdan ixtiyoriy (server "count ta sham, toTimestamp'dan
        # orqaga" deb hisoblashi kerak), lekin buni aniq ko'rsatish xavfsiz
        # va hujjatga mosroq. Oyna kengligi so'ralgan count+period'ga
        # nisbatan avtomatik hisoblanadi (bozor tanaffuslari/dam olish
        # kunlari uchun 4x zaxira bilan), shuning uchun har qanday
        # timeframe/count uchun ishlaydi, kodga qattiq sonlar yozilmaydi.
        period_minutes = {"1min": 1, "5min": 5}[timeframe]
        window_minutes = max(count * period_minutes * 4, 60)
        req.fromTimestamp = req.toTimestamp - (window_minutes * 60 * 1000)

        # MUHIM TUZATISH: avval kutubxonaning ICHKI standart 5s Deferred
        # timeout'i ishlatilgan edi (chunki _send_and_await'ga
        # response_timeout uzatilmagan bo'lsa, default=5.0 qo'llanadi),
        # bu esa pastdagi 15s (yoki undan uzunroq) tashqi `timeout` bilan
        # mos kelmagan — trendbar javobi 5 soniyadan ko'proq vaqt olganda,
        # kutubxona o'zi CHAQIRUVCHI TASHQI TIMEOUT YETMASDAN OLDIN
        # Deferred'ni bekor qilib, TimeoutError bilan xato qaytargan.
        # Endi ichki timeout tashqi `timeout` bilan BIR XIL qiymatga
        # tenglashtirildi — faqat shu so'rov uchun, boshqa metodlarga
        # (auth, order, balance) tegmaydi.
        self._send_and_await(req, _on_trendbars, response_timeout=timeout)

        if not done.wait(timeout=timeout):
            raise CTraderError(f"Trendbar so'rovi {timeout}s ichida javob bermadi")

        return result["bars"]

    def reconcile_open_positions(self):
        """
        Hozir broker'da ochiq turgan barcha pozitsiyalar ro'yxatini so'raydi.
        Worker qayta ishga tushganda (deploy/restart) xotiradagi holatni
        broker bilan MOSLASHTIRISH uchun ishlatiladi — bu juda muhim,
        aks holda restart'dan keyin bot "esidan chiqargan" pozitsiyalarni
        boshqarishni to'xtatib qo'yishi mumkin.
        """
        req = ProtoOAReconcileReq()
        req.ctidTraderAccountId = self._account_id
        deferred = self._client.send(req)
        deferred.addErrback(self._log_deferred_error, context="Reconcile")
        return deferred
