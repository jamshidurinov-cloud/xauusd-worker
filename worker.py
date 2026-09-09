"""
Tilla (XAUUSD) Wyckoff Spring/Upthrust signal beruvchi va soatlik holat
xabar qiluvchi Telegram bot.

Ikki rejimda ishlaydi (Render'da ikkita alohida Cron Job sifatida sozlanadi):

  python main.py signal   -> har 5 daqiqada: faqat SPRING/UPTHRUST chiqqanda
                              to'liq signal + grafik + AI tahlil yuboradi.
                              Signal bo'lmasa, jim chiqadi (xabar yubormaydi).

  python main.py status   -> har soatda: joriy narx, diapazon va
                              range/uchburchak holatini qisqa xabar qilib yuboradi.
"""

import os
import sys
import requests

# ---------- Sozlamalar (Render'da Environment Variables sifatida kiritiladi) ----------
TWELVEDATA_API_KEY = os.environ.get("TWELVEDATA_API_KEY")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
NTFY_TOPIC = os.environ.get("NTFY_TOPIC")  # ntfy.sh orqali tezkor push-bildirishnoma (ixtiyoriy)
WORKER_URL = os.environ.get("WORKER_URL")  # Trade execution Worker manzili (hali sozlanmagan)
WORKER_SECRET_KEY = os.environ.get("WORKER_SECRET_KEY")  # Worker bilan aloqa uchun maxfiy kalit
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")      # signal tracking uchun (Gist)
GIST_ID = os.environ.get("GIST_ID")                # signal tracking uchun (Gist)

REQUIRED_VARS = {
    "TWELVEDATA_API_KEY": TWELVEDATA_API_KEY,
    "TELEGRAM_BOT_TOKEN": TELEGRAM_BOT_TOKEN,
    "TELEGRAM_CHAT_ID": TELEGRAM_CHAT_ID,
}

RANGE_LOOKBACK = 20      # diapazonni aniqlash uchun necha sveчadan foydalanish (soatlik holat uchun)


def check_env_vars():
    missing = [name for name, value in REQUIRED_VARS.items() if not value]
    if missing:
        print(f"XATOLIK: quyidagi environment variable'lar topilmadi: {', '.join(missing)}")
        sys.exit(1)


# ============================================================================
# MA'LUMOT OLISH (TwelveData)
# ============================================================================

def build_price_data_from_candles(df):
    """Alohida API so'rovi yubormasdan, allaqachon olingan svechalar ma'lumotidan
    narx va o'zgarish foizini hisoblab chiqaradi (TwelveData so'rovlar sonini tejash uchun)."""
    last = df.iloc[-1]
    prev = df.iloc[-2] if len(df) > 1 else last
    price = float(last["close"])
    prev_close = float(prev["close"])
    change = price - prev_close
    percent_change = (change / prev_close * 100) if prev_close else 0.0
    return {
        "price": price,
        "change": change,
        "percent_change": percent_change,
        "high": float(last["high"]),
        "low": float(last["low"]),
        "volume": float(last.get("volume", 0)),
    }


def get_gold_price():
    """TwelveData orqali XAU/USD narxi va o'zgarish foizini oladi.
    Eslatma: hozir asosiy oqimda ishlatilmaydi (API so'rovlarini tejash uchun
    build_price_data_from_candles ishlatiladi), lekin zarur bo'lsa alohida chaqirsa bo'ladi."""
    url = "https://api.twelvedata.com/quote"
    params = {"symbol": "XAU/USD", "apikey": TWELVEDATA_API_KEY}
    resp = requests.get(url, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    if "code" in data and data.get("code") != 200:
        raise RuntimeError(f"TwelveData xatosi: {data.get('message')}")

    def to_float(v):
        try:
            return float(v)
        except (TypeError, ValueError):
            return v

    return {
        "price": to_float(data.get("close")),
        "change": to_float(data.get("change")),
        "percent_change": to_float(data.get("percent_change")),
        "high": to_float(data.get("high")),
        "low": to_float(data.get("low")),
        "volume": to_float(data.get("volume")),
    }


def get_gold_candles(interval="5min", outputsize=100, max_retries=3):
    """TwelveData'dan oxirgi svechalar tarixini (OHLCV) oladi. Tasodifiy tarmoq
    sekinligi (timeout) tufayli butun ishga tushish behuda ketmasligi uchun,
    xatolik bo'lsa bir necha marta qayta urinadi (har safar biroz uzunroq
    kutish vaqti bilan)."""
    import pandas as pd
    import time

    url = "https://api.twelvedata.com/time_series"
    params = {
        "symbol": "XAU/USD",
        "interval": interval,
        "outputsize": outputsize,
        "timezone": "UTC",
        "apikey": TWELVEDATA_API_KEY,
    }

    last_error = None
    for attempt in range(max_retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=30 + attempt * 10)
            resp.raise_for_status()
            data = resp.json()
            if "values" not in data:
                raise RuntimeError(f"TwelveData time_series xatosi: {data.get('message', data)}")
            break
        except Exception as e:
            last_error = e
            if attempt < max_retries:
                time.sleep(2 + attempt * 2)
                continue
            raise last_error

    df = pd.DataFrame(data["values"])
    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df.set_index("datetime").sort_index()
    for col in ["open", "high", "low", "close", "volume"]:
        if col in df.columns:
            df[col] = df[col].astype(float)
        else:
            df[col] = 0.0

    return df



def get_forex_calendar_events(hours_ahead=24):
    """Forex Factory'ning ochiq JSON kalendaridan yaqin soatlardagi yuqori ta'sirli
    USD iqtisodiy yangiliklarini oladi (Fed, NFP, CPI kabi — bular XAUUSD'ga eng
    ko'p ta'sir qiladigan voqealar). Diqqat: bu manzilga 5 daqiqada faqat 2 marta
    so'rov yuborish mumkin — shuning uchun faqat soatlik status rejimida chaqiriladi."""
    import datetime as dt
    from dateutil import parser as date_parser

    url = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
    resp = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
    resp.raise_for_status()

    text = resp.text.strip()
    if text.startswith("<") or "Request Denied" in text:
        raise RuntimeError("Forex Factory limitga tegib qoldi (5 daqiqada 2 so'rovdan ko'p)")

    events = resp.json()
    now = dt.datetime.now(dt.timezone.utc)
    horizon = now + dt.timedelta(hours=hours_ahead)

    result = []
    for e in events:
        try:
            event_time = date_parser.parse(e.get("date", ""))
            if event_time.tzinfo is None:
                event_time = event_time.replace(tzinfo=dt.timezone.utc)
        except (ValueError, TypeError):
            continue

        if e.get("country") == "USD" and e.get("impact") == "High" and now <= event_time <= horizon:
            result.append({
                "title": e.get("title", "Noma'lum voqea"),
                "time": event_time,
                "forecast": e.get("forecast", ""),
                "previous": e.get("previous", ""),
            })

    result.sort(key=lambda x: x["time"])
    return result


# ============================================================================
# QOIDA DVIGATELI - Wyckoff Spring / Upthrust / Range holati
# ============================================================================

# JACKPOT, moslashuvchan range va Spring/Upthrust mantiqi alohida faylda
# (jackpot_signal.py) - shu yerdan import qilinadi:
from jackpot_signal import (
    RANGE_TOLERANCE_PCT, CONFIRM_CANDLES, TEST_TOLERANCE_PCT, TEST_SEARCH_WINDOW,
    find_swing_points, cluster_equal_levels, detect_dynamic_range,
    detect_dynamic_spring_upthrust, detect_jackpot_signal, detect_ob_fvg_entry,
)
from smc_lib import detect_smc_official_signal


PROMINENCE_WINDOW = 40   # Sweep uchun: "ajralib turgan" darajani aniqlash oynasi
PROMINENCE_MIN_HISTORY = 10  # ishonchli referens uchun kamida shuncha oldingi sveчa kerak
BOS_SWING_WINDOW = 6     # BOS uchun: eng yaqin tasdiqlangan swing nuqta oynasi
FRESH_BREAK_WINDOW = 3   # "yangi sinish" uchun oxirgi nechta sveчani tekshirish
                         # (bitta o'tkazib yuborilgan Cron ishga tushishiga chidamli
                         # bo'lish uchun - masalan tarmoq/timeout xatoligi tufayli)


def detect_smc_composite(df, lookback=144, min_fvg_mult=0.02, min_sweep_mult=0.15,
                          prominence_window=PROMINENCE_WINDOW, bos_swing_window=BOS_SWING_WINDOW):
    """ENG KUCHLI SMC/ICT signal: Liquidity Sweep + FVG + BOS/CHoCH ketma-ketligi.
    Faqat BOS/CHoCH aynan JORIY (oxirgi) svechada tasdiqlansa signal beradi —
    shu bilan har bir voqea faqat bir marta xabar qilinadi.

    Ikkita turli referens ishlatiladi:
    - SWEEP uchun: oldingi `prominence_window` (40) sveчaning ENG EKSTREMAL narxi —
      "ajralib turgan, ko'zga tashlanadigan" daraja
    - BOS uchun: ENG YAQIN tasdiqlangan swing nuqta (`bos_swing_window`=6 sveчa) —
      BOS'ning vazifasi narx yo'nalishi tezkor o'zgarganini payqash

    Soxta (ahamiyatsiz) signallarni kamaytirish uchun ikkita filtr qo'shilgan:
    - min_fvg_mult: FVG (bo'shliq) kamida o'rtacha svecha kattaligining shuncha
      qismi bo'lishi kerak (juda mayda, ahamiyatsiz "bo'shliq"larni rad etadi)
    - min_sweep_mult: sweep chuqurligi (narx swing darajasidan qanchalik pastga/
      yuqoriga chiqqani) kamida shuncha bo'lishi kerak (arzimas sweep'larni rad etadi)
    """
    if len(df) < lookback:
        return None

    sub = df.iloc[-lookback:]
    highs = sub["high"].values
    lows = sub["low"].values
    closes = sub["close"].values
    times = sub.index
    n = len(sub)
    cur = n - 1

    avg_candle_range = (sub["high"] - sub["low"]).mean()
    min_fvg_size = avg_candle_range * min_fvg_mult
    min_sweep_depth = avg_candle_range * min_sweep_mult

    swing_high_idx, swing_low_idx = find_swing_points(highs, lows, window=bos_swing_window, exclude_last=True)
    nearest_swing_high_before = lambda idx: next((i for i in reversed(swing_high_idx) if i < idx), None)
    nearest_swing_low_before = lambda idx: next((i for i in reversed(swing_low_idx) if i < idx), None)

    def prominent_high(idx):
        """idx'dan oldingi `prominence_window` ichidagi HAQIQIY swing cho'qqilar
        orasidan, eng ekstremalidan boshlab, idx'gacha HECH KIM BUZMAGAN (undan
        oshib ketmagan) birinchisini tanlaydi. Agar oraliq balandroq nuqta bilan
        allaqachon "buzilgan" bo'lsa (ya'ni undan keyin narx yanada yuqoriga
        chiqib ketgan bo'lsa), bu daraja endi 'muhim, sinalmagan' hisoblanmaydi
        va rad etiladi."""
        start = max(0, idx - prominence_window)
        if idx - start < PROMINENCE_MIN_HISTORY:
            return None
        candidates = [i for i in swing_high_idx if start <= i < idx]
        if not candidates:
            return None
        for i in sorted(candidates, key=lambda i: -highs[i]):
            level = highs[i]
            segment_max = highs[i + 1:idx].max() if i + 1 < idx else -float("inf")
            if segment_max <= level:
                return level
        return None

    def prominent_low(idx):
        """idx'dan oldingi `prominence_window` ichidagi HAQIQIY swing tublar
        orasidan, eng ekstremalidan boshlab, idx'gacha HECH KIM BUZMAGAN
        birinchisini tanlaydi (yuqoridagi mantiqning pastga versiyasi)."""
        start = max(0, idx - prominence_window)
        if idx - start < PROMINENCE_MIN_HISTORY:
            return None
        candidates = [i for i in swing_low_idx if start <= i < idx]
        if not candidates:
            return None
        for i in sorted(candidates, key=lambda i: lows[i]):
            level = lows[i]
            segment_min = lows[i + 1:idx].min() if i + 1 < idx else float("inf")
            if segment_min >= level:
                return level
        return None

    diag = []

    # --- BULLISH: sell-side sweep -> bullish FVG -> BOS yuqoriga ---
    bos_ref_idx = nearest_swing_high_before(cur)
    last_swing_high = highs[bos_ref_idx] if bos_ref_idx is not None else None
    if last_swing_high is None:
        diag.append("BULLISH: yaqin swing high topilmadi")
    else:
        break_idx = None
        for m in range(max(1, cur - FRESH_BREAK_WINDOW + 1), cur + 1):
            if closes[m] > last_swing_high and closes[m - 1] <= last_swing_high:
                break_idx = m
                break  # eng ERTAROQ (haqiqiy) sinish momentini olamiz
        if break_idx is None:
            diag.append(f"BULLISH: fresh_break yo'q (last_swing_high={last_swing_high:.2f}, "
                        f"closes[cur]={closes[cur]:.2f}, closes[cur-1]={closes[cur-1]:.2f})")
        else:
            fvg_idx = None
            for j in range(2, cur + 1):
                if (lows[j] - highs[j - 2]) >= min_fvg_size:
                    fvg_idx = j
            if fvg_idx is None:
                diag.append(f"BULLISH: fresh_break BOR (BOS={last_swing_high:.2f}), lekin FVG topilmadi "
                            f"(min_fvg_size={min_fvg_size:.3f})")
            else:
                best_sweep = None
                for k in range(1, fvg_idx):
                    sl = prominent_low(k)
                    if sl is None:
                        continue
                    if (sl - lows[k]) >= min_sweep_depth and closes[k] > sl:
                        if best_sweep is None or lows[k] < best_sweep["low"]:
                            best_sweep = {"idx": k, "low": lows[k], "sl": sl}
                if best_sweep is not None:
                    return {
                        "type": "smc_bullish",
                        "sweep_time": str(times[best_sweep["idx"]]),
                        "sweep_level": best_sweep["sl"],
                        "fvg_time": str(times[fvg_idx]),
                        "bos_time": str(times[break_idx]),
                        "bos_level": last_swing_high,
                        "current_close": closes[cur],
                    }
                diag.append(f"BULLISH: fresh_break BOR, FVG BOR, lekin sweep topilmadi "
                            f"(min_sweep_depth={min_sweep_depth:.3f})")

    # --- BEARISH: buy-side sweep -> bearish FVG -> BOS pastga ---
    bos_ref_idx2 = nearest_swing_low_before(cur)
    last_swing_low = lows[bos_ref_idx2] if bos_ref_idx2 is not None else None
    if last_swing_low is None:
        diag.append("BEARISH: yaqin swing low topilmadi")
    else:
        break_idx2 = None
        for m in range(max(1, cur - FRESH_BREAK_WINDOW + 1), cur + 1):
            if closes[m] < last_swing_low and closes[m - 1] >= last_swing_low:
                break_idx2 = m
                break
        if break_idx2 is None:
            diag.append(f"BEARISH: fresh_break yo'q (last_swing_low={last_swing_low:.2f}, "
                        f"closes[cur]={closes[cur]:.2f}, closes[cur-1]={closes[cur-1]:.2f})")
        else:
            fvg_idx = None
            for j in range(2, cur + 1):
                if (lows[j - 2] - highs[j]) >= min_fvg_size:
                    fvg_idx = j
            if fvg_idx is None:
                diag.append(f"BEARISH: fresh_break BOR (BOS={last_swing_low:.2f}), lekin FVG topilmadi "
                            f"(min_fvg_size={min_fvg_size:.3f})")
            else:
                best_sweep = None
                for k in range(1, fvg_idx):
                    sh = prominent_high(k)
                    if sh is None:
                        continue
                    if (highs[k] - sh) >= min_sweep_depth and closes[k] < sh:
                        if best_sweep is None or highs[k] > best_sweep["high"]:
                            best_sweep = {"idx": k, "high": highs[k], "sh": sh}
                if best_sweep is not None:
                    return {
                        "type": "smc_bearish",
                        "sweep_time": str(times[best_sweep["idx"]]),
                        "sweep_level": best_sweep["sh"],
                        "bos_time": str(times[break_idx2]),
                        "fvg_time": str(times[fvg_idx]),
                        "bos_level": last_swing_low,
                        "current_close": closes[cur],
                    }
                diag.append(f"BEARISH: fresh_break BOR, FVG BOR, lekin sweep topilmadi "
                            f"(min_sweep_depth={min_sweep_depth:.3f})")

    print("[SMC DEBUG] " + " | ".join(diag))
    return None


def get_trend_bias(df, period=144, threshold_pct=0.05):
    """Mavjud ma'lumot asosida (qo'shimcha API so'rovisiz) umumiy trend yo'nalishini
    taxminan aniqlaydi: joriy narxni so'nggi `period` sveчaning o'rtacha narxi bilan
    solishtiradi. Bu haqiqiy yuqori timeframe emas, balki tezkor va bepul proksi."""
    if len(df) < period:
        return "neutral"
    sma = df["close"].tail(period).mean()
    current = df["close"].iloc[-1]
    if sma == 0:
        return "neutral"
    diff_pct = (current - sma) / sma * 100
    if diff_pct > threshold_pct:
        return "bullish"
    elif diff_pct < -threshold_pct:
        return "bearish"
    return "neutral"


def get_htf_bias(df, htf="1h", period=30, threshold_pct=0.05):
    """CHINAKAM kattaroq timeframe (masalan 1 soatlik) trend yo'nalishini aniqlaydi.
    Qo'shimcha API so'rovisiz — mavjud (5m yoki 1m) ma'lumotni 1H svechalarga
    'yig'ib chiqaradi' (resample), so'ng shu asosda SMA solishtiradi.

    Oxirgi (hali to'liq tugallanmagan) 1H sveчa har doim tashlab yuboriladi —
    aks holda to'liq bo'lmagan svechani "yakunlangan" deb noto'g'ri hisoblash xavfi bor."""
    if len(df) < 10:
        return "neutral"

    htf_df = df.resample(htf).agg({
        "open": "first", "high": "max", "low": "min", "close": "last",
    }).dropna()

    if len(htf_df) < 2:
        return "neutral"

    htf_df = htf_df.iloc[:-1]  # oxirgi, hali tugallanmagan 1H sveчani tashlab yuboramiz

    if len(htf_df) < period:
        return "neutral"

    sma = htf_df["close"].tail(period).mean()
    current = df["close"].iloc[-1]  # eng joriy (haqiqiy) narx
    if sma == 0:
        return "neutral"
    diff_pct = (current - sma) / sma * 100
    if diff_pct > threshold_pct:
        return "bullish"
    elif diff_pct < -threshold_pct:
        return "bearish"
    return "neutral"


def find_htf_zones(df, htf="1h", lookback=30, min_fvg_mult=0.5, min_ob_mult=1.5):
    """Kattaroq timeframe'dagi (1H) so'nggi OB va FVG zonalarini topadi — bu MTF
    (5m/1m) signal chiqqanda, narx qo'shimcha ravishda 1H'dagi muhim zonaga ham
    to'g'ri kelayotganini (yoki kelmayotganini) ma'lumot sifatida ko'rsatish uchun."""
    if len(df) < 10:
        return []

    htf_df = df.resample(htf).agg({
        "open": "first", "high": "max", "low": "min", "close": "last",
    }).dropna()
    if len(htf_df) < 2:
        return []
    htf_df = htf_df.iloc[:-1]  # oxirgi, tugallanmagan 1H sveчani tashlab yuboramiz
    if len(htf_df) < 5:
        return []

    sub = htf_df.tail(lookback)
    highs = sub["high"].values
    lows = sub["low"].values
    opens = sub["open"].values
    closes = sub["close"].values
    n = len(sub)
    avg_range = (sub["high"] - sub["low"]).mean()
    if avg_range == 0:
        return []
    min_fvg_size = avg_range * min_fvg_mult

    zones = []

    # FVG zonalari (3-sveчalik bo'shliqlar)
    for j in range(2, n):
        if lows[j] - highs[j - 2] >= min_fvg_size:
            zones.append((highs[j - 2], lows[j], "bullish_fvg"))
        if lows[j - 2] - highs[j] >= min_fvg_size:
            zones.append((highs[j], lows[j - 2], "bearish_fvg"))

    # OB zonalari (qarama-qarshi svecha + undan keyingi kuchli harakat)
    for i in range(0, n - 2):
        move_after = closes[min(i + 2, n - 1)] - closes[i]
        if closes[i] < opens[i] and move_after >= avg_range * min_ob_mult:
            zones.append((lows[i], highs[i], "bullish_ob"))
        if closes[i] > opens[i] and -move_after >= avg_range * min_ob_mult:
            zones.append((lows[i], highs[i], "bearish_ob"))

    return zones


def detect_range_state(df, lookback=RANGE_LOOKBACK, tight_threshold_pct=0.5):
    """Joriy holat qanday diapazon/uchburchak turiga to'g'ri kelishini aniqlaydi:
    - bullish_squeeze: pastki chegara ko'tarilib, yuqoriga qisilmoqda
    - bearish_squeeze: yuqori chegara pasayib, pastga qisilmoqda
    - symmetrical_triangle: ikkala tomondan torayapti
    - flat_range: torayish yo'q, lekin narx tor oraliqda (oddiy gorizontal diapazon)
    - None: aniq diapazon yo'q (narx keng harakatda / trendda)
    """
    if len(df) < lookback:
        return None

    window = df.iloc[-lookback:]
    half = lookback // 2
    first_half = window.iloc[:half]
    second_half = window.iloc[half:]

    high_first, high_second = first_half["high"].max(), second_half["high"].max()
    low_first, low_second = first_half["low"].min(), second_half["low"].min()

    width_first = high_first - low_first
    width_second = high_second - low_second
    current_price = window["close"].iloc[-1]

    if current_price <= 0:
        return None

    width_pct = (width_second / current_price) * 100

    if width_pct > tight_threshold_pct * 3:
        return None

    narrowing = width_second < width_first * 0.75
    high_falling = high_second < high_first
    low_rising = low_second > low_first

    if narrowing and low_rising and not high_falling:
        rtype = "bullish_squeeze"
    elif narrowing and high_falling and not low_rising:
        rtype = "bearish_squeeze"
    elif narrowing and low_rising and high_falling:
        rtype = "symmetrical_triangle"
    else:
        rtype = "flat_range"

    return {
        "type": rtype,
        "range_high": high_second,
        "range_low": low_second,
        "width_pct": round(width_pct, 3),
    }


# ============================================================================
# GRAFIK CHIZISH
# ============================================================================

def make_chart_image(df, path="/tmp/chart.png", interval="5min"):
    """OHLCV ma'lumotidan katta, aniq o'qiladigan candlestick + volume grafik chizadi.
    Eslatma: faqat GRAFIK uchun, har svechaning 'open'ini oldingi svechaning
    'close'iga moslashtiramiz (TradingView'dagi kabi uzluksiz ko'rinish uchun) -
    bu asl ma'lumotni (signal aniqlashda ishlatiladigan) o'zgartirmaydi, faqat
    rasmni "ideal"roq ko'rsatadi."""
    import mplfinance as mpf

    plot_df = df.copy()
    prev_close = plot_df["close"].shift(1)
    new_open = prev_close.fillna(plot_df["open"])
    plot_df["open"] = new_open
    # high/low'ni yangi 'open' bilan mos qilib kengaytiramiz (vizual uzilish bo'lmasligi uchun)
    plot_df["high"] = plot_df[["high", "open"]].max(axis=1)
    plot_df["low"] = plot_df[["low", "open"]].min(axis=1)

    mc = mpf.make_marketcolors(
        up="#26a69a", down="#ef5350",
        edge="inherit", wick="inherit", volume="in",
    )
    style = mpf.make_mpf_style(
        base_mpf_style="charles",
        marketcolors=mc,
        gridstyle="--",
        gridcolor="#dddddd",
        facecolor="white",
        rc={"font.size": 11, "axes.labelsize": 12, "axes.titlesize": 14},
    )

    mpf.plot(
        plot_df,
        type="candle",
        volume=False,
        style=style,
        title=f"\nXAUUSD - so'nggi {len(df)} ta {interval} sveча",
        ylabel="Narx (USD)",
        figsize=(16, 9),
        tight_layout=True,
        scale_padding={"left": 0.3, "right": 0.7, "top": 0.8, "bottom": 0.5},
        savefig=dict(fname=path, dpi=220, bbox_inches="tight"),
    )
    return path


# ============================================================================
# SIGNAL TRACKING (GitHub Gist orqali - Cron Job'lar orasida "yodda tutish" uchun)
# ============================================================================
# Render Cron Job'lari har safar yangidan boshlanadi (hech narsani "yodda tutmaydi"),
# shuning uchun signal tarixini tashqi joyda (kichik GitHub Gist fayli) saqlaymiz.
# Agar GITHUB_TOKEN/GIST_ID sozlanmagan bo'lsa, tracking jim o'tkazib yuboriladi
# (bot signal berishda davom etadi, faqat statistika yig'ilmaydi).

GIST_API_URL = "https://api.github.com/gists"
TIMEOUT_CANDLES = 50  # signal shuncha sveчadan keyin ham SL/TP5 ga tegmasa, "muddati o'tdi" deb yopiladi


def interval_to_minutes(interval):
    """'1min' -> 1, '5min' -> 5 kabi. Noma'lum format kelsa 5 deb hisoblaydi."""
    try:
        return int("".join(ch for ch in interval if ch.isdigit()))
    except (ValueError, TypeError):
        return 5


def timeout_seconds_for(interval):
    return interval_to_minutes(interval) * TIMEOUT_CANDLES * 60


def tracking_enabled():
    return bool(GITHUB_TOKEN and GIST_ID)


def load_signal_log():
    """Gist'dan signal tarixini o'qiydi. Muammo bo'lsa bo'sh ro'yxat qaytaradi."""
    if not tracking_enabled():
        return []
    try:
        resp = requests.get(
            f"{GIST_API_URL}/{GIST_ID}",
            headers={"Authorization": f"Bearer {GITHUB_TOKEN}", "Accept": "application/vnd.github+json"},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        content = data["files"]["signals.json"]["content"]
        import json
        return json.loads(content)
    except Exception as e:
        print(f"Signal logni o'qishda xatolik: {e}")
        try:
            send_telegram_message(f"⚠️ Signal logni O'QISHDA xatolik (tarix yo'qolishi mumkin!): {e}")
        except Exception:
            pass
        return []


def save_signal_log(log):
    """Signal tarixini Gist'ga qaytadan yozadi."""
    if not tracking_enabled():
        return
    import json
    try:
        resp = requests.patch(
            f"{GIST_API_URL}/{GIST_ID}",
            headers={"Authorization": f"Bearer {GITHUB_TOKEN}", "Accept": "application/vnd.github+json"},
            json={"files": {"signals.json": {"content": json.dumps(log, indent=2, ensure_ascii=False)}}},
            timeout=15,
        )
        resp.raise_for_status()
    except Exception as e:
        print(f"Signal logni saqlashda xatolik: {e}")
        try:
            send_telegram_message(f"⚠️ Signal logni SAQLASHDA xatolik (tracking ishlamadi): {e}")
        except Exception:
            pass


SL_BUFFER = 0.03  # sweep darajasidan qo'shimcha zaxira (USD) - tasodifiy tebranishdan himoya


FVG_SL_BUFFER = 1.0  # FVG asosidagi SL uchun maxsus zaxira (USD) - kengroq, chunki
                      # sweep asosidagi SL_BUFFER (0.03)dan farqli, bu tor SL


def compute_sl_level(signal):
    """Signal strukturasidan SL (stop-loss) darajasini aniqlaydi, kichik zaxira
    (SL_BUFFER) bilan — bu narx aynan sweep/event darajasiga qaytadan tegib,
    lekin buzmasdan o'tgan holatda SL tasodifan darhol urilib qolishining oldini
    oladi. Barcha signal turlariga bir xilda qo'llanadi (avval faqat JACKPOT'da
    bor edi, endi barchasida).

    ESLATMA: smc_official_* uchun SL endi Sweep emas, balki FVG asosida
    (1 dollar zaxira bilan) - bu strategiyaning asl maqsadiga ("sweep'dan keyin
    darrov keladigan FVG") mos, torroq, tezroq TP'larga yetuvchi SL beradi.
    JACKPOT va Spring/Upthrust'da FVG tushunchasi yo'q, shuning uchun ular
    hamon sweep/event asosida qoladi."""
    if signal["type"] == "smc_bullish":
        return signal["sweep_level"] - SL_BUFFER
    if signal["type"] == "smc_bearish":
        return signal["sweep_level"] + SL_BUFFER
    if signal["type"] == "dynamic_spring":
        return signal["event_low"] - SL_BUFFER
    if signal["type"] == "dynamic_upthrust":
        return signal["event_high"] + SL_BUFFER
    if signal["type"] == "jackpot_spring":
        return signal["event_low"] - SL_BUFFER
    if signal["type"] == "jackpot_upthrust":
        return signal["event_high"] + SL_BUFFER
    if signal["type"] == "smc_official_bullish":
        return signal["fvg_bottom"] - FVG_SL_BUFFER
    if signal["type"] == "smc_official_bearish":
        return signal["fvg_top"] + FVG_SL_BUFFER
    if signal["type"] == "ob_fvg_bullish":
        return signal["zone_bottom"] - SL_BUFFER
    return signal["zone_top"] + SL_BUFFER  # ob_fvg_bearish


def get_signal_event_key(signal):
    """Signalning 'o'ziga xos voqea vaqti'ni qaytaradi - bu bir xil voqea
    (masalan bir xil BOS) qayta-qayta xabar qilinmasligi uchun solishtirish
    kaliti sifatida ishlatiladi."""
    for field in ("bos_time", "test_time", "event_time", "broken_time", "fvg_time", "sweep_time"):
        if field in signal and signal[field] is not None:
            return signal[field]
    return None


def is_direction_cooldown_active(signal, interval, cooldown_candles=3):
    """Agar oxirgi `cooldown_candles` svecha ichida, SHU YO'NALISHDA (bullish/
    bearish) allaqachon signal yuborilgan bo'lsa - True qaytaradi. Bu - bitta
    katta bozor harakati davomida bir necha marta (turli FVG'lar orqali)
    signal kelishining oldini oladi, chunki ular aslida bitta imkoniyat."""
    if not tracking_enabled():
        return False
    direction = "bullish" if signal["type"] in (
        "smc_bullish", "smc_official_bullish", "dynamic_spring", "jackpot_spring", "ob_fvg_bullish"
    ) else "bearish"
    import datetime as dt
    now = dt.datetime.now(dt.timezone.utc)
    cooldown_sec = interval_to_minutes(interval) * cooldown_candles * 60
    log = load_signal_log()
    for e in reversed(log):
        if e.get("interval") != interval or e.get("direction") != direction:
            continue
        try:
            e_time = dt.datetime.fromisoformat(e["time"])
        except Exception:
            continue
        return (now - e_time).total_seconds() <= cooldown_sec
    return False


def is_duplicate_signal(signal, interval):
    """Gist'dagi so'nggi yozuvlar orasida, xuddi shu turdagi va xuddi shu voqea
    vaqtiga ega signal allaqachon yuborilganmi tekshiradi. FRESH_BREAK_WINDOW
    kengaytirilgani uchun (bir necha svecha), bir xil voqea ketma-ket bir
    nechta ishga tushishda aniqlanishi mumkin - bu funksiya takrorni oldini oladi."""
    if not tracking_enabled():
        return False
    key = get_signal_event_key(signal)
    if key is None:
        return False
    log = load_signal_log()
    for e in log:
        if e.get("type") == signal["type"] and e.get("interval") == interval \
                and e.get("event_key") == key:
            return True
    return False


def send_to_worker(event_key, direction, entry_price, sl_price, tp_dict, timeframe):
    """Trade execution Worker'ga (hali qurilmagan/sozlanmagan) signal ma'lumotini
    yuboradi. WORKER_URL sozlanmagan bo'lsa, jim o'tkazib yuboriladi - hozirgi
    Telegram/Gist ishlashiga hech qanday ta'sir qilmaydi."""
    if not WORKER_URL:
        return
    import requests
    payload = {
        "event_key": event_key,
        "symbol": "XAUUSD",
        "timeframe": timeframe,
        "strategy": "SMC_Sweep_FVG",
        "direction": direction,  # "BUY" yoki "SELL"
        "entry_price": entry_price,
        "sl_price": sl_price,
        "tp2": tp_dict.get("TP2"),
        "tp3": tp_dict.get("TP3"),
        "tp5": tp_dict.get("TP5"),
        "tp10": tp_dict.get("TP10"),
        "tp15": tp_dict.get("TP15"),
    }
    headers = {"Authorization": f"Bearer {WORKER_SECRET_KEY}"}
    try:
        requests.post(WORKER_URL, json=payload, headers=headers, timeout=5)
    except Exception as e:
        print(f"Worker'ga yuborishda xato: {e}")


def log_new_signal(signal, price_data, interval):
    """Yangi chiqqan signalni SL/TP darajalari bilan tarixga qo'shadi
    (natijasi keyinroq svecha-svecha tekshiriladi)."""
    if not tracking_enabled():
        return
    import datetime as dt

    direction = "bullish" if signal["type"] in ("smc_bullish", "smc_official_bullish", "dynamic_spring", "jackpot_spring", "ob_fvg_bullish") else "bearish"
    entry_price = float(price_data["price"])
    sl_level = float(compute_sl_level(signal))
    risk = abs(entry_price - sl_level)
    if risk == 0:
        risk = entry_price * 0.001  # nolga bo'linishdan himoya

    sign = 1 if direction == "bullish" else -1
    tp2_level = entry_price + sign * 2 * risk
    tp3_level = entry_price + sign * 3 * risk
    tp5_level = entry_price + sign * 5 * risk
    tp10_level = entry_price + sign * 10 * risk
    tp15_level = entry_price + sign * 15 * risk
    event_key = get_signal_event_key(signal)
    now_utc = dt.datetime.now(dt.timezone.utc)
    in_session = is_active_session(now_utc)

    # SL/TP tayyor bo'ldi - Worker sinov jarayonida bo'lgani uchun, endi
    # sessiya ichida HAM, tashqarisida HAM yuboriladi (ikkalasi ham statistik
    # jihatdan musbat). Faqat bozor HAFTALIK yopilish/ochilish atrofidagi
    # (Juma yopilishdan 1 soat oldin, Yakshanba ochilgandan 1 soat keyingacha)
    # notekis davr chetlab o'tiladi.
    in_buffer = is_market_transition_buffer(now_utc)
    if not in_buffer:
        send_to_worker(
            event_key=event_key,
            direction="BUY" if direction == "bullish" else "SELL",
            entry_price=entry_price,
            sl_price=sl_level,
            tp_dict={"TP2": tp2_level, "TP3": tp3_level, "TP5": tp5_level,
                     "TP10": tp10_level, "TP15": tp15_level},
            timeframe=interval,
        )
    else:
        print(f"[WORKER] Bozor haftalik yopilish/ochilish buferida - Worker'ga yuborilmadi (faqat Gist/Telegram'da qoladi).")

    log = load_signal_log()
    log.append({
        "time": now_utc.isoformat(),
        "type": signal["type"],
        "interval": interval,
        "session": "active" if in_session else "outside",
        "event_key": event_key,
        "direction": direction,
        "entry_price": entry_price,
        "sl_level": sl_level,
        "tp2_level": tp2_level,
        "tp3_level": tp3_level,
        "tp5_level": tp5_level,
        "tp10_level": tp10_level,
        "tp15_level": tp15_level,
        "best_tp": 0,
        "checked": False,
        "outcome": None,
    })
    save_signal_log(log)
    print(f"[TRACKING] Signal muvaffaqiyatli saqlandi. Jami yozuvlar soni: {len(log)}")

    # Tasdiqlash: darhol qayta o'qib, haqiqatan saqlanganini tekshiramiz
    verify_log = load_signal_log()
    if len(verify_log) != len(log):
        msg = (f"⚠️ TRACKING NOMUVOFIQLIK: saqlashdan keyin {len(log)} ta yozuv "
               f"kutilgan edi, lekin qayta o'qishda {len(verify_log)} ta topildi!")
        print(msg)
        try:
            send_telegram_message(msg)
        except Exception:
            pass


def evaluate_pending_signals():
    """Hali yopilmagan signallarni, ularning o'z intervalidagi svechalar bo'yicha
    (signal chiqqandan keyingi barcha svechalarni ketma-ket ko'rib) tekshiradi:
    SL yoki TP2x/3x/5x qaysi biri birinchi urilgan bo'lsa, shuni natija qiladi.
    Bir necha bor chaqirilsa ham xavfsiz — allaqachon tekshirilgan svechalar
    qayta hisoblanadi, lekin natija o'zgarmaydi (idempotent)."""
    if not tracking_enabled():
        return None

    import datetime as dt

    log = load_signal_log()
    pending = [e for e in log if not e.get("checked")]
    if not pending:
        checked = [e for e in log if e.get("checked")]
        return _build_stats(log, checked)

    # Har bir kerakli interval uchun sveчalarni bir marta olamiz (API tejash uchun)
    intervals_needed = sorted(set(e.get("interval", "5min") for e in pending))
    candles_cache = {}
    for iv in intervals_needed:
        try:
            candles_cache[iv] = get_gold_candles(interval=iv, outputsize=300)
        except Exception as e:
            print(f"Tracking uchun sveчa olishda xatolik ({iv}): {e}")

    now = dt.datetime.now(dt.timezone.utc)
    changed = False

    for entry in pending:
        iv = entry.get("interval", "5min")
        df = candles_cache.get(iv)
        if df is None:
            continue
        try:
            signal_time = dt.datetime.fromisoformat(entry["time"])
        except (ValueError, KeyError):
            continue

        if "sl_level" not in entry:
            # Eski formatdagi yozuv (SL/TP maydonlarisiz) - yopib qo'yamiz, statistikaga aralashtirmaymiz
            entry["checked"] = True
            entry["outcome"] = "legacy_skipped"
            changed = True
            continue

        idx = df.index
        idx_naive = idx.tz_localize(None) if idx.tz is not None else idx
        signal_time_naive = signal_time.replace(tzinfo=None)
        sub = df[idx_naive > signal_time_naive]
        timeout_sec = timeout_seconds_for(entry.get("interval", "5min"))
        if sub.empty:
            # Muddat (interval'ga mos) o'tgan bo'lsa va hali ma'lumot yo'q bo'lsa - "muddati tugadi" deb yopamiz
            if (now - signal_time).total_seconds() > timeout_sec:
                entry["checked"] = True
                entry["outcome"] = f"tp{entry['best_tp']}" if entry["best_tp"] else "timeout"
                changed = True
            continue

        direction = entry["direction"]
        sl = entry["sl_level"]
        tp_levels = {2: entry.get("tp2_level"), 3: entry.get("tp3_level"), 5: entry.get("tp5_level"),
                     10: entry.get("tp10_level"), 15: entry.get("tp15_level")}
        best_tp_before = entry.get("best_tp", 0)
        best_tp = best_tp_before
        hit_sl = False

        for _, candle in sub.iterrows():
            lo, hi = candle["low"], candle["high"]

            sl_touched = (lo <= sl) if direction == "bullish" else (hi >= sl)
            if sl_touched:
                hit_sl = True
                break

            for level_num in (2, 3, 5, 10, 15):
                if best_tp >= level_num or tp_levels[level_num] is None:
                    continue
                level_price = tp_levels[level_num]
                tp_touched = (hi >= level_price) if direction == "bullish" else (lo <= level_price)
                if tp_touched:
                    best_tp = level_num

            if best_tp == 15:
                break

        entry["best_tp"] = best_tp
        if hit_sl:
            entry["checked"] = True
            entry["outcome"] = f"tp{best_tp}" if best_tp else "loss"
            changed = True
        elif best_tp == 15:
            entry["checked"] = True
            entry["outcome"] = "tp15"
            changed = True
        elif (now - signal_time).total_seconds() > timeout_sec:
            entry["checked"] = True
            entry["outcome"] = f"tp{best_tp}" if best_tp else "timeout"
            changed = True
        elif best_tp != best_tp_before:
            changed = True  # progress o'zgardi, saqlaymiz

    if changed:
        save_signal_log(log)

    checked = [e for e in log if e.get("checked")]
    return _build_stats(log, checked)


R_MAP = {"loss": -1, "timeout": 0, "tp2": 2, "tp3": 3, "tp5": 5, "tp10": 10, "tp15": 15}


def _stats_for_subset(checked):
    total = len(checked)
    losses = sum(1 for e in checked if e["outcome"] == "loss")
    timeouts = sum(1 for e in checked if e["outcome"] == "timeout")
    tp2 = sum(1 for e in checked if e["outcome"] == "tp2")
    tp3 = sum(1 for e in checked if e["outcome"] == "tp3")
    tp5 = sum(1 for e in checked if e["outcome"] == "tp5")
    tp10 = sum(1 for e in checked if e["outcome"] == "tp10")
    tp15 = sum(1 for e in checked if e["outcome"] == "tp15")
    wins = tp2 + tp3 + tp5 + tp10 + tp15
    win_rate = round(wins / total * 100, 1) if total else None
    total_r = sum(R_MAP.get(e["outcome"], 0) for e in checked)
    avg_r = round(total_r / total, 2) if total else None
    return {
        "total_checked": total, "wins": wins, "losses": losses, "timeouts": timeouts,
        "tp2": tp2, "tp3": tp3, "tp5": tp5, "tp10": tp10, "tp15": tp15, "win_rate": win_rate,
        "total_r": total_r, "avg_r": avg_r,
    }


ROLLING_WINDOW_HOURS = 4  # "so'nggi N soatlik" statistika oynasi


def _build_stats(log, checked):
    import datetime as dt

    valid_outcomes = {"loss", "timeout", "tp2", "tp3", "tp5", "tp10", "tp15"}
    checked = [e for e in checked if e.get("outcome") in valid_outcomes]

    overall = _stats_for_subset(checked)

    by_interval = {}
    for iv in sorted(set(e.get("interval", "5min") for e in checked)):
        subset = [e for e in checked if e.get("interval", "5min") == iv]
        by_interval[iv] = _stats_for_subset(subset)

    # Signal turi bo'yicha (JACKPOT, OB/FVG, SMC, oddiy Spring/Upthrust) - qaysi
    # strategiya eng yaxshi ishlayotganini ko'rish uchun. bullish/bearish birlashtiriladi
    # (masalan jackpot_spring + jackpot_upthrust -> "jackpot"), chunki yo'nalish emas,
    # strategiya turi muhim.
    TYPE_GROUPS = {
        "jackpot_spring": "jackpot", "jackpot_upthrust": "jackpot",
        "ob_fvg_bullish": "ob_fvg", "ob_fvg_bearish": "ob_fvg",
        "smc_bullish": "smc", "smc_bearish": "smc",
        "smc_official_bullish": "smc", "smc_official_bearish": "smc",
        "dynamic_spring": "dynamic", "dynamic_upthrust": "dynamic",
    }
    by_type = {}
    for grp in sorted(set(TYPE_GROUPS.get(e.get("type"), "boshqa") for e in checked)):
        subset = [e for e in checked if TYPE_GROUPS.get(e.get("type"), "boshqa") == grp]
        by_type[grp] = _stats_for_subset(subset)

    # Sessiya bo'yicha (London/NY ichida vs tashqarisida) - session filtri
    # haqiqatan foydali/farqli natija berayotganini tekshirish uchun. Eski
    # (session maydonisiz) yozuvlar "noma'lum" guruhga tushadi, statistikani
    # buzmaydi (alohida ko'rsatiladi, lekin filtrlanmaydi).
    by_session = {}
    for sess in sorted(set(e.get("session", "noma'lum") for e in checked)):
        subset = [e for e in checked if e.get("session", "noma'lum") == sess]
        by_session[sess] = _stats_for_subset(subset)

    # So'nggi ROLLING_WINDOW_HOURS soat ichida YOPILGAN signallar (umumiy statistika
    # o'chirilmaydi, bu faqat qo'shimcha, "hozir qanday ketyapti" ko'rsatkichi)
    now = dt.datetime.now(dt.timezone.utc)
    recent = []
    for e in checked:
        try:
            t = dt.datetime.fromisoformat(e["time"])
        except (ValueError, KeyError):
            continue
        if (now - t).total_seconds() <= ROLLING_WINDOW_HOURS * 3600:
            recent.append(e)
    recent_stats = _stats_for_subset(recent)

    pending_entries = [e for e in log if not e.get("checked")]
    pending_breakdown = {0: 0, 2: 0, 3: 0}
    for e in pending_entries:
        bt = e.get("best_tp", 0)
        pending_breakdown[bt] = pending_breakdown.get(bt, 0) + 1

    overall["pending"] = len(pending_entries)
    overall["pending_breakdown"] = pending_breakdown
    overall["by_interval"] = by_interval
    overall["by_type"] = by_type
    overall["by_session"] = by_session
    overall["recent"] = recent_stats
    return overall


# ============================================================================
# TELEGRAM YUBORISH
# ============================================================================

def send_telegram_message(text):
    """Matnli xabar yuboradi. Uzun bo'lsa avtomatik bo'laklarga bo'ladi.
    Eslatma: 'remove_keyboard' - bu avvalgi (boshqa loyihadan qolgan) reply
    keyboard panelini butunlay o'chirib tashlash uchun, xavfsiz - doim yuborilsa
    ham zarari yo'q."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    max_len = 4000
    chunks = [text[i:i + max_len] for i in range(0, len(text), max_len)] or [text]
    for chunk in chunks:
        resp = requests.post(
            url,
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": chunk,
                "disable_web_page_preview": True,
                "reply_markup": {"remove_keyboard": True},
            },
            timeout=15,
        )
        resp.raise_for_status()


def send_telegram_document(file_path, caption=""):
    """Grafikni HUJJAT sifatida (siqilmasdan, yuqori sifatda) yuboradi."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
    with open(file_path, "rb") as f:
        resp = requests.post(
            url,
            data={"chat_id": TELEGRAM_CHAT_ID, "caption": caption[:1000]},
            files={"document": f},
            timeout=30,
        )
    resp.raise_for_status()


def send_ntfy_alert(title, message, priority="high"):
    """ntfy.sh orqali tezkor push-bildirishnoma yuboradi (Telegram'ga QO'SHIMCHA,
    uni almashtirmaydi). Telefon qulflangan/uxlab yotgan holatda ham darhol
    yetib borishi uchun. NTFY_TOPIC sozlanmagan bo'lsa, jim o'tkazib yuboriladi."""
    if not NTFY_TOPIC:
        print("[NTFY] NTFY_TOPIC sozlanmagan (bo'sh/yo'q) - push xabar yuborilmadi.")
        return
    try:
        resp = requests.post(
            f"https://ntfy.sh/{NTFY_TOPIC}",
            data=message.encode("utf-8"),
            headers={
                "Title": title.encode("utf-8"),
                "Priority": priority,
                "Tags": "rotating_light",
            },
            timeout=10,
        )
        print(f"[NTFY] Yuborildi (topic={NTFY_TOPIC}), status={resp.status_code}")
    except Exception as e:
        print(f"ntfy.sh xabar yuborishda xatolik: {e}")


# ============================================================================
# REJIM 1: SIGNAL TEKSHIRUV (har 5 daqiqada)
# ============================================================================

def is_active_session(timestamp):
    """London ochilishidan Nyu-York yopilishigacha bo'lgan (fasllarga qarab suriladigan)
    davrni xavfsiz qamrab oladigan UTC oralig'i: 06:00 - 23:00. Bu oraliqdan tashqarida
    (asosan Osiyo sessiyasi) savdo hajmi kamroq, harakat ko'proq shovqinli bo'ladi."""
    try:
        hour = timestamp.hour
    except AttributeError:
        return True  # vaqt aniqlanmasa, filtrlamaymiz (xavfsiz tomonga)
    return 6 <= hour < 23


def is_market_transition_buffer(now_utc, buffer_minutes=60):
    """Bozor bufer vaqtlarini aniqlaydi - bu davrlarda narx harakati notekis,
    hajm/likvidlik past, spread keng bo'lishi mumkin. Ikkita holat tekshiriladi:

    1. HAFTALIK: Juma yopilishi (~22:00 UTC) va Yakshanba ochilishi (~22:00 UTC)
       atrofidagi 1 soatlik bufer.
    2. KUNLIK: har kuni (Dushanba-Juma) Nyu-York yopilishi va Osiyo (Sidney)
       ochilishi orasidagi ~21:00-23:00 UTC "sokin oraliq" - Nyu-York
       yopilgach hajm keskin qurib qoladi, Tokio (00:00 UTC) ochilgunicha
       harakat notekis bo'lishi mumkin.

    Aniq vaqtlar taxminiy standart (broker/DST'ga qarab biroz farq qilishi
    mumkin)."""
    weekday = now_utc.weekday()  # Monday=0 ... Friday=4, Saturday=5, Sunday=6
    minutes_of_day = now_utc.hour * 60 + now_utc.minute
    close_minutes = 22 * 60  # taxminan 22:00 UTC - Juma yopilishi
    open_minutes = 22 * 60   # taxminan 22:00 UTC - Yakshanba ochilishi

    if weekday == 4 and (close_minutes - buffer_minutes) <= minutes_of_day <= close_minutes:
        return True  # Juma, yopilishdan 1 soat oldin
    if weekday == 6 and open_minutes <= minutes_of_day <= (open_minutes + buffer_minutes):
        return True  # Yakshanba, ochilishdan 1 soat keyingacha

    # Kunlik NY->Osiyo "sokin oraliq": Dushanba-Juma, 21:00-23:00 UTC
    if weekday in (0, 1, 2, 3, 4):
        daily_start = 21 * 60
        daily_end = 23 * 60
        if daily_start <= minutes_of_day < daily_end:
            return True

    return False


def run_signal_check(df, price_data, interval="5min"):
    # Eng kuchli signal birinchi tekshiriladi — agar u chiqsa, boshqalar tekshirilmaydi
    jackpot = detect_jackpot_signal(df, lookback=144)
    ob_fvg = None if jackpot else detect_ob_fvg_entry(df, lookback=144)
    # 🔥 SMC signal endi 'smartmoneyconcepts' (LuxAlgo'dan portlangan, sinalgan)
    # kutubxonasi asosida - BOS va CHoCH'ni aniq, pattern-matching orqali ajratadi
    smc = None if (jackpot or ob_fvg) else detect_smc_official_signal(df, lookback=144)
    dynamic = None if (jackpot or ob_fvg or smc) else detect_dynamic_spring_upthrust(df, lookback=144)
    signal = jackpot or ob_fvg or smc or dynamic

    if not signal:
        # DEBUG: range topilgan-topilmaganini va joriy narxni logga yozib chiqaramiz -
        # bu signal nega chiqmaganini keyinroq aniq tahlil qilish uchun kerak
        range_info = detect_dynamic_range(df, lookback=144)
        last_close = df["close"].iloc[-1]
        if range_info:
            print(f"[{interval}] Signal yo'q. Range topildi: "
                  f"{range_info['range_low']:.2f} - {range_info['range_high']:.2f}, "
                  f"joriy narx: {last_close:.2f}")
        else:
            print(f"[{interval}] Signal yo'q. Range topilmadi (teng cho'qqi/tub yo'q), "
                  f"joriy narx: {last_close:.2f}")
        return

    if is_duplicate_signal(signal, interval):
        print(f"[{interval}] Signal topildi ({signal['type']}), lekin bu voqea "
              f"allaqachon xabar qilingan (event_key={get_signal_event_key(signal)}) — takrorlanmaydi.")
        return

    if is_direction_cooldown_active(signal, interval):
        print(f"[{interval}] Signal topildi ({signal['type']}), lekin shu yo'nalishda "
              f"yaqinda (oxirgi 3 svecha ichida) allaqachon signal yuborilgan — "
              f"bitta harakatning davomi bo'lishi mumkin, takrorlanmaydi.")
        return

    chart_path = make_chart_image(df.tail(150), interval=interval)

    tf_tag = f"[{interval}]"
    bias = get_trend_bias(df)
    signal_direction = "bullish" if signal["type"] in ("smc_bullish", "smc_official_bullish", "dynamic_spring", "jackpot_spring", "ob_fvg_bullish") else "bearish"
    trend_warning = ""
    if bias != "neutral" and bias != signal_direction:
        bias_uz = "yuqoriga" if bias == "bullish" else "pastga"
        trend_warning = f"\n⚠️ Trendga qarshi (umumiy {bias_uz})"

    session_warning = ""
    if not is_active_session(df.index[-1]):
        session_warning = "\n⏰ Sessiyadan tashqarida (kam hajm)"

    htf_warning = ""
    htf_zone_info = ""
    entry_price = float(price_data["price"])
    if interval != "1min":
        # 1H konteksti faqat 5m uchun hisoblanadi - 1m uchun yetarli tarixiy
        # ma'lumot (450 svecha = ~7.5 soat) 1H hisob-kitobi uchun kifoya emas
        htf_bias = get_htf_bias(df)
        if htf_bias != "neutral" and htf_bias != signal_direction:
            htf_bias_uz = "yuqoriga" if htf_bias == "bullish" else "pastga"
            htf_warning = f"\n📐 1H trendga qarshi (umumiy {htf_bias_uz})"

        htf_zones = find_htf_zones(df)
        matching_zones = [z for z in htf_zones if z[0] <= entry_price <= z[1]]
        if matching_zones:
            zone_types_uz = {
                "bullish_fvg": "FVG", "bearish_fvg": "FVG",
                "bullish_ob": "OB", "bearish_ob": "OB",
            }
            types_found = sorted(set(zone_types_uz.get(z[2], z[2]) for z in matching_zones))
            htf_zone_info = f"\nℹ️ 1H {'/'.join(types_found)} zonasida"

    if signal["type"] == "jackpot_spring":
        emoji, label = "🎰🟢", f"{tf_tag} JACKPOT: Spring + Test (BULLISH)"
        caption = (
            f"{emoji} {label}\n"
            f"Narx: {price_data['price']} USD\n"
            f"Range: {signal['range_low']:.2f} - {signal['range_high']:.2f}\n"
            f"Sweep: {signal['event_low']:.2f}  |  Test: {signal['test_low']:.2f}\n"
            f"Hozir: {signal['current_close']:.2f}"
        )
    elif signal["type"] == "jackpot_upthrust":
        emoji, label = "🎰🔴", f"{tf_tag} JACKPOT: Upthrust + Test (BEARISH)"
        caption = (
            f"{emoji} {label}\n"
            f"Narx: {price_data['price']} USD\n"
            f"Range: {signal['range_low']:.2f} - {signal['range_high']:.2f}\n"
            f"Sweep: {signal['event_high']:.2f}  |  Test: {signal['test_high']:.2f}\n"
            f"Hozir: {signal['current_close']:.2f}"
        )
    elif signal["type"] == "smc_official_bullish":
        structure_note = f" + {signal['structure_kind']}" if signal["has_structure"] else ""
        emoji, label = "🔥🟢", f"{tf_tag} SMC: Sweep + FVG{structure_note} (BULLISH)"
        caption = (
            f"{emoji} {label}\n"
            f"Narx: {price_data['price']} USD\n"
            f"Sweep darajasi: {signal['sweep_level']:.2f}\n"
            f"FVG: {signal['fvg_bottom']:.2f} - {signal['fvg_top']:.2f}"
        )
    elif signal["type"] == "smc_official_bearish":
        structure_note = f" + {signal['structure_kind']}" if signal["has_structure"] else ""
        emoji, label = "🔥🔴", f"{tf_tag} SMC: Sweep + FVG{structure_note} (BEARISH)"
        caption = (
            f"{emoji} {label}\n"
            f"Narx: {price_data['price']} USD\n"
            f"Sweep darajasi: {signal['sweep_level']:.2f}\n"
            f"FVG: {signal['fvg_bottom']:.2f} - {signal['fvg_top']:.2f}"
        )
    elif signal["type"] == "ob_fvg_bullish":
        emoji, label = "🎯🟢", f"{tf_tag} OB/FVG ENTRY (BULLISH) — retracement tasdiqlandi"
        caption = (
            f"{emoji} {label}\n"
            f"Narx: {price_data['price']} USD\n"
            f"BOS darajasi: {signal['bos_level']:.2f}\n"
            f"Entry zona: {signal['zone_bottom']:.2f} - {signal['zone_top']:.2f}\n"
            f"Hozir: {signal['entry_close']:.2f}"
        )
    elif signal["type"] == "ob_fvg_bearish":
        emoji, label = "🎯🔴", f"{tf_tag} OB/FVG ENTRY (BEARISH) — retracement tasdiqlandi"
        caption = (
            f"{emoji} {label}\n"
            f"Narx: {price_data['price']} USD\n"
            f"BOS darajasi: {signal['bos_level']:.2f}\n"
            f"Entry zona: {signal['zone_bottom']:.2f} - {signal['zone_top']:.2f}\n"
            f"Hozir: {signal['entry_close']:.2f}"
        )
    elif signal["type"] == "dynamic_spring":
        emoji, label = "🟢", f"{tf_tag} SPRING (range'ga qaytish + tasdiqlangan davomiylik)"
        caption = (
            f"{emoji} {label}\n"
            f"Narx: {price_data['price']} USD\n"
            f"Range: {signal['range_low']:.2f} - {signal['range_high']:.2f}\n"
            f"Voqea narxi: {signal['event_close']:.2f} → hozir: {signal['current_close']:.2f}"
        )
    else:
        emoji, label = "🔴", f"{tf_tag} UPTHRUST (range'ga qaytish + tasdiqlangan davomiylik)"
        caption = (
            f"{emoji} {label}\n"
            f"Narx: {price_data['price']} USD\n"
            f"Range: {signal['range_low']:.2f} - {signal['range_high']:.2f}\n"
            f"Voqea narxi: {signal['event_close']:.2f} → hozir: {signal['current_close']:.2f}"
        )

    caption += trend_warning + session_warning + htf_warning + htf_zone_info
    send_telegram_document(chart_path, caption=caption)
    send_ntfy_alert(title=f"🚨 {tf_tag} {emoji} Yangi signal!", message=caption[:400])

    # Signal tarixga yoziladi - natijasi keyinroq (soatlik status'da) tekshiriladi
    log_new_signal(signal, price_data, interval)

    print(f"{label} signali yuborildi.")


# ============================================================================
# REJIM 2: SOATLIK HOLAT (har soatda)
# ============================================================================

RANGE_TYPE_NAMES = {
    "bullish_squeeze": "📈 Yuqoriga qisilish (bullish squeeze) — pastki chegara ko'tarilmoqda",
    "bearish_squeeze": "📉 Pastga qisilish (bearish squeeze) — yuqori chegara pasaymoqda",
    "symmetrical_triangle": "🔺 Simmetrik uchburchak — ikkala tomondan torayapti",
    "flat_range": "📦 Oddiy gorizontal diapazon — tor oraliqda tebranmoqda",
}


def run_hourly_status(df, price_data, interval="5min"):
    lookback = RANGE_LOOKBACK
    if len(df) < lookback + 1:
        send_telegram_message(f"🕐 [{interval}] Soatlik holat: ma'lumot yetarli emas.")
        return

    window = df.iloc[-(lookback + 1):-1]
    range_low = window["low"].min()
    range_high = window["high"].max()

    range_state = detect_range_state(df, lookback=lookback)

    lines = [
        f"🕐 [{interval}] Soatlik holat",
        f"Narx: {price_data['price']} USD",
        f"Diapazon (so'nggi {lookback} sveча): {range_low:.2f} – {range_high:.2f}",
    ]

    if range_state:
        lines.append(f"\n{RANGE_TYPE_NAMES.get(range_state['type'], range_state['type'])}")
    else:
        lines.append("\n📐 Holat: Aniq diapazon/uchburchak shakli yo'q (trend/keng harakat)")

    lines.append("\nSignal: Hozircha spring/upthrust aniqlanmadi (aniqlansa alohida xabar keladi)")

    try:
        events = get_forex_calendar_events(hours_ahead=24)
        if events:
            lines.append("\n📅 Yaqin 24 soatdagi muhim USD yangiliklari:")
            for e in events[:5]:
                time_str = e["time"].strftime("%d.%m %H:%M UTC")
                extra = ""
                if e["forecast"] or e["previous"]:
                    extra = f" (bashorat: {e['forecast']}, oldingi: {e['previous']})"
                lines.append(f"- {time_str} — {e['title']}{extra}")
        else:
            lines.append("\n📅 Yaqin 24 soatda yuqori ta'sirli USD yangiligi yo'q.")
    except Exception as e:
        lines.append(f"\n⚠️ Kalendar ma'lumoti olinmadi: {e}")

    if tracking_enabled():
        try:
            stats = evaluate_pending_signals()
            if stats and stats["total_checked"] > 0:
                lines.append(
                    f"\n📈 Umumiy statistika: {stats['total_checked']} ta yopilgan signal — "
                    f"aniqlik: {stats['win_rate']}%, jami: {stats['total_r']:+g}R "
                    f"(o'rtacha {stats['avg_r']:+g}R/signal)\n"
                    f"🚀 TP15x: {stats['tp15']} | 💎 TP10x: {stats['tp10']} | 🔥 TP5x: {stats['tp5']} | "
                    f"🟢 TP3x: {stats['tp3']} | 🟡 TP2x: {stats['tp2']} | "
                    f"❌ SL: {stats['losses']} | ⏱ muddati o'tgan: {stats['timeouts']}"
                )
                for iv, s in stats["by_interval"].items():
                    lines.append(
                        f"\n▫️ [{iv}]: {s['total_checked']} ta — aniqlik: {s['win_rate']}%, "
                        f"{s['total_r']:+g}R (o'rt {s['avg_r']:+g}R) "
                        f"(🚀{s['tp15']} 💎{s['tp10']} 🔥{s['tp5']} 🟢{s['tp3']} 🟡{s['tp2']} "
                        f"❌{s['losses']} ⏱{s['timeouts']})"
                    )
                lines.append("\n📊 Strategiya bo'yicha:")
                TYPE_LABELS = {"jackpot": "🎰 JACKPOT", "ob_fvg": "📍 OB/FVG",
                               "smc": "🔥 SMC", "dynamic": "🟢 Spring/Upthrust", "boshqa": "Boshqa"}
                for grp, s in stats["by_type"].items():
                    label = TYPE_LABELS.get(grp, grp)
                    lines.append(
                        f"\n{label}: {s['total_checked']} ta — aniqlik: {s['win_rate']}%, "
                        f"{s['total_r']:+g}R (o'rt {s['avg_r']:+g}R)"
                    )

                SESSION_LABELS = {"active": "🟢 Sessiya ichida", "outside": "🌙 Sessiyadan tashqari",
                                   "noma'lum": "❔ Noma'lum (eski)"}
                by_session = {k: v for k, v in stats["by_session"].items() if v["total_checked"] > 0}
                if by_session:
                    lines.append("\n🕐 Sessiya bo'yicha:")
                    for sess, s in by_session.items():
                        label = SESSION_LABELS.get(sess, sess)
                        lines.append(
                            f"\n{label}: {s['total_checked']} ta — aniqlik: {s['win_rate']}%, "
                            f"{s['total_r']:+g}R (o'rt {s['avg_r']:+g}R)"
                        )

                r = stats["recent"]
                if r["total_checked"] > 0:
                    lines.append(
                        f"\n🕓 So'nggi {ROLLING_WINDOW_HOURS} soat: {r['total_checked']} ta — "
                        f"aniqlik: {r['win_rate']}%, {r['total_r']:+g}R "
                        f"(🚀{r['tp15']} 💎{r['tp10']} 🔥{r['tp5']} 🟢{r['tp3']} 🟡{r['tp2']} "
                        f"❌{r['losses']} ⏱{r['timeouts']})"
                    )
                else:
                    lines.append(f"\n🕓 So'nggi {ROLLING_WINDOW_HOURS} soat: yopilgan signal yo'q.")
                if stats["pending"] > 0:
                    pb = stats["pending_breakdown"]
                    parts = []
                    if pb.get(10, 0) > 0:
                        parts.append(f"💎 {pb[10]} ta 10x da")
                    if pb.get(5, 0) > 0:
                        parts.append(f"🔥 {pb[5]} ta 5x da")
                    if pb.get(3, 0) > 0:
                        parts.append(f"🟢 {pb[3]} ta 3x da")
                    if pb.get(2, 0) > 0:
                        parts.append(f"🟡 {pb[2]} ta 2x da")
                    if pb.get(0, 0) > 0:
                        parts.append(f"⏳ {pb[0]} ta hali TP urmagan")
                    lines.append(f"\n⏳ Kuzatilayotgan signallar ({stats['pending']}): " + ", ".join(parts))
            elif stats:
                lines.append(f"\n📈 Signal statistikasi: hali yopilgan signal yo'q (kuzatilmoqda: {stats['pending']}).")
        except Exception as e:
            lines.append(f"\n⚠️ Signal statistikasini olishda xatolik: {e}")

    send_telegram_message("\n".join(lines))
    print(f"[{interval}] Soatlik holat yuborildi.")


# ============================================================================
# ASOSIY DASTUR
# ============================================================================

def main():
    check_env_vars()
    mode = sys.argv[1] if len(sys.argv) > 1 else "signal"
    interval = sys.argv[2] if len(sys.argv) > 2 else "5min"

    try:
        candles_df = get_gold_candles(interval=interval, outputsize=450)
    except Exception as e:
        send_telegram_message(f"⚠️ Sveча ma'lumotini olishda xatolik ({interval}): {e}")
        sys.exit(1)

    price_data = build_price_data_from_candles(candles_df)

    if mode == "signal":
        run_signal_check(candles_df, price_data, interval=interval)
    elif mode == "status":
        run_hourly_status(candles_df, price_data, interval=interval)
    else:
        print(f"Noma'lum rejim: {mode}. 'signal' yoki 'status' bo'lishi kerak.")
        sys.exit(1)


if __name__ == "__main__":
    main()
