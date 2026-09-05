# XAUUSD Avtomat-Savdo Worker — o'rnatish qo'llanmasi

## Fayllar tuzilishi

| Fayl | Vazifasi |
|---|---|
| `ctrader_client.py` | cTrader Open API bilan past darajadagi ulanish (auth, order, SL/TP) |
| `risk_manager.py` | Lot hisoblash, jami risk chegarasi, kunlik circuit breaker |
| `trade_manager.py` | TP/SL trailing mantig'i (checkpoint bir qadam oldin) |
| `swing_detector.py` | Trailing SL uchun mustaqil swing narxini aniqlash (TwelveData orqali) |
| `worker.py` | Bosh fayl — hammasini birlashtiradi, Flask HTTP server + trailing sikli |
| `main_py_addition.py` | main.py'ga qo'shiladigan `send_signal_to_worker()` funksiyasi (namuna) |
| `.env.example` | Barcha kerakli environment variable'lar ro'yxati |
| `requirements.txt` | Python kutubxonalari |

## O'rnatish tartibi

1. **Kutubxonalarni o'rnatish** (Render avtomatik qiladi, lekin lokal test uchun):
   ```
   pip install -r requirements.txt
   ```

2. **`.env.example`ni asos qilib, Render'da Environment Variables bo'limiga
   barcha qiymatlarni kiriting** (WORKER_SECRET_KEY, CTRADER_CLIENT_ID,
   CTRADER_CLIENT_SECRET, CTRADER_ACCESS_TOKEN, CTRADER_ACCOUNT_ID,
   DEMO_MODE=true, va h.k.)

3. **Render'da yangi service yarating — "Background Worker" yoki
   "Web Service" turi** (Cron Job EMAS, chunki bu 24/7 ishlaydigan process).
   Start command: `python worker.py`

4. **main.py'ga `main_py_addition.py`dagi funksiyani qo'shing** —
   `log_new_signal()` ichida, SL/TP hisoblab bo'lingandan keyin chaqiring.

5. **main.py'ning Render environment'iga ham qo'shing:**
   - `WORKER_URL` — worker.py service'ining Render URL'i (masalan
     `https://sizning-worker.onrender.com`)
   - `WORKER_SECRET_KEY` — worker.py bilan BIR XIL qiymat

## MAJBURIY: ishga tushirishdan oldin tekshirish kerak bo'lgan narsalar

Bu kod **sinovdan o'tmagan** — cTrader Open API'ning aniq protobuf maydon
nomlari kutubxona versiyasiga qarab ozgina farq qilishi mumkin. Ishga
tushirishdan oldin:

1. **DEMO hisobda, eng kichik lot (0.01) bilan bitta qo'lda test order**
   yuboring (`send_market_order` funksiyasini alohida chaqirib) — order
   muvaffaqiyatli ochilishini, SL/TP to'g'ri narxda qo'yilishini tekshiring.

2. **`pip_value_per_lot` qiymatini tasdiqlang** — `worker.py`da
   `pip_value_per_lot = float(_symbol_info.lot_size)` qatori bor, bu
   XAUUSD uchun standart taxmin (1 lot = 100 oz). cTrader symbol
   ma'lumotidan olingan `lot_size`ning haqiqiy ma'nosini bitta test order
   natijasi bilan solishtirib tasdiqlang.

3. **Balans va narx bo'linuvchilarini tekshiring** (`moneyDigits`,
   symbol `digits`) — bular noto'g'ri bo'lsa butun risk hisob-kitobi
   xato bo'lib qoladi.

4. **`swing_detector.py` — bu ODDIY fraktal yondashuv**, sizning asosiy
   botingizdagi SMC/Wyckoff swing aniqlashi bilan bir xil emas. Bu
   boshlang'ich versiya uchun ishlatiladigan taxminiy yechim — aniqlik
   muhim bo'lsa, main.py'dagi haqiqiy swing logikasini umumiy modulga
   chiqarib, worker.py ham shuni ishlatishi tavsiya etiladi.

5. **Execution event handling** (`_on_execution_event` funksiyasi
   `worker.py`da) — hozircha faqat log qiladi. SL urilib pozitsiya
   avtomatik yopilganda, `trade_manager`/`risk_manager`dan o'sha
   pozitsiyani olib tashlash logikasi ALOHIDA yakunlanishi kerak —
   bu joriy versiyada TODO sifatida qoldirilgan.

## Xavfsizlik eslatmalari

- `WORKER_SECRET_KEY` — kamida 32 belgili tasodifiy satr, hech qachon
  kodga yozilmasin, faqat environment variable orqali.
- `DEMO_MODE` noaniq bo'lsa dastur ATAYLAB ishga tushmaydi (fail-fast).
- `/health` endpoint token so'ramaydi (Render health-check uchun), lekin
  hech qanday maxfiy ma'lumot qaytarmaydi.
- Real hisobga o'tishdan oldin demo'da kamida bir necha kun, turli
  bozor sharoitida (tinch/tez harakat) sinab ko'rish tavsiya etiladi.
