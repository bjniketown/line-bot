import os, hashlib, hmac, base64, time, re, json, threading
from collections import OrderedDict
from datetime import datetime, timezone, timedelta
from flask import Flask, request, abort
import anthropic, requests

app = Flask(__name__)

LINE_TOKEN      = os.environ["LINE_TOKEN"]
LINE_SECRET     = os.environ["LINE_SECRET"]
OWNER_LINE_UID  = os.environ.get("OWNER_LINE_UID", "")
UPSTASH_URL     = os.environ.get("UPSTASH_URL", "")     # Upstash Redis REST 網址
UPSTASH_TOKEN   = os.environ.get("UPSTASH_TOKEN", "")   # Upstash Redis token
claude = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_KEY"])


# ── Upstash Redis 對話記憶（跨重啟持久化）────────────────────────────────
def _redis(command: list):
    """執行一個 Upstash Redis REST 指令，失敗時靜默回傳 None。"""
    if not UPSTASH_URL:
        return None
    try:
        r = requests.post(
            UPSTASH_URL,
            headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"},
            json=command,
            timeout=3,
        )
        return r.json().get("result") if r.ok else None
    except Exception:
        return None

_local_histories: dict = {}   # Redis 失效時的 in-memory fallback
_local_daily:     dict = {}   # 每日呼叫計數的本地 fallback（key: "dlimit:xxx:YYYY-MM-DD"）
_local_customers: set  = set() # 客戶 UID 名單的本地 fallback

def get_history(uid: str) -> list:
    raw = _redis(["GET", f"hist:{uid}"])
    if raw is not None:
        try:
            return json.loads(raw)
        except Exception:
            pass
    # Redis 無回應或解析失敗 → 用本地記憶
    return _local_histories.get(uid, [])

def set_history(uid: str, history: list):
    # 同時寫入 Redis 和本地，Redis 失效時本地仍有記憶
    _local_histories[uid] = history
    _redis(["SET", f"hist:{uid}", json.dumps(history, ensure_ascii=False), "EX", 86400])


def _fetch_qr_code_url():
    """啟動時從 LINE API 取得官方帳號 QR code 網址"""
    try:
        resp = requests.get(
            "https://api.line.me/v2/bot/info",
            headers={"Authorization": f"Bearer {LINE_TOKEN}"},
            timeout=5,
        )
        if resp.ok:
            basic_id = resp.json().get("basicId", "")
            if basic_id:
                return f"https://qr-official.line.me/gs/M_{basic_id.lstrip('@')}.png"
    except Exception:
        pass
    return ""

QR_CODE_URL = _fetch_qr_code_url()

# ── 台灣時區日期（注入給 Claude，避免星期/日期算錯）────────────────────
_TZ_TW    = timezone(timedelta(hours=8))
_WEEKDAYS = ["星期一", "星期二", "星期三", "星期四", "星期五", "星期六", "星期日"]

def current_date_text() -> str:
    now = datetime.now(_TZ_TW)
    return (
        f"現在台灣時間：{now.strftime('%Y年%m月%d日')} "
        f"{_WEEKDAYS[now.weekday()]} {now.strftime('%H:%M')}"
    )

def _today_str() -> str:
    return datetime.now(_TZ_TW).strftime("%Y-%m-%d")

def _secs_till_midnight() -> int:
    now      = datetime.now(_TZ_TW)
    midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return max(60, int((midnight - now).total_seconds()))

SHARE_KEYWORDS = ["分享", "加好友", "好友碼", "qr", "掃碼", "掃描", "推薦朋友", "介紹朋友", "轉介紹"]

SYSTEM_TEXT = """你是「老鄰居豆干絲」的 LINE 客服助理，請用繁體中文、親切友善的語氣回覆客戶。

【品牌基本資訊】
- 店名：老鄰居豆干絲
- 位置：台中市東勢區豐勢路中盛巷24號（東勢美食街內）
- 門市電話：04-25882881
- 門市營業時間：週一至六 08:00-13:30 / 16:00-18:00；週日 08:00-13:30；週四固定全日公休
- LINE 客服時間：同門市營業時間，非上班時間留言將於隔天早上 8:00 優先回覆

【品牌故事】
- 老鄰居於 2000 年（921 大地震後）在東勢美食街開業
- 早期販售麵食（陽春麵、餛飩麵、酸辣麵、牛肉麵、榨菜肉絲麵、水餃）及豆干絲等眾多小菜
- 生意鼎盛，門口常大排長龍，是美食街數一數二的人氣店家
- 2020 年 COVID-19 疫情期間，老闆決定專注於豆干絲並擴大宅配服務，延續老鄰居的好味道

【產品與定價】
1. 招牌豆干絲 210 克/包
   - 宅配：70 元（真空包裝）
   - 門市：60 元（一般包裝）/ 70 元（真空包裝，建議前一天預訂，當天訂需先詢問老闆確認有無現貨）
2. 香滷花生（土豆）210 克/份
   - 宅配：100 元（真空包裝）
   - 門市一般包裝：50 元 或 100 元（詢問客人要哪個份量/價格）
   - 門市真空包裝：100 元（需提前預訂才有貨）
3. 天然昆布（海帶）160 克/份
   - 宅配：100 元（真空包裝）
   - 門市一般包裝：50 元 或 100 元（詢問客人要哪個份量/價格）
   - 門市真空包裝：100 元（需提前預訂才有貨）
4. 油潑辣子（辣油）250ml/罐，120 元（植物油全手工製作，無添加防腐劑）
   - 門市常備約 10 罐；若需求超過 10 罐，請先詢問老闆確認庫存
- 豆干絲真空包（宅配）附：蒜泥水 + 辣油調料包；不附蔥花（食品法規：生食與熟食不可共裝，避免大腸桿菌超標）
- 豆干絲門市一般包裝：蔥花、蒜泥水直接加入，辣油可獨立包裝（非真空、當天食用）

【運費規則（請務必按此計算，不可出錯）】
每 50 包為一箱，整箱免運費。不足一箱的「餘數」依下列規則加收運費：
- 餘數 1–38 包 → 加收運費 225 元
- 餘數 39–49 包 → 加收運費 290 元
- 餘數 0（整箱）→ 免運費

計算步驟：總包數 ÷ 50，商數為整箱數（免運），餘數套用上方規則。

實際範例：
- 38 包 → 餘數 38 → 運費 225 元，總計 38×70+225 = 2,885 元
- 39 包 → 餘數 39 → 運費 290 元，總計 39×70+290 = 3,020 元
- 49 包 → 餘數 49 → 運費 290 元，總計 49×70+290 = 3,720 元
- 50 包 → 整箱，餘數 0 → 免運費，總計 50×70 = 3,500 元
- 51 包 → 1 整箱 + 餘數 1 → 運費 225 元，總計 51×70+225 = 3,795 元
- 88 包 → 1 整箱 + 餘數 38 → 運費 225 元，總計 88×70+225 = 6,385 元
- 89 包 → 1 整箱 + 餘數 39 → 運費 290 元，總計 89×70+290 = 6,520 元
- 99 包 → 1 整箱 + 餘數 49 → 運費 290 元，總計 99×70+290 = 7,220 元
- 100 包 → 2 整箱，餘數 0 → 免運費，總計 100×70 = 7,000 元
- 110 包 → 2 整箱 + 餘數 10 → 運費 225 元，總計 110×70+225 = 7,925 元

划算提醒（客戶詢問時主動建議）：
- 訂 39–49 包時，建議湊滿 50 包，省運費且均價更低
- 訂 89–99 包時，建議湊滿 100 包，同樣更划算

所有品項可混搭合算（油潑辣子 1 罐 = 1 單位）

【保存方式】
- 豆干絲、香滷花生、天然昆布：冷凍（-18 度）保存，賞味期限出貨日起 10 天（包裝標示為出貨日 +11 天）
- 油潑辣子：冷藏保存，可保存約 1 年（冷藏目的：防止香氣揮發、減緩食用油氧化）
  使用注意：挖取時湯匙務必保持乾燥；瓶底有辣子沉澱物，使用前先攪拌均勻再取用
- 退冰後直接食用，不需加熱，以免影響口感與風味

【付款方式】
- 僅接受銀行轉帳（不接受貨到付款、信用卡、LINE Pay、街口）
- 銀行：807 永豐銀行
- 分支代號：1217（郵局 ATM 跨行才需填）
- 帳號：16801800434858
- 戶名：詹益全
- 請於出貨前完成匯款，匯款後回傳末四碼讓客服確認

【訂購所需資訊】
▶ 宅配客戶（五項缺一不可）：
1. 收件人全名
2. 收件地址
3. 聯絡電話
4. 訂購品項與數量
5. 希望出貨日期（收件日為出貨日 +1 天；繁盛期除外）

▶ 門市自取客戶（三項即可）：
1. 貴姓 + 稱謂（小姐／先生）
2. 聯絡電話
3. 預計取貨時間（須在營業時間內）

【出貨說明】
- 使用黑貓冷凍宅配（-18 度全程冷凍），出貨隔日可收到
- 出貨後客服會在 LINE 提供 12 碼黑貓宅配單號
- 黑貓客服：412-8888（手機撥打請加 02）
- 繁盛期（年節、雙 11）無法保證指定到貨日，建議提早下單

【常見問答 FAQ】

Q: 要怎麼下單？
A: 請先告訴我您要門市自取還是宅配？
   - 門市自取：提供貴姓+稱謂（小姐/先生）、電話、預計取貨時間即可
   - 宅配：請提供收件人全名、收件地址、聯絡電話、品項與數量、希望出貨日期

Q: 出貨日期跟收件日期一樣嗎？
A: 不一樣！出貨日是我們交給黑貓的日期，收件日通常是出貨日的隔天。例如週一出貨，正常週二可收到。繁盛期（年節、雙 11）物流較忙，收件時間可能延遲 1–2 天。

Q: 有貨到付款嗎？
A: 沒有，目前僅接受銀行轉帳，請於出貨前完成匯款並回傳末四碼。

Q: 可以幫朋友訂嗎？收件人和付款人不同可以嗎？
A: 完全可以！請提供收件人的姓名、地址、電話即可，付款人另外匯款後回傳末四碼確認。

Q: 可以一次寄到不同地址嗎？
A: 可以，每個地址算一張訂單、一個箱子，運費各自計算，請分別提供每筆的收件資訊。

Q: 可以兩張訂單合併付款嗎？
A: 可以，請將兩筆金額加總匯款，並在 LINE 告知各訂單明細，匯款後回傳末四碼。

Q: 運費怎麼算？
A: 每 50 包為一箱，整箱免運費。不足一箱的餘數：1–38 包加收 225 元，39–49 包加收 290 元。例如：50 包免運；55 包（1 整箱 + 5 包餘數）加收 225 元；100 包（2 整箱）免運費。

Q: 50 包免運，不同產品可以混算嗎？
A: 可以！所有品項都可合計計算單位數（油潑辣子 1 罐 = 1 單位），達 50 單位即為整箱免運。

Q: 39 包跟 50 包哪個比較划算？
A: 39 包需加 290 元運費，總計約 3,020 元；50 包免運費，總計 3,500 元，每包均價更低，建議直接湊 50 包更划算！

Q: 訂超過 50 包怎麼計算運費？
A: 每 50 包一箱，整箱免運，超出的餘數才計運費。例如：88 包（1 整箱 + 38 包餘數）加收 225 元；89 包（1 整箱 + 39 包餘數）加收 290 元；100 包（2 整箱）免運費。訂 89–99 包時建議湊到 100 包更划算！

Q: 1 罐油潑辣子算幾單位？
A: 1 罐算 1 單位，可與豆干絲等其他品項合計計算是否達免運門檻。

Q: 最快什麼時候出貨？
A: 出貨排程需由客服確認（通常週一、三、五出貨），請告知希望的出貨日，客服會確認是否可行。

Q: 出貨後幾天可以收到？
A: 一般出貨隔天可收件；繁盛期可能需 1–2 個工作天。

Q: 可以指定收件日期或時段嗎？
A: 一般時期可協調指定到貨日；繁盛期（年節、雙 11）物流繁忙，無法保證指定到貨日。

Q: 週末可以出貨嗎？
A: 週六視情況而定，週日與週四（固定公休）不出貨，請洽客服確認。

Q: 連假或年節期間還有出貨嗎？
A: 年節期間暫停出貨，年前最後出貨日約農曆年前 2 月 11 日前後，年後恢復日期會提前公告。

Q: 可以事先預訂、之後再出貨嗎？
A: 可以！請確認好數量與希望出貨日期，付款後會幫您排入出貨排程。

Q: 颱風天還會出貨嗎？
A: 基本上會出貨，若遇颱風假等特殊情況會提前通知調整。

Q: 豆干絲出貨是冷凍還是冷藏？
A: 使用黑貓冷凍宅配（-18 度），全程冷凍出貨。收到時若稍微退冰屬正常現象，請立即放入冷凍保存。

Q: 保存期限多久？
A: 豆干絲冷凍保存，賞味期限出貨日起 10 天（包裝標示為出貨日 +11 天）。油潑辣子冷藏約 1 年。

Q: 要加熱嗎？怎麼吃最好？
A: 不需要加熱！退冰後直接食用最美味，再加熱反而影響口感。

Q: 退冰後還能放回冷凍嗎？
A: 建議退冰後盡快食用，避免反覆冷凍解凍影響品質。

Q: 收到時已完全退冰，還能吃嗎？
A: 若仍冰涼請立即放入冷凍，品質不受影響。若完全常溫，請立即拍照並聯絡客服處理。

Q: 收到豆干絲顏色怪怪的，正常嗎？
A: 冷凍後顏色可能稍有變化，退冰後會恢復正常，請放心食用。若有疑慮請拍照聯絡客服。

Q: 油潑辣子需要冷藏嗎？
A: 是的，建議放冷藏保存，防止氧化並保持香氣，冷藏可保存約 1 年。

Q: 銀行轉帳要填分行代號嗎？
A: 一般網路銀行轉帳不需要，只需填銀行代碼 807 和帳號。若使用郵局 ATM 跨行轉帳，才需要填分支代號 1217。

Q: 有信用卡、LINE Pay 嗎？
A: 很抱歉，目前僅接受銀行轉帳，不支援刷卡或行動支付。

Q: 匯款後要怎麼通知你們？
A: 請在 LINE 回傳匯款末四碼，例如「已匯款，末四碼 1234」即可。

Q: 我忘記付款了，訂單還有效嗎？
A: 沒問題，只要出貨前完成匯款即可。若快到出貨日請盡快匯款並通知客服。

Q: 門市在哪裡？幾點營業？
A: 位於台中市東勢區，門市電話 04-25882881。營業時間：週一至六 08:00-13:30 / 16:00-18:00，週日 08:00-13:30，週四公休。

Q: 可以去門市自取嗎？
A: 可以！門市各品項皆可自取，歡迎親自來訪。請問您需要哪些品項？方便確認包裝與價格。

Q: 門市豆干絲的價格？
A: 門市豆干絲有兩種包裝：
   - 一般包裝：60 元/包（建議當天或短期食用）
   - 真空包裝：70 元/包（保鮮更穩定，適合存放或送禮）
   請問您需要哪一種呢？真空包裝建議前一天預訂，當天訂購需先確認是否有現貨。

Q: 門市昆布（海帶）的價格？
A: 門市天然昆布有兩種一般包裝：
   - 50 元/份
   - 100 元/份
   請問您需要哪個呢？若需要真空包裝，只有 100 元/份 可選，且須提前預訂才有貨。

Q: 門市花生（土豆）的價格？
A: 門市香滷花生有兩種一般包裝：
   - 50 元/份
   - 100 元/份
   請問您需要哪個呢？若需要真空包裝，只有 100 元/份 可選，且須提前預訂才有貨。

Q: 為什麼宅配的豆干絲沒有蔥花？
A: 這是依據食品法規的規定——生食與熟食不可共同放在同一個包裝袋內。蔥花屬於生食，若與熟食豆干絲共裝，大腸桿菌落菌數檢驗一定無法通過（我們定期送驗大腸桿菌，品質有保障）。所以宅配真空包內附的是蒜泥水 + 辣油，不附蔥花，請放心這是合規做法，並非品質問題。

Q: 門市買豆干絲，醬料怎麼包？
A: 醬料包法依數量不同：
   - 1–3 包：蔥花、蒜泥水直接加入豆干絲，辣油可選擇獨立包裝
   - 4 包以上：蔥花、蒜泥水、辣油全部獨立包裝，方便外帶後分次取用、保存更佳
   如果您需要 3 包，建議可以多帶 1 包變成 4 包，醬料分開放，保存上方便很多！

Q: 昆布、花生的醬料怎麼包？
A: 天然昆布和香滷花生不附蔥花、蒜泥，只有辣油可以另外獨立包裝。

Q: 豆干絲、昆布、花生素食可以吃嗎？
A: 可以！豆干絲、天然昆布、香滷花生皆為純素食，素食者放心食用。

Q: 油潑辣子素食可以吃嗎？怎麼保存？
A: 油潑辣子以植物油製作，素食者也可以安心食用！
   保存方式：冷藏保存約 1 年，冷藏能防止香氣揮發、減緩油氧化。
   使用小提醒：挖取時湯匙務必保持乾燥；瓶底有辣子沉澱物，使用前先攪拌均勻再取用。

Q: 門市買跟宅配有什麼差別？
A: 門市可選一般包裝（豆干絲 60 元）或真空包裝（70 元）；宅配則一律是真空包裝（70 元）出貨。
   昆布、花生門市有 50 元及 100 元一般包裝可選，真空包裝需提前預訂。

Q: 自取需要自備保冰袋嗎？
A: 是的，門市自取不提供保冰袋，請自備保冷袋或冰塊。

Q: 訂單可以修改嗎？
A: 出貨當天早上前都可以修改（數量、品項、地址等），請在 LINE 告知客服即可。

Q: 可以取消訂單嗎？
A: 出貨前可以取消。若已匯款，退款事宜請洽客服處理。出貨後無法取消。

Q: 可以追加品項嗎？
A: 出貨前可以追加，客服會重新計算金額並更新訂單。

Q: 出貨後可以改地址嗎？
A: 出貨後無法更改，需自行聯絡黑貓客服 412-8888（手機請加 02）處理。

Q: 如何追蹤包裹？
A: 出貨後客服會在 LINE 提供黑貓 12 碼宅配單號，可至黑貓官網查詢，或電 412-8888。

Q: 包裹超過預期時間沒到怎麼辦？
A: 請用宅配單號至黑貓官網查詢，若顯示異常請聯絡黑貓客服 412-8888，或通知我們協助追蹤。

Q: 不在家沒收到包裹怎麼辦？
A: 請查看門口或信箱是否有黑貓「投遞通知單」，可聯絡黑貓 412-8888 重新安排投遞。

Q: 黑貓配送失敗，包裹被退回怎麼辦？
A: 請聯絡黑貓 412-8888 重新安排，若包裹退回請通知我們客服確認後續處理方式。

Q: 有發票嗎？
A: 本店免用統一發票，可提供收據。請告知抬頭（公司名或個人姓名）及開立方式。

Q: 有優惠或折扣嗎？
A: 50 包以上免運費是目前最優惠的方式，商品本身不另設數量折扣。

Q: 收到的包數少了怎麼辦？
A: 請先仔細清點（包裝堆疊有時容易誤算），並拍照後聯絡客服，我們會查出貨記錄協助處理。

Q: 收到商品有問題怎麼辦？
A: 請立即拍照並在 LINE 告知客服說明問題狀況，我們會盡快協助解決。

Q: 外箱破損，產品還好嗎？
A: 請拍照記錄外箱與內容物狀況並告知客服。若產品損壞，我們會協助向黑貓申請理賠或補送。

Q: 我之前訂過，這次可以用舊資料嗎？
A: 可以！請告知訂購人資訊及這次的數量與出貨日期，客服會幫您沿用舊資料建立新訂單。

Q: 有沒有在蝦皮或其他電商平台？
A: 目前只透過 LINE 官方帳號接受訂購，沒有在其他平台上架，請認明本帳號。

Q: 可以送到偏遠地區或離島嗎？
A: 可送達大部分地區，偏遠或離島可能有額外運費或限制，請提供地址讓客服確認。

Q: 豆干絲有沒有添加防腐劑？
A: 油潑辣子全手工製作無任何添加劑。豆干絲以傳統工法製作，詳細成分請參考包裝標示。

Q: 適合送禮嗎？有禮盒嗎？
A: 真空包裝既適合自用也適合送禮，包裝乾淨清爽。目前沒有獨立禮盒，但多款搭配裝箱也很有誠意！

Q: 門市有提供餐具嗎？
A: 很抱歉，門市不提供餐具，請自行準備。

Q: 門市有廁所嗎？可以借廁所嗎？
A: 很抱歉，門市沒有對外開放的廁所。廁所建置在套房內，基於隱私關係無法外借，還請見諒！附近如有需要可留意公共廁所。

Q: 老鄰居是從什麼時候開始的？
A: 老鄰居於 2000 年（921 大地震後）在東勢美食街開業。早期除了豆干絲小菜，也販售各式麵食，生意非常好，門口常大排長龍！2020 年疫情期間老闆決定專注豆干絲並擴大宅配服務，把老鄰居的好滋味送到全台各地。

Q: 門市在哪裡？怎麼找？
A: 門市位於台中市東勢區豐勢路中盛巷24號，在東勢美食街裡面，電話 04-25882881。
   營業時間：週一至六 08:00–13:30 / 16:00–18:00，週日 08:00–13:30，週四全日公休。

【回覆原則】
1. 語氣親切友善，稱呼對方「您」
2. 回覆簡潔清楚，避免過長
3. 涉及出貨日期確認、庫存等需人工判斷的問題，請回覆：「這部分需由客服確認排程，請稍候，或直撥 04-25882881」
4. 若問題超出知識範圍，請回覆：「我幫您轉達給客服，請稍候片刻，或直撥 04-25882881」
5. 不確定的事情不要捏造，告知需由客服確認
6. 【購買意願必問】當客人表達購買意願時，第一步務必先詢問：「請問您是要門市自取，還是宅配到府呢？」確認後再收集對應資料，不可混用規則。
   - 門市自取：收集 貴姓+稱謂（小姐/先生）、電話、預計取貨時間（三項）
   - 宅配：收集 全名、電話、收件地址、品項數量、希望出貨日期（五項，缺一不可）
7. 【宅配時間說明】宅配客人若詢問幾天收到，務必說明：出貨日不等於收件日，正常收件為出貨日 +1 天；繁盛期除外。
8. 【非營業時間取貨】若客人詢問非營業時間取貨（週四全日、或每日非營業時段），婉轉拒絕並告知營業時間：週一至六 08:00–13:30 / 16:00–18:00，週日 08:00–13:30。
9. 【門市取貨必問】確認門市自取後，主動詢問品項與包裝種類；宅配一律真空包裝，不需詢問。
   - 豆干絲：詢問一般（60元）或真空（70元）；真空包裝建議前一天預訂
   - 昆布／花生：詢問 50 元或 100 元；若需真空包裝須提前預訂
   - 油潑辣子：120 元/罐，若需 10 罐以上請先詢問老闆庫存
10. 【門市醬料主動提示】客人門市購買豆干絲 3 包時，主動告知「4 包以上醬料獨立包裝，方便保存，是否要多帶一包？」
11. 【素食確認】若客人在對話中曾詢問過素食相關問題，回覆結尾務必再次確認：「請問您是素食者嗎？以便我們為您備餐。」
12. 【門市無餐具】客人詢問門市是否提供餐具，回覆：門市不提供餐具，請自行準備。
13. 【禁止回答的範圍】競爭對手比較、政治宗教話題、與老鄰居業務無關的問題、法律醫療財務建議

【訂單完成標記（系統專用，重要）】

▶ 宅配訂單：當客戶提供以下四項資料（缺一不可），在回覆最後一行加上標記：
  必要資料：收件人全名、收件地址、聯絡電話、品項與數量
  （出貨日期不在此，由客服另行確認）
  <<ORDER:姓名|電話|品項簡述>>
  例：<<ORDER:王小明|0912345678|豆干絲30包>>

▶ 門市自取訂單：當客戶提供以下三項資料（缺一不可），在回覆最後一行加上標記：
  必要資料：貴姓+稱謂（小姐/先生）、聯絡電話、預計取貨時間
  <<PICKUP:稱謂姓氏|電話|取貨時間|品項簡述>>
  例：<<PICKUP:王小姐|0912345678|明天上午10點|豆干絲5包一般包裝>>

以上標記不得讓客戶看到，資訊不齊全時絕對不加。"""

RATE_LIMIT_SECONDS      = 1    # 每位用戶最少間隔秒數，防止惡意洗版
MAX_CLAUDE_PER_USER_DAY = 30   # 每位用戶每天最多呼叫 Claude 次數
MAX_CLAUDE_GLOBAL_DAY   = 500  # 全局每天最多呼叫 Claude 次數（防爆紅費用爆炸）

# histories 已改用 Upstash Redis（get_history / set_history）
last_request = {}   # uid -> 上次呼叫時間（重啟清空，rate limit 不影響正常使用）

# ── 快速規則回覆（完全不呼叫 Claude，省 token）──────────────────────
EXACT_REPLIES = {
    "你好": "您好！我是老鄰居豆干絲的客服助理 😊 請問有什麼可以幫您的嗎？",
    "哈囉": "您好！我是老鄰居豆干絲的客服助理 😊 請問有什麼可以幫您的嗎？",
    "嗨":   "您好！我是老鄰居豆干絲的客服助理 😊 請問有什麼可以幫您的嗎？",
    "hi":   "您好！我是老鄰居豆干絲的客服助理 😊 請問有什麼可以幫您的嗎？",
    "hello":"您好！我是老鄰居豆干絲的客服助理 😊 請問有什麼可以幫您的嗎？",
    "早安": "早安！請問有什麼可以幫您的嗎？😊",
    "午安": "午安！請問有什麼可以幫您的嗎？😊",
    "晚安": "晚安！如有需要歡迎留言，我們會盡快回覆 😊",
    "謝謝": "不客氣！有需要隨時詢問 😊",
    "感謝": "不客氣！有需要隨時詢問 😊",
    "謝啦": "不客氣！有需要隨時詢問 😊",
    "謝了": "不客氣！有需要隨時詢問 😊",
    "好":   "好的！如有其他問題歡迎詢問 😊",
    "ok":   "好的！如有其他問題歡迎詢問 😊",
    "了解": "好的！如有其他問題歡迎詢問 😊",
    "收到": "感謝您！如有其他問題歡迎詢問 😊",
    "再見": "謝謝光臨，歡迎再來！😊",
    "掰掰": "謝謝光臨，歡迎再來！😊",
    "bye":  "謝謝光臨，歡迎再來！😊",
    "店取": "好的！門市自取請提供以下資料 😊\n1. 貴姓 + 稱謂（小姐／先生）\n2. 聯絡電話\n3. 預計取貨時間\n\n營業時間：週一至六 08:00–13:30 / 16:00–18:00，週日 08:00–13:30，週四公休",
    "自取": "好的！門市自取請提供以下資料 😊\n1. 貴姓 + 稱謂（小姐／先生）\n2. 聯絡電話\n3. 預計取貨時間\n\n營業時間：週一至六 08:00–13:30 / 16:00–18:00，週日 08:00–13:30，週四公休",
    "宅配": "好的！宅配請提供以下資料 😊\n1. 收件人全名\n2. 收件地址\n3. 聯絡電話\n4. 訂購品項與數量\n5. 希望出貨日期\n\n收件日為出貨日隔天，繁盛期可能延遲 1–2 天",
}

# 只要訊息包含以下任一關鍵字，直接回傳對應答案（不呼叫 Claude）
# 格式：(統計標籤, 關鍵字列表, 回覆內容)
KEYWORD_RULES = [
    ("💰 價格查詢",
     ["多少錢", "幾元", "幾塊", "定價", "售價", "價格", "price"],
     "老鄰居豆干絲各品項售價：\n"
     "・招牌豆干絲 210g\n"
     "  宅配 70 元（真空）/ 門市 60 元（一般）\n"
     "・香滷花生 210g → 100 元\n"
     "・天然昆布 160g → 100 元\n"
     "・油潑辣子 250ml → 120 元\n\n"
     "需要試算含運費的總價嗎？告訴我數量就好 😊"),

    ("🚚 運費免運",
     ["運費", "免運"],
     "運費規則（每 50 包為一箱）：\n"
     "・整箱（50 的倍數）→ 免運費 🎉\n"
     "・餘數 1–38 包 → 加收 225 元\n"
     "・餘數 39–49 包 → 加收 290 元\n\n"
     "小提示：\n"
     "訂 39–49 包 → 湊到 50 包更划算！\n"
     "訂 89–99 包 → 湊到 100 包更划算！\n\n"
     "需要試算嗎？請告訴我您的數量 😊"),

    ("🏦 付款方式",
     ["帳號", "匯款", "轉帳", "付款", "銀行"],
     "付款方式：銀行轉帳\n"
     "・銀行代碼：807（永豐銀行）\n"
     "・帳號：16801800434858\n"
     "・戶名：詹益全\n\n"
     "匯款後請在 LINE 回傳末四碼 📲\n"
     "（不支援貨到付款、信用卡、LINE Pay）"),

    ("📋 訂購方式",
     ["怎麼訂", "如何訂", "要怎麼", "訂購方式", "下單方式"],
     "訂購請提供以下資訊 📋\n"
     "1. 收件人姓名\n"
     "2. 收件地址\n"
     "3. 聯絡電話\n"
     "4. 品項與數量\n"
     "5. 希望出貨日期\n\n"
     "客服確認後會傳送匯款資訊，出貨前完成匯款即可 😊"),

    ("📍 門市地址",
     ["門市", "地址", "在哪", "實體店"],
     "門市資訊：\n"
     "📍 台中市東勢區豐勢路中盛巷24號\n"
     "📞 04-25882881\n\n"
     "營業時間：\n"
     "・週一至六：08:00–13:30 / 16:00–18:00\n"
     "・週日：08:00–13:30\n"
     "・週四：公休"),

    ("📦 配送時間",
     ["幾天到", "幾天收", "何時到", "多久到", "配送"],
     "出貨使用黑貓冷凍宅配（-18°C 全程冷凍）\n"
     "・一般：出貨隔天可收到\n"
     "・繁盛期（年節、雙11）：可能需 1–2 天\n\n"
     "出貨後客服會在 LINE 提供 12 碼宅配單號 😊"),
]

class LRUCache:
    def __init__(self, maxsize=500):
        self._cache = OrderedDict()
        self._maxsize = maxsize

    def __contains__(self, key):
        return key in self._cache

    def __getitem__(self, key):
        self._cache.move_to_end(key)
        return self._cache[key]

    def __setitem__(self, key, value):
        if key in self._cache:
            self._cache.move_to_end(key)
        self._cache[key] = value
        if len(self._cache) > self._maxsize:
            self._cache.popitem(last=False)


faq_cache = LRUCache(maxsize=500)  # 全域問答快取：normalized 問句 → Claude 回答

ORDER_TAG  = re.compile(r'<<ORDER:([^>]+)>>',  re.IGNORECASE)
PICKUP_TAG = re.compile(r'<<PICKUP:([^>]+)>>', re.IGNORECASE)


def extract_order(text):
    """從 Claude 回應中取出訂單/取貨標記，回傳 (乾淨文字, 類型, 摘要)。
    類型：'order'=宅配, 'pickup'=門市自取, None=無標記"""
    m = ORDER_TAG.search(text)
    if m:
        return ORDER_TAG.sub("", text).strip(), "order", m.group(1).strip()
    m = PICKUP_TAG.search(text)
    if m:
        return PICKUP_TAG.sub("", text).strip(), "pickup", m.group(1).strip()
    return text, None, None


def notify_owner(customer_uid, order_type, order_summary):
    """用 Push Message 推播新訂單給老闆。"""
    if not OWNER_LINE_UID:
        return
    if order_type == "pickup":
        header = "🏪 門市自取通知"
        footer = "請確認取貨時間並備料 ✅"
    else:
        header = "🔔 宅配新訂單通知"
        footer = "請盡快確認訂單並安排出貨日期 ✅"
    msg = (
        f"{header}\n\n"
        f"摘要：{order_summary}\n"
        f"客戶 LINE UID：\n{customer_uid}\n\n"
        f"{footer}"
    )
    try:
        requests.post(
            "https://api.line.me/v2/bot/message/push",
            headers={"Authorization": f"Bearer {LINE_TOKEN}"},
            json={"to": OWNER_LINE_UID, "messages": [{"type": "text", "text": msg}]},
            timeout=10,
        )
    except Exception:
        pass


def _track_faq(label: str):
    """將問題分類計數寫入 Redis Sorted Set（失敗靜默略過，不影響主流程）。"""
    _redis(["ZINCRBY", "faq_stats", "1", label])


def quick_rule_reply(text):
    """打招呼/感謝/關鍵字 → 直接回傳，完全不呼叫 Claude。"""
    t = text.strip()
    # 完全比對（不分大小寫）
    exact = EXACT_REPLIES.get(t) or EXACT_REPLIES.get(t.lower())
    if exact:
        return exact
    # 關鍵字比對（先做，避免「運費」等2字關鍵字被2字規則誤攔）
    for label, keywords, reply_text in KEYWORD_RULES:
        if any(kw in t for kw in keywords):
            _track_faq(label)
            return reply_text
    # 超短訊息（2字以內且非問句）→ 親切回應
    if len(t) <= 2 and "?" not in t and "？" not in t:
        return "您好！請問有什麼可以幫您的嗎？😊"
    return None


def cache_key(text):
    """正規化問句，提高快取命中率。"""
    return text.strip().lower().replace(" ", "").replace("　", "")


def _daily_allowed(uid: str) -> bool:
    """檢查今日 Claude 呼叫額度並計數。允許則回 True，超限則回 False。"""
    today      = _today_str()
    uid_key    = f"dlimit:{uid}:{today}"
    global_key = f"dlimit:global:{today}"
    ttl        = _secs_till_midnight()

    uid_cnt    = int(_redis(["GET", uid_key])    or _local_daily.get(uid_key,    0))
    global_cnt = int(_redis(["GET", global_key]) or _local_daily.get(global_key, 0))

    if uid_cnt >= MAX_CLAUDE_PER_USER_DAY or global_cnt >= MAX_CLAUDE_GLOBAL_DAY:
        return False

    new_uid    = uid_cnt + 1
    new_global = global_cnt + 1
    _redis(["SET", uid_key,    str(new_uid),    "EX", ttl])
    _redis(["SET", global_key, str(new_global), "EX", ttl])
    _local_daily[uid_key]    = new_uid
    _local_daily[global_key] = new_global
    return True


def verify(body, sig):
    h = hmac.new(LINE_SECRET.encode(), body, hashlib.sha256).digest()
    return hmac.compare_digest(base64.b64encode(h).decode(), sig)


def reply(token, messages):
    """messages 可以是文字字串，或 LINE message 物件的 list"""
    if isinstance(messages, str):
        messages = [{"type": "text", "text": messages}]
    try:
        requests.post(
            "https://api.line.me/v2/bot/message/reply",
            headers={"Authorization": f"Bearer {LINE_TOKEN}"},
            json={"replyToken": token, "messages": messages},
            timeout=10,
        )
    except Exception:
        pass


def push_message(uid, messages):
    """Push message：不需要 reply token，可在背景執行。"""
    if isinstance(messages, str):
        messages = [{"type": "text", "text": messages}]
    try:
        requests.post(
            "https://api.line.me/v2/bot/message/push",
            headers={"Authorization": f"Bearer {LINE_TOKEN}"},
            json={"to": uid, "messages": messages},
            timeout=15,
        )
    except Exception:
        pass


_FAST_TIMEOUT = 20  # 秒：reply token 有效期 30 秒，等 Claude 最長 20 秒用免費 reply；超時才用 push

# ── 自動補單提醒 ────────────────────────────────────────────────────────────
_ORDER_INTENT_RE = re.compile(
    r'(?:訂|要訂|下單|要買|購買).{0,15}\d+\s*[包罐箱]'
    r'|\d+\s*[包罐箱].{0,15}(?:訂|要訂|下單|要買|購買)'
)
_ADDRESS_RE = re.compile(r'[縣市].{0,30}[區鄉鎮]|[路街巷]\s*\d+\s*號')

def _has_order_intent(text: str) -> bool:
    return bool(_ORDER_INTENT_RE.search(text))

def _has_address_in_history(uid: str) -> bool:
    history = get_history(uid)
    user_text = " ".join(m["content"] for m in history[-8:] if m["role"] == "user")
    return bool(_ADDRESS_RE.search(user_text))

def _maybe_push_address_reminder(uid: str, msg: str, claude_reply: str):
    """安全網：客戶有訂購意圖但近期對話沒有地址，且 Claude 也沒問，才補推提醒。"""
    if not _has_order_intent(msg):
        return
    # Claude 的回覆已問地址 → 不重複
    if "地址" in claude_reply or "收件" in claude_reply:
        return
    # 近期對話已提供地址 → 不需提醒
    if _has_address_in_history(uid):
        return
    push_message(
        uid,
        "提醒您，完成宅配訂單還需要提供：\n"
        "・收件人姓名\n"
        "・收件地址\n"
        "・聯絡電話\n\n"
        "提供後客服會立刻為您安排出貨 😊"
    )


def _handle_claude(token, uid, text):
    """快慢分路：快則直接 reply；超時先回「處理中」再 push 結果。"""
    result_holder = [None]
    done = threading.Event()

    def worker():
        try:
            result_holder[0] = ask_with_cache(uid, text)
        except Exception:
            result_holder[0] = "很抱歉，系統暫時忙碌，請稍後再試或直撥 04-25882881"
        finally:
            done.set()

    threading.Thread(target=worker, daemon=True).start()

    if done.wait(timeout=_FAST_TIMEOUT):
        reply(token, result_holder[0])
        _maybe_push_address_reminder(uid, text, result_holder[0])
    else:
        reply(token, "⏳ 稍等一下，我幫您確認中...")
        def push_when_done():
            done.wait()
            push_message(uid, result_holder[0])
            _maybe_push_address_reminder(uid, text, result_holder[0])
        threading.Thread(target=push_when_done, daemon=True).start()


def register_customer(uid: str):
    """將用戶 UID 加入客戶名單（Redis SET + 本地 fallback）。老闆本人不列入。"""
    if uid == OWNER_LINE_UID:
        return
    _local_customers.add(uid)
    _redis(["SADD", "customers", uid])


def multicast(uids: list, messages):
    """LINE Multicast API：一次最多 500 人，超過自動分批。"""
    if isinstance(messages, str):
        messages = [{"type": "text", "text": messages}]
    for i in range(0, len(uids), 500):
        batch = uids[i:i + 500]
        try:
            requests.post(
                "https://api.line.me/v2/bot/message/multicast",
                headers={"Authorization": f"Bearer {LINE_TOKEN}"},
                json={"to": batch, "messages": messages},
                timeout=30,
            )
        except Exception:
            pass


def build_image_messages(url: str, caption: str = "") -> list:
    """組合圖片（+ 選填文字說明）的 LINE message list。"""
    msgs = []
    if caption:
        msgs.append({"type": "text", "text": caption})
    msgs.append({
        "type": "image",
        "originalContentUrl": url,
        "previewImageUrl": url,
    })
    return msgs


def broadcast_to_all(messages) -> int:
    """推播給所有客戶，回傳實際推播人數。messages 可以是字串或 LINE message list。"""
    result = _redis(["SMEMBERS", "customers"])
    uids = list(result) if result else list(_local_customers)
    if not uids:
        return 0
    multicast(uids, messages)
    return len(uids)


def is_share_request(text):
    t = text.lower()
    return any(kw in t for kw in SHARE_KEYWORDS)


def share_messages():
    """回傳分享好友用的訊息組合（文字 + QR code 圖片）"""
    msgs = [
        {
            "type": "text",
            "text": (
                "感謝您願意將老鄰居豆干絲分享給朋友！🧡\n"
                "請朋友掃描下方 QR Code 加入我們的官方 LINE，\n"
                "即可訂購或詢問任何問題 😊"
            ),
        }
    ]
    if QR_CODE_URL:
        msgs.append({
            "type": "image",
            "originalContentUrl": QR_CODE_URL,
            "previewImageUrl": QR_CODE_URL,
        })
    return msgs


_MODELS = [
    "claude-haiku-4-5-20251001",  # 主力：最快最便宜
    "claude-haiku-4-5",           # fallback 1：舊版 haiku
    "claude-haiku-3-5-20241022",  # fallback 2：上一代 haiku
]

def _call_claude(history: list) -> str:
    """依序嘗試 _MODELS，第一個成功的回傳結果；全部失敗才丟例外。"""
    system_blocks = [
        {"type": "text", "text": SYSTEM_TEXT,
         "cache_control": {"type": "ephemeral"}},
        {"type": "text", "text": current_date_text()},
    ]
    last_err = None
    for model in _MODELS:
        try:
            r = claude.messages.create(
                model=model,
                max_tokens=600,
                system=system_blocks,
                messages=history,
            )
            return r.content[0].text
        except anthropic.APIStatusError as e:
            # 額度不足 / 服務過載 → 不值得再試其他 model
            if "credit" in str(e).lower() or e.status_code == 529:
                raise
            # 404 model not found / 400 bad request → 試下一個
            last_err = e
        except Exception as e:
            last_err = e
    raise last_err


def ask(uid, msg):
    """呼叫 Claude，回傳 (乾淨文字, 是否有訂單)。"""
    history = get_history(uid)
    history.append({"role": "user", "content": msg})
    history = history[-10:]
    try:
        raw = _call_claude(history)
    except anthropic.APIStatusError as e:
        if "credit" in str(e).lower() or e.status_code == 529:
            return "很抱歉，服務暫時無法使用，請直撥 04-25882881", False
        return "很抱歉，系統暫時忙碌，請稍後再試或直撥 04-25882881", False
    except Exception:
        return "很抱歉，系統暫時忙碌，請稍後再試或直撥 04-25882881", False

    clean, order_type, order_info = extract_order(raw)
    if order_info:
        notify_owner(uid, order_type, order_info)

    history.append({"role": "assistant", "content": clean})
    set_history(uid, history)
    return clean, bool(order_info)


def ask_with_cache(uid, msg):
    """先查快取省 token；未命中才呼叫 Claude。有訂單的回答不快取。"""
    context_starts = ("那", "這", "剛", "你說", "您說", "之前", "上面")
    use_cache = len(msg) >= 6 and not any(msg.startswith(w) for w in context_starts)

    key = cache_key(msg)
    if use_cache and key in faq_cache:
        cached = faq_cache[key]
        history = get_history(uid)
        history.append({"role": "user", "content": msg})
        history.append({"role": "assistant", "content": cached})
        set_history(uid, history[-10:])
        _track_faq(f"🤖 {key[:35]}")   # 快取命中也統計
        return cached

    _track_faq(f"🤖 {key[:35]}")       # Claude 新問題統計
    clean, is_order = ask(uid, msg)
    if use_cache and not is_order:
        faq_cache[key] = clean
    return clean


@app.route("/webhook", methods=["POST"])
def webhook():
    if not verify(request.get_data(), request.headers.get("X-Line-Signature", "")):
        abort(400)
    for e in request.json.get("events", []):
        if e["type"] == "message" and e["message"]["type"] == "text":
            text  = e["message"]["text"]
            token = e["replyToken"]
            uid   = e["source"]["userId"]
            # ── 自動記錄客戶名單 ─────────────────────────────────────────────
            register_customer(uid)

            # ── 老闆專用指令（非老闆傳送會當一般訊息處理）────────────────────
            if uid == OWNER_LINE_UID:
                t = text.strip()
                # 查詢自己的 UID
                if t == "!myid":
                    reply(token, f"您的 LINE UID：\n{uid}")
                    continue
                # 查詢客戶人數
                if t == "!customers":
                    count = int(_redis(["SCARD", "customers"]) or len(_local_customers))
                    reply(token, f"📋 目前客戶名單：{count} 位")
                    continue
                # 熱門問題統計 Top 10
                if t == "!stats":
                    rows = _redis(["ZREVRANGE", "faq_stats", "0", "9", "WITHSCORES"])
                    if not rows:
                        reply(token, "尚無統計資料")
                    else:
                        lines = ["📊 熱門問題 Top 10\n"]
                        for i in range(0, len(rows), 2):
                            label = rows[i]
                            count = int(float(rows[i + 1]))
                            lines.append(f"{i//2+1}. {label}  ×{count}")
                        reply(token, "\n".join(lines))
                    continue
                # 推播給所有客戶
                if t.startswith("!broadcast "):
                    msg = t[len("!broadcast "):].strip()
                    if not msg:
                        reply(token, "請輸入推播內容，例如：\n!broadcast 端午節優惠開跑！")
                    else:
                        threading.Thread(
                            target=lambda m=msg, tk=token: reply(tk, f"✅ 已推播給 {broadcast_to_all(m)} 位客戶"),
                            daemon=True,
                        ).start()
                    continue
                # 群發圖片（可附文字說明）
                if t.startswith("!img "):
                    parts = t[len("!img "):].strip().split(None, 1)
                    img_url = parts[0] if parts else ""
                    caption = parts[1] if len(parts) > 1 else ""
                    if not img_url.startswith("https://"):
                        reply(token,
                              "⚠️ 圖片網址必須以 https:// 開頭\n\n"
                              "建議步驟：\n"
                              "1. 前往 imgur.com 上傳圖片\n"
                              "2. 右鍵圖片 → 複製圖片網址\n"
                              "3. 再傳 !img https://i.imgur.com/xxx.jpg")
                    else:
                        msgs = build_image_messages(img_url, caption)
                        threading.Thread(
                            target=lambda m=msgs, tk=token: reply(tk, f"✅ 已推播圖片給 {broadcast_to_all(m)} 位客戶"),
                            daemon=True,
                        ).start()
                    continue
            else:
                # 非老闆：查詢 UID（保留方便新用戶對機器人輸入 !myid 取得自己的 UID）
                if text.strip() == "!myid":
                    reply(token, f"您的 LINE UID：\n{uid}")
                    continue

            now = time.time()
            if now - last_request.get(uid, 0) < RATE_LIMIT_SECONDS:
                reply(token, "您傳訊息太快了，請稍後再試 😊")
                continue
            last_request[uid] = now

            # ── 快速回覆（不呼叫 Claude）→ 直接 reply，立即送出 ──────────
            if is_share_request(text):
                reply(token, share_messages())
                continue

            rule = quick_rule_reply(text)
            if rule:
                reply(token, rule)
                continue

            # ── Claude 呼叫 → 快慢分路 ──────────────────────────────────────
            if uid != OWNER_LINE_UID and not _daily_allowed(uid):
                reply(token, "您今日的詢問次數已達上限，請明天再試，或直撥 04-25882881 😊")
                continue
            threading.Thread(
                target=_handle_claude,
                args=(token, uid, text),
                daemon=True,
            ).start()
    return "OK"


@app.route("/ping")
def ping():
    return "pong"


if __name__ == "__main__":
    app.run()
