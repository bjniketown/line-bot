import os, hashlib, hmac, base64, time, re, json
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

def get_history(uid: str) -> list:
    raw = _redis(["GET", f"hist:{uid}"])
    return json.loads(raw) if raw else []

def set_history(uid: str, history: list):
    # 每位用戶對話記憶保存 24 小時
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

SHARE_KEYWORDS = ["分享", "加好友", "好友碼", "qr", "掃碼", "掃描", "推薦朋友", "介紹朋友", "轉介紹"]

SYSTEM_TEXT = """你是「老鄰居豆干絲」的 LINE 客服助理，請用繁體中文、親切友善的語氣回覆客戶。

【品牌基本資訊】
- 店名：老鄰居豆干絲
- 位置：台中市東勢區豐勢路中盛巷24號
- 門市電話：04-25882881
- 門市營業時間：週一至六 08:00-13:30 / 16:00-18:00；週日 08:00-13:30；週四固定公休
- LINE 客服時間：同門市營業時間，非上班時間留言將於隔天早上 8:00 優先回覆

【產品與定價】
1. 招牌豆干絲：210 克/包，宅配 70 元（真空包裝），門市 60 元（一般包裝）
2. 香滷花生：210 克/包，100 元（真空包裝）
3. 天然昆布：160 克/包，100 元（真空包裝）
4. 油潑辣子：250ml/罐，120 元（全手工製作，無添加防腐劑）
- 每包豆干絲附有蒜汁 + 辣油調料包

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
- 油潑辣子：冷藏保存，可保存約 1 年
- 退冰後直接食用，不需加熱，以免影響口感與風味

【付款方式】
- 僅接受銀行轉帳（不接受貨到付款、信用卡、LINE Pay、街口）
- 銀行：807 永豐銀行
- 分支代號：1217（郵局 ATM 跨行才需填）
- 帳號：16801800434858
- 戶名：詹益全
- 請於出貨前完成匯款，匯款後回傳末四碼讓客服確認

【訂購所需資訊】
1. 收件人姓名
2. 收件地址
3. 聯絡電話
4. 訂購品項與數量
5. 希望出貨日期（由客服確認是否可行）

【出貨說明】
- 使用黑貓冷凍宅配（-18 度全程冷凍），出貨隔日可收到
- 出貨後客服會在 LINE 提供 12 碼黑貓宅配單號
- 黑貓客服：412-8888（手機撥打請加 02）
- 繁盛期（年節、雙 11）無法保證指定到貨日，建議提早下單

【常見問答 FAQ】

Q: 要怎麼下單？
A: 請直接提供：收件人姓名、收件地址、聯絡電話、品項與數量、希望出貨日期，客服確認後會傳送付款資訊給您。

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
A: 可以！門市有兩種包裝可選，請問您需要哪一種？
   - 一般包裝：60 元/包（非真空，建議當天或短期食用）
   - 真空包裝：70 元/包（保鮮品質更穩定，適合送禮或存放）
   歡迎親自來訪！

Q: 門市買跟宅配有什麼差別？
A: 門市可選一般包裝（60 元）或真空包裝（70 元）；宅配則一律是真空包裝（70 元）出貨。

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

【回覆原則】
1. 語氣親切友善，稱呼對方「您」
2. 回覆簡潔清楚，避免過長
3. 涉及出貨日期確認、庫存等需人工判斷的問題，請回覆：「這部分需由客服確認排程，請稍候，或直撥 04-25882881」
4. 若問題超出知識範圍，請回覆：「我幫您轉達給客服，請稍候片刻，或直撥 04-25882881」
5. 不確定的事情不要捏造，告知需由客服確認
6. 【門市取貨必問】客人詢問門市取貨或自取時，必須主動詢問包裝種類：「請問需要一般包裝（60 元/包）還是真空包裝（70 元/包）呢？」宅配一律為真空包裝，不需詢問。
7. 【禁止回答的範圍】競爭對手比較、政治宗教話題、與老鄰居業務無關的問題、法律醫療財務建議

【訂單完成標記（系統專用，重要）】
當客戶在對話中已提供齊全的訂購資訊，包括：
  收件人姓名、收件地址、聯絡電話、品項與數量、希望出貨日期（五項缺一不可）
請在你回覆的最後一行加上此標記（不要讓客戶看到）：
  <<ORDER:姓名|電話|品項簡述>>
例：<<ORDER:王小明|0912345678|豆干絲30包>>
若五項資訊不完整，絕對不要加此標記。"""

RATE_LIMIT_SECONDS = 3   # 每位用戶最少間隔秒數，防止惡意洗版

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
}

# 只要訊息包含以下任一關鍵字，直接回傳對應答案（不呼叫 Claude）
KEYWORD_RULES = [
    (["多少錢", "幾元", "幾塊", "定價", "售價", "價格", "price"],
     "老鄰居豆干絲各品項售價：\n"
     "・招牌豆干絲 210g\n"
     "  宅配 70 元（真空）/ 門市 60 元（一般）\n"
     "・香滷花生 210g → 100 元\n"
     "・天然昆布 160g → 100 元\n"
     "・油潑辣子 250ml → 120 元\n\n"
     "需要試算含運費的總價嗎？告訴我數量就好 😊"),

    (["運費", "免運"],
     "運費規則（每 50 包為一箱）：\n"
     "・整箱（50 的倍數）→ 免運費 🎉\n"
     "・餘數 1–38 包 → 加收 225 元\n"
     "・餘數 39–49 包 → 加收 290 元\n\n"
     "小提示：\n"
     "訂 39–49 包 → 湊到 50 包更划算！\n"
     "訂 89–99 包 → 湊到 100 包更划算！\n\n"
     "需要試算嗎？請告訴我您的數量 😊"),

    (["帳號", "匯款", "轉帳", "付款", "銀行"],
     "付款方式：銀行轉帳\n"
     "・銀行代碼：807（永豐銀行）\n"
     "・帳號：16801800434858\n"
     "・戶名：詹益全\n\n"
     "匯款後請在 LINE 回傳末四碼 📲\n"
     "（不支援貨到付款、信用卡、LINE Pay）"),

    (["怎麼訂", "如何訂", "要怎麼", "訂購方式", "下單方式"],
     "訂購請提供以下資訊 📋\n"
     "1. 收件人姓名\n"
     "2. 收件地址\n"
     "3. 聯絡電話\n"
     "4. 品項與數量\n"
     "5. 希望出貨日期\n\n"
     "客服確認後會傳送匯款資訊，出貨前完成匯款即可 😊"),

    (["門市", "地址", "在哪", "實體店"],
     "門市資訊：\n"
     "📍 台中市東勢區豐勢路中盛巷24號\n"
     "📞 04-25882881\n\n"
     "營業時間：\n"
     "・週一至六：08:00–13:30 / 16:00–18:00\n"
     "・週日：08:00–13:30\n"
     "・週四：公休"),

    (["幾天到", "幾天收", "何時到", "多久到", "配送"],
     "出貨使用黑貓冷凍宅配（-18°C 全程冷凍）\n"
     "・一般：出貨隔天可收到\n"
     "・繁盛期（年節、雙11）：可能需 1–2 天\n\n"
     "出貨後客服會在 LINE 提供 12 碼宅配單號 😊"),
]

faq_cache = {}  # 全域問答快取：normalized 問句 → Claude 回答

ORDER_TAG = re.compile(r'<<ORDER:([^>]+)>>', re.IGNORECASE)


def extract_order(text):
    """從 Claude 回應中取出訂單標記，回傳 (乾淨文字, 訂單摘要或 None)。"""
    m = ORDER_TAG.search(text)
    if m:
        return ORDER_TAG.sub("", text).strip(), m.group(1).strip()
    return text, None


def notify_owner(customer_uid, order_summary):
    """用 Push Message 推播新訂單給老闆。"""
    if not OWNER_LINE_UID:
        return
    msg = (
        f"🔔 新訂單通知\n\n"
        f"摘要：{order_summary}\n"
        f"客戶 LINE UID：\n{customer_uid}\n\n"
        f"請盡快在 LINE 官方帳號後台聯繫客戶確認訂單 ✅"
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


def quick_rule_reply(text):
    """打招呼/感謝/關鍵字 → 直接回傳，完全不呼叫 Claude。"""
    t = text.strip()
    # 完全比對（不分大小寫）
    exact = EXACT_REPLIES.get(t) or EXACT_REPLIES.get(t.lower())
    if exact:
        return exact
    # 超短訊息（2字以內且非問句）→ 親切回應
    if len(t) <= 2 and "?" not in t and "？" not in t:
        return "您好！請問有什麼可以幫您的嗎？😊"
    # 關鍵字比對
    for keywords, reply_text in KEYWORD_RULES:
        if any(kw in t for kw in keywords):
            return reply_text
    return None


def cache_key(text):
    """正規化問句，提高快取命中率。"""
    return text.strip().lower().replace(" ", "").replace("　", "")


def verify(body, sig):
    h = hmac.new(LINE_SECRET.encode(), body, hashlib.sha256).digest()
    return hmac.compare_digest(base64.b64encode(h).decode(), sig)


def reply(token, messages):
    """messages 可以是文字字串，或 LINE message 物件的 list"""
    if isinstance(messages, str):
        messages = [{"type": "text", "text": messages}]
    requests.post(
        "https://api.line.me/v2/bot/message/reply",
        headers={"Authorization": f"Bearer {LINE_TOKEN}"},
        json={"replyToken": token, "messages": messages},
    )


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


def ask(uid, msg):
    """呼叫 Claude，回傳 (乾淨文字, 是否有訂單)。"""
    history = get_history(uid)
    history.append({"role": "user", "content": msg})
    history = history[-10:]
    try:
        r = claude.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=600,
            system=[
                # 第一塊：知識庫（長，啟用 cache 省費用）
                {"type": "text", "text": SYSTEM_TEXT,
                 "cache_control": {"type": "ephemeral"}},
                # 第二塊：當下日期時間（短，每次更新，不 cache）
                {"type": "text", "text": current_date_text()},
            ],
            messages=history,
        )
        raw = r.content[0].text
    except anthropic.APIStatusError as e:
        if "credit" in str(e).lower() or e.status_code == 529:
            return "很抱歉，服務暫時無法使用，請直撥 04-25882881", False
        return "很抱歉，系統暫時忙碌，請稍後再試或直撥 04-25882881", False
    except Exception:
        return "很抱歉，系統暫時忙碌，請稍後再試或直撥 04-25882881", False

    clean, order_info = extract_order(raw)
    if order_info:
        notify_owner(uid, order_info)

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
        return cached

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
            # 管理員指令：查詢自己的 LINE UID（用於設定 OWNER_LINE_UID 環境變數）
            if text.strip() == "!myid":
                reply(token, f"您的 LINE UID：\n{uid}")
                continue
            now = time.time()
            if now - last_request.get(uid, 0) < RATE_LIMIT_SECONDS:
                reply(token, "您傳訊息太快了，請稍後再試 😊")
                continue
            last_request[uid] = now
            if is_share_request(text):
                reply(token, share_messages())
            else:
                rule = quick_rule_reply(text)
                reply(token, rule if rule else ask_with_cache(uid, text))
    return "OK"


if __name__ == "__main__":
    app.run()
