# 老鄰居 LINE Bot（Python 版）交班日志

---

## 專案基本資料

主程式：D:\Desktop\line-bot\app.py
GitHub：bjniketown/line-bot
線上服務：https://line-bot-endb.onrender.com
後台入口：https://line-bot-endb.onrender.com/store?token=Xl3xup6rm@
訂單後台：https://line-bot-endb.onrender.com/orders?token=Xl3xup6rm@
最近對話：https://line-bot-endb.onrender.com/recent?token=Xl3xup6rm@

---

## 工作規則

- 只編輯 D:\Desktop\line-bot\app.py
- 只有使用者說「上傳」才 commit + push
- Python 指令一律用 `py`，不用 `python` 或 `python3`
- 修改前必須先 Read/Grep 確認程式碼，不可猜測
- **日志規則：** 最近 14 天保留詳細記錄，超過 14 天的條目直接刪除（歷史查 `git log`）。commit message 需寫清楚做了什麼、為什麼、驗證結果。

---

## 目前有效的規則與已知問題

> 此區塊為活文件：問題解決就刪除，不是追加。

### ⚠️ 未上傳的改動（待業主說「上傳」）

- **06-16 A**：客人問運費必須 calc_delivery 實算 + 疑問句不等於下單確認（SYSTEM_TEXT rule 8）
- **06-16 D**：節日不等於公休 + 客人問某日是否營業必須查表（SYSTEM_TEXT rule 12）

### ⚠️ 機器人偶爾跳過訂單寫入 Supabase（持續發生）

Claude 說「訂單已成立」但未呼叫 `create_order` / `create_pickup`，Supabase 無記錄。
Prompt 規則已多次加強，但仍偶發。**每天下班前建議掃一次 `/orders` 後台確認當日訂單完整性。**
發現缺漏：用 `check_ling.py` 查電話 → 手動補建（流程見 reference_supabase_insert.md）。

### ⚠️ CRM 假電話根治待實作

`_fetch_and_save_line_profile` 查不到 `line_uid` 時會新建假電話記錄（`line_{uid[:12]}`），與舊客真實記錄並存。
目前靠 `save_customer_profile` 存入真電話後自動刪假電話（06-02 B）暫時緩解，但根本解法是：查不到 `line_uid` 時改為補綁到現有記錄，而非新建。

### 📅 _HOLIDAYS_2026 每年 12 月底需人工更新

`current_date_text()` 裡的 `_HOLIDAYS_2026` 字典，每年 12 月底手動補下一年節日清單，否則節日標注會失效。

---

## 最近 14 天詳細記錄

> 超過 14 天的條目刪除，歷史查 `git log`。

### 2026-06-02 A — 日期對照表延長 + 禁止自行推算星期
**commit：** 0a9362d / 822cce0
日期對照表 30→60 天；明確禁止 Claude 自行推算星期幾，一律查表或呼叫 validate_pickup_time。

### 2026-06-02 B — CRM 假電話重複問題根治
**commit：** 0da86a5
存入真實電話後自動刪同 line_uid 的假電話記錄（`phone LIKE line_%`）。

### 2026-06-02 C — chat_logs 全量儲存 + 人格分析調整
**commit：** b4bd206
所有 user 訊息全存 chat_logs；人格分析觸發條件 30→50 則、取樣 60→100 則。

### 2026-06-02 D — CRM 月報好友統計拆分
**commit：** c151e23
/report 好友統計改為新增/封鎖/淨成長三欄。

### 2026-06-07 A — 手動補建訂單（讚讚讚阿姨 #86）
機器人跳過 `create_pickup`，手動補建 Supabase 訂單 #86（480元，已取貨）。

### 2026-06-07 B — 新客戶流程不重複詢問已知資訊
**commit：** 921d1b0
`found=false` 後先回顧已知資訊，已知者不重問。

### 2026-06-07 C — 修正簡訊導入舊客戶被誤判為新客戶
**commit：** d04d520
第一次查無 UID 改回「請先詢問電話再查」，帶電話第二次查無才確認新客。

### 2026-06-07 D — 快取回覆補 [CACHE] log
**commit：** 434589f
快取命中補 log，Render 可見所有回覆。

### 2026-06-07 E — SYSTEM_TEXT 精簡整理
**commit：** c1a361c
修正重複規則編號；刪除 FAQ 區塊 19 條重複內容。

### 2026-06-08 A～E — 新客電話不重問、移除免運靜態回覆、地址追問修正、分段提示、Debounce 精簡
**commit：** 3607178 / 75d853a / b28922b / 7c5b2dc / 875c32d
五個小修，詳見 git log。

### 2026-06-09 A — 新增「宅配問題」關鍵字靜態回覆
**commit：** 322df8a
按 rich menu「宅配問題」直接回宅配懶人包，不再進 Claude。

### 2026-06-09 B — 新增機器人回覆總開關（bot_paused）
**commit：** 648dd32
後台一鍵暫停/恢復所有回覆，暫停期間仍記錄 UID。

### 2026-06-09 D — 日期對照表加入節日標注
**commit：** 1cbcefe
對照表格式改為 `06/19(星期五/端午節)`，加入 2026 全年節日。

### 2026-06-10 A — 月報沉睡客喚醒統計條件修正
移除「本月需有訂單」條件，改為「本月綁定 UID 的舊客戶」即算喚醒。

### 2026-06-10 C — 禁止用當下時刻判斷今天自取時間
**commit：** 1e383d8
所有日期一律呼叫 validate_pickup_time，不可依當下時刻自行判斷是否在營業中。

### 2026-06-10 E — 手動補建訂單（陳宜君 #110）
機器人跳過訂單寫入，手動補建 #110（3,500元宅配，出貨 06-24）。

### 2026-06-11 A — 月報沉睡客卡片說明文字修正
**commit：** 6a35861
卡片說明同步改為「本月綁定UID的舊客戶」。

### 2026-06-11 B — /recent 頁面載入速度優化
**commit：** 0237a55
查顯示名稱改為批次查詢，頁面從 3–4 秒加速至即時。

### 2026-06-11 C/D — 手動補建訂單（林淑莉 #111、楊欣儒 #112）
6/10 訂單完整性查核，兩筆漏記補建（均 status=completed）。

### 2026-06-11 F/G — 批次更新失效門號備註（共 17 筆）
業主確認的失效門號，customers.notes 標記「門號已無效」。

### 2026-06-11 H/I — 日期推算三向禁止
**commit：** 0d31fe6 / 03330c7
補上「星期幾→必須查表找日期」與「節日名稱→必須查表找 MM/DD」兩條禁止規則。

### 2026-06-13 A — 移除 [RAW] log 300 字截斷
**commit：** 7bbd438
機器人回覆完整記錄，不再截斷。

### 2026-06-13 B — 自取取貨時間立即驗證 + 詢問時段依週日修正
**commit：** e85ea6b
客人說出時間後必須立即呼叫 validate_pickup_time；週日只列上午時段。

### 2026-06-14 A — 手動補建訂單（張齡方 #124）
機器人跳過訂單寫入，手動補建 #124（925元宅配，出貨 06-15，status=pending）。

### 2026-06-14 B — 黃明俊備註更新
確認 Supabase 無訂單記錄，customers 備註寫入「宅配取消放鳥，也不主動通知」。

### 2026-06-16 A — 客人問運費必須實算 + 疑問句不等於下單（⚠️ 未上傳）
SYSTEM_TEXT rule 8：問運費必須呼叫 calc_delivery；疑問句一律視為詢問，不可 create_order。
觸發：孫維悌問「包郵？」被閃避→重問「運費是225元？」被誤判成下單確認，成立錯誤訂單。

### 2026-06-16 B — 孫維悌錯誤訂單 #132 取消
訂單 #132（925元）status 改為 cancelled，Redis 保留。

### 2026-06-16 C — 辣油包規則 + 昆布花生醬料區分 + 訂單備註欄位
**commit：** 9548231
多要辣油包→婉拒推薦辣子；create_pickup/create_order 新增 order_notes 欄位供葷素備註。

### 2026-06-16 D — 節日不等於公休 + 問某日是否營業必須查表（⚠️ 未上傳）
SYSTEM_TEXT rule 12：節日標注僅供出貨參考，門市國定假日照常營業，公休只有週四。
觸發：吳小姐問 6/21 是否營業，機器人自編「端午節公休」，實際 6/21 是普通週日正常營業。

### 2026-06-16 E — 補靜態回覆 [KEYWORD] log + 客人訊息移除 80 字截斷
**commit：** 5110c02
[MSG] 客人訊息完整記錄；quick_rule_reply 命中後補 [KEYWORD] log。

### 2026-06-16 F — 孫維悌訂單 #133 取消 + 高風險客戶標記
訂單 #133（1625元）status 改為 cancelled；customers 備註與 customer_personality 均寫入高風險標記。
