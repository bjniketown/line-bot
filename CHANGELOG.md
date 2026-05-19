# 老鄰居 LINE 機器人 — 修改紀錄

---

## 2026-05-02（第一次大幅更新）

### 新增功能
- **繁盛時期公告**：後台可填寫原因、起訖日期，Claude 會主動提醒宅配客人物流較忙
- **關店公告加強**：關店訊息加入「未來可預訂自取」說明
- **水餃說明**：冷凍/生水餃皆有，常溫不可宅配，已在 FAQ 中補充

### 規則新增
- **Rule 9 — 水餃混搭宅配**：水餃＋宅配品項同時訂購時，拆開處理，不拒絕整筆；水餃另安排門市自取
- **Rule 12 — 自取時間驗證（最終版）**：自取訂單與宅配排程完全無關，出現「排程」「出貨日」「滿檔」等字眼視為嚴重錯誤
- **Rule 13 — 收件日反推**：客人說「5/13 收到」→ 反推出貨日 → 若不可出貨，列出最近兩個方案請客人選，不可直接改期不說明
- **Rule 14 — 門市包裝**：自取預設一般包，絕對不可主動詢問包裝選項；客人主動要求才改真空包（70元）

### 安全性修正
- **移除 Google Fonts**：管理後台改用系統字型，避免 token 透過 Referer 洩漏給 Google
- **新增 no-referrer meta tag**：`<meta name='referrer' content='no-referrer'>`

### Bug 修正
- **「已付款」被 KEYWORD_RULES 攔截**：加入付款確認 bypass（已匯款、付好了、末四碼、純四位數字等）
- **自取客人問付款/運費被誤攔**：加入自取情境 bypass
- **含數字運費問題被攔截**：加入 `運費 + 數字` bypass，讓 Claude 計算
- **花生/昆布只顯示 100 元**：更新 KEYWORD_RULES 顯示 50 或 100 元（門市兩種規格）
- **清除記憶未清客戶資料**：`清除記憶` 指令同時清除 `profile:{uid}`

### 確認事項
- **老闆通知功能**：目前無此功能。`OWNER_LINE_UID` 只用於豁免每日限制與排除老闆帳號，不會主動推播訂單給老闆

---

## 2026-05-19（CRM 建立 + 機器人修正）

### CRM 系統建立
- **Supabase 整合**：新增 customers、addresses、orders 三張表，取代 Redis phone_profile
- **雙表設計**：customers（主檔）+ addresses（收件地址），一個客人可儲存多個地址
- **1,226 筆客戶資料**從本地 CSV 整理上傳，含地址合併（縣市＋區＋街道）、去重複、備註保留
- **後台 /customers 更新**：改從 Supabase 讀取，顯示地址1/地址2，備註可直接編輯
- **機器人同步**：新訂單成立時自動寫入 Supabase customers + addresses，Redis 保留為快取

### 機器人規則修正
- **fix: _TIME_WARNING_KEYWORDS 新增「還沒開門」等詞語**（commit 3bdca30）：避免合法時間被誤判為未開門
- **fix: Rule 12 補充含明確小時數直接判定**（commit fa6689c、61faa70）：「下午5點」「下午5點30」等明確時間直接確認，不再多問一輪
- **fix: _replace_total() 擴充**：新增 regex 捕捉「總金額為 X 元」格式，防止金額重複顯示

### 環境變數新增（Render）
- `SUPABASE_URL`：`https://mnazxeogpkwwhuassruu.supabase.co`
- `SUPABASE_KEY`：publishable key（anon）

---

## 使用說明

每次對話結束前，請告訴 Claude「把今天的重要決定存起來」，Claude 會：
1. 將新規則/決定存入記憶檔（跨對話保留）
2. 將具體修改補充到這個 CHANGELOG.md

