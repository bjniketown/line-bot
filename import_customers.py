"""
老鄰居客戶資料匯入腳本
執行一次即可，將 CSV 歷史資料批次匯入 Upstash Redis
使用方式：python import_customers.py
"""
import csv, json, re, os, requests

# ── 設定（直接填入，或從環境變數讀取）──────────────────────────────────────
UPSTASH_URL   = os.environ.get("UPSTASH_URL",   "")  # 填入你的 Upstash URL
UPSTASH_TOKEN = os.environ.get("UPSTASH_TOKEN", "")  # 填入你的 Token
CSV_FILE      = os.path.join(os.path.dirname(__file__), "老鄰居客戶資料.csv")

# ── 電話標準化 ──────────────────────────────────────────────────────────────
_STRIP_RE = re.compile(r'[\s\-\(\)\.]')

def normalize_phone(phone: str) -> str:
    p = _STRIP_RE.sub('', str(phone).strip())
    if p.startswith('+886'):
        p = '0' + p[4:]
    elif p.startswith('886'):
        p = '0' + p[3:]
    return p

def is_valid_phone(p: str) -> bool:
    return bool(re.match(r'^09\d{8}$', p))

# ── Redis 操作 ──────────────────────────────────────────────────────────────
def redis_set(key: str, value: str):
    r = requests.post(
        UPSTASH_URL,
        headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"},
        json=["SET", key, value],
        timeout=5,
    )
    return r.ok

# ── 地址組合 ──────────────────────────────────────────────────────────────
def build_address(row: dict) -> str:
    """優先取 Home Address，其次 Business Address。"""
    # Home Address
    parts = [
        row.get("Home State", "").strip(),
        row.get("Home City", "").strip(),
        row.get("Home Street", "").strip(),
    ]
    addr = "".join(p for p in parts if p)
    if addr:
        return addr
    # Business Address（備用）
    parts2 = [
        row.get("Business State", "").strip(),
        row.get("Business City", "").strip(),
        row.get("Business Address", "").strip(),
    ]
    return "".join(p for p in parts2 if p)

# ── 主程式 ──────────────────────────────────────────────────────────────────
def main():
    if not UPSTASH_URL or not UPSTASH_TOKEN:
        print("❌ 請先設定 UPSTASH_URL 和 UPSTASH_TOKEN")
        return

    success = 0
    skip_no_phone = 0
    skip_invalid  = 0
    skip_dup      = 0
    seen_phones   = set()

    with open(CSV_FILE, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    print(f"📂 讀取到 {len(rows)} 筆資料，開始匯入...\n")

    for row in rows:
        # 取姓名
        name = row.get("Display Name", "").strip()
        if not name:
            first = row.get("First Name", "").strip()
            last  = row.get("Last Name",  "").strip()
            name  = f"{last}{first}".strip()
        if not name:
            name = "客人"

        # 取電話（優先 Home Phone，其次 Mobile Phone）
        raw_phone = row.get("Home Phone", "").strip() or row.get("Mobile Phone", "").strip()
        if not raw_phone:
            skip_no_phone += 1
            continue

        phone = normalize_phone(raw_phone)
        if not is_valid_phone(phone):
            skip_invalid += 1
            continue

        # 重複電話：保留第一筆（通常是主要地址）
        if phone in seen_phones:
            skip_dup += 1
            continue
        seen_phones.add(phone)

        # 組合地址
        address = build_address(row)

        profile = {
            "name":    name,
            "phone":   phone,
            "address": address,
            "source":  "歷史匯入",
        }

        key = f"phone_profile:{phone}"
        ok  = redis_set(key, json.dumps(profile, ensure_ascii=False))
        if ok:
            success += 1
            print(f"  ✅ {name} / {phone} / {address[:20] if address else '（無地址）'}")
        else:
            print(f"  ❌ 寫入失敗：{name} / {phone}")

    print(f"""
========================================
匯入完成！
  ✅ 成功：{success} 筆
  ⏭  無電話跳過：{skip_no_phone} 筆
  ⏭  電話格式錯誤跳過：{skip_invalid} 筆
  ⏭  重複電話跳過：{skip_dup} 筆
  📊 合計處理：{len(rows)} 筆
========================================
""")

if __name__ == "__main__":
    main()
