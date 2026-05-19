"""
一次性遷移腳本：把 Upstash Redis 的 phone_profile:* 資料全部匯入 Supabase customers 表。
執行方式：
  1. 確認 .env 或環境變數已設定 UPSTASH_URL / UPSTASH_TOKEN / SUPABASE_URL / SUPABASE_KEY
  2. python migrate_to_supabase.py
"""
import os, json, requests
from datetime import datetime, timezone, timedelta

UPSTASH_URL   = os.environ.get("UPSTASH_URL", "")
UPSTASH_TOKEN = os.environ.get("UPSTASH_TOKEN", "")
SUPABASE_URL  = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY  = os.environ.get("SUPABASE_KEY", "")

_TZ_TW = timezone(timedelta(hours=8))

def _redis(command):
    r = requests.post(
        UPSTASH_URL,
        headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"},
        json=command,
        timeout=10,
    )
    return r.json().get("result") if r.ok else None

def _supa_upsert(records: list):
    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/customers",
        headers={
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates",
        },
        json=records,
        timeout=10,
    )
    return r.ok, r.text

def main():
    print("取得 Redis phone_profile:* keys...")
    keys = _redis(["KEYS", "phone_profile:*"]) or []
    print(f"找到 {len(keys)} 筆客戶資料")

    if not keys:
        print("沒有資料可遷移。")
        return

    records = []
    for key in keys:
        raw = _redis(["GET", key])
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue
        phone = key.replace("phone_profile:", "")
        record = {
            "phone": phone,
            "name": data.get("name", ""),
            "address": data.get("address", ""),
            "line_uid": data.get("line_uid", ""),
            "notes": "",
            "updated_at": datetime.now(_TZ_TW).isoformat(),
        }
        records.append(record)
        print(f"  準備：{phone} / {record['name']}")

    print(f"\n開始寫入 Supabase（共 {len(records)} 筆）...")
    batch_size = 50
    for i in range(0, len(records), batch_size):
        batch = records[i:i+batch_size]
        ok, msg = _supa_upsert(batch)
        if ok:
            print(f"  ✓ 第 {i+1}–{i+len(batch)} 筆寫入成功")
        else:
            print(f"  ✗ 第 {i+1}–{i+len(batch)} 筆寫入失敗：{msg}")

    print("\n遷移完成！")

if __name__ == "__main__":
    main()
