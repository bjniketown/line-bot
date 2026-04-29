import os, hashlib, hmac, base64
from flask import Flask, request, abort
import anthropic, requests

app = Flask(__name__)

LINE_TOKEN  = os.environ["LINE_TOKEN"]
LINE_SECRET = os.environ["LINE_SECRET"]
claude = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_KEY"])

# ↓↓↓ 修改這裡，填入你的公司資訊 ↓↓↓
SYSTEM = """你是客服助理，請用繁體中文親切回覆客戶。
公司名稱：老鄰居
主要業務：（填入你的業務）
若無法回答，請說：「我幫您轉接真人客服，請稍候」"""

histories = {}

def verify(body, sig):
    h = hmac.new(LINE_SECRET.encode(), body, hashlib.sha256).digest()
    return hmac.compare_digest(base64.b64encode(h).decode(), sig)

def reply(token, text):
    requests.post(
        "https://api.line.me/v2/bot/message/reply",
        headers={"Authorization": f"Bearer {LINE_TOKEN}"},
        json={"replyToken": token, "messages": [{"type": "text", "text": text}]}
    )

def ask(uid, msg):
    histories.setdefault(uid, [])
    histories[uid].append({"role": "user", "content": msg})
    histories[uid] = histories[uid][-10:]
    r = claude.messages.create(
        model="claude-haiku-4-5",
        max_tokens=500,
        system=SYSTEM,
        messages=histories[uid]
    )
    ans = r.content[0].text
    histories[uid].append({"role": "assistant", "content": ans})
    return ans

@app.route("/webhook", methods=["POST"])
def webhook():
    if not verify(request.get_data(), request.headers.get("X-Line-Signature","")):
        abort(400)
    for e in request.json.get("events", []):
        if e["type"] == "message" and e["message"]["type"] == "text":
            reply(e["replyToken"], ask(e["source"]["userId"], e["message"]["text"]))
    return "OK"

if __name__ == "__main__":
    app.run()
