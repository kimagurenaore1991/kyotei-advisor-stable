import imaplib
import email
from email.header import decode_header
import re
import time
import asyncio
from datetime import datetime, timedelta, timezone
from app_config import PAYMENT_EMAIL, PAYMENT_PASS, PAYMENT_IMAP_SERVER, JST
from supabase_client import get_supabase_client

# 料金設定
PRICE_TIERS = {
    1200: 90, # 初回/キャンペーン: 90日
    1500: 90, # 通常更新: 90日
    500: 30   # 1ヶ月
}

def decode_str(s):
    if s is None: return ""
    decoded = decode_header(s)
    parts = []
    for content, charset in decoded:
        if isinstance(content, bytes):
            parts.append(content.decode(charset or 'utf-8', errors='ignore'))
        else:
            parts.append(content)
    return "".join(parts)

def get_email_body(msg):
    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            content_disposition = str(part.get("Content-Disposition"))
            if content_type == "text/plain" and "attachment" not in content_disposition:
                return part.get_payload(decode=True).decode('utf-8', errors='ignore')
    else:
        return msg.get_payload(decode=True).decode('utf-8', errors='ignore')
    return ""

async def process_payment(sender_name, amount):
    """入金を検知し、DBを更新する"""
    if amount < 500:
        print(f"[MONITOR] Amount too low: {amount} from {sender_name}")
        return

    days_to_add = PRICE_TIERS.get(amount, 30) # 該当なしは30日
    if amount >= 1200: days_to_add = 90 # 1200以上ならとりあえず90日

    supabase = get_supabase_client()
    
    # payment_name が一致するユーザーを検索
    # 大文字小文字や空白の差を考慮するため、取得後にフィルタリング
    try:
        res = supabase.table("profiles").select("id, email, premium_until").execute()
        if not res.data: return

        # 全角半角、空白を正規化して比較
        def normalize(s):
            if not s: return ""
            # スペース削除、大文字化、全角→半角（簡易）
            return re.sub(r'\s+', '', s).upper()

        target_name = normalize(sender_name)
        matched_user = None
        
        # profilesを取得し、payment_nameが一致するかチェック
        # profilesテーブルに payment_name カラムがある前提
        profile_res = supabase.table("profiles").select("*").execute()
        for profile in profile_res.data:
            p_name = profile.get("payment_name")
            if p_name and normalize(p_name) == target_name:
                matched_user = profile
                break
        
        if matched_user:
            user_id = matched_user["id"]
            current_until_str = matched_user.get("premium_until")
            
            now = datetime.now(timezone.utc)
            if current_until_str:
                current_until = datetime.fromisoformat(current_until_str.replace('Z', '+00:00'))
                # すでに期限内なら、その期限から延長。切れていたら今から延長。
                start_date = max(now, current_until)
            else:
                start_date = now
            
            new_until = start_date + timedelta(days=days_to_add)
            
            update_data = {
                "premium_until": new_until.isoformat(),
                "is_premium": True,
                "last_payment_at": now.isoformat()
            }
            
            supabase.table("profiles").update(update_data).eq("id", user_id).execute()
            print(f"[MONITOR] SUCCESS: Updated user {matched_user['email']} (+{days_to_add} days). New until: {new_until}")
        else:
            print(f"[MONITOR] No matching user found for name: '{sender_name}' (Normalized: '{target_name}')")
            
    except Exception as e:
        print(f"[MONITOR ERROR] process_payment failed: {e}")

def check_emails():
    """Gmailをチェックして入金メールを探す"""
    try:
        mail = imaplib.IMAP4_SSL(PAYMENT_IMAP_SERVER)
        mail.login(PAYMENT_EMAIL, PAYMENT_PASS)
        mail.select("inbox")

        # 未読または最近のメールを検索 (過去1日分程度)
        date = (datetime.now() - timedelta(days=1)).strftime("%d-%b-%Y")
        status, messages = mail.search(None, f'(SINCE {date})')
        
        if status != 'OK': return

        for num in messages[0].split():
            status, data = mail.fetch(num, '(RFC822)')
            if status != 'OK': continue
            
            raw_email = data[0][1]
            msg = email.message_from_bytes(raw_email)
            
            subject = decode_str(msg.get("Subject"))
            body = get_email_body(msg)
            
            # --- SMBC 判定 ---
            if "振込入金のお知らせ" in subject and "三井住友銀行" in body:
                # 抽出例:
                # ■お振込金額
                # 500円
                # ■お振込人
                # ヤマダ タロウ 様
                amt_match = re.search(r"■お振込金額\s+([\d,]+)円", body)
                name_match = re.search(r"■お振込人\s+([^\n]+?)\s+様", body)
                
                if amt_match and name_match:
                    amount = int(amt_match.group(1).replace(",", ""))
                    sender = name_match.group(1).strip()
                    print(f"[MONITOR] Found SMBC Deposit: {amount} JPY from {sender}")
                    asyncio.run(process_payment(sender, amount))

            # --- PayPay 判定 ---
            elif "PayPay残高を受け取りました" in subject:
                # 抽出例:
                # 500円
                # ヤマダさんからPayPay残高を受け取りました。
                amt_match = re.search(r"([\d,]+)円", body)
                name_match = re.search(r"([^\nー]+?)さんからPayPay残高を受け取りました", body)
                
                if amt_match and name_match:
                    amount = int(amt_match.group(1).replace(",", ""))
                    sender = name_match.group(1).strip()
                    print(f"[MONITOR] Found PayPay Deposit: {amount} JPY from {sender}")
                    asyncio.run(process_payment(sender, amount))

        mail.logout()
    except Exception as e:
        print(f"[MONITOR ERROR] check_emails: {e}")

async def payment_monitor_loop():
    """定期実行ループ"""
    print("[MONITOR] Starting payment monitor loop...")
    while True:
        try:
            # 別のスレッドやプロセスで実行する代わりに、
            # I/O待ちを考慮して同期的なcheck_emailsをスレッドで叩くと良いが、
            # シンプルに1回実行
            await asyncio.to_thread(check_emails)
        except Exception as e:
            print(f"[MONITOR ERROR] Loop error: {e}")
        
        await asyncio.sleep(120) # 2分おき
