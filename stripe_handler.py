import stripe
from datetime import datetime, timedelta, timezone
from app_config import (
    STRIPE_API_KEY, STRIPE_WEBHOOK_SECRET, 
    STRIPE_PRICE_ID_MONTHLY, STRIPE_PRICE_ID_90DAY
)
from supabase_client import get_supabase_client

stripe.api_key = STRIPE_API_KEY

def create_subscription_session(user_id: str, user_email: str, price_id: str, success_url: str, cancel_url: str):
    """
    Stripe Checkout Sessionを作成する
    3日間のトライアルを適用する
    """
    try:
        session = stripe.checkout.Session.create(
            payment_method_types=['card'],
            customer_email=user_email,
            line_items=[{
                'price': price_id,
                'quantity': 1,
            }],
            mode='subscription',
            subscription_data={
                'trial_period_days': 3, # ユーザー要望の3日間トライアル
                'metadata': {
                    'supabase_user_id': user_id
                }
            },
            success_url=success_url,
            cancel_url=cancel_url,
            metadata={
                'supabase_user_id': user_id
            }
        )
        return {"url": session.url}
    except Exception as e:
        print(f"[STRIPE ERROR] create_subscription_session: {e}")
        return {"error": str(e)}

def handle_stripe_webhook(payload, sig_header):
    """
    StripeからのWebhookを処理し、Supabaseのユーザープロファイルを更新する
    """
    try:
        event = stripe.Webhook.construct_event(
            payload, sig_header, STRIPE_WEBHOOK_SECRET
        )
    except Exception as e:
        print(f"[STRIPE WEBHOOK ERROR] {e}")
        return False

    # 支払い成功、または定期購読作成のイベントを処理
    if event['type'] == 'checkout.session.completed':
        session = event['data']['object']
        user_id = session.get('metadata', {}).get('supabase_user_id')
        update_user_premium_status(user_id, "premium")
    
    elif event['type'] == 'customer.subscription.deleted':
        # 購読解除時の処理
        subscription = event['data']['object']
        user_id = subscription.get('metadata', {}).get('supabase_user_id')
        update_user_premium_status(user_id, "free")

    return True

def update_user_premium_status(user_id: str, status: str):
    """
    Supabaseのprofilesテーブルを更新する
    """
    if not user_id: return
    
    supabase = get_supabase_client()
    if not supabase: return
    
    try:
        # 有効期限の計算 (1ヶ月更新を想定)
        # 実際にはStripeから期間を取得するのが正確だが、簡易版として実装
        ends_at = (datetime.now(timezone.utc) + timedelta(days=31)).isoformat()
        
        data = {
            "subscription_status": status,
            "is_premium": True if status == "premium" else False,
            "subscription_ends_at": ends_at if status == "premium" else None,
            "updated_at": datetime.now(timezone.utc).isoformat()
        }
        
        supabase.table("profiles").update(data).eq("id", user_id).execute()
        print(f"[STRIPE] Updated user {user_id} status to {status}")
    except Exception as e:
        print(f"[SUPABASE SYNC ERROR] {e}")
