import os
import json
import logging
import httpx
from fastapi import FastAPI, Request, BackgroundTasks, HTTPException, Query
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional, List
from supabase import create_client, Client

# Configure Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment Variables
VERIFY_TOKEN = os.environ.get("VERIFY_TOKEN", "your_secure_verify_token")
WHATSAPP_TOKEN = os.environ.get("WHATSAPP_TOKEN", "your_meta_graph_api_token")
PHONE_NUMBER_ID = os.environ.get("PHONE_NUMBER_ID", "your_sender_phone_id")
MERCHANT_PHONE = os.environ.get("MERCHANT_PHONE", "your_merchant_number")

SUPABASE_URL = os.environ.get("SUPABASE_URL", "your_supabase_url")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "your_supabase_service_key")

# Initialize Clients
app = FastAPI(title="B2B WhatsApp Order Router")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- Pydantic Models for Business Logic ---

class OrderItem(BaseModel):
    category: str
    model: str
    quantity: int

class FlowPayload(BaseModel):
    company_name: str
    items: List[OrderItem]
    delivery_window: str

# --- Helper Functions: Meta Graph API ---

async def send_whatsapp_interactive(to: str, text: str, buttons: List[Dict[str, str]]):
    """Sends an interactive button message via Meta Graph API."""
    url = f"https://graph.facebook.com/v19.0/{PHONE_NUMBER_ID}/messages"
    headers = {
        "Authorization": f"Bearer {WHATSAPP_TOKEN}",
        "Content-Type": "application/json"
    }
    
    interactive_buttons = [
        {
            "type": "reply",
            "reply": {"id": btn["id"], "title": btn["title"]}
        } for btn in buttons
    ]

    payload = {
        "messaging_product": "whatsapp",
        "to": to,
        "type": "interactive",
        "interactive": {
            "type": "button",
            "body": {"text": text},
            "action": {"buttons": interactive_buttons}
        }
    }

    async with httpx.AsyncClient() as client:
        response = await client.post(url, headers=headers, json=payload)
        response.raise_for_status()

async def send_whatsapp_text(to: str, text: str):
    """Sends a standard text message."""
    url = f"https://graph.facebook.com/v19.0/{PHONE_NUMBER_ID}/messages"
    headers = {
        "Authorization": f"Bearer {WHATSAPP_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "messaging_product": "whatsapp",
        "to": to,
        "type": "text",
        "text": {"body": text}
    }
    async with httpx.AsyncClient() as client:
        await client.post(url, headers=headers, json=payload)

# --- Background Task Routines ---

async def process_new_flow_order(customer_phone: str, flow_data: str):
    """Handles SUBMITTED -> MERCHANT_REVIEW routing."""
    try:
        # 1. Parse Flow JSON
        parsed_flow = json.loads(flow_data)
        validated_data = FlowPayload(**parsed_flow)

        # 2. Upsert Customer (Fetch ID)
        cust_res = supabase.table("customers").upsert(
            {"phone_number": customer_phone, "company_name": validated_data.company_name},
            on_conflict="phone_number"
        ).execute()
        customer_id = cust_res.data[0]['id']

        # 3. Create Order (SUBMITTED)
        items_json = [item.model_dump() for item in validated_data.items]
        order_res = supabase.table("orders").insert({
            "customer_id": customer_id,
            "status": "SUBMITTED",
            "items": {"details": items_json, "delivery": validated_data.delivery_window}
        }).execute()
        order_id = order_res.data[0]['id']

        # 4. Transition to MERCHANT_REVIEW
        supabase.table("orders").update({"status": "MERCHANT_REVIEW"}).eq("id", order_id).execute()

        # 5. Alert Merchant via Interactive Buttons
        summary_text = f"New Order from {validated_data.company_name}!\nOrder ID: {order_id}\nReview items in system."
        buttons = [
            {"id": f"confirm_{order_id}", "title": "Confirm Stock"},
            {"id": f"modify_{order_id}", "title": "Modify Quantity"}
        ]
        await send_whatsapp_interactive(MERCHANT_PHONE, summary_text, buttons)
        logger.info(f"Order {order_id} routed to Merchant.")

    except Exception as e:
        logger.error(f"Error processing flow order: {str(e)}")

async def process_merchant_action(button_id: str):
    """Handles MERCHANT_REVIEW -> COUNTER_OFFER routing."""
    try:
        action, order_id = button_id.split("_", 1)
        
        # Fetch Order & Customer
        order_res = supabase.table("orders").select("*, customers(phone_number)").eq("id", order_id).execute()
        if not order_res.data:
            return
        
        order = order_res.data[0]
        customer_phone = order['customers']['phone_number']

        if action == "modify":
            # State Transition: COUNTER_OFFER
            supabase.table("orders").update({"status": "COUNTER_OFFER"}).eq("id", order_id).execute()
            
            # Notify Customer
            alert = f"Hello! The merchant has proposed a quantity modification for your order ({order_id}). Please check the portal to approve."
            await send_whatsapp_text(customer_phone, alert)
            logger.info(f"Order {order_id} transitioned to COUNTER_OFFER.")
            
        elif action == "confirm":
            # State Transition: CONFIRMED
            supabase.table("orders").update({"status": "CONFIRMED"}).eq("id", order_id).execute()
            await send_whatsapp_text(customer_phone, f"Great news! Your order ({order_id}) is confirmed and being prepared.")

    except Exception as e:
        logger.error(f"Error processing merchant action: {str(e)}")

# --- Endpoints ---

@app.get("/webhook")
async def verify_webhook(
    hub_mode: str = Query(None, alias="hub.mode"),
    hub_challenge: str = Query(None, alias="hub.challenge"),
    hub_verify_token: str = Query(None, alias="hub.verify_token")
):
    """Handles Meta's initial verification handshake."""
    if hub_mode == "subscribe" and hub_verify_token == VERIFY_TOKEN:
        logger.info("Webhook verified successfully!")
        return int(hub_challenge)
    raise HTTPException(status_code=403, detail="Verification failed")

@app.post("/webhook")
async def webhook_ingest(request: Request, background_tasks: BackgroundTasks):
    """Ingests incoming JSON payloads and delegates to background tasks."""
    try:
        body = await request.json()
        
        # Guard clause for empty or malformed webhooks
        if "entry" not in body:
            return {"status": "ok"}

        for entry in body["entry"]:
            for change in entry.get("changes", []):
                value = change.get("value", {})
                
                # Check for Messages
                if "messages" in value:
                    for msg in value["messages"]:
                        sender_phone = msg.get("from")
                        
                        # 1. Handle Flow Submission (New Order)
                        if msg.get("type") == "interactive" and "nfm_reply" in msg["interactive"]:
                            flow_response = msg["interactive"]["nfm_reply"]["response_json"]
                            background_tasks.add_task(process_new_flow_order, sender_phone, flow_response)
                        
                        # 2. Handle Merchant Button Reply
                        elif msg.get("type") == "interactive" and "button_reply" in msg["interactive"]:
                            # Only process button replies if they come from the designated merchant number
                            if sender_phone == MERCHANT_PHONE:
                                button_id = msg["interactive"]["button_reply"]["id"]
                                background_tasks.add_task(process_merchant_action, button_id)

        # Meta requires an immediate 200 OK
        return {"status": "ok"}

    except Exception as e:
        logger.error(f"Webhook processing error: {str(e)}")
        # Always return 200 to prevent Meta from disabling the webhook due to timeout/errors
        return {"status": "ok"}
