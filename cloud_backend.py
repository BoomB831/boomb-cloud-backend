import requests

SUPABASE_URL = "https://myvizknnotmredrotljj.supabase.co"
SUPABASE_KEY = "sb_publishable_-ieYicT4NzzNjKEQHHjf6g_wFWcfECj"

headers = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}


def upload_sale(data):
    url = f"{SUPABASE_URL}/rest/v1/продажби_в_магазина"

    r = requests.post(url, json=data, headers=headers)

    if r.status_code in (200, 201):
        return True

    print("Cloud sync error:", r.text)
    return False

import json
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Optional

import requests

from cloudcart_config_clean import (
    SUPABASE_URL,
    SUPABASE_SERVICE_ROLE_KEY,
    SUPABASE_PUBLISHABLE_KEY,
    CLOUD_BACKEND_HOST,
    CLOUD_BACKEND_PORT,
    CLOUD_BACKEND_WEBHOOK_TOKEN,
)

SUPABASE_TABLE = "cloudcart_orders"

WEBHOOK_PATHS = {"/cloudcart/webhook", "/webhook/order"}


def _extract_items(payload: Dict[str, Any], attrs: Dict[str, Any]) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []

    source_items = attrs.get("items") or attrs.get("products") or []
    if isinstance(source_items, list):
        for item in source_items:
            if not isinstance(item, dict):
                continue
            items.append({
                "barcode": str(item.get("barcode") or item.get("sku") or item.get("ean") or item.get("code") or ""),
                "name": str(item.get("name") or item.get("title") or item.get("product_name") or "Продукт"),
                "qty": int(item.get("quantity") or item.get("qty") or item.get("ordered_quantity") or 0),
                "picked": 0,
            })

    if items:
        return items

    included = payload.get("included", []) or []
    if isinstance(included, list):
        for inc in included:
            if not isinstance(inc, dict):
                continue

            inc_type = str(inc.get("type", "")).lower()
            if inc_type not in ("order_products", "order-product", "orderproduct", "items", "products"):
                continue

            inc_attrs = inc.get("attributes", {}) or {}
            items.append({
                "barcode": str(inc_attrs.get("barcode") or inc_attrs.get("sku") or inc_attrs.get("ean") or inc_attrs.get("code") or ""),
                "name": str(inc_attrs.get("name") or inc_attrs.get("title") or inc_attrs.get("product_name") or "Продукт"),
                "qty": int(inc_attrs.get("quantity") or inc_attrs.get("qty") or inc_attrs.get("ordered_quantity") or 0),
                "picked": 0,
            })

    return items


def _extract_order_record(payload: Dict[str, Any]) -> Dict[str, Any]:
    data = payload.get("data", {}) or {}
    attrs = data.get("attributes", {}) or {}

    order_id = str(data.get("id") or attrs.get("id") or "")
    order_number = str(attrs.get("order_number") or attrs.get("number") or order_id)

    first_name = str(attrs.get("customer_first_name") or "").strip()
    last_name = str(attrs.get("customer_last_name") or "").strip()
    full_name = f"{first_name} {last_name}".strip()

    customer_name = (
        full_name
        or str(attrs.get("customer_name") or "").strip()
        or str(attrs.get("billing_name") or "").strip()
        or str(attrs.get("shipping_name") or "").strip()
        or "Клиент"
    )

    customer_email = str(attrs.get("customer_email") or attrs.get("email") or "")
    status = str(attrs.get("status") or attrs.get("order_status") or "")

    items = _extract_items(payload, attrs)

    return {
        "order_id": order_id,
        "order_number": order_number,
        "customer_name": customer_name,
        "customer_email": customer_email,
        "status": status,
        "items_json": items,
        "raw_json": payload,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def save_cloudcart_order_to_supabase(payload: Dict[str, Any]) -> Dict[str, Any]:
    record = _extract_order_record(payload)

    supabase_headers = {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }

    base_url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}"

    check_resp = requests.get(
        base_url,
        params={
            "select": "id",
            "order_id": f"eq.{record['order_id']}",
            "limit": "1",
        },
        headers=supabase_headers,
        timeout=30,
    )
    check_resp.raise_for_status()
    existing_rows = check_resp.json()

    if existing_rows:
        save_resp = requests.patch(
            base_url,
            params={"order_id": f"eq.{record['order_id']}"},
            json=record,
            headers=supabase_headers,
            timeout=30,
        )
    else:
        save_resp = requests.post(
            base_url,
            json=record,
            headers=supabase_headers,
            timeout=30,
        )

    save_resp.raise_for_status()
    return record


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        if isinstance(value, str):
            value = value.replace(",", ".").strip()
        return float(value)
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(_safe_float(value, default))
    except Exception:
        return default


def _normalize_money(value: Any) -> float:
    if value in (None, ""):
        return 0.0

    if isinstance(value, int):
        return value / 100.0

    if isinstance(value, str):
        s = value.strip().replace(",", ".")
        if not s:
            return 0.0
        if s.isdigit():
            return float(s) / 100.0
        return _safe_float(s, 0.0)

    val = _safe_float(value, 0.0)
    if float(val).is_integer() and val >= 100:
        return val / 100.0
    return val


def _supabase_headers_write() -> Dict[str, str]:
    return {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }


def _supabase_headers_read() -> Dict[str, str]:
    return {
        "apikey": SUPABASE_PUBLISHABLE_KEY,
        "Authorization": f"Bearer {SUPABASE_PUBLISHABLE_KEY}",
        "Content-Type": "application/json",
    }


def _extract_items_from_payload(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []

    data = payload.get("data", {}) or {}
    attrs = data.get("attributes", {}) or {}
    included = payload.get("included", []) or []

    source_items = (
        attrs.get("items")
        or attrs.get("products")
        or attrs.get("order_products")
        or attrs.get("ordered_products")
        or []
    )

    if isinstance(source_items, list):
        for item in source_items:
            if not isinstance(item, dict):
                continue

            barcode = (
                item.get("barcode")
                or item.get("sku")
                or item.get("ean")
                or item.get("code")
                or ""
            )
            name = (
                item.get("name")
                or item.get("title")
                or item.get("product_name")
                or "Продукт"
            )
            qty = _safe_int(
                item.get("quantity")
                or item.get("qty")
                or item.get("ordered_quantity")
                or 0,
                0,
            )

            items.append(
                {
                    "barcode": str(barcode),
                    "name": str(name),
                    "qty": qty,
                    "picked": 0,
                }
            )

    if not items:
        for inc in included:
            if not isinstance(inc, dict):
                continue

            inc_type = str(inc.get("type", "")).lower()
            inc_attrs = inc.get("attributes", {}) or {}

            if inc_type not in (
                "order_products",
                "order-product",
                "ordered_products",
                "ordered-product",
                "items",
                "item",
            ):
                continue

            barcode = (
                inc_attrs.get("barcode")
                or inc_attrs.get("sku")
                or inc_attrs.get("ean")
                or inc_attrs.get("code")
                or ""
            )
            name = (
                inc_attrs.get("name")
                or inc_attrs.get("title")
                or inc_attrs.get("product_name")
                or "Продукт"
            )
            qty = _safe_int(
                inc_attrs.get("quantity")
                or inc_attrs.get("qty")
                or inc_attrs.get("ordered_quantity")
                or 0,
                0,
            )

            items.append(
                {
                    "barcode": str(barcode),
                    "name": str(name),
                    "qty": qty,
                    "picked": 0,
                }
            )

    return items


def _extract_order_record(payload: Dict[str, Any]) -> Dict[str, Any]:
    data = payload.get("data", {}) or {}
    attrs = data.get("attributes", {}) or {}

    order_id = str(data.get("id", "")).strip()
    order_number = str(
        attrs.get("number")
        or attrs.get("order_number")
        or order_id
    ).strip()

    first_name = str(attrs.get("customer_first_name") or "").strip()
    last_name = str(attrs.get("customer_last_name") or "").strip()
    customer_name = (
        f"{first_name} {last_name}".strip()
        or attrs.get("customer_name")
        or attrs.get("client_name")
        or "Клиент"
    )

    total = _normalize_money(
        attrs.get("price_total")
        or attrs.get("total")
        or attrs.get("grand_total")
        or 0
    )

    items = _extract_items_from_payload(payload)

    return {
        "order_id": order_id,
        "order_number": order_number,
        "customer_name": customer_name,
        "customer_email": str(attrs.get("customer_email") or ""),
        "status": str(attrs.get("status") or attrs.get("order_status") or ""),
        "currency": str(attrs.get("currency") or "EUR"),
        "total": total,
        "items_json": items,
        "raw_json": payload,
        "updated_at": _now_iso(),
    }


def upsert_cloudcart_order(payload: Dict[str, Any]) -> Dict[str, Any]:
    record = _extract_order_record(payload)

    if not record["order_id"]:
        raise ValueError("Missing order_id in webhook payload")

    url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}"
    response = requests.post(
        url,
        headers=_supabase_headers_write(),
        params={"on_conflict": "order_id"},
        json=record,
        timeout=30,
    )
    response.raise_for_status()
    return record


def fetch_cloud_orders(limit: int = 100) -> List[Dict[str, Any]]:
    url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}"
    response = requests.get(
        url,
        headers=_supabase_headers_read(),
        params={
            "select": "order_id,order_number,customer_name,status,total,currency,items_json,updated_at",
            "order": "updated_at.desc",
            "limit": str(limit),
        },
        timeout=30,
    )
    response.raise_for_status()
    rows = response.json() or []

    result: List[Dict[str, Any]] = []
    for row in rows:
        result.append(
            {
                "id": str(row.get("order_id", "")),
                "number": str(row.get("order_number", "")),
                "client": str(row.get("customer_name", "") or "Клиент"),
                "status": str(row.get("status", "")),
                "products": row.get("items_json", []) or [],
                "total": _safe_float(row.get("total"), 0.0),
                "currency": str(row.get("currency", "EUR")),
                "raw": row,
            }
        )
    return result


def fetch_cloud_order(order_id: str) -> Optional[Dict[str, Any]]:
    url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}"
    response = requests.get(
        url,
        headers=_supabase_headers_read(),
        params={
            "select": "order_id,order_number,customer_name,status,total,currency,items_json,updated_at",
            "order_id": f"eq.{order_id}",
            "limit": "1",
        },
        timeout=30,
    )
    response.raise_for_status()
    rows = response.json() or []
    if not rows:
        return None

    row = rows[0]
    return {
        "id": str(row.get("order_id", "")),
        "number": str(row.get("order_number", "")),
        "client": str(row.get("customer_name", "") or "Клиент"),
        "status": str(row.get("status", "")),
        "products": row.get("items_json", []) or [],
        "total": _safe_float(row.get("total"), 0.0),
        "currency": str(row.get("currency", "EUR")),
        "raw": row,
    }


class CloudWebhookHandler(BaseHTTPRequestHandler):
    server_version = "BoomBCloudBackend/1.0"

    def _send_json(self, code: int, payload: Dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        if self.path in ("/", "/health"):
            self._send_json(200, {"ok": True, "service": "cloud_backend"})
            return
        self._send_json(404, {"ok": False, "error": "Not found"})
        if self.path in WEBHOOK_PATHS:
            self._send_json(200, {"ok": True, "webhook": True, "paths": list(WEBHOOK_PATHS)}) 

    def do_POST(self) -> None:
        if path in WEBHOOK_PATHS:
            print(f"[cloud_backend] webhook hit: {path}")

            token = self.headers.get("X-Webhook-Token", "")
            if token != CLOUD_BACKEND_WEBHOOK_TOKEN:
                print("[cloud_backend] invalid webhook token")
                self._send_json(403, {"ok": False, "error": "invalid token"})
                return

            try:
                content_length = int(self.headers.get("Content-Length", "0"))
                raw_body = self.rfile.read(content_length) if content_length > 0 else b"{}"

                print(f"[cloud_backend] raw body size: {len(raw_body)}")

                payload = json.loads(raw_body.decode("utf-8") or "{}")
                record = save_cloudcart_order_to_supabase(payload)

                print(
                    f"[cloud_backend] order saved: "
                    f"order_id={record['order_id']} "
                    f"number={record['order_number']} "
                    f"items={len(record['items_json'])}"
                )

                self._send_json(
                    200,
                    {
                        "ok": True,
                        "saved": True,
                        "order_id": record["order_id"],
                        "order_number": record["order_number"],
                    },
                )
            except Exception as exc:
                print(f"[cloud_backend] webhook error: {exc}")
                self._send_json(500, {"ok": False, "error": str(exc)})
            return


    def log_message(self, format: str, *args: Any) -> None:
        print("[cloud_backend]", format % args)


def run_cloud_backend() -> None:
    server = ThreadingHTTPServer((CLOUD_BACKEND_HOST, CLOUD_BACKEND_PORT), CloudWebhookHandler)
    print(f"Cloud backend running on http://{CLOUD_BACKEND_HOST}:{CLOUD_BACKEND_PORT}")
    print("Webhook path: /cloudcart/webhook")
    server.serve_forever()


if __name__ == "__main__":
    run_cloud_backend()