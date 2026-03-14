from __future__ import annotations

import json
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import requests

import cloudcart_config_clean as cfg

SUPABASE_URL = getattr(cfg, "SUPABASE_URL", "")
SUPABASE_SERVICE_ROLE_KEY = getattr(cfg, "SUPABASE_SERVICE_ROLE_KEY", "")
SUPABASE_PUBLISHABLE_KEY = getattr(cfg, "SUPABASE_PUBLISHABLE_KEY", "")
CLOUD_BACKEND_HOST = getattr(cfg, "CLOUD_BACKEND_HOST", "0.0.0.0")
CLOUD_BACKEND_PORT = int(getattr(cfg, "CLOUD_BACKEND_PORT", 8788))
CLOUD_BACKEND_WEBHOOK_TOKEN = getattr(cfg, "CLOUD_BACKEND_WEBHOOK_TOKEN", "")

SUPABASE_TABLE = "cloudcart_orders"
WEBHOOK_PATHS = {"/cloudcart/webhook", "/webhook/order"}


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


def _headers_write() -> Dict[str, str]:
    return {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }


def _headers_read() -> Dict[str, str]:
    key = SUPABASE_PUBLISHABLE_KEY or SUPABASE_SERVICE_ROLE_KEY
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }


def upload_sale(total: float, payment: float, change_amount: float, items: List[Dict[str, Any]], note: str = "") -> bool:
    try:
        if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
            return False

        sale_payload = {
            "total": float(total),
            "payment": float(payment),
            "change_amount": float(change_amount),
            "source": "store_pos",
            "note": note,
        }
        sales_url = f"{SUPABASE_URL}/rest/v1/store_sales"
        r = requests.post(sales_url, json=sale_payload, headers=_headers_write(), timeout=30)
        if r.status_code not in (200, 201):
            print("Cloud sync sale error:", r.status_code, r.text)
            return False

        rows = r.json() or []
        if not rows:
            return False
        sale_id = rows[0]["id"]

        payload_items = []
        for item in items:
            qty = float(item.get("qty", 0))
            price = float(item.get("price", 0))
            payload_items.append(
                {
                    "sale_id": sale_id,
                    "barcode": str(item.get("barcode", "")),
                    "product_name": str(item.get("name", "")),
                    "product_id": str(item.get("pid", "")),
                    "unit_price": price,
                    "qty": qty,
                    "line_total": qty * price,
                }
            )

        if payload_items:
            items_url = f"{SUPABASE_URL}/rest/v1/store_sale_items"
            r2 = requests.post(items_url, json=payload_items, headers=_headers_write(), timeout=30)
            if r2.status_code not in (200, 201):
                print("Cloud sync items error:", r2.status_code, r2.text)
                return False
        return True
    except Exception as exc:
        print("Cloud backend exception:", exc)
        return False


def _extract_items_from_payload(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    data = payload.get("data", {}) or {}
    attrs = data.get("attributes", {}) or {}
    included = payload.get("included", []) or []

    items: List[Dict[str, Any]] = []

    source_items = attrs.get("items") or attrs.get("products") or attrs.get("order_products") or []
    if isinstance(source_items, list):
        for item in source_items:
            if not isinstance(item, dict):
                continue
            items.append(
                {
                    "barcode": str(item.get("barcode") or item.get("sku") or item.get("ean") or item.get("code") or ""),
                    "name": str(item.get("name") or item.get("title") or item.get("product_name") or "Продукт"),
                    "qty": _safe_int(item.get("quantity") or item.get("qty") or item.get("ordered_quantity") or 0),
                    "picked": 0,
                }
            )
    if items:
        return items

    if isinstance(included, list):
        for inc in included:
            if not isinstance(inc, dict):
                continue
            inc_type = str(inc.get("type", "")).lower().replace("-", "_")
            if "order_product" not in inc_type and inc_type not in ("items", "products"):
                continue
            inc_attrs = inc.get("attributes", {}) or {}
            items.append(
                {
                    "barcode": str(inc_attrs.get("barcode") or inc_attrs.get("sku") or inc_attrs.get("ean") or inc_attrs.get("code") or ""),
                    "name": str(inc_attrs.get("name") or inc_attrs.get("title") or inc_attrs.get("product_name") or "Продукт"),
                    "qty": _safe_int(inc_attrs.get("quantity") or inc_attrs.get("qty") or inc_attrs.get("ordered_quantity") or 0),
                    "picked": 0,
                }
            )
    return items


def _extract_order_record(payload: Dict[str, Any]) -> Dict[str, Any]:
    # Supports CloudCart JSON:API and a flatter webhook payload
    data = payload.get("data")
    if isinstance(data, dict):
        attrs = data.get("attributes", {}) or {}
        order_id = str(data.get("id") or attrs.get("id") or "").strip()
        order_number = str(attrs.get("number") or attrs.get("order_number") or order_id).strip()
        first_name = str(attrs.get("customer_first_name") or "").strip()
        last_name = str(attrs.get("customer_last_name") or "").strip()
        customer_name = (
            f"{first_name} {last_name}".strip()
            or str(attrs.get("customer_name") or attrs.get("client_name") or attrs.get("billing_name") or attrs.get("shipping_name") or "Клиент")
        )
        customer_email = str(attrs.get("customer_email") or attrs.get("email") or "")
        status = str(attrs.get("status") or attrs.get("order_status") or "")
        currency = str(attrs.get("currency") or attrs.get("currency_code") or "EUR")
        total = _normalize_money(attrs.get("price_total") or attrs.get("total") or attrs.get("grand_total") or 0)
        items_json = _extract_items_from_payload(payload)
    else:
        customer = payload.get("customer", {}) or {}
        order_id = str(payload.get("id") or payload.get("order_id") or "").strip()
        order_number = str(payload.get("order_number") or payload.get("number") or order_id).strip()
        customer_name = (
            f"{customer.get('first_name', '')} {customer.get('last_name', '')}".strip()
            or str(payload.get("customer_name") or payload.get("client") or "Клиент")
        )
        customer_email = str(customer.get("email") or payload.get("customer_email") or "")
        status = str(payload.get("status") or payload.get("order_status") or "")
        currency = str(payload.get("currency") or payload.get("currency_code") or "EUR")
        total = _normalize_money(payload.get("total") or payload.get("price_total") or payload.get("grand_total") or 0)
        items_json = []
        for item in payload.get("items", []) or []:
            if not isinstance(item, dict):
                continue
            items_json.append(
                {
                    "barcode": str(item.get("barcode") or item.get("sku") or item.get("ean") or item.get("code") or ""),
                    "name": str(item.get("product_name") or item.get("name") or item.get("title") or "Продукт"),
                    "qty": _safe_int(item.get("quantity") or item.get("qty") or item.get("ordered_quantity") or 0),
                    "picked": 0,
                }
            )

    return {
        "order_id": order_id,
        "order_number": order_number,
        "customer_name": customer_name,
        "customer_email": customer_email,
        "status": status,
        "currency": currency,
        "total": total,
        "items_json": items_json,
        "raw_json": payload,
        "updated_at": _now_iso(),
    }


def save_cloudcart_order_to_supabase(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        raise RuntimeError("Missing Supabase configuration")

    record = _extract_order_record(payload)
    if not record["order_id"]:
        raise ValueError("Missing order_id in webhook payload")

    base_url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}"

    check_resp = requests.get(
        base_url,
        params={"select": "id", "order_id": f"eq.{record['order_id']}", "limit": "1"},
        headers=_headers_write(),
        timeout=30,
    )
    check_resp.raise_for_status()
    existing_rows = check_resp.json() or []

    if existing_rows:
        save_resp = requests.patch(
            base_url,
            params={"order_id": f"eq.{record['order_id']}"},
            json=record,
            headers=_headers_write(),
            timeout=30,
        )
    else:
        save_resp = requests.post(base_url, json=record, headers=_headers_write(), timeout=30)

    save_resp.raise_for_status()
    return record


def fetch_cloud_orders(limit: int = 100) -> List[Dict[str, Any]]:
    if not SUPABASE_URL:
        return []

    url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}"
    resp = requests.get(
        url,
        headers=_headers_read(),
        params={
            "select": "order_id,order_number,customer_name,status,total,currency,items_json,updated_at",
            "order": "updated_at.desc",
            "limit": str(limit),
        },
        timeout=30,
    )
    resp.raise_for_status()
    rows = resp.json() or []

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
    if not SUPABASE_URL:
        return None

    url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}"
    resp = requests.get(
        url,
        headers=_headers_read(),
        params={
            "select": "order_id,order_number,customer_name,status,total,currency,items_json,updated_at",
            "order_id": f"eq.{order_id}",
            "limit": "1",
        },
        timeout=30,
    )
    resp.raise_for_status()
    rows = resp.json() or []
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
    server_version = "BoomBCloudBackend/2.1"

    def _send_json(self, code: int, payload: Dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        path = urlparse(self.path).path
        if path in ("/", "/health"):
            self._send_json(200, {"ok": True, "service": "cloud_backend"})
            return
        if path in WEBHOOK_PATHS:
            self._send_json(200, {"ok": True, "webhook": True, "paths": sorted(WEBHOOK_PATHS)})
            return
        self._send_json(404, {"ok": False, "error": "Not found"})

    def do_POST(self) -> None:
        path = urlparse(self.path).path
        if path not in WEBHOOK_PATHS:
            self._send_json(404, {"ok": False, "error": "Unknown webhook path", "path": path})
            return

        content_length = int(self.headers.get("Content-Length", "0") or "0")
        body = self.rfile.read(content_length) if content_length > 0 else b"{}"
        body_text = body.decode("utf-8", errors="replace")

        print("=" * 70)
        print("[cloud_backend] WEBHOOK RECEIVED")
        print("[cloud_backend] path:", path)
        print("[cloud_backend] headers:", dict(self.headers))
        print("[cloud_backend] raw body:", body_text)

        try:
            data = json.loads(body_text or "{}")
        except Exception as exc:
            print("[cloud_backend] INVALID JSON:", exc)
            self._send_json(400, {"ok": False, "error": "Invalid JSON"})
            return

        try:
            record = save_cloudcart_order_to_supabase(data)
            print(
                "[cloud_backend] ORDER SAVED:",
                f"order_id={record['order_id']}",
                f"order_number={record['order_number']}",
                f"items={len(record['items_json'])}",
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
            print("[cloud_backend] SAVE ERROR:", repr(exc))
            self._send_json(500, {"ok": False, "error": str(exc)})

    def log_message(self, fmt: str, *args: Any) -> None:
        print("[cloud_backend]", fmt % args)


def run_cloud_backend() -> None:
    server = ThreadingHTTPServer((CLOUD_BACKEND_HOST, CLOUD_BACKEND_PORT), CloudWebhookHandler)
    print(f"Cloud backend running on http://{CLOUD_BACKEND_HOST}:{CLOUD_BACKEND_PORT}")
    print(f"Webhook paths: {', '.join(sorted(WEBHOOK_PATHS))}")
    server.serve_forever()


if __name__ == "__main__":
    run_cloud_backend()
