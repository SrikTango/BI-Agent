import os
import time
import json
from datetime import datetime
from typing import List, Dict, Any, Tuple

import requests
import pandas as pd
from flask import Flask, request, jsonify
from dotenv import load_dotenv

# Optional: for nicer summaries
import openai

load_dotenv()

#MONDAY_API_KEY = os.getenv("MONDAY_API_KEY")
MONDAY_API_KEY = "eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjYyODIzNzc0NywiYWFpIjoxMSwidWlkIjoxMDA0OTU1NTMsImlhZCI6IjIwMjYtMDMtMDNUMTU6MTM6MDMuMDAwWiIsInBlciI6Im1lOndyaXRlIiwiYWN0aWQiOjM0MDM2MjY1LCJyZ24iOiJhcHNlMiJ9.ooAkPNMizMM671JyA8hP-PdCfLfZIfUTFJrw7LT-6pM"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
WORK_ORDERS_BOARD_NAME = os.getenv("WORK_ORDERS_BOARD_NAME", "Work Orders")
DEALS_BOARD_NAME = os.getenv("DEALS_BOARD_NAME", "Deals")

MONDAY_API_URL = "https://api.monday.com/v2"

HEADERS = {
    "Authorization": MONDAY_API_KEY,
    "Content-Type": "application/json"
}

if OPENAI_API_KEY:
    openai.api_key = OPENAI_API_KEY

app = Flask(__name__)

def trace_entry(action: str, details: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "action": action,
        "details": details
    }

def monday_graphql(query: str, variables: Dict[str, Any] = None, action_trace: List[Dict] = None) -> Dict[str, Any]:
    payload = {"query": query}
    if variables:
        payload["variables"] = variables
    start = time.time()
    resp = requests.post(MONDAY_API_URL, headers=HEADERS, json=payload, timeout=30)
    elapsed = time.time() - start
    entry = trace_entry("monday_graphql", {
        "query_snippet": query.strip().replace("\n", " ")[:300],
        "variables": variables,
        "status_code": resp.status_code,
        "elapsed_seconds": round(elapsed, 3)
    })
    if action_trace is not None:
        action_trace.append(entry)
    resp.raise_for_status()
    data = resp.json()
    if "errors" in data:
        if action_trace is not None:
            action_trace.append(trace_entry("monday_error", {"errors": data["errors"]}))
        raise Exception(f"monday.com GraphQL error: {data['errors']}")
    return data["data"]

def find_board_id_by_name(board_name: str, action_trace: List[Dict]) -> int:
    query = """
    query ($name: String!) {
      boards (limit:100, query: $name) {
        id
        name
      }
    }
    """
    data = monday_graphql(query, {"name": board_name}, action_trace)
    boards = data.get("boards", [])
    for b in boards:
        if b["name"].strip().lower() == board_name.strip().lower():
            return int(b["id"])
    if boards:
        return int(boards[0]["id"])
    raise ValueError(f"Board named '{board_name}' not found")

def fetch_board_items(board_id: int, action_trace: List[Dict]) -> List[Dict[str, Any]]:
    query = """
    query ($boardId: Int!) {
      boards (ids: [$boardId]) {
        items {
          id
          name
          column_values {
            id
            title
            text
            value
          }
        }
      }
    }
    """
    data = monday_graphql(query, {"boardId": board_id}, action_trace)
    boards = data.get("boards", [])
    if not boards:
        return []
    items = boards[0].get("items", [])
    return items

def items_to_df(items: List[Dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for it in items:
        base = {"item_id": it.get("id"), "item_name": it.get("name")}
        for col in it.get("column_values", []):
            col_name = col.get("title") or col.get("id")
            base[col_name] = col.get("text")
        rows.append(base)
    df = pd.DataFrame(rows)
    return df

def parse_currency(x: Any) -> float:
    if pd.isna(x):
        return 0.0
    s = str(x).strip()
    if s == "":
        return 0.0
    s = s.replace(",", "").replace("$", "").replace("₹", "").replace("€", "")
    if "-" in s:
        parts = [p.strip() for p in s.split("-") if p.strip()]
        try:
            nums = [float(p.replace("k","000").replace("K","000")) for p in parts]
            return max(nums)
        except:
            pass
    try:
        if s.lower().endswith("k"):
            return float(s[:-1]) * 1000
        return float(s)
    except:
        return 0.0

def parse_date(x: Any) -> pd.Timestamp:
    try:
        return pd.to_datetime(x, errors="coerce")
    except:
        return pd.NaT

def normalize_sector(s: Any) -> str:
    if pd.isna(s):
        return "Unknown"
    s = str(s).strip().lower()
    mapping = {
        "energy": ["energy", "energy sector", "oil & gas", "oil and gas", "renewables"],
        "healthcare": ["healthcare", "health care", "med"],
        "finance": ["finance", "financial", "banking"],
        "manufacturing": ["manufacturing", "mfg"]
    }
    for k, variants in mapping.items():
        for v in variants:
            if v in s:
                return k.capitalize()
    return s.capitalize()

def parse_question(question: str) -> Dict[str, Any]:
    q = question.lower()
    parsed = {"sector": None, "timeframe": None, "metrics": []}
    sectors = ["energy", "healthcare", "finance", "manufacturing"]
    for s in sectors:
        if s in q:
            parsed["sector"] = s.capitalize()
            break
    if "quarter" in q or "this quarter" in q:
        parsed["timeframe"] = "this_quarter"
    elif "last quarter" in q:
        parsed["timeframe"] = "last_quarter"
    elif "year" in q:
        parsed["timeframe"] = "year"
    if "pipeline" in q or "pipeline health" in q:
        parsed["metrics"].append("pipeline")
    if "revenue" in q:
        parsed["metrics"].append("revenue")
    if "deals" in q or "deal" in q:
        parsed["metrics"].append("deals")
    return parsed

def compute_pipeline_metrics(deals_df: pd.DataFrame, work_df: pd.DataFrame, parsed: Dict[str, Any]) -> Dict[str, Any]:
    df = deals_df.copy()
    col_map = {}
    for c in df.columns:
        lc = c.lower()
        if "stage" in lc:
            col_map["stage"] = c
        if "value" in lc or "amount" in lc or "budget" in lc:
            col_map["value"] = c
        if "sector" in lc or "industry" in lc:
            col_map["sector"] = c
        if "close" in lc or "date" in lc:
            if "close" in lc:
                col_map["close_date"] = c
            elif "date" in lc and "close" not in col_map:
                col_map["close_date"] = c
    for k in ["stage", "value", "sector", "close_date"]:
        if k not in col_map:
            col_map[k] = None
    if col_map["value"]:
        df["_value_num"] = df[col_map["value"]].apply(parse_currency)
    else:
        df["_value_num"] = 0.0
    if col_map["sector"]:
        df["_sector_norm"] = df[col_map["sector"]].apply(normalize_sector)
    else:
        df["_sector_norm"] = "Unknown"
    if col_map["stage"]:
        df["_stage"] = df[col_map["stage"]].fillna("Unknown")
    else:
        df["_stage"] = "Unknown"
    if col_map["close_date"]:
        df["_close_date"] = df[col_map["close_date"]].apply(parse_date)
    else:
        df["_close_date"] = pd.NaT
    now = pd.Timestamp.now()
    def quarter_range_for(tag: str) -> Tuple[pd.Timestamp, pd.Timestamp]:
        if tag == "this_quarter":
            q = (now.month - 1) // 3 + 1
            start = pd.Timestamp(year=now.year, month=3*(q-1)+1, day=1)
            end = (start + pd.offsets.QuarterEnd())
            return start, end
        elif tag == "last_quarter":
            q = (now.month - 1) // 3 + 1
            if q == 1:
                start = pd.Timestamp(year=now.year-1, month=10, day=1)
            else:
                start = pd.Timestamp(year=now.year, month=3*(q-2)+1, day=1)
            end = (start + pd.offsets.QuarterEnd())
            return start, end
        else:
            return pd.Timestamp.min, pd.Timestamp.max
    start, end = quarter_range_for(parsed.get("timeframe"))
    if parsed.get("sector"):
        sector_filter = parsed["sector"]
        df_filtered = df[df["_sector_norm"].str.lower() == sector_filter.lower()]
    else:
        df_filtered = df
    if parsed.get("timeframe"):
        df_filtered = df_filtered[(df_filtered["_close_date"] >= start) & (df_filtered["_close_date"] <= end)]
    total_pipeline = float(df_filtered["_value_num"].sum())
    count_deals = int(len(df_filtered))
    avg_deal = float(df_filtered["_value_num"].mean()) if count_deals > 0 else 0.0
    by_stage = df_filtered.groupby("_stage")["_value_num"].agg(["sum", "count"]).reset_index().to_dict(orient="records")
    open_stages = ["proposal", "negotiation", "open", "in progress", "working"]
    df_open = df_filtered[df_filtered["_stage"].str.lower().isin(open_stages)]
    open_value = float(df_open["_value_num"].sum())
    metrics = {
        "total_pipeline_value": total_pipeline,
        "deal_count": count_deals,
        "average_deal_value": avg_deal,
        "value_by_stage": by_stage,
        "open_pipeline_value_estimate": open_value,
        "timeframe_start": str(start.date()) if parsed.get("timeframe") else None,
        "timeframe_end": str(end.date()) if parsed.get("timeframe") else None
    }
    return metrics

@app.route("/query", methods=["POST"])
def handle_query():
    payload = request.get_json(force=True)
    question = payload.get("question", "")
    if not question:
        return jsonify({"error": "Please provide a 'question' in JSON body."}), 400
    action_trace: List[Dict] = []
    try:
        parsed = parse_question(question)
        action_trace.append(trace_entry("parse_question", {"question": question, "parsed": parsed}))
        work_board_id = find_board_id_by_name(WORK_ORDERS_BOARD_NAME, action_trace)
        deals_board_id = find_board_id_by_name(DEALS_BOARD_NAME, action_trace)
        action_trace.append(trace_entry("found_boards", {"work_board_id": work_board_id, "deals_board_id": deals_board_id}))
        work_items = fetch_board_items(work_board_id, action_trace)
        deals_items = fetch_board_items(deals_board_id, action_trace)
        work_df = items_to_df(work_items)
        deals_df = items_to_df(deals_items)
        action_trace.append(trace_entry("fetched_counts", {"work_items": len(work_df), "deals_items": len(deals_df)}))
        metrics = compute_pipeline_metrics(deals_df, work_df, parsed)
        action_trace.append(trace_entry("computed_metrics", {"metrics_summary": {
            "total_pipeline_value": metrics["total_pipeline_value"],
            "deal_count": metrics["deal_count"]
        }}))
        if OPENAI_API_KEY:
            prompt = (
                f"Produce a concise founder-level summary (3-5 sentences) of the following metrics.\n\n"
                f"Question: {question}\n\n"
                f"Parsed: {json.dumps(parsed)}\n\n"
                f"Metrics: {json.dumps(metrics)}\n\n"
                f"Also include one short data-quality caveat sentence.\n"
            )
            try:
                resp = openai.Completion.create(
                    engine="text-davinci-003",
                    prompt=prompt,
                    max_tokens=200,
                    temperature=0.2
                )
                summary = resp.choices[0].text.strip()
                action_trace.append(trace_entry("openai_summary", {"note": "openai used for summary"}))
            except Exception as e:
                summary = (
                    f"Total pipeline for {parsed.get('sector') or 'all sectors'}: "
                    f"{metrics['total_pipeline_value']:.2f}. Deals: {metrics['deal_count']}. "
                    f"Avg deal: {metrics['average_deal_value']:.2f}."
                )
                action_trace.append(trace_entry("openai_error", {"error": str(e)}))
        else:
            summary = (
                f"Total pipeline for {parsed.get('sector') or 'all sectors'} between "
                f"{metrics.get('timeframe_start') or 'start'} and {metrics.get('timeframe_end') or 'end'}: "
                f"{metrics['total_pipeline_value']:.2f}. Deals: {metrics['deal_count']}. "
                f"Open pipeline estimate: {metrics['open_pipeline_value_estimate']:.2f}."
            )
        caveats = []
        if deals_df.empty:
            caveats.append("No deals found on the Deals board; check board name and column mappings.")
        if (deals_df.applymap(lambda x: str(x).strip() == "" if not pd.isna(x) else True).sum().sum()) > 0:
            caveats.append("Some rows contain empty fields; values may be underreported.")
        if not caveats:
            caveats.append("Data cleaned and normalized; some free-text fields required heuristic parsing.")
        response = {
            "question": question,
            "parsed_query": parsed,
            "summary": summary,
            "metrics": metrics,
            "data_quality_caveats": caveats,
            "action_trace": action_trace
        }
        return jsonify(response)
    except Exception as e:
        action_trace.append(trace_entry("error", {"error": str(e)}))
        return jsonify({
            "error": "Failed to process query",
            "message": str(e),
            "action_trace": action_trace
        }), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3000, debug=True)
