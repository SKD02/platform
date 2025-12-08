import pandas as pd
import re
from decimal import Decimal, InvalidOperation
from typing import Any, List, Tuple, Dict
from datetime import datetime, date
from pathlib import Path
from functools import lru_cache
from typing import Dict
import pandas as pd

def get_path(d: dict, dotted: str, default=None):
    cur = d
    for p in dotted.split("."):
        if not isinstance(cur, dict):
            return default
        cur = cur.get(p)
        if cur is None:
            return default
    return cur

def get_any(d: dict, paths, default=""):
    candidates = []

    for p in paths:
        v = get_path(d, p, None)
        if v not in (None, "", "null", "None", "-", "—"):
            val = str(v).strip().upper()
            if val:
                candidates.append(val)

    if not candidates:
        return default.upper()

    def clean_length(s: str) -> int:
        cleaned = re.sub(r"[^A-Za-zА-Яа-яЁё0-9]", "", s)
        return len(cleaned)

    def relevance_score(x: str):
        return (
            clean_length(x),             
            -candidates.index(x)        
        )

    best = max(candidates, key=relevance_score)
    return best.strip().upper()


BASE_DIR = Path(__file__).resolve().parent
CLASSIF_DIR = BASE_DIR / "classifier"

COUNTRIES_CSV   = CLASSIF_DIR / "countries_classificator.csv"
UNITS_CSV       = CLASSIF_DIR / "15 — КЛАССИФИКАТОР ЕДИНИЦ ИЗМЕРЕНИЯ.csv"
INCOTERMS_CSV   = CLASSIF_DIR / "13 — КЛАССИФИКАТОР УСЛОВИЙ ПОСТАВКИ.csv"


@lru_cache()
def get_country_mapping() -> Dict[str, str]:
    csv_path = CLASSIF_DIR / "countries_classificator.csv"
    countries = pd.read_csv(csv_path)

    mapping: Dict[str, str] = {}
    for _, row in countries.iterrows():
        short = str(row["ShortName"]).strip()
        for col in ["Numeric", "ShortName", "FullName",
                    "Alpha2", "Alpha3", "ISO_Name", "ISO_ShortName"]:
            val = row.get(col)
            if pd.notna(val):
                key = str(val).strip().upper()
                if key:
                    mapping[key] = short
    return mapping

def normalize_country(country_str):
    if country_str is None or pd.isna(country_str):
        return ""

    country_str = str(country_str)
    raw_parts = []
    for part in country_str.split(","):
        for sub in part.split("/"):
            p = sub.strip()
            if p:
                raw_parts.append(p)

    parts_upper = [p.upper() for p in raw_parts]

    mapping = get_country_mapping()

    for p in parts_upper:
        if p in mapping:
            return mapping[p]

    return country_str.strip()


def get_country_code(name: str) -> str:
    countries = pd.read_csv(COUNTRIES_CSV)
    if not name or pd.isna(name):
        return ""
    
    name = str(name)
    parts = [p.strip().upper() for part in name.split(",") for p in part.split("/")]
    
    columns_to_check = ["ShortName", "Alpha2", "Alpha3", "ISO_Name", "ISO_ShortName"]
    mapping = {}
    for _, row in countries.iterrows():
        alpha2 = str(row["Alpha2"]).strip()
        for col in columns_to_check:
            val = row[col]
            if pd.notna(val):
                mapping[str(val).strip().upper()] = alpha2

    for p in parts:
        if p in mapping:
            return mapping[p]
    return ""

def get_country_name(code: str) -> str:
    if not code or pd.isna(code):
        return ""

    countries = pd.read_csv(COUNTRIES_CSV)

    code = str(code).strip().upper()
    for _, row in countries.iterrows():
        alpha2 = str(row.get("Alpha2", "")).strip().upper()
        alpha3 = str(row.get("Alpha3", "")).strip().upper()

        if code in (alpha2, alpha3):
            return str(row.get("ShortName", "")).strip()

    return ""

def extract_index(address: str) -> str:
    if not address:
        return ""
    patterns = [
        r"\b\d{6}\b",          
        r"\b\d{5}\b",          
        r"\b\d{3}-\d{3}\b",    
        r"\b\d{3} \d{2}\b",   
        r"\b\d{5}-\d{4}\b",   
    ]
    for pattern in patterns:
        match = re.search(pattern, address)
        if match:
            return match.group(0)
    return ""

def get_tnved(data: dict) -> int:
    goods = data.get("invoice", {}).get("Товары", [])
    if not isinstance(goods, list):
        return 0
    codes = {str(g.get("Код ТНВЭД")).strip() for g in goods if g.get("Код ТНВЭД")}
    if len(codes) == 0:
        return ""
    else:
        return codes

def get_total_places(data: dict) -> int:
    goods = data.get("packing", {}).get("Товары", [])
    if not isinstance(goods, list):
        return "Пустой"
    total = 0
    for g in goods:
        val = g.get("Количество мест")
        if val is None:
            continue
        s = str(val).strip()
        nums = re.findall(r"\d+", s)
        if nums:
            total += sum(int(n) for n in nums)
    return total

def get_product_country(data: dict) -> str:
    goods = data.get("invoice", {}).get("Товары", [])
    otpravitel_country = normalize_country(get_any(data, [
    "invoice.Отправитель.Страна",
    "contract.Общая информация.Стороны.Отправитель.Страна"]))

    if not isinstance(goods, list):
        return 0
    product_country = {str(g.get("Страна-производитель")).strip() for g in goods if g.get("Страна-производитель")}
    if len(product_country) == 0:
        return otpravitel_country
    else:
        return product_country

def get_unit_tnved(data: dict) -> tuple[dict, dict, dict]:
    units_df = pd.read_csv(UNITS_CSV, dtype=str).fillna("")
    code_to_short = {}
    variant_to_code = {}
    code_to_name = {str(row["Код"]).strip(): str(row["Наименование"]).strip()
                    for _, row in units_df.iterrows()
                    if "Код" in row and "Наименование" in row}

    for _, row in units_df.iterrows():
        code = str(row.get("Код", "")).strip()
        name = str(row.get("Наименование", "")).strip()
        short = str(row.get("Условное обозначение", "")).strip()

        if code:
            code_to_name[code] = name
            code_to_short[code] = short or name 
        for col in ["Наименование", "Условное обозначение", "Наименование_EN", "Сокращение_EN", "Дополнительно"]:
            val = row.get(col, "")
            if not val:
                continue
            for v in str(val).split(","):
                v_norm = str(v).strip().upper()
                if v_norm:
                    variant_to_code[v_norm] = code
    goods = (data or {}).get("invoice", {}).get("Товары", [])
    if isinstance(goods, dict):
        goods = [goods]
    if not isinstance(goods, list):
        return {}, {}, {}

    qty_map: dict[str, list[str]] = {}
    name_map: dict[str, set[str]] = {}
    code_map: dict[str, set[str]] = {}

    for g in goods:
        tnved = str(g.get("Код ТНВЭД")).strip()
        if not tnved:
            continue

        unit_raw = g.get("Единица измерения")
        qty_raw  = g.get("Количество")

        if unit_raw not in (None, ""):
            u_norm = str(unit_raw).strip().upper().replace("\u00A0", " ")
            u_norm_alt = u_norm.replace(".", "")
            u_code = (
                variant_to_code.get(u_norm)
                or variant_to_code.get(u_norm_alt)
                or ""
            )
            u_name = code_to_short.get(u_code, "")
        else:
            u_code, u_name = "", ""

        if not u_code or not u_name:
            u_code, u_name = "796", "ШТ"

        if qty_raw not in (None, ""):
            qty_map.setdefault(tnved, []).append(str(qty_raw).strip())

        if u_name:
            name_map.setdefault(tnved, set()).add(u_name)
        if u_code:
            code_map.setdefault(tnved, set()).add(u_code)
    qty_by_tnved = {}
    for k, vs in qty_map.items():
        total = 0
        for v in vs:
            try:
                total += int(str(v).replace(",", ".").replace(" ", ""))
            except ValueError:
                continue
        qty_by_tnved[k] = str(total) 
    unit_name_by_tnved  = {k: "; ".join(sorted(vs)) for k, vs in name_map.items()}
    unit_code_by_tnved  = {k: "; ".join(sorted(vs)) for k, vs in code_map.items()}

    return qty_by_tnved, unit_name_by_tnved, unit_code_by_tnved

def get_units_product(data: dict,units_csv_path: str = UNITS_CSV,joiner: str = "\n",) -> dict[str, str]:
    units_df = pd.read_csv(units_csv_path, dtype=str).fillna("")
    code_to_name = {}
    code_to_short = {}
    variant_to_code = {}

    for _, row in units_df.iterrows():
        code = str(row.get("Код", "")).strip()
        name = str(row.get("Наименование", "")).strip()
        short = str(row.get("Условное обозначение", "")).strip()

        if code:
            code_to_name[code] = name
            code_to_short[code] = short or name 

        for col in ("Наименование", "Условное обозначение", "Наименование_EN", "Сокращение_EN", "Дополнительно"):
            val = row.get(col, "")
            if not val:
                continue
            for v in str(val).split(","):
                v_norm = str(v).strip().upper().replace("\u00A0", " ")
                if v_norm:
                    variant_to_code[v_norm] = code

    inv = (data or {}).get("invoice", {}) or {}
    manufacturer = str(((inv.get("Отправитель") or {}).get("Название компании")) or "").strip()

    goods = inv.get("Товары", [])
    if isinstance(goods, dict):
        goods = [goods]
    if not isinstance(goods, list):
        return {}

    out: dict[str, list[str]] = {}

    for g in goods:
        code = str(g.get("Код ТНВЭД") or "").strip()
        if not code:
            continue

        article = str(g.get("Наименование") or g.get("Описание") or "").strip()
        qty = "" if g.get("Количество") in (None, "") else str(g.get("Количество")).strip()
        unit_raw = g.get("Единица измерения")
        if unit_raw not in (None, ""):
            u_norm = str(unit_raw).strip().upper().replace("\u00A0", " ")
            u_norm_alt = u_norm.replace(".", "")
            u_code = (
                variant_to_code.get(u_norm)
                or variant_to_code.get(u_norm_alt)
                or ""
            )
            u_name = code_to_short.get(u_code, "")
        else:
            u_code, u_name = "", ""
        if not u_code or not u_name:
            u_code, u_name = "796", "ШТ"

        line = (
            f"Производитель: {manufacturer} "
            f"Торг. знак, марка ОТСУТСТВУЕТ "
            f"Модель ОТСУТСТВУЕТ "
            f"Артикул {article} "
            f"Кол-во {qty} {u_name} ({u_code})"
        )

        out.setdefault(code, []).append(line)
    return {k: joiner.join(vs) for k, vs in out.items()}

def get_brutto_sum (data: dict) -> int:
    goods = data.get("packing", {}).get("Товары", [])
    if not isinstance(goods, list):
        return 0.0

    total = 0.0
    for g in goods:
        v = g.get("Масса брутто")
        if not v:
            continue
        s = str(v).replace("\u00A0", "").strip()
        if not re.fullmatch(r"[0-9.,\s]+", s):
            s = re.sub(r"[^0-9.,]", "", s)
        s = s.replace(" ", "").replace(",", ".")
        try:
            total += float(s)
        except ValueError:
            continue

    return round(total, 2)

def get_brutto (data: dict) -> int:
    codes_raw = get_tnved(data)
    if isinstance(codes_raw, set):
        tnved_set = {c for c in codes_raw if c}
    elif isinstance(codes_raw, str) and codes_raw:
        tnved_set = {codes_raw}
    else:
        tnved_set = set()

    if not tnved_set:
        return {}

    invoice_goods = (data.get("invoice", {}) or {}).get("Товары", []) or []
    packing_goods = (data.get("packing", {}) or {}).get("Товары", []) or []

    if len(tnved_set) == 1:
        only_code = next(iter(tnved_set))
        total = 0.0
        for g in packing_goods:
            v = g.get("Масса брутто")
            if v in (None, ""):
                continue
            s = str(v).replace("\u00A0", "").replace(" ", "").replace(",", ".")
            try:
                if not re.fullmatch(r"[0-9.]+", s):
                    s = re.sub(r"[^0-9.,]", "", s).replace(",", ".")
                total += float(s)
            except ValueError:
                pass
        return {only_code: total} if total else {only_code: 0.0}
    inv_rows = []
    for gi in invoice_goods:
        name_i = str(gi.get("Наименование") or gi.get("Описание") or "")
        nname_i = " ".join(name_i.lower().replace("\u00A0", " ").split())
        code_i = str(gi.get("Код ТНВЭД")).strip()
        if nname_i and code_i:
            inv_rows.append((nname_i, code_i))

    agg = {}
    for gp in packing_goods:
        name_p = str(gp.get("Наименование") or gp.get("Описание") or "")
        nname_p = " ".join(name_p.lower().replace("\u00A0", " ").split())

        v = gp.get("Масса брутто")
        s = "0" if v in (None, "") else str(v).replace("\u00A0", "").replace(" ", "").replace(",", ".")
        try:
            if not re.fullmatch(r"[0-9.]+", s):
                s = re.sub(r"[^0-9.,]", "", s).replace(",", ".")
            brutto = float(s)
        except ValueError:
            brutto = 0.0
        if brutto == 0.0:
            continue

        matched_code = None
        for n_i, code_i in inv_rows:
            if n_i in nname_p or nname_p in n_i:
                matched_code = code_i
                break

        if matched_code:
            agg[matched_code] = agg.get(matched_code, 0.0) + brutto
    rounded_agg = {}
    for code, value in agg.items():
        rounded_agg[code] = round(value, 3)
    
    return rounded_agg

def get_netto(data: dict) -> dict:
    codes_raw = get_tnved(data)
    if isinstance(codes_raw, set):
        tnved_set = {c for c in codes_raw if c}
    elif isinstance(codes_raw, str) and codes_raw:
        tnved_set = {codes_raw}
    else:
        tnved_set = set()

    if not tnved_set:
        return {}

    invoice_goods = (data.get("invoice", {}) or {}).get("Товары", []) or []
    packing_goods = (data.get("packing", {}) or {}).get("Товары", []) or []

    if len(tnved_set) == 1:
        only_code = next(iter(tnved_set))
        total = 0.0
        for g in packing_goods:
            v = g.get("Масса нетто")
            if v in (None, ""):
                continue
            s = str(v).replace("\u00A0", "").replace(" ", "").replace(",", ".")
            try:
                if not re.fullmatch(r"[0-9.]+", s):
                    s = re.sub(r"[^0-9.,]", "", s).replace(",", ".")
                total += float(s)
            except ValueError:
                pass
        total_rounded = round(total, 3) if total else 0.0
        return {only_code: total_rounded}

    inv_rows = []
    for gi in invoice_goods:
        name_i = str(gi.get("Наименование") or gi.get("Описание") or "")
        nname_i = " ".join(name_i.lower().replace("\u00A0", " ").split())
        code_i = str(gi.get("Код ТНВЭД")).strip()
        if nname_i and code_i:
            inv_rows.append((nname_i, code_i))

    agg = {}
    for gp in packing_goods:
        name_p = str(gp.get("Наименование") or gp.get("Описание") or "")
        nname_p = " ".join(name_p.lower().replace("\u00A0", " ").split())

        v = gp.get("Масса брутто")
        s = "0" if v in (None, "") else str(v).replace("\u00A0", "").replace(" ", "").replace(",", ".")
        try:
            brutto = float(s)
        except ValueError:
            brutto = 0.0
        if brutto == 0.0:
            continue

        matched_code = None
        for n_i, code_i in inv_rows:
            if n_i in nname_p or nname_p in n_i:
                matched_code = code_i
                break

        if matched_code:
            agg[matched_code] = agg.get(matched_code, 0.0) + brutto

    rounded_agg = {}
    for code, value in agg.items():
        rounded_agg[code] = round(value, 3)
    
    return rounded_agg


def get_seats (data: dict) -> int:
    codes_raw = get_tnved(data)
    if isinstance(codes_raw, set):
        tnved_set = {c for c in codes_raw if c}
    elif isinstance(codes_raw, str) and codes_raw:
        tnved_set = {codes_raw}
    else:
        tnved_set = set()

    if not tnved_set:
        return {}

    invoice_goods = (data.get("invoice", {}) or {}).get("Товары", []) or []
    packing_goods = (data.get("packing", {}) or {}).get("Товары", []) or []

    if len(tnved_set) == 1:
        only_code = next(iter(tnved_set))
        total = 0
        for g in packing_goods:
            v = g.get("Количество мест")
            if v in (None, ""):
                continue
            s = str(v).replace("\u00A0", "").replace(" ", "").replace(",", ".")
            try:
                if not re.fullmatch(r"[0-9.]+", s):
                    s = re.sub(r"[^0-9.,]", "", s).replace(",", ".")
                total += int(s)
            except ValueError:
                pass
        return {only_code: total} if total else {only_code: 0}

    inv_rows = []
    for gi in invoice_goods:
        name_i = str(gi.get("Наименование") or gi.get("Описание") or "")
        nname_i = " ".join(name_i.lower().replace("\u00A0", " ").split())
        code_i = str(gi.get("Код ТНВЭД")).strip()
        if nname_i and code_i:
            inv_rows.append((nname_i, code_i))

    agg = {}
    for gp in packing_goods:
        name_p = str(gp.get("Наименование") or gp.get("Описание") or "")
        nname_p = " ".join(name_p.lower().replace("\u00A0", " ").split())

        v = gp.get("Количество мест")
        s = "0" if v in (None, "") else str(v).replace("\u00A0", "").replace(" ", "").replace(",", ".")
        try:
            if not re.fullmatch(r"[0-9.]+", s):
                s = re.sub(r"[^0-9.,]", "", s).replace(",", ".")
            seats = int(s)
        except ValueError:
            seats = 0
        if seats == 0:
            continue

        matched_code = None
        for n_i, code_i in inv_rows:
            if n_i in nname_p or nname_p in n_i:
                matched_code = code_i
                break

        if matched_code:
            agg[matched_code] = agg.get(matched_code, 0) + seats
    return agg

def get_currency(data: dict) -> str:
    def _recurse(node):
        if isinstance(node, dict):
            if "Валюта" in node:
                return node["Валюта"]
            for v in node.values():
                res = _recurse(v)
                if res:
                    return res
        elif isinstance(node, list):
            for item in node:
                res = _recurse(item)
                if res:
                    return res
        return None

    return _recurse(data)

def _to_decimal(x) -> Decimal:
    if x is None:
        return Decimal("0")
    if isinstance(x, Decimal):
        return x
    try:
        s = str(x).strip().replace("\u00A0", "").replace(" ", "").replace(",", ".")
        if s == "":
            return Decimal("0")
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return Decimal("0")

def get_total_sum_invoice(data: Dict) -> Decimal: 
    goods_packing = data.get("packing", {}).get("Перевозка", {}).get("Товары", []) 
    goods_invoice = data.get("invoice", {}).get("Товары", []) 
    sum_pack = Decimal("0") 
    sum_inv = Decimal("0") 
    if isinstance(goods_packing, list): 
        for g in goods_packing: 
            if not isinstance(g, dict): 
                continue 
            price = _to_decimal(g.get("Цена")) 
            qty = _to_decimal(g.get("Количество")) 
            total = _to_decimal(g.get("Стоимость")) 
            if total > 0: 
                item_total = total 
            else: 
                item_total = price * qty 
            sum_pack += item_total 
    if isinstance(goods_invoice, list): 
        for g in goods_invoice: 
            if not isinstance(g, dict): 
                continue 
            price = _to_decimal(g.get("Цена")) 
            qty = _to_decimal(g.get("Количество")) 
            total = _to_decimal(g.get("Стоимость")) 
            if total > 0: 
                item_total = total 
            else: 
                item_total = price * qty 
            sum_inv += item_total 
    tol = Decimal("0.01") 
    if sum_inv == 0 and sum_pack == 0: 
        return Decimal("0") 
    if abs(sum_inv - sum_pack) <= tol:
        return sum_inv if sum_inv > 0 else sum_pack
    return sum_inv if sum_inv > 0 else sum_pack 


def get_total_sum_tnved(data: Dict) -> Decimal:
    goods_packing = (data.get("packing", {}) or {}).get("Перевозка", {}).get("Товары", []) or []
    goods_invoice = (data.get("invoice", {}) or {}).get("Товары", []) or []
    tol = Decimal("0.01")

    def _n(x) -> str:
        return " ".join(str(x or "").lower().replace("\u00A0", " ").split())

    inv_sum: Dict[str, Decimal] = {}
    inv_index = [] 
    for g in goods_invoice:
        if not isinstance(g, dict):
            continue
        code = str(g.get("Код ТНВЭД") or g.get("Код ТН ВЭД") or "").strip()
        if not code:
            continue
        name_i = _n(g.get("Наименование") or g.get("Описание"))
        inv_index.append((name_i, code))

        price = _to_decimal(g.get("Цена"))
        qty   = _to_decimal(g.get("Количество"))
        total = _to_decimal(g.get("Стоимость"))
        item_total = total if total > 0 else (price * qty)
        inv_sum[code] = inv_sum.get(code, Decimal("0")) + item_total

    pack_sum: Dict[str, Decimal] = {}
    for g in goods_packing:
        if not isinstance(g, dict):
            continue
        name_p = _n(g.get("Наименование") or g.get("Описание"))

        price = _to_decimal(g.get("Цена"))
        qty   = _to_decimal(g.get("Количество"))
        total = _to_decimal(g.get("Стоимость"))
        item_total = total if total > 0 else (price * qty)
        if item_total <= 0:
            continue

        matched_code = None
        for n_i, code in inv_index:
            if n_i and name_p and (n_i in name_p or name_p in n_i):
                matched_code = code
                break

        if matched_code:
            pack_sum[matched_code] = pack_sum.get(matched_code, Decimal("0")) + item_total

    result: Dict[str, Decimal] = {}
    all_codes = set(inv_sum.keys()) | set(pack_sum.keys())
    for code in all_codes:
        si = inv_sum.get(code, Decimal("0"))
        sp = pack_sum.get(code, Decimal("0"))
        if si == 0 and sp == 0:
            continue
        if abs(si - sp) <= tol:
            result[code] = si if si > 0 else sp
        else:
            result[code] = si if si > 0 else sp

    return result

def _norm_str(v):
    if v is None:
        return ""
    s = str(v).strip()
    s = s.replace("\n", " ").replace("\r", " ")
    s = " ".join(s.split())
    return s

def _is_eu_label(s: str) -> bool:
    if not s:
        return False
    up = s.upper()
    if any(tok in up for tok in ("ЕВРОСОЮЗ", "EUROPEAN UNION")):
        return True
    if up.strip() in ("EU", "ЕВРОСОЮЗ"):
        return True
    if "EURO" in up and not re.search(r"\b[A-Z]{2,}\b", up):
        return True
    return False

def _is_unknown(s: str) -> bool:
    if not s:
        return True
    up = s.upper().strip()
    return up in ("", "-", "N/A", "UNKNOWN", "НЕИЗВЕСТНО", "НЕИЗВЕСТНА", "NO DATA")

def collect_origin_values(data: dict) -> list:
    countries = pd.read_csv(COUNTRIES_CSV)
    valid_countries = set()
    for col in countries.columns:
        valid_countries.update(countries[col].astype(str).str.upper().str.strip().tolist())
    valid_countries = {c for c in valid_countries if c and c not in {"NONE", "NULL", "-", "—"}}

    vals = []
    inv_goods = data.get("invoice", {}).get("Товары", [])
    if isinstance(inv_goods, list):
        for g in inv_goods:
            if isinstance(g, dict):
                v = (
                    g.get("Страна-производитель")
                    or g.get("Страна происхождения")
                    or g.get("Страна-изготовитель")
                )
                if v is not None:
                    norm_v = _norm_str(v)
                    if norm_v.upper() in valid_countries:
                        vals.append(norm_v)
                    else:
                        vals.append("")

    pack_goods = data.get("packing", {}).get("Перевозка", {}).get("Товары", [])
    if isinstance(pack_goods, list):
        for g in pack_goods:
            if isinstance(g, dict):
                v = (
                    g.get("Страна-производитель")
                    or g.get("Страна происхождения")
                    or g.get("Страна-изготовитель")
                )
                if v is not None:
                    norm_v = _norm_str(v)
                    if norm_v.upper() in valid_countries:
                        vals.append(norm_v)
                    else:
                        vals.append("")

    return vals



def get_transport(data: dict) -> Tuple[int, str, str]:
    if not isinstance(data, dict):
        return 0, "", ""
    has_transport = any(isinstance(k, str) and k.startswith("transport") for k in data.keys())
    if not has_transport:
        return 0, "", ""

    if "transport_road" in data:
        road = data.get("transport_road", {})
        per = road.get("Перевозка", {})
        reg = per.get("Регистрационный номер", {})

        tyagach_vals = reg.get("Тягач", [])
        pricep_vals = reg.get("Прицеп", [])

        tyagach_pattern = re.compile(r"[A-Z]\d{3}[A-Z]{2}\d{2,3}", re.IGNORECASE)
        pricep_pattern = re.compile(r"[A-Z]{2}\d{6,7}", re.IGNORECASE)
        
        if not isinstance(tyagach_vals, list):
            tyagach_vals = [tyagach_vals] if tyagach_vals else []
        if not isinstance(pricep_vals, list):
            pricep_vals = [pricep_vals] if pricep_vals else []

        tyagach_clean = [str(x).strip().upper() for x in tyagach_vals if tyagach_pattern.fullmatch(str(x).strip().upper())]
        pricep_clean = [str(x).strip().upper() for x in pricep_vals if pricep_pattern.fullmatch(str(x).strip().upper())]

        tyagach_str = "; ".join(tyagach_clean)
        pricep_str = "; ".join(pricep_clean)
        count = len(tyagach_clean) + len(pricep_clean)

        if tyagach_str and pricep_str:
            return count, f"{tyagach_str}/{pricep_str}", "RU"
        elif tyagach_str:
            return count, tyagach_str, "RU"
        elif pricep_str:
            return count, pricep_str, "RU"
        else:
            return 0, "", ""
    
    if "transport_air" in data:
        air = data.get("transport_air") or {}
        shipping = air.get("Перевозка") or {}
        carrier  = shipping.get("Перевозчик") or {}

        reys = carrier.get("Номер рейса", "")
        if isinstance(reys, list):
            reys = next((v for v in reys if isinstance(v, str) and v.strip()), "")
        reys = "" if reys is None else str(reys)
        
        flight_pattern = re.compile(r"\b([A-ZА-Я0-9]{2,3})[-\s]?(\d{3,4})([A-ZА-Я])?\b", re.IGNORECASE)
        match = flight_pattern.fullmatch(reys)
        flight = ""
        if match:
            flight = match.group(0).upper()
        else:
            company = carrier.get("Авиакомпания") or carrier.get("Авиакомпания (Carrier)")
            company = "" if company is None else str(company).strip()
            combined = (company + reys).strip()
            match = flight_pattern.search(combined)
            if match:
                flight = match.group(0).upper()

        if flight:
            return 1, flight, ""
        else:
            return 0, "", ""

    if "transport_rail" in data:
        rail = data.get("transport_rail") or {}
        wagon_pat = re.compile(r"\b\d{10}\b")
        shipping = rail.get("Перевозка") or {}
        items    = rail.get("Товары") or []
        common   = rail.get("Общая информация") or {}

        priority = []
        for k in ("Вагон", "Номер вагона", "Номер вагона/тележки"):
            if isinstance(shipping, dict) and k in shipping:
                priority.append(shipping.get(k))
        if isinstance(items, list):
            for it in items:
                if isinstance(it, dict) and "Маркировка" in it:
                    priority.append(it.get("Маркировка"))
        for k in ("Номер вагона", "Вагон"):
            if isinstance(common, dict) and k in common:
                priority.append(common.get(k))

        for cand in priority:
            s = ""
            if isinstance(cand, list):
                s = next((v for v in cand if isinstance(v, str) and v.strip()), "; ".join(map(str, cand)))
            elif isinstance(cand, str):
                s = cand
            elif cand is not None:
                s = str(cand)
            mm = wagon_pat.search(s or "")
            if mm:
                return 1, mm.group(0), ""

        scopes = [shipping, common]
        if isinstance(items, list):
            scopes.extend(items)

        for scope in scopes:
            stack = [scope]
            while stack:
                node = stack.pop()
                if isinstance(node, dict):
                    stack.extend(node.values())
                elif isinstance(node, list):
                    stack.extend(node)
                elif isinstance(node, str):
                    mm = wagon_pat.search(node)
                    if mm:
                        return 1, mm.group(0), ""

        return 0, "", ""
    return 0, "", ""


def get_incoterms(incoterms_str):
    found_code = ""
    place = ""
    s = "" if incoterms_str is None else str(incoterms_str).strip()
    if s == "":
        return "", ""

    inc = pd.read_csv(INCOTERMS_CSV, dtype=str)
    codes = [str(x).strip().upper() for x in inc["Код условия поставки"].dropna().tolist()]

    up = s.upper()
    for code in codes:
        if not code:
            continue
        if re.search(rf"\b{re.escape(code)}\b", up):
            found_code = code
            place = re.sub(rf"\b{re.escape(code)}\b", "", s, flags=re.IGNORECASE)
            place = re.sub(r"^[\s,.;:–—\-]+", "", place).strip() 
            place = re.sub(r"[\s,.;:–—\-]+$", "", place).strip()   
            break
    if not found_code:
        place = s

    return found_code, place

def get_transport_type(data: dict, reys: str) -> str:
    if not isinstance(data, dict):
        return ""
    transport_type = next((k for k in data if k.startswith("transport_")), None)
    if transport_type == "transport_road":
        if "/" in reys and len(reys.split("/")) >= 3:
            return "32"
        if "/" in reys and len(reys.split("/")) == 2:
            return "31"

        else:
            return "31"
    if transport_type == "transport_air":
        return "40"

    if transport_type == "transport_sea":
        return "10"
    
    if transport_type == "transport_rail":
        return "20"

def get_all_docx(data: dict, g25_1: str) -> Dict[str, List[str]]:
    transport_map = {
        "40": "02017",  
        "50": "02019", 
        "10": "02011",  
        "20": "02013",  
        "30": "02015",  
        "31": "02015",
        "32": "02015",
        "71": "02018",  
        "72": "02018",  
        "80": "02012",  
    }

    default_name_by_mode = {
        "03011": "ДОГОВОР МЕЖДУНАРОДНОГО ДИСТРИБЬЮТЕРА",
        "04021": "СЧЕТ-ФАКТУРА (ИНВОЙС) К ДОГОВОРУ",
        "02015": "ТРАНСПОРТНАЯ НАКЛАДНАЯ (CMR)",
        "02017": "АВИАНАКЛАДНАЯ",
        "02013": "ЖЕЛЕЗНОДОРОЖНАЯ НАКЛАДНАЯ",
        "02011": "КОНОСАМЕНТ",
        "04031": "СЧЕТ ЗА ПЕРЕВОЗКУ",
    }

    def _is_empty(v: Any) -> bool:
        return v in (None, "", "null", "None", "-", "—")

    def first_non_empty(dct: dict, keys: List[str], default=""):
        if not isinstance(dct, dict):
            return default
        for k in keys:
            v = dct.get(k)
            if not _is_empty(v):
                return v
        return default

    def clean_number(num: Any) -> str:
        if _is_empty(num):
            return "БН"
        text = str(num).strip()
        matches = re.findall(r"[A-Za-zА-Яа-яЁё0-9/-]+", text)
        for m in matches:
            has_digit = re.search(r"\d", m)
            has_letter = re.search(r"[A-Za-zА-Яа-яЁё]", m)
            if has_digit and (has_letter or re.fullmatch(r"[0-9/-]+", m)):
                cleaned = m.strip("-/ ").strip()
                if re.search(r"\d", cleaned):
                    return cleaned
        return text

    def to_iso(val: Any) -> str:
        if _is_empty(val):
            return ""
        if isinstance(val, datetime):
            return val.date().isoformat()
        if isinstance(val, date):
            return val.isoformat()

        s = str(val).strip()
        m = re.match(r"^(\d{4}-\d{2}-\d{2})", s)
        if m:
            return m.group(1)
        m = re.match(r"^(\d{2})\.(\d{2})\.(\d{4})$", s)
        if m:
            dd, mm, yyyy = m.group(1), m.group(2), m.group(3)
            return f"{yyyy}-{mm}-{dd}"
        m = re.match(r"^(\d{2})/(\d{2})/(\d{4})$", s)
        if m:
            dd, mm, yyyy = m.group(1), m.group(2), m.group(3)
            return f"{yyyy}-{mm}-{dd}"
        return s

    out = {
        "mode_codes": [],
        "kind_codes": [],
        "names": [],
        "numbers": [],
        "dates_iso": [],
        "begin_iso": [],
        "end_iso": [],
        "record_ids": [],
    }

    def add_doc(mode_code: str, number: Any, date: Any, name: str = ""):
        mode_code = str(mode_code or "").strip()
        if not mode_code:
            return

        num = clean_number(number)
        dt_iso = to_iso(date)
        nm = (str(name).strip() if not _is_empty(name) else "") or default_name_by_mode.get(mode_code, "")

        out["mode_codes"].append(mode_code)
        out["kind_codes"].append("0")
        out["names"].append(nm.strip())
        out["numbers"].append(num)
        out["dates_iso"].append(dt_iso)

    c_info = (data.get("contract") or {}).get("Общая информация", {}) or {}
    c_num = first_non_empty(c_info, ["Номер контракта"])
    c_dt  = first_non_empty(c_info, ["Дата заключения"])
    c_nm  = first_non_empty(c_info, ["Наименование документа"])
    add_doc("03011", c_num, c_dt, c_nm)

    inv_info = (data.get("invoice") or {}).get("Общая информация", {}) or {}
    i_num = first_non_empty(inv_info, ["Номер инвойса"])
    i_dt  = first_non_empty(inv_info, ["Дата инвойса"])
    i_nm  = first_non_empty(inv_info, ["Наименование документа"])
    add_doc("04021", i_num, i_dt, i_nm)

    payment_info = (data.get("payment") or {}).get("Общая информация", {}) or {}
    payment_num = first_non_empty(payment_info, ["Номер счета"])
    payment_dt  = first_non_empty(payment_info, ["Дата счета"])
    payment_nm  = first_non_empty(payment_info, ["Наименование документа"])
    add_doc("04031", payment_num, payment_dt, payment_nm)

    t_code = transport_map.get(str(g25_1).strip(), "")
    if t_code:
        candidates = []
        g25 = str(g25_1).strip()

        if g25 in ("30", "31", "32"):
            candidates += ["transport_road", "transport_auto", "transport"]
        elif g25 == "40":
            candidates += ["transport_air", "transport"]
        elif g25 == "20":
            candidates += ["transport_rail", "transport"]
        elif g25 in ("10", "80"):
            candidates += ["transport_sea", "transport_river", "transport_water", "transport"]
        elif g25 == "50":
            candidates += ["transport_post", "transport_mail", "transport"]
        else:
            candidates += ["transport"]

        for k in list(data.keys()):
            if isinstance(k, str) and k.startswith("transport_") and k not in candidates:
                candidates.append(k)

        tr_block = None
        for key in candidates:
            obj = data.get(key)
            if isinstance(obj, dict) and isinstance(obj.get("Общая информация"), dict):
                tr_block = obj
                break

        tr_info = (tr_block or {}).get("Общая информация", {}) or {}
        t_num = first_non_empty(
            tr_info,
            [
                "Номер накладной",
                "Номер транспортного документа",
                "Номер документа",
                "Номер CMR",
                "Номер авианакладной",
                "Номер ж/д накладной",
                "Номер коносамента",
                "B/L No",
                "AWB No",
            ],
        )
        t_dt = first_non_empty(
            tr_info,
            [
                "Дата накладной",
                "Дата транспортного документа",
                "Дата документа",
                "Дата CMR",
                "Дата авианакладной",
                "Дата ж/д накладной",
                "Дата коносамента",
            ],
        )
        t_nm = first_non_empty(tr_info, ["Наименование документа", "Название документа", "Тип документа"])
        add_doc(t_code, t_num, t_dt, t_nm)

    return out

