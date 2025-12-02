import pandas as pd
from db import get_declaration_date
import re
from decimal import Decimal, InvalidOperation
import streamlit as st
from typing import Any, List, Tuple, Dict
from datetime import datetime, date
import re, difflib


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

    def is_cyrillic(s: str) -> bool:
        """Проверка, содержит ли строка кириллицу"""
        return bool(re.search(r"[А-Яа-яЁё]", s))

    # ===== критерий релевантности =====
    # 1) язык (русский выше латиницы)
    # 2) длина строки (чем длиннее, тем лучше)
    # 3) если равные — первый по порядку
    def clean_length(s: str) -> int:
        """Считает длину по буквам и цифрам (без кавычек, запятых, пробелов и т.п.)"""
        # удаляем всё, кроме букв и цифр
        cleaned = re.sub(r"[^A-Za-zА-Яа-яЁё0-9]", "", s)
        return len(cleaned)

    # ===== критерий релевантности =====
    # 1) язык (русский выше латиницы)
    # 2) длина строки (по буквам/цифрам)
    # 3) первый по порядку (если всё одинаково)
    def relevance_score(x: str):
        return (
            #1 if is_cyrillic(x) else 0,   # приоритет русского
            clean_length(x),              # чистая длина без знаков
            -candidates.index(x)           # первый по порядку (раньше — выше)
        )

    best = max(candidates, key=relevance_score)
    return best.strip().upper()


# Базовая папка для этого модуля (например, back/graph.py)
BASE_DIR = Path(__file__).resolve().parent
CLASSIF_DIR = BASE_DIR / "classifier"

@lru_cache()
def get_country_mapping() -> Dict[str, str]:
    """
    Загружаем классификатор стран один раз и строим словарь:
    все возможные варианты -> ShortName.
    """
    csv_path = CLASSIF_DIR / "countries_classificator.csv"
    # при необходимости добавь encoding="utf-8-sig"
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
    """
    Нормализуем страну:
      - разбиваем строку по ',' и '/'
      - приводим к верхнему регистру
      - ищем в mapping; если нашли — возвращаем ShortName
      - иначе возвращаем исходную строку без пробелов по краям
    """
    if country_str is None or pd.isna(country_str):
        return ""

    country_str = str(country_str)

    # разбиваем по возможным разделителям: ',' и '/'
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

    # если ничего не нашли — возвращаем оригинал (без пробелов)
    return country_str.strip()


def get_country_code(name: str) -> str:
    countries = pd.read_csv(r"C:\Users\sidor\Desktop\streamlit\countries_classificator.csv")
    if not name or pd.isna(name):
        return ""
    
    name = str(name)
    
    # Разбиваем по возможным разделителям (',', '/')
    parts = [p.strip().upper() for part in name.split(",") for p in part.split("/")]
    
    columns_to_check = ["ShortName", "Alpha2", "Alpha3", "ISO_Name", "ISO_ShortName"]
    
    # Создаем словарь: любое значение -> Alpha2
    mapping = {}
    for _, row in countries.iterrows():
        alpha2 = str(row["Alpha2"]).strip()
        for col in columns_to_check:
            val = row[col]
            if pd.notna(val):
                mapping[str(val).strip().upper()] = alpha2
    
    # Ищем первое совпадение и возвращаем Alpha2
    for p in parts:
        if p in mapping:
            return mapping[p]
    return ""

def get_country_name(code: str) -> str:
    """
    Возвращает ShortName страны по её коду (Alpha2 или Alpha3).
    Пример: 'RU' → 'Россия'
    """
    if not code or pd.isna(code):
        return ""

    # читаем классификатор
    countries = pd.read_csv(r"C:\Users\sidor\Desktop\streamlit\countries_classificator.csv")

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
        r"\b\d{6}\b",          # 6 знаков
        r"\b\d{5}\b",          # 5-знаков
        r"\b\d{3}-\d{3}\b",    # 6 знаков через дефис
        r"\b\d{3} \d{2}\b",    # 5 знаков через пробел
        r"\b\d{5}-\d{4}\b",    # 5 знаков - 4 знака
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
        #st.info("Не указаны коды ТН ВЭД")
        return ""
    else:
        return codes

def get_total_places(data: dict) -> int:
    """Суммирует все численные значения из 'Количество мест' в packing.Перевозка.Товары"""
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
            try:
                total += sum(int(n) for n in nums)
            except Exception:
                st.warning(f"Не удалось преобразовать: {s}")
        else:
            st.warning(f"Нет числа в значении 'Количество мест': {s}")

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
        #st.info("В качестве страны-производителя указана страна отправителя")
        return otpravitel_country
    else:
        return product_country

def get_unit_tnved(data: dict) -> tuple[dict, dict, dict]:
    # 1) Классификатор ЕИ
    units_df = pd.read_csv(r"C:\Users\sidor\Desktop\streamlit\alta_classifiers\15 — КЛАССИФИКАТОР ЕДИНИЦ ИЗМЕРЕНИЯ.csv",dtype=str).fillna("")
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
 
    # 2) Товары из invoice
    goods = (data or {}).get("invoice", {}).get("Товары", [])
    if isinstance(goods, dict):
        goods = [goods]
    if not isinstance(goods, list):
        return {}, {}, {}

    # 3) Агрегация по коду ТНВЭД
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
            u_name = code_to_short.get(u_code, "")  # теперь используем сокращённое обозначение
        else:
            u_code, u_name = "", ""

        if not u_code or not u_name:
            u_code, u_name = "796", "ШТ"

        # if not unit_raw:
        #     # если ЕИ нет — всё равно сохраним qty, но без ЕИ
        #     if qty_raw not in (None, ""):
        #         qty_map.setdefault(tnved, []).append(str(qty_raw).strip())
        #     name_map.setdefault(tnved, set()).add("ШТ")
        #     code_map.setdefault(tnved, set()).add("796")
        #     continue

        # u_norm = str(unit_raw).strip().upper()
        # u_code = variant_to_code.get(u_norm, "")
        # u_name = code_to_name.get(u_code, "")

        if qty_raw not in (None, ""):
            qty_map.setdefault(tnved, []).append(str(qty_raw).strip())

        if u_name:
            name_map.setdefault(tnved, set()).add(u_name)
        if u_code:
            code_map.setdefault(tnved, set()).add(u_code)

    # 4) Приводим к «плоским» строкам
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

def get_units_product(data: dict,units_csv_path: str = r"C:\Users\sidor\Desktop\streamlit\alta_classifiers\15 — КЛАССИФИКАТОР ЕДИНИЦ ИЗМЕРЕНИЯ.csv",joiner: str = "\n",) -> dict[str, str]:
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
            code_to_short[code] = short or name  # если нет обозначения, берём полное имя

        # индексы вариантов, по которым будем искать код
        for col in ("Наименование", "Условное обозначение", "Наименование_EN", "Сокращение_EN", "Дополнительно"):
            val = row.get(col, "")
            if not val:
                continue
            for v in str(val).split(","):
                v_norm = str(v).strip().upper().replace("\u00A0", " ")
                if v_norm:
                    variant_to_code[v_norm] = code

    # === 2) Источник данных ===
    inv = (data or {}).get("invoice", {}) or {}
    manufacturer = str(((inv.get("Отправитель") or {}).get("Название компании")) or "").strip()

    goods = inv.get("Товары", [])
    if isinstance(goods, dict):
        goods = [goods]
    if not isinstance(goods, list):
        return {}

    # === 3) Формируем строки по каждому товару и группируем по коду ТНВЭД ===
    out: dict[str, list[str]] = {}

    for g in goods:
        code = str(g.get("Код ТНВЭД") or "").strip()
        if not code:
            continue

        article = str(g.get("Наименование") or g.get("Описание") or "").strip()
        qty = "" if g.get("Количество") in (None, "") else str(g.get("Количество")).strip()

        # определяем ЕИ (имя и код)
        unit_raw = g.get("Единица измерения")
        if unit_raw not in (None, ""):
            u_norm = str(unit_raw).strip().upper().replace("\u00A0", " ")
            u_norm_alt = u_norm.replace(".", "")
            u_code = (
                variant_to_code.get(u_norm)
                or variant_to_code.get(u_norm_alt)
                or ""
            )
            u_name = code_to_short.get(u_code, "")  # теперь используем сокращённое обозначение
        else:
            u_code, u_name = "", ""

        # дефолты, если не нашли
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

    # === 4) Склейка строк по коду ===
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
        # оставляем только цифры, запятые и точки
        if not re.fullmatch(r"[0-9.,\s]+", s):
            s = re.sub(r"[^0-9.,]", "", s)
        s = s.replace(" ", "").replace(",", ".")
        try:
            total += float(s)
        except ValueError:
            continue

    return round(total, 2)

def get_brutto (data: dict) -> int:
    # 0) взять уникальные коды из invoice через твою функцию
    codes_raw = get_tnved(data)
    if isinstance(codes_raw, set):
        tnved_set = {c for c in codes_raw if c}
    elif isinstance(codes_raw, str) and codes_raw:
        tnved_set = {codes_raw}
    else:
        tnved_set = set()

    # если кодов нет — нечего агрегировать
    if not tnved_set:
        return {}

    invoice_goods = (data.get("invoice", {}) or {}).get("Товары", []) or []
    packing_goods = (data.get("packing", {}) or {}).get("Товары", []) or []

    # 1) если код ровно один — просто суммируем все брутто
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

    # 2) кодов несколько — посимвольное сопоставление по названию
    # подготовим нормализованные имена invoice -> код
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
        # ищем вхождение в обе стороны
        for n_i, code_i in inv_rows:
            if n_i in nname_p or nname_p in n_i:
                matched_code = code_i
                break

        if matched_code:
            agg[matched_code] = agg.get(matched_code, 0.0) + brutto
        # если не нашли — пропускаем (по твоей логике максимально просто)
    rounded_agg = {}
    for code, value in agg.items():
        rounded_agg[code] = round(value, 3)
    
    return rounded_agg


# def get_netto (data: dict) -> int:
#     goods = data.get("packing", {}).get("Перевозка", {}).get("Товары", [])
#     if not isinstance(goods, list):
#         return 0
#     netto = {str(g.get("Масса нетто")).strip() for g in goods if g.get("Масса нетто")}
#     if len(netto) == 0:
#         return ""
#     else:
#         return netto

# def get_netto (data: dict) -> int:
#         # 0) взять уникальные коды из invoice через твою функцию
#     codes_raw = get_tnved(data)
#     if isinstance(codes_raw, set):
#         tnved_set = {c for c in codes_raw if c}
#     elif isinstance(codes_raw, str) and codes_raw:
#         tnved_set = {codes_raw}
#     else:
#         tnved_set = set()

#     # если кодов нет — нечего агрегировать
#     if not tnved_set:
#         return {}

#     invoice_goods = (data.get("invoice", {}) or {}).get("Товары", []) or []
#     packing_goods = (data.get("packing", {}) or {}).get("Товары", []) or []

#     # 1) если код ровно один — просто суммируем все брутто
#     if len(tnved_set) == 1:
#         only_code = next(iter(tnved_set))
#         total = 0.0
#         for g in packing_goods:
#             v = g.get("Масса нетто")
#             if v in (None, ""):
#                 continue
#             s = str(v).replace("\u00A0", "").replace(" ", "").replace(",", ".")
#             try:
#                 if not re.fullmatch(r"[0-9.]+", s):
#                     s = re.sub(r"[^0-9.,]", "", s).replace(",", ".")
#                 total += float(s)
#             except ValueError:
#                 pass
#         return {only_code: total} if total else {only_code: 0.0}

#     # 2) кодов несколько — посимвольное сопоставление по названию
#     # подготовим нормализованные имена invoice -> код
#     inv_rows = []
#     for gi in invoice_goods:
#         name_i = str(gi.get("Наименование") or gi.get("Описание") or "")
#         nname_i = " ".join(name_i.lower().replace("\u00A0", " ").split())
#         code_i = str(gi.get("Код ТНВЭД")).strip()
#         if nname_i and code_i:
#             inv_rows.append((nname_i, code_i))

#     agg = {}
#     for gp in packing_goods:
#         name_p = str(gp.get("Наименование") or gp.get("Описание") or "")
#         nname_p = " ".join(name_p.lower().replace("\u00A0", " ").split())

#         v = gp.get("Масса брутто")
#         s = "0" if v in (None, "") else str(v).replace("\u00A0", "").replace(" ", "").replace(",", ".")
#         try:
#             brutto = float(s)
#         except ValueError:
#             brutto = 0.0
#         if brutto == 0.0:
#             continue

#         matched_code = None
#         # ищем вхождение в обе стороны
#         for n_i, code_i in inv_rows:
#             if n_i in nname_p or nname_p in n_i:
#                 matched_code = code_i
#                 break

#         if matched_code:
#             agg[matched_code] = agg.get(matched_code, 0.0) + brutto
#         # если не нашли — пропускаем (по твоей логике максимально просто)

#     return agg

def get_netto(data: dict) -> dict:
    # 0) взять уникальные коды из invoice через твою функцию
    codes_raw = get_tnved(data)
    if isinstance(codes_raw, set):
        tnved_set = {c for c in codes_raw if c}
    elif isinstance(codes_raw, str) and codes_raw:
        tnved_set = {codes_raw}
    else:
        tnved_set = set()

    # если кодов нет — нечего агрегировать
    if not tnved_set:
        return {}

    invoice_goods = (data.get("invoice", {}) or {}).get("Товары", []) or []
    packing_goods = (data.get("packing", {}) or {}).get("Товары", []) or []

    # 1) если код ровно один — просто суммируем все брутто
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
        # Округляем результат до 3 знаков
        total_rounded = round(total, 3) if total else 0.0
        return {only_code: total_rounded}

    # 2) кодов несколько — посимвольное сопоставление по названию
    # подготовим нормализованные имена invoice -> код
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
        # ищем вхождение в обе стороны
        for n_i, code_i in inv_rows:
            if n_i in nname_p or nname_p in n_i:
                matched_code = code_i
                break

        if matched_code:
            agg[matched_code] = agg.get(matched_code, 0.0) + brutto
        # если не нашли — пропускаем (по твоей логике максимально просто)

    # Округляем все значения в словаре до 3 знаков
    rounded_agg = {}
    for code, value in agg.items():
        rounded_agg[code] = round(value, 3)
    
    return rounded_agg


def get_seats (data: dict) -> int:
    # 0) взять уникальные коды из invoice через твою функцию
    codes_raw = get_tnved(data)
    if isinstance(codes_raw, set):
        tnved_set = {c for c in codes_raw if c}
    elif isinstance(codes_raw, str) and codes_raw:
        tnved_set = {codes_raw}
    else:
        tnved_set = set()

    # если кодов нет — нечего агрегировать
    if not tnved_set:
        return {}

    invoice_goods = (data.get("invoice", {}) or {}).get("Товары", []) or []
    packing_goods = (data.get("packing", {}) or {}).get("Товары", []) or []

    # 1) если код ровно один — просто суммируем все брутто
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

    # 2) кодов несколько — посимвольное сопоставление по названию
    # подготовим нормализованные имена invoice -> код
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
        # ищем вхождение в обе стороны
        for n_i, code_i in inv_rows:
            if n_i in nname_p or nname_p in n_i:
                matched_code = code_i
                break

        if matched_code:
            agg[matched_code] = agg.get(matched_code, 0) + seats
        # если не нашли — пропускаем (по твоей логике максимально просто)

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
    """Привести любое значение к Decimal, безопасно."""
    if x is None:
        return Decimal("0")
    if isinstance(x, Decimal):
        return x
    try:
        # строки: убрать пробелы/nbsp, заменить запятую на точку
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

    # Нормализация имени (нижний регистр, схлопывание пробелов/nbsp)
    def _n(x) -> str:
        return " ".join(str(x or "").lower().replace("\u00A0", " ").split())

    # --- 1) Суммы по invoice: {код -> сумма}
    inv_sum: Dict[str, Decimal] = {}
    inv_index = []  # [(name_norm, code)]
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

    # --- 2) Суммы по packing (через матчинг по имени к invoice): {код -> сумма}
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
        # простое посимвольное сопоставление по вхождению (в обе стороны)
        for n_i, code in inv_index:
            if n_i and name_p and (n_i in name_p or name_p in n_i):
                matched_code = code
                break

        if matched_code:
            pack_sum[matched_code] = pack_sum.get(matched_code, Decimal("0")) + item_total

    # --- 3) Итог по каждому коду
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
# === Графа 16: определяем страну происхождения по Страна-производитель ===

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
    # если явно написано EUROPEAN UNION / ЕВРОСОЮЗ или встречается "EU" отдельно
    if any(tok in up for tok in ("ЕВРОСОЮЗ", "EUROPEAN UNION")):
        return True
    # допускаем краткие "EU" и похожие метки, но экранируем случаи типа "GERMANY, EU" — всё равно трактуем как EU-label только если строка явно про объединение
    if up.strip() in ("EU", "ЕВРОСОЮЗ"):
        return True
    # также если в строке встречается слово "EURO" и нет явного имени страны — считаем EU
    if "EURO" in up and not re.search(r"\b[A-Z]{2,}\b", up):
        return True
    return False

def _is_unknown(s: str) -> bool:
    if not s:
        return True
    up = s.upper().strip()
    return up in ("", "-", "N/A", "UNKNOWN", "НЕИЗВЕСТНО", "НЕИЗВЕСТНА", "NO DATA")

def collect_origin_values(data: dict) -> list:
    """Собрать все значения 'Страна-производитель' из invoice.Товары и packing.Перевозка.Товары."""
    countries = pd.read_csv(r"C:\Users\sidor\Desktop\streamlit\countries_classificator.csv")
    # допустимые варианты (берём все колонки: Код, Alpha2, Alpha3, Name и т.п.)
    valid_countries = set()
    for col in countries.columns:
        valid_countries.update(countries[col].astype(str).str.upper().str.strip().tolist())
    valid_countries = {c for c in valid_countries if c and c not in {"NONE", "NULL", "-", "—"}}

    vals = []

    # invoice.Товары
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
                        vals.append("")  # если страна не найдена

    # packing.Перевозка.Товары
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

def _norm_reg_number(s: str) -> str:
    """Нормализовать регистрационный номер: убрать пробелы, привести к единому виду."""
    if s is None:
        return ""
    s = str(s)
    # убрать новые строки и лишние пробелы
    s = s.replace("\n", " ").replace("\r", " ")
    s = " ".join(s.split())
    # удалить пробелы внутри номера (в примерах номера идут без пробелов)
    s = s.replace(" ", "")
    return s.strip()

def _ensure_list(x: Any) -> List:
    """Если x - строка/None -> [x] или пустой список; если уже list -> return as is."""
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]

def _to_str_list(maybe_list) -> List[str]:
    """Преобразовать возможную строку/список в список нормализованных строк (удаляем пустые)."""
    out = []
    for item in _ensure_list(maybe_list):
        if item is None:
            continue
        s = _norm_reg_number(item)
        if s != "":
            out.append(s)
    return out

# def format_transport_registration(reg_info: Any) -> Tuple[int, str]:
#     """
#     Возвращает (count, formatted_string)
#     - count = количество уникальных транспортных средств (тягачей + прицепов)
#     - formatted_string = "count:entries" где entries разделяются ';', внутри пары используется '/'
#     Поддерживаем dict {"Тягач":..., "Прицеп":...}, list или строку.
#     """
#     if reg_info is None:
#         return 0, ""

#     # Словарь: отдельный путь обработки
#     if isinstance(reg_info, dict):
#         tractors = _to_str_list(
#             reg_info.get("Тягач") 
#         )
#         trailers = _to_str_list(
#             reg_info.get("Прицеп")
#         )

#         # Формируем элементы списка для вывода: парные элементы если возможно
#         entries = []
#         maxlen = max(len(tractors), len(trailers))
#         for i in range(maxlen):
#             t = tractors[i] if i < len(tractors) else ""
#             tr = trailers[i] if i < len(trailers) else ""
#             if t and tr:
#                 entries.append(f"{t}/{tr}")
#             elif t:
#                 entries.append(t)
#             elif tr:
#                 entries.append(tr)

#         # подсчёт: считаем все уникальные номера тракторов + прицепов
#         unique_vehicles = set(tractors + trailers)
#         # удаляем пустую строку если есть
#         unique_vehicles.discard("")
#         count = len(unique_vehicles)

#         if not entries:
#             return 0, ""
#         joined = ";".join(entries)
#         return count, f"{count}:{joined}"

#     # Если список или строка — трактуем как набор одиночных ТС (каждый отдельный)
#     if isinstance(reg_info, list):
#         items = _to_str_list(reg_info)
#         unique = set(items)
#         unique.discard("")
#         if not items:
#             return 0, ""
#         count = len(unique)
#         return count, f"{count}:{';'.join(items)}"

    # # строка/число
    # s = _norm_reg_number(reg_info)
    # if s == "":
    #     return 0, ""
    # return 1, f"1:{s}"


import re
from typing import Tuple

def get_transport(data: dict) -> Tuple[int, str, str]:
    """
    Строго по JSON-схеме:
      - transport_road: 'Регистрационный номер' → 'Тягач'/'Прицеп' → 'Тягач/Прицеп'
      - transport_air : 'Перевозка' → 'Перевозчик' → 'Номер рейса' (паттерн 2 символа + 4 цифры), при отсутствии — поиск ТОЛЬКО внутри ветки transport_air
      - transport_rail: ищем 10-значный номер вагона ТОЛЬКО в 'Перевозка', 'Товары', 'Общая информация' (приоритет — явные поля)
    Возвращает: (found_flag: 0|1, value: str, "")
    """
    if not isinstance(data, dict):
        return 0, "", ""

    # есть ли вообще transport-ключи
    has_transport = any(isinstance(k, str) and k.startswith("transport") for k in data.keys())
    if not has_transport:
        return 0, "", ""

    # -------------------- ROAD --------------------
    if "transport_road" in data:
        road = data.get("transport_road", {})
        per = road.get("Перевозка", {})
        reg = per.get("Регистрационный номер", {})

        tyagach_vals = reg.get("Тягач", [])
        pricep_vals = reg.get("Прицеп", [])

        tyagach_pattern = re.compile(r"[A-Z]\d{3}[A-Z]{2}\d{2,3}", re.IGNORECASE)
        pricep_pattern = re.compile(r"[A-Z]{2}\d{6,7}", re.IGNORECASE)
        
        # приведение к спискам
        if not isinstance(tyagach_vals, list):
            tyagach_vals = [tyagach_vals] if tyagach_vals else []
        if not isinstance(pricep_vals, list):
            pricep_vals = [pricep_vals] if pricep_vals else []

        # очистка и фильтрация строк
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
    
    # -------------------- AIR --------------------
    if "transport_air" in data:
        air = data.get("transport_air") or {}
        shipping = air.get("Перевозка") or {}
        carrier  = shipping.get("Перевозчик") or {}

        reys = carrier.get("Номер рейса", "")
        if isinstance(reys, list):
            reys = next((v for v in reys if isinstance(v, str) and v.strip()), "")
        reys = "" if reys is None else str(reys)
        
        #flight_pattern = re.compile(r"\b([A-ZА-Я0-9]{2})[-\s]?(\d{4})\b", re.IGNORECASE)
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


    # -------------------- RAIL --------------------
    if "transport_rail" in data:
        rail = data.get("transport_rail") or {}
        wagon_pat = re.compile(r"\b\d{10}\b")

        # допустимые узлы по схеме
        shipping = rail.get("Перевозка") or {}
        items    = rail.get("Товары") or []
        common   = rail.get("Общая информация") or {}

        # приоритетные кандидаты (явные поля)
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

        # проверяем приоритетные
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

        # затем полный поиск ТОЛЬКО в допустимых узлах
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

    # если это другой transport_* — по минималке ничего не извлекаем
    return 0, "", ""


def get_incoterms(incoterms_str):
    # дефолты
    found_code = ""
    place = ""

    # нормализация входа
    s = "" if incoterms_str is None else str(incoterms_str).strip()
    if s == "":
        return "", ""

    # читаем справочник кодов
    inc = pd.read_csv(r"C:\Users\sidor\Desktop\streamlit\alta_classifiers\13 — КЛАССИФИКАТОР УСЛОВИЙ ПОСТАВКИ.csv", dtype=str)
    codes = [str(x).strip().upper() for x in inc["Код условия поставки"].dropna().tolist()]

    up = s.upper()

    # ищем первый встреченный код (полное совпадение по слову)
    for code in codes:
        if not code:
            continue
        if re.search(rf"\b{re.escape(code)}\b", up):
            found_code = code
            # вырезаем код из оригинальной строки и чистим разделители
            place = re.sub(rf"\b{re.escape(code)}\b", "", s, flags=re.IGNORECASE)
            place = re.sub(r"^[\s,.;:–—\-]+", "", place).strip()   # убрать лидирующие разделители
            place = re.sub(r"[\s,.;:–—\-]+$", "", place).strip()   # и хвостовые
            break

    # если код не нашли — пусть place будет исходной строкой (для ручной правки)
    if not found_code:
        place = s

    return found_code, place

# def nature_transaction(data: dict):
#     summa_contract = get_any(data, ["contract.Оплата контракта.Общая сумма"])
#     rate = cb_rate(date_declaration, get_currency(data)) 

#     try:
#         summa_contract = Decimal(summa_contract)
#     except (ValueError, TypeError, InvalidOperation):
#         clean = re.sub(r"[^\d,\.]", "", str(summa_contract))
#         clean = clean.replace(".", "")
#         clean = clean.replace(",", ".")
#         summa_contract = Decimal(clean) if clean else 0

#     summa_contract = summa_contract * rate
#     summa_contract = summa_contract.quantize(Decimal("0.01"))

#     return "06" if summa_contract < 3000000 else 00

def get_transport_type(data: dict, reys: str) -> str:
    if not isinstance(data, dict):
        return ""
    transport_type = next((k for k in data if k.startswith("transport_")), None)
    if transport_type == "transport_road":
        # 32 — Состав транспортных средств (3+ номеров через /)
        if "/" in reys and len(reys.split("/")) >= 3:
            return "32"

        # 31 — Состав транспортных средств (2 номера через /)
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

# def get_all_docx(data: dict, g25_1: str):
#     transport_map = {
#             "40": "02017",  # авиа
#             "50": "02019",  # почта
#             "10": "02011",  # морской/речной
#             "20": "02013",  # ж/д
#             "30": "02015",  # авто (CMR)
#             "31": "02015",
#             "32": "02015",
#             "71": "02018",  # трубопровод
#             "72": "02018",  # ЛЭП
#             "80": "02012",  # внутренний водный
#         }
#     def clean_number(num: str) -> str:
#         if not num:
#             return ""

#         text = str(num)
#         # Находим кандидаты: буквы, цифры, дефисы, слеши
#         matches = re.findall(r"[A-Za-zА-Яа-яЁё0-9/-]+", text)

#         for m in matches:
#             # Требуется хотя бы одна цифра
#             has_digit = re.search(r"\d", m)
#             # ИЛИ комбинация буква+цифра, ИЛИ просто цифры
#             has_letter = re.search(r"[A-Za-zА-Яа-яЁё]", m)

#             if has_digit and (has_letter or re.fullmatch(r"[0-9/-]+", m)):
#                 # убираем ведущие и лишние дефисы/слеши
#                 cleaned = m.strip("-/ ").strip()
#                 # не должен быть пустым или состоять только из разделителей
#                 if re.search(r"\d", cleaned):
#                     return cleaned

#         return ""

#     def make_line(code: str, num: str, date: str) -> str:
#         num = clean_number(num)
#         if not num:
#             num = "Без номера"
#         if date:
#             try:
#                 date = datetime.datetime.fromisoformat(str(date)).strftime("%d.%m.%Y")
#             except Exception:
#                 date = str(date)
#         else:
#             date = "Без даты"
#         return f"{code}/0 № {num} ОТ {date}"
    
#     result = []

#     # контракт
#     c_num = data.get("contract", {}).get("Общая информация", {}).get("Номер контракта", "")
#     c_date = data.get("contract", {}).get("Общая информация", {}).get("Дата заключения", "")
#     line = make_line("03011", c_num, c_date)
#     if line: result.append(line)

#     # инвойс
#     i_num = data.get("invoice", {}).get("Общая информация", {}).get("Номер инвойса", "")
#     i_date = data.get("invoice", {}).get("Общая информация", {}).get("Дата инвойса", "")
#     line = make_line("04021", i_num, i_date)
#     if line: result.append(line)

#     # транспорт
#     t_code = transport_map.get(str(g25_1), "")
#     t_num = data.get("transport", {}).get("Общая информация", {}).get("Номер накладной", "")
#     t_date = data.get("transport", {}).get("Общая информация", {}).get("Дата накладной", "")
#     if t_code:
#         line = make_line(t_code, t_num, t_date)
#         if line: result.append(line)

#     return result

def get_all_docx(data: dict, g25_1: str) -> Dict[str, List[str]]:
    transport_map = {
        "40": "02017",  # авиа
        "50": "02019",  # почта
        "10": "02011",  # морской/речной
        "20": "02013",  # ж/д
        "30": "02015",  # авто (CMR)
        "31": "02015",
        "32": "02015",
        "71": "02018",  # трубопровод
        "72": "02018",  # ЛЭП
        "80": "02012",  # внутренний водный
    }

    default_name_by_mode = {
        "03011": "Внешнеторговый контракт (договор)",
        "04021": "Инвойс (счет-фактура)",
        "02015": "Автотранспортная накладная (CMR)",
        "02017": "Авианакладная",
        "02013": "Ж/д накладная",
        "02011": "Морская накладная / коносамент",
        "02019": "Почтовые документы",
        "02018": "Документы на трубопровод/ЛЭП",
        "02012": "Документы внутреннего водного транспорта",
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
        # fallback: просто отдать как есть
        return text

    def to_iso(val: Any) -> str:
        if _is_empty(val):
            return ""

        # даты как объекты
        if isinstance(val, datetime):
            return val.date().isoformat()
        if isinstance(val, date):
            return val.isoformat()

        s = str(val).strip()

        # YYYY-MM-DD или YYYY-MM-DDTHH:MM...
        m = re.match(r"^(\d{4}-\d{2}-\d{2})", s)
        if m:
            return m.group(1)

        # DD.MM.YYYY
        m = re.match(r"^(\d{2})\.(\d{2})\.(\d{4})$", s)
        if m:
            dd, mm, yyyy = m.group(1), m.group(2), m.group(3)
            return f"{yyyy}-{mm}-{dd}"

        # DD/MM/YYYY
        m = re.match(r"^(\d{2})/(\d{2})/(\d{4})$", s)
        if m:
            dd, mm, yyyy = m.group(1), m.group(2), m.group(3)
            return f"{yyyy}-{mm}-{dd}"

        # если пришло что-то уже “похожее на ISO”, не ломаем
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

        # # если нет ни номера, ни даты — пропускаем
        # if not num and not dt_iso:
        #     return

        out["mode_codes"].append(mode_code)
        out["kind_codes"].append("0")
        out["names"].append(nm.strip())
        out["numbers"].append(num)
        out["dates_iso"].append(dt_iso)

    # -------- CONTRACT --------
    c_info = (data.get("contract") or {}).get("Общая информация", {}) or {}
    c_num = first_non_empty(c_info, ["Номер контракта", "Номер договора", "№ договора", "Contract No", "Contract number"])
    c_dt  = first_non_empty(c_info, ["Дата заключения", "Дата договора", "Contract Date", "Contract date"])
    c_nm  = first_non_empty(c_info, ["Наименование документа", "Название документа", "Тип документа"])
    add_doc("03011", c_num, c_dt, c_nm)

    # -------- INVOICE --------
    inv_info = (data.get("invoice") or {}).get("Общая информация", {}) or {}
    i_num = first_non_empty(inv_info, ["Номер инвойса", "Номер счета", "Счет №", "Invoice No", "Invoice number"])
    i_dt  = first_non_empty(inv_info, ["Дата инвойса", "Дата счета", "Invoice Date", "Invoice date"])
    i_nm  = first_non_empty(inv_info, ["Наименование документа", "Название документа", "Тип документа"])
    add_doc("04021", i_num, i_dt, i_nm)

    # -------- TRANSPORT --------
    t_code = transport_map.get(str(g25_1).strip(), "")
    if t_code:
        # кандидаты по виду транспорта + общий fallback
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

        # если нет — добираем все transport_* ключи
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