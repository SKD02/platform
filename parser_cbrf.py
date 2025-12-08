# parser_cbrf_fixed.py
import requests
from bs4 import BeautifulSoup
from decimal import Decimal, InvalidOperation
import re
import csv

BASE_URL = "https://www.cbr.ru/currency_base/daily/"

def _decimal_of_raw(s) -> Decimal:
    if s is None:
        return Decimal("0")
    t = str(s).strip()
    t = t.replace("\u00A0", "").replace(" ", "")
    t = re.sub(r"[^0-9\-,.]", "", t)
    t = t.replace(",", ".")
    try:
        return Decimal(t)
    except (InvalidOperation, ValueError):
        return Decimal("0")

def cb_rate(date_ddmmyyyy: str, currency_code: str) -> Decimal:
    inp = (currency_code or "").strip()
    alpha = None
    if re.fullmatch(r"[A-Za-z]{3}", inp):
        alpha = inp.upper()
    else:
        try:
            with open(r"C:\Users\sidor\Desktop\streamlit\alta_classifiers\23 — КЛАССИФИКАТОР ВАЛЮТ.csv", "r", encoding="utf-8-sig", newline="") as f:
                reader = csv.reader(f)
                rows = list(reader)
                i_num, i_alpha, i_name = 0, 1, 2
                if rows and any("валют" in (c or "").lower() for c in rows[0]):
                    data = rows[1:]
                else:
                    data = rows
                lat2cyr = {
                    "A": "А", "B": "В", "C": "С", "E": "Е", "H": "Н", "K": "К",
                    "M": "М", "O": "О", "P": "Р", "T": "Т", "X": "Х", "Y": "У"
                }

                def fix_lookalikes(s: str) -> str:
                    s = s.upper()
                    return "".join(lat2cyr.get(ch, ch) for ch in s)

                norm_inp = re.sub(r"\s+", " ", inp).strip()
                norm_inp = fix_lookalikes(norm_inp)

                digits_inp = re.sub(r"\D", "", inp)

                for row in data:
                    if len(row) <= 2:
                        continue
                    num   = (row[i_num]   or "").strip()
                    alph  = (row[i_alpha] or "").strip().upper()
                    name  = (row[i_name]  or "").strip()

                    name_fixed = fix_lookalikes(re.sub(r"\s+", " ", name).strip())

                    if not alph:
                        continue
                    if norm_inp and norm_inp == name_fixed:
                        alpha = alph
                        break
                    if digits_inp and re.sub(r"\D", "", num) == digits_inp:
                        alpha = alph
                        break
        except FileNotFoundError:
            return "Файла нет"

    effective_code = alpha if alpha else inp.upper()
    params = {"UniDbQuery.Posted": "True", "UniDbQuery.To": date_ddmmyyyy}
    r = requests.get(BASE_URL, params=params, timeout=20)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    tbl = None
    for t in soup.find_all("table"):
        headers = " ".join([th.get_text(strip=True).upper() for th in t.find_all("th")])
        if "ВАЛЮТА" in headers and "КУРС" in headers:
            tbl = t
            break
    if tbl is None:
        raise RuntimeError("Не удалось найти таблицу курсов на странице")

    rows = []
    for tr in tbl.find_all("tr"):
        tds = tr.find_all(["td", "th"])
        if not tds:
            continue
        row = [td.get_text(" ", strip=True) for td in tds]
        if len(row) >= 5 and re.fullmatch(r"\d+", row[0].strip()):
            rows.append(row)

    found = None
    for row in rows:
        col_upper = [c.strip().upper() for c in row]
        if effective_code in col_upper:  
            found = row
            break

    if not found:
        return f"В таблице не найдена валюта {effective_code} на дату {date_ddmmyyyy}"
    idx_rate = len(found) - 1
    idx_units = 2 if len(found) > 2 else 2

    raw_rate = found[idx_rate]
    raw_units = found[idx_units] if idx_units < len(found) else "1"

    rate = _decimal_of_raw(raw_rate)
    try:
        units = int(re.sub(r"\D", "", raw_units)) if raw_units else 1
        if units == 0:
            units = 1
    except Exception:
        units = 1
    rate_per_unit = rate / Decimal(units)
    return rate_per_unit
