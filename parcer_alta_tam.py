import requests, re, urllib.parse, pandas as pd
from bs4 import BeautifulSoup
from pypdf import PdfReader
from io import BytesIO
import warnings
from pdfminer.high_level import extract_text
import logging, warnings
from pypdf.errors import PdfReadWarning
import os
import google.generativeai as genai
import mimetypes
from urllib.parse import urljoin
import json   

base_url = "https://www.alta.ru/tam/"
base_svh_url = "https://www.alta.ru/"  

proxies = {'http': '45.182.176.38:9947'}
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Trident/7.0; rv:11.0) like Gecko'}

def get_html_data(url):
    r = requests.get(url, proxies=proxies, headers=headers)
    html_data = r.text
    return html_data

def _parse_license(lic_text: str):
    if not lic_text:
        return "", ""

    text = " ".join(str(lic_text).split()).strip() 

    license_number = text
    license_date_iso = ""

    parts = text.split(" действует с ", 1)
    if len(parts) == 2:
        license_number = parts[0].strip()
        tail = parts[1]

        m = re.search(r"(\d{2})[.\-/](\d{2})[.\-/](\d{4})", tail)
        if m:
            day, month, year = m.groups()
            license_date_iso = f"{year}-{month}-{day}"

    return license_number, license_date_iso

def _parse_svh_address(address: str):
    if not address:
        return {
            "CountryCode": "",
            "CountryName": "",
            "Region": "",
            "City": "",
            "StreetHouse": "",
        }

    text = " ".join(str(address).split())
    parts = [p.strip() for p in text.split(",") if p.strip()]

    country_code = ""
    country_name = ""
    region = ""
    city = ""
    street_house = ""

    if parts:
        first = parts[0]
        m = re.match(r"^\s*([A-Z]{2})\s*-\s*(\d{4,6})\s*$", first, flags=re.I)
        if m:
            country_code = m.group(1).upper()
            parts = parts[1:]

    if country_code == "RU":
        country_name = "РОССИЯ"

    if parts:
        region = parts[0]
    remaining = parts[1:] if len(parts) > 1 else []

    city_candidate = remaining[0] if remaining else ""
    rest_after_city = remaining[1:] if len(remaining) > 1 else []

    def normalize_city_name(s: str) -> str:
        s = s.strip()
        up = s.upper()
        if up.startswith("П. "):
            return "ПОС." + up[2:]
        return up
    
    street_markers = ["УЛ", "УЛ.", "ПР.", "ПР ", "ПР-Т", "ПР-Т.", "ПРОСП", "Ш.", "ШОССЕ", "Д.", "КОРП.", "КОРП "]

    up_city_candidate = city_candidate.upper()
    is_street_in_city_candidate = any(m in up_city_candidate for m in street_markers)

    if city_candidate and not is_street_in_city_candidate:
        city = normalize_city_name(city_candidate)
        street_parts = rest_after_city
    else:
        city = region.upper() if region else ""
        street_parts = [p for p in remaining if p] 

    street = ", ".join(street_parts)
    street_up = street.upper()
    street_up = re.sub(r"УЧ\.\s*Ж/Д", "УЧАСТОК Ж.Д.", street_up, flags=re.I)
    street_up = re.sub(r"Ж/Д", "Ж.Д.", street_up, flags=re.I)
    street_up = re.sub(r"\bШ\.\b", "ШОССЕ", street_up, flags=re.I)

    return {
        "CountryCode": country_code,
        "CountryName": country_name,
        "Region": region.upper() if region else "",
        "City": city,
        "StreetHouse": street_up,
    }

def get_svh_data(kod_tp: str):
    url  = urljoin(base_url, kod_tp) + "/"
    html_data = get_html_data(url)
    soup = BeautifulSoup(html_data, 'html.parser')
    svh_data = {}

    for svh in soup.find_all("div", attrs={"boxSubstrate boxSubstrate-offset-0 p-10 mb10"}):
        a = svh.find("a")
        if not a:
            continue

        svh_url_part = a.get("href")
        svh_url = urljoin(base_svh_url, svh_url_part)

        name_el = svh.find("div", attrs={"h3"})
        addr_el = svh.find(
            "div",
            attrs={
                "pTam_fieldColumn pTam_fieldColumn-list "
                "pTam_fieldColumn-left lightgray"
            },
        )
        lic_el = svh.find(
            "div",
            attrs={"pTam_fieldColumn pTam_fieldColumn-list pTam_fieldColumn-right"},
        )

        name = (name_el.text if name_el else "").strip()
        address = (addr_el.text if addr_el else "").strip()
        lic = (lic_el.text if lic_el else "").strip()

        address_norm = " ".join(address.split())
        lic_norm = " ".join(lic.split())
        license_number, license_date_iso = _parse_license(lic_norm)
        addr_parts = _parse_svh_address(address_norm)

        svh_data[name] = {
            "Наименование СВХ": name.strip(),
            "Ссылка на сайт": svh_url.strip(),
            "Адрес": address_norm,
            "Лицензия": lic_norm,
            "Номер лицензии": license_number,
            "Дата лицензии": license_date_iso,
            "CountryCode":  addr_parts["CountryCode"],
            "CountryName":  addr_parts["CountryName"],
            "Region":       addr_parts["Region"],
            "City":         addr_parts["City"],
            "StreetHouse":  addr_parts["StreetHouse"],
        }

    if len(kod_tp) == 8 and not svh_data:
        name = f"Не найдено СВХ. Проверьте информацию на сайте {url}"
        svh_data = {
            "Наименование СВХ": name
        }
        return svh_data
    
    return svh_data
