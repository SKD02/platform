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
import fitz
import pprint as pp     

base_url = "https://www.alta.ru/tam/"
base_svh_url = "https://www.alta.ru/"  

proxies = {'http': '45.182.176.38:9947'}
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Trident/7.0; rv:11.0) like Gecko'}

def get_html_data(url):
    r = requests.get(url, proxies=proxies, headers=headers)
    html_data = r.text
    return html_data

def get_tp_name(kod_tp: str):
    url  = urljoin(base_url, kod_tp) + "/"
    html_data = get_html_data(url)
    soup = BeautifulSoup(html_data, 'html.parser')
    for svh in soup.find_all("div", attrs = {"pTam_right boxSubstrate boxSubstrate-offset-0 mb10"}):
        h1 = svh.find("h1")
        return h1.get_text(strip = True)
    if len(kod_tp) == 8 and not h1:
        return f"Проверьте информацию на сайте: {url}"

