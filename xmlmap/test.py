# minimal_run.py
import json
from mappers import fill_ESADout_CU_with_gt

payload = {
  "g1_1": "ИМ",
  "g1_2": "40",
  "g1_3": "ЭД",
  "g2_1": "",
  "g2_2": "",
  "g2_3": "HUBEI BAYER AUTO TECH CO., LTD",
  "g2_4": "CN",
  "g2_5": "КИТАЙ",
  "g2_6": "",
  "g2_7": "HUBEI PROVINCE",
  "g2_8": "SHIYAN CITY",
  "g2_9": "WANTONG INDUSTRY AREA",
  "g2_10": "117-28",
  "g2_11": "",
  "g2_addr_invoice": "ADD:NO.117-28, WANTONG INDUSTRY AREA, ZHANGWAN DISTRICT, SHIYAN CITY, HUBEI PROVINCE, CHINA.",
  "g2_addr_contract": "NO.117-28, WANTONG INDUSTRY AREA, ZHANGWAN DISTRICT, SHIYAN CITY, HUBEI PROVINCE, CHINA",
  "g3_1": "1",
  "g3_2": "1",
  "g4_1": "",
  "g4_2": "",
  "g5_1": "1",
  "g6_1": "3",
  "g7_1": "",
  "g8_1": "",
  "g8_2": "",
  "g8_3": "СМ. ГРАФУ 14 ДТ",
  "g8_4": "",
  "g8_5": "",
  "g8_6": "",
  "g8_7": "",
  "g8_8": "",
  "g8_9": "",
  "g8_10": "",
  "g8_11": "",
  "g9_1": "",
  "g9_2": "",
  "g9_3": "СМ. ГРАФУ 14 ДТ",
  "g9_4": "",
  "g9_5": "",
  "g9_6": "",
  "g9_7": "",
  "g9_8": "",
  "g9_9": "",
  "g9_10": "",
  "g9_11": "",
  "g11_1": "CN",
  "g12_currency": "RUB",
  "g12_logistics": "39098.53",
  "g12_insurance": "",
  "g12_1": "552084.85",
  "g14_1": "7810459349",
  "g14_2": "35515083",
  "g14_3": "LLC KONDR",
  "g14_4": "RU",
  "g14_5": "РОССИЯ",
  "g14_6": "190020",
  "g14_7": "",
  "g14_8": "САНКТ-ПЕТЕРБУРГ",
  "g14_9": "НАБ. ОБВОДНОГО КАНАЛА",
  "g14_10": "138",
  "g14_11": "",
  "g14_addr_invoice": "ADD:RUSSIA",
  "g14_addr_contract": "РОССИЙСКАЯ ФЕДЕРАЦИЯ, 190020, Г. САНКТ-ПЕТЕРБУРГ, НАБ. ОБВОДНОГО КАНАЛА, Д. 138, КОРПУС 7, ЛИТЕРА А, ПОМЕЩЕНИЕ 2Н",
  "g15_1": "CN",
  "g15_2": "КИТАЙ",
  "g16_1": "",
  "g16_2": "НЕИЗВЕСТНА",
  "g17_1": "RU",
  "g17_2": "РОССИЯ",
  "g18_1": "1",
  "g18_2": "6114300000",
  "g18_3": "",
  "g19_1": "1",
  "g20_1": "",
  "g20_2": "FBO",
  "g21_1": "",
  "g22_1": "CNY",
  "g22_2": "46200",
  "g23_1": "11.1036",
  "g24_1": "010",
  "g24_2": "06",
  "g25_1": "20",
  "g26_1": "20",
  "g29_1": "10210130",
  "g29_2": "",
  "g30_1": "11",
  "g30_2": "10210130",
  "svh": {
    "ООО \"ВОСХОД\"": {
      "Наименование СВХ": "ООО \"ВОСХОД\"",
      "Ссылка на сайт": "https://www.alta.ru/svh/svh-10210301210100829/",
      "Адрес": "RU - 196626, Санкт-Петербург, п. Шушары, уч. ж/д \"Московское шоссе-река Кузьминка\"",
      "Лицензия": "10210/301210/10082/9 действует с 10.11.2025 г."
    },
    "АО \"Логистика-Терминал\"": {
      "Наименование СВХ": "АО \"Логистика-Терминал\"",
      "Ссылка на сайт": "https://www.alta.ru/svh/svh-10210161215101196/",
      "Адрес": "RU - 196626, Санкт-Петербург, Московское ш., д. 54А",
      "Лицензия": "10210/161215/10119/6 действует с 08.10.2025 г."
    }
  },
  "g31_1": "NO INTERCOOLER CORE",
  "g31_additional": "",
  "qty_1": "50",
  "qty_2": "",
  "g31_ois": "",
  "g31_places": "3",
  "g31_pallets": "",
  "g31_origin": "КИТАЙ",
  "g32_1": "1",
  "g33_1_list": ["8708913509"],
  "g34_1_list": ["CN"],
  "g35_1_list": ["1214.0"],
  "g36_1": "00",
  "g37_1_list": ["4000000"],
  "g38_1_list": ["1122.0"],
  "g39_1_list": [""],
  "g40_1_list": [""],
  "g41_1_list": ["80"],
  "g41_2_list": ["ШТ"],
  "g41_3_list": ["796"],
  "g42_1_list": ["46200"],
  "g43_1_list": ["1"],
  "g44_1_list": ["03011/0 № LBY20250326 ОТ 26.03.2025\n04021/0 № LBY20250326 ОТ 26.03.2025\n02013/0 № 36027914 ОТ Без даты"],
  "g45_1_list": ["552084.85"],
  "g46_1_list": ["6896.84"]
}

# 1) сделаем плоские ключи под сигнатуру функции
kwargs = dict(payload)

# 2) маппинг "*_list" -> одиночные поля, которые ждёт fill_ESADout_CU_with_gt
kwargs["g33_1"] = payload.get("g33_1", (payload.get("g33_1_list") or [""])[0])
kwargs["g34_1"] = payload.get("g34_1", (payload.get("g34_1_list") or [""])[0])
kwargs["g35_1"] = payload.get("g35_1", (payload.get("g35_1_list") or [""])[0])
kwargs["g37_1"] = payload.get("g37_1", (payload.get("g37_1_list") or [""])[0])
kwargs["g38_1"] = payload.get("g38_1", (payload.get("g38_1_list") or [""])[0])
kwargs["g39_1"] = payload.get("g39_1", (payload.get("g39_1_list") or [""])[0])
kwargs["g40_1"] = payload.get("g40_1", (payload.get("g40_1_list") or [""])[0])
kwargs["g41_1"] = payload.get("g41_1", (payload.get("g41_1_list") or [""])[0])
kwargs["g41_2"] = payload.get("g41_2", (payload.get("g41_2_list") or [""])[0])
kwargs["g41_3"] = payload.get("g41_3", (payload.get("g41_3_list") or [""])[0])
kwargs["g42_1"] = payload.get("g42_1", (payload.get("g42_1_list") or [""])[0])
kwargs["g43"]   = payload.get("g43",   (payload.get("g43_1_list") or [""])[0])
kwargs["g44_1"] = payload.get("g44_1", (payload.get("g44_1_list") or [""])[0])
kwargs["g45_1"] = payload.get("g45_1", (payload.get("g45_1_list") or [""])[0])
kwargs["g46_1"] = payload.get("g46_1", (payload.get("g46_1_list") or [""])[0])

# 3) вызываем функцию (лишние ключи из payload типа svh/qty_1/qty_2 и т.п. ОТФИЛЬТРУЕМ)
allowed = set(fill_ESADout_CU_with_gt.__code__.co_varnames)
kwargs = {k: v for k, v in kwargs.items() if k in allowed}

esad = fill_ESADout_CU_with_gt(**kwargs)
esad.save(r"C:\Users\sidor\Desktop\file", extension="xml")
print(esad)
