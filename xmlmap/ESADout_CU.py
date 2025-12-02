from lxml import etree as ET
from typing import Optional

from .base import IXMLElement
from .ESADout_CUGoodsShipment import ESADout_CUGoodsShipment, get_goods_shipment
from .FilledPerson import FilledPerson, get_filled_person
from .CUESADCustomsRepresentative import CUESADCustomsRepresentative, get_CUESADCustomsRepresentative


class DocumentID(IXMLElement):
    """Класс для cat_ru:DocumentID"""
    def __init__(self, document_id: str):
        self.document_id = document_id

    def to_xml(self) -> ET.Element:
        # Определяем namespace map для элемента
        nsmap = {
            None: 'urn:customs.ru:CommonAggregateTypes:5.24.0'
        }
        elem = ET.Element('DocumentID', nsmap=nsmap)
        elem.text = self.document_id
        return elem


class EECEDocHeaderAddInfo(IXMLElement):
    """Класс для EECEDocHeaderAddInfo"""
    
    def __init__(self, 
                 e_doc_code: str = None,
                 e_doc_date_time: str = None,
                 language_code: str = None,
                 source_country_code: str = None,
                 destination_country_code: str = None):
        self.e_doc_code = e_doc_code
        self.e_doc_date_time = e_doc_date_time
        self.language_code = language_code
        self.source_country_code = source_country_code
        self.destination_country_code = destination_country_code

    def to_xml(self) -> ET.Element:
        # Определяем namespace map для корневого элемента
        nsmap = {
            None: 'urn:customs.ru:RUSCommonAggregateTypes:5.24.0'
        }
        elem = ET.Element('EECEDocHeaderAddInfo', nsmap=nsmap)
        
        # Для дочерних элементов не нужно определять nsmap, они наследуют от родителя
        e_doc_code_elem = ET.Element('EDocCode')
        e_doc_code_elem.text = self.e_doc_code
        elem.append(e_doc_code_elem)
        
        e_doc_date_time_elem = ET.Element('EDocDateTime')
        e_doc_date_time_elem.text = self.e_doc_date_time
        elem.append(e_doc_date_time_elem)
        
        language_code_elem = ET.Element('LanguageCode')
        language_code_elem.text = self.language_code
        elem.append(language_code_elem)
        
        source_country_code_elem = ET.Element('SourceCountryCode')
        source_country_code_elem.text = self.source_country_code
        elem.append(source_country_code_elem)
        
        destination_country_code_elem = ET.Element('DestinationCountryCode')
        destination_country_code_elem.text = self.destination_country_code
        elem.append(destination_country_code_elem)
        return elem


class CustomsProcedure(IXMLElement):
    """Класс для CustomsProcedure"""
    def __init__(self, procedure: str):
        self.procedure = procedure

    def to_xml(self) -> ET.Element:
        # Этот элемент будет в основном namespace, поэтому nsmap не нужен
        elem = ET.Element('CustomsProcedure')
        elem.text = self.procedure
        return elem


class CustomsModeCode(IXMLElement):
    """Класс для CustomsModeCode"""
    def __init__(self, code: str):
        self.code = code

    def to_xml(self) -> ET.Element:
        elem = ET.Element('CustomsModeCode')
        elem.text = self.code
        return elem


class ElectronicDocumentSign(IXMLElement):
    """Класс для ElectronicDocumentSign"""
    def __init__(self, sign: str):
        self.sign = sign

    def to_xml(self) -> ET.Element:
        elem = ET.Element('ElectronicDocumentSign')
        elem.text = self.sign
        return elem


class RecipientCountryCode(IXMLElement):
    """Класс для RecipientCountryCode"""
    def __init__(self, country_code: str):
        self.country_code = country_code

    def to_xml(self) -> ET.Element:
        elem = ET.Element('RecipientCountryCode')
        elem.text = self.country_code
        return elem


class ESADout_CU(IXMLElement):
    """Основной класс для ESADout_CU документа"""
    
    def __init__(self, 
                 document_mode_id: str,
                 document_id: DocumentID,
                 customs_procedure: CustomsProcedure,
                 customs_mode_code: CustomsModeCode,
                 electronic_document_sign: ElectronicDocumentSign,
                 recipient_country_code: RecipientCountryCode,
                 goods_shipment: Optional[ESADout_CUGoodsShipment]= None,
                 filled_person: Optional[FilledPerson]= None,
                 customs_representative: Optional[CUESADCustomsRepresentative]= None,
                 ece_doc_header_add_info: Optional[EECEDocHeaderAddInfo] = None):
        
        self.document_mode_id = document_mode_id
        self.document_id = document_id
        self.ece_doc_header_add_info = ece_doc_header_add_info
        self.customs_procedure = customs_procedure
        self.customs_mode_code = customs_mode_code
        self.electronic_document_sign = electronic_document_sign
        self.recipient_country_code = recipient_country_code
        self.goods_shipment = goods_shipment
        self.filled_person = filled_person
        self.customs_representative = customs_representative

    def to_xml(self) -> ET.Element:
        """Преобразовать объект в XML элемент с namespace"""
        # Определяем namespace map для корневого элемента
        nsmap = {
            None: 'urn:customs.ru:Information:CustomsDocuments:ESADout_CU:5.24.0',
            'RUScat_ru': 'urn:customs.ru:RUSCommonAggregateTypes:5.24.0',
            'cat_ru': 'urn:customs.ru:CommonAggregateTypes:5.24.0',
            'RUDECLcat': 'urn:customs.ru:RUDeclCommonAggregateTypesCust:5.24.0',
            'catESAD_cu': 'urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0'
        }

        # Создаем корневой элемент с namespace
        root = ET.Element('ESADout_CU', nsmap=nsmap)
        root.set('DocumentModeID', self.document_mode_id)

        # Добавляем дочерние элементы
        root.append(self.document_id.to_xml())
        
        if self.ece_doc_header_add_info:
            root.append(self.ece_doc_header_add_info.to_xml())
        
        root.append(self.customs_procedure.to_xml())
        root.append(self.customs_mode_code.to_xml())
        root.append(self.electronic_document_sign.to_xml())
        root.append(self.recipient_country_code.to_xml())
        
        if self.goods_shipment:
            root.append(self.goods_shipment.to_xml())
        if self.filled_person:
            root.append(self.filled_person.to_xml())
        if self.customs_representative:
            root.append(self.customs_representative.to_xml())
            
        return root


def get_ESADout_CU():
    # Создание объекта с данными
    document_id = DocumentID("C3311921-1FCD-42FB-B038-1786E96F85DE")
    customs_procedure = CustomsProcedure("ИМ")
    customs_mode_code = CustomsModeCode("40")
    electronic_document_sign = ElectronicDocumentSign("ЭД")
    recipient_country_code = RecipientCountryCode("RU")

    ece_doc_header_add_info = EECEDocHeaderAddInfo(
        e_doc_code="R.036",
        e_doc_date_time="2025-04-22T12:58:00",
        language_code="RU",
        source_country_code="RU",
        destination_country_code="RU"
    )

    esa_dout_cu = ESADout_CU(
        document_mode_id="1006107E",
        document_id=document_id,
        customs_procedure=customs_procedure,
        customs_mode_code=customs_mode_code,
        electronic_document_sign=electronic_document_sign,
        recipient_country_code=recipient_country_code,
        ece_doc_header_add_info=ece_doc_header_add_info,
        goods_shipment=get_goods_shipment(),
        filled_person=get_filled_person(),
        customs_representative=get_CUESADCustomsRepresentative(),
    )
    return esa_dout_cu


if __name__ == "__main__":
    esa_dout_cu = get_ESADout_CU()
    print(esa_dout_cu)