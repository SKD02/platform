from lxml import etree as ET

from base import IXMLElement
from ESADout_CU import ESADout_CU
from DTSout_CU import DTSout_CU
from GTDoutCustomsMark import GTDoutCustomsMark


class Container(IXMLElement):
    """Класс для описания Container"""
    def __init__(self, content: IXMLElement = None):
        self.content = content

    def to_xml(self) -> ET.Element:
        """Преобразовать объект в XML элемент"""
        doc_body_elem = ET.Element('ContainerDoc')
        if self.content:
            content_elem = self.content.to_xml()
            doc_body_elem.append(content_elem)
        return doc_body_elem


class DocBody(IXMLElement):
    """Класс для описания DocBody"""
    def __init__(self, content: IXMLElement = None):
        self.content = content

    def to_xml(self) -> ET.Element:
        """Преобразовать объект в XML элемент"""
        doc_body_elem = ET.Element('DocBody')
        if self.content:
            content_elem = self.content.to_xml()
            doc_body_elem.append(content_elem)
        return doc_body_elem


class EDContainer(IXMLElement):
    def __init__(self, document_mode_id="1006058E", document_id=None):
        self.document_mode_id = document_mode_id
        self.document_id = document_id
        # согласно XML у нас три элемента
        self.ESADout_CU: ESADout_CU = None
        self.DTSout_CU: DTSout_CU = None
        self.GTDoutCustomsMark: GTDoutCustomsMark = None

    def containarize(self, doc: IXMLElement):
        """Положить в контейнер"""
        return Container(
            DocBody(
                doc
            )
        )

    def to_xml(self) -> ET.Element:
        """Преобразовать объект в XML элемент"""
        nsmap = {
            None: 'urn:customs.ru:Information:ExchangeDocuments:ED_Container:5.24.0',
            'cat_ru': 'urn:customs.ru:CommonAggregateTypes:5.24.0',

        }

        root = ET.Element('ED_Container', nsmap=nsmap)
        root.set('DocumentModeID', self.document_mode_id)

        if self.document_id:
            root.append(self.document_id.to_xml())
        
        if self.ESADout_CU:
            esad_elem = self.containarize(self.ESADout_CU).to_xml()
            root.append(esad_elem)
            
        if self.DTSout_CU:
            dts_elem = self.containarize(self.DTSout_CU).to_xml()
            root.append(dts_elem)
            
        if self.GTDoutCustomsMark:
            gtd_elem = self.containarize(self.GTDoutCustomsMark).to_xml()
            root.append(gtd_elem)
            
        return root