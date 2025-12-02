from lxml import etree as ET
from .base import IXMLElement
from .FilledPerson import DocArchIdDetails


class BrokerRegistryDocDetails(IXMLElement):
    """Класс для RUDECLcat:BrokerRegistryDocDetails"""
    
    def __init__(self,
                 doc_kind_code: str = None,
                 registration_number_id: str = None):
        self.doc_kind_code = doc_kind_code
        self.registration_number_id = registration_number_id
    
    def to_xml(self) -> ET.Element:
        # Определяем nsmap для этого элемента
        nsmap = {
            'RUDECLcat': 'urn:customs.ru:RUDeclCommonAggregateTypesCust:5.24.0'
        }
        
        elem = ET.Element('{urn:customs.ru:RUDeclCommonAggregateTypesCust:5.24.0}BrokerRegistryDocDetails', nsmap=nsmap)
        
        if self.doc_kind_code:
            kind_code_elem = ET.Element('{urn:customs.ru:RUDeclCommonAggregateTypesCust:5.24.0}DocKindCode')
            kind_code_elem.text = self.doc_kind_code
            elem.append(kind_code_elem)
        
        if self.registration_number_id:
            reg_number_elem = ET.Element('{urn:customs.ru:RUDeclCommonAggregateTypesCust:5.24.0}RegistrationNumberId')
            reg_number_elem.text = self.registration_number_id
            elem.append(reg_number_elem)
        
        return elem


class RepresentativeContractDetails(IXMLElement):
    """Класс для RUDECLcat:RepresentativeContractDetails"""
    
    def __init__(self,
                 pr_document_name: str = None,
                 pr_document_number: str = None,
                 pr_document_date: str = None,
                 doc_kind_code: str = None,
                 doc_arch_id_details: DocArchIdDetails = None):
        self.pr_document_name = pr_document_name
        self.pr_document_number = pr_document_number
        self.pr_document_date = pr_document_date
        self.doc_kind_code = doc_kind_code
        self.doc_arch_id_details = doc_arch_id_details
    
    def to_xml(self) -> ET.Element:
        # Определяем nsmap для этого элемента
        nsmap = {
            'RUDECLcat': 'urn:customs.ru:RUDeclCommonAggregateTypesCust:5.24.0',
            'cat_ru': 'urn:customs.ru:CommonAggregateTypes:5.24.0',
            'RUScat_ru': 'urn:customs.ru:RUSCommonAggregateTypes:5.24.0'
        }
        
        elem = ET.Element('{urn:customs.ru:RUDeclCommonAggregateTypesCust:5.24.0}RepresentativeContractDetails', nsmap=nsmap)
        
        if self.pr_document_name:
            doc_name_elem = ET.Element('{urn:customs.ru:CommonAggregateTypes:5.24.0}PrDocumentName')
            doc_name_elem.text = self.pr_document_name
            elem.append(doc_name_elem)
        
        if self.pr_document_number:
            doc_number_elem = ET.Element('{urn:customs.ru:CommonAggregateTypes:5.24.0}PrDocumentNumber')
            doc_number_elem.text = self.pr_document_number
            elem.append(doc_number_elem)
        
        if self.pr_document_date:
            doc_date_elem = ET.Element('{urn:customs.ru:CommonAggregateTypes:5.24.0}PrDocumentDate')
            doc_date_elem.text = self.pr_document_date
            elem.append(doc_date_elem)
        
        if self.doc_kind_code:
            kind_code_elem = ET.Element('{urn:customs.ru:RUSCommonAggregateTypes:5.24.0}DocKindCode')
            kind_code_elem.text = self.doc_kind_code
            elem.append(kind_code_elem)
        
        if self.doc_arch_id_details:
            elem.append(self.doc_arch_id_details.to_xml())
        
        return elem


class CUESADCustomsRepresentative(IXMLElement):
    """Класс для CUESADCustomsRepresentative"""
    
    def __init__(self,
                 broker_registry_doc_details: BrokerRegistryDocDetails = None,
                 representative_contract_details: RepresentativeContractDetails = None):
        self.broker_registry_doc_details = broker_registry_doc_details
        self.representative_contract_details = representative_contract_details
    
    def to_xml(self) -> ET.Element:
        # Определяем полный nsmap для корневого элемента
        nsmap = {
            None: 'urn:customs.ru:Information:CustomsDocuments:GTDoutCustomsMark:5.25.0',
            'catESAD_ru': 'urn:customs.ru:RUCommonAggregateTypes:5.24.0',
            'RUScat_ru': 'urn:customs.ru:RUSCommonAggregateTypes:5.24.0',
            'catESAD_cu': 'urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0',
            'cltESAD_cu': 'urn:customs.ru:CUESADCommonLeafTypes:5.17.0',
            'CategoryCust': 'urn:customs.ru:Categories:3.0.0',
            'cat_ru': 'urn:customs.ru:CommonAggregateTypes:5.24.0',
            'clt_ru': 'urn:customs.ru:CommonLeafTypes:5.10.0',
            'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
            'RUDECLcat': 'urn:customs.ru:RUDeclCommonAggregateTypesCust:5.24.0'
        }
        
        elem = ET.Element('CUESADCustomsRepresentative', nsmap=nsmap)
        
        if self.broker_registry_doc_details:
            elem.append(self.broker_registry_doc_details.to_xml())
        
        if self.representative_contract_details:
            elem.append(self.representative_contract_details.to_xml())
        
        return elem


def get_CUESADCustomsRepresentative():
    # Пример создания CUESADCustomsRepresentative
    customs_representative = CUESADCustomsRepresentative(
        broker_registry_doc_details=BrokerRegistryDocDetails(
            doc_kind_code="09034",
            registration_number_id="0843"
        ),
        representative_contract_details=RepresentativeContractDetails(
            pr_document_name="ДОГОВОР С ТАМОЖЕННЫМ ПРЕДСТАВИТЕЛЕМ",
            pr_document_number="0843-22/168",
            pr_document_date="2022-11-22",
            doc_kind_code="11002",
            doc_arch_id_details=DocArchIdDetails(
                electronic_document_id="be98be6b-0a4c-4045-b1c0-be12ef13c34a",
                electronic_arch_id="3e6fbe94-ae85-42fe-be50-0b7de474c730",
                document_mode_id="1006196E"
            )
        )
    )

    return customs_representative


if __name__ == "__main__":
    customs_representative = get_CUESADCustomsRepresentative()
    print(customs_representative)