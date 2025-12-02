from lxml import etree as ET
from .base import IXMLElement


class CommunicationDetails(IXMLElement):
    """Класс для RUScat_ru:CommunicationDetails"""
    
    def __init__(self, phone: str = None):
        self.phone = phone
    
    def to_xml(self) -> ET.Element:
        # Определяем nsmap для этого элемента
        nsmap = {
            'cat_ru': 'urn:customs.ru:CommonAggregateTypes:5.24.0'
        }
        elem = ET.Element('{urn:customs.ru:RUSCommonAggregateTypes:5.24.0}CommunicationDetails', nsmap=nsmap)
        if self.phone:
            phone_elem = ET.Element('{urn:customs.ru:CommonAggregateTypes:5.24.0}Phone')
            phone_elem.text = self.phone
            elem.append(phone_elem)
        return elem


class DocArchIdDetails(IXMLElement):
    """Класс для RUDECLcat:DocArchIdDetails"""
    
    def __init__(self, 
                 electronic_document_id: str = None,
                 electronic_arch_id: str = None,
                 document_mode_id: str = None):
        self.electronic_document_id = electronic_document_id
        self.electronic_arch_id = electronic_arch_id
        self.document_mode_id = document_mode_id
    
    def to_xml(self) -> ET.Element:
        # Определяем nsmap для этого элемента
        nsmap = {
            'catESAD_cu': 'urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0'
        }
        elem = ET.Element('{urn:customs.ru:RUDeclCommonAggregateTypesCust:5.24.0}DocArchIdDetails', nsmap=nsmap)
        if self.electronic_document_id:
            doc_id_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}ElectronicDocumentID')
            doc_id_elem.text = self.electronic_document_id
            elem.append(doc_id_elem)
        if self.electronic_arch_id:
            arch_id_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}ElectronicArchID')
            arch_id_elem.text = self.electronic_arch_id
            elem.append(arch_id_elem)
        if self.document_mode_id:
            mode_id_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}DocumentModeID')
            mode_id_elem.text = self.document_mode_id
            elem.append(mode_id_elem)
        return elem


class SigningDetails(IXMLElement):
    """Класс для RUDECLcat:SigningDetails"""
    
    def __init__(self,
                 person_surname: str = None,
                 person_name: str = None,
                 person_middle_name: str = None,
                 person_post: str = None,
                 communication_details: CommunicationDetails = None,
                 signing_date: str = None):
        self.person_surname = person_surname
        self.person_name = person_name
        self.person_middle_name = person_middle_name
        self.person_post = person_post
        self.communication_details = communication_details
        self.signing_date = signing_date
    
    def to_xml(self) -> ET.Element:
        # Определяем nsmap для этого элемента
        nsmap = {
            'cat_ru': 'urn:customs.ru:CommonAggregateTypes:5.24.0',
            'RUScat_ru': 'urn:customs.ru:RUSCommonAggregateTypes:5.24.0'
        }
        elem = ET.Element('{urn:customs.ru:RUDeclCommonAggregateTypesCust:5.24.0}SigningDetails', nsmap=nsmap)
        
        if self.person_surname:
            surname_elem = ET.Element('{urn:customs.ru:CommonAggregateTypes:5.24.0}PersonSurname')
            surname_elem.text = self.person_surname
            elem.append(surname_elem)
        
        if self.person_name:
            name_elem = ET.Element('{urn:customs.ru:CommonAggregateTypes:5.24.0}PersonName')
            name_elem.text = self.person_name
            elem.append(name_elem)
        
        if self.person_middle_name:
            middle_name_elem = ET.Element('{urn:customs.ru:CommonAggregateTypes:5.24.0}PersonMiddleName')
            middle_name_elem.text = self.person_middle_name
            elem.append(middle_name_elem)
        
        if self.person_post:
            post_elem = ET.Element('{urn:customs.ru:CommonAggregateTypes:5.24.0}PersonPost')
            post_elem.text = self.person_post
            elem.append(post_elem)
        
        if self.communication_details:
            elem.append(self.communication_details.to_xml())
        
        if self.signing_date:
            signing_date_elem = ET.Element('{urn:customs.ru:RUSCommonAggregateTypes:5.24.0}SigningDate')
            signing_date_elem.text = self.signing_date
            elem.append(signing_date_elem)
        
        return elem


class SignatoryPersonIdentityDetails(IXMLElement):
    """Класс для RUDECLcat:SignatoryPersonIdentityDetails"""
    
    def __init__(self,
                 identity_card_code: str = None,
                 identity_card_name: str = None,
                 identity_card_series: str = None,
                 identity_card_number: str = None,
                 identity_card_date: str = None,
                 organization_name: str = None,
                 doc_arch_id_details: DocArchIdDetails = None):
        self.identity_card_code = identity_card_code
        self.identity_card_name = identity_card_name
        self.identity_card_series = identity_card_series
        self.identity_card_number = identity_card_number
        self.identity_card_date = identity_card_date
        self.organization_name = organization_name
        self.doc_arch_id_details = doc_arch_id_details
    
    def to_xml(self) -> ET.Element:
        # Определяем nsmap для этого элемента
        nsmap = {
            'RUScat_ru': 'urn:customs.ru:RUSCommonAggregateTypes:5.24.0',
            'catESAD_cu': 'urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0'
        }
        elem = ET.Element('{urn:customs.ru:RUDeclCommonAggregateTypesCust:5.24.0}SignatoryPersonIdentityDetails', nsmap=nsmap)
        
        if self.identity_card_code:
            card_code_elem = ET.Element('{urn:customs.ru:RUSCommonAggregateTypes:5.24.0}IdentityCardCode')
            card_code_elem.text = self.identity_card_code
            elem.append(card_code_elem)
        
        if self.identity_card_name:
            card_name_elem = ET.Element('{urn:customs.ru:RUSCommonAggregateTypes:5.24.0}IdentityCardName')
            card_name_elem.text = self.identity_card_name
            elem.append(card_name_elem)
        
        if self.identity_card_series:
            card_series_elem = ET.Element('{urn:customs.ru:RUSCommonAggregateTypes:5.24.0}IdentityCardSeries')
            card_series_elem.text = self.identity_card_series
            elem.append(card_series_elem)
        
        if self.identity_card_number:
            card_number_elem = ET.Element('{urn:customs.ru:RUSCommonAggregateTypes:5.24.0}IdentityCardNumber')
            card_number_elem.text = self.identity_card_number
            elem.append(card_number_elem)
        
        if self.identity_card_date:
            card_date_elem = ET.Element('{urn:customs.ru:RUSCommonAggregateTypes:5.24.0}IdentityCardDate')
            card_date_elem.text = self.identity_card_date
            elem.append(card_date_elem)
        
        if self.organization_name:
            org_name_elem = ET.Element('{urn:customs.ru:RUSCommonAggregateTypes:5.24.0}OrganizationName')
            org_name_elem.text = self.organization_name
            elem.append(org_name_elem)
        
        if self.doc_arch_id_details:
            elem.append(self.doc_arch_id_details.to_xml())
        
        return elem


class PowerOfAttorneyDetails(IXMLElement):
    """Класс для RUDECLcat:PowerOfAttorneyDetails"""
    
    def __init__(self,
                 pr_document_name: str = None,
                 pr_document_number: str = None,
                 pr_document_date: str = None,
                 doc_start_date: str = None,
                 doc_validity_date: str = None,
                 doc_kind_code: str = None,
                 doc_arch_id_details: DocArchIdDetails = None):
        self.pr_document_name = pr_document_name
        self.pr_document_number = pr_document_number
        self.pr_document_date = pr_document_date
        self.doc_start_date = doc_start_date
        self.doc_validity_date = doc_validity_date
        self.doc_kind_code = doc_kind_code
        self.doc_arch_id_details = doc_arch_id_details
    
    def to_xml(self) -> ET.Element:
        # Определяем nsmap для этого элемента
        nsmap = {
            'cat_ru': 'urn:customs.ru:CommonAggregateTypes:5.24.0',
            'RUScat_ru': 'urn:customs.ru:RUSCommonAggregateTypes:5.24.0',
            'catESAD_cu': 'urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0'
        }
        elem = ET.Element('{urn:customs.ru:RUDeclCommonAggregateTypesCust:5.24.0}PowerOfAttorneyDetails', nsmap=nsmap)
        
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
        
        if self.doc_start_date:
            start_date_elem = ET.Element('{urn:customs.ru:RUSCommonAggregateTypes:5.24.0}DocStartDate')
            start_date_elem.text = self.doc_start_date
            elem.append(start_date_elem)
        
        if self.doc_validity_date:
            validity_date_elem = ET.Element('{urn:customs.ru:RUSCommonAggregateTypes:5.24.0}DocValidityDate')
            validity_date_elem.text = self.doc_validity_date
            elem.append(validity_date_elem)
        
        if self.doc_kind_code:
            kind_code_elem = ET.Element('{urn:customs.ru:RUSCommonAggregateTypes:5.24.0}DocKindCode')
            kind_code_elem.text = self.doc_kind_code
            elem.append(kind_code_elem)
        
        if self.doc_arch_id_details:
            elem.append(self.doc_arch_id_details.to_xml())
        
        return elem


class FilledPerson(IXMLElement):
    """Класс для FilledPerson"""
    
    def __init__(self,
                 signing_details: SigningDetails = None,
                 signatory_person_identity_details: SignatoryPersonIdentityDetails = None,
                 power_of_attorney_details: PowerOfAttorneyDetails = None):
        self.signing_details = signing_details
        self.signatory_person_identity_details = signatory_person_identity_details
        self.power_of_attorney_details = power_of_attorney_details
    
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
        
        elem = ET.Element('FilledPerson', nsmap=nsmap)
        
        if self.signing_details:
            elem.append(self.signing_details.to_xml())
        
        if self.signatory_person_identity_details:
            elem.append(self.signatory_person_identity_details.to_xml())
        
        if self.power_of_attorney_details:
            elem.append(self.power_of_attorney_details.to_xml())
        
        return elem


def get_filled_person():
    # Пример создания FilledPerson
    filled_person = FilledPerson(
        signing_details=SigningDetails(
            person_surname="ЖУКОВ",
            person_name="НИКОЛАЙ",
            person_middle_name="СЕРГЕЕВИЧ",
            person_post="МЕНЕДЖЕР ПО ТАМОЖЕННЫМ ОПЕРАЦИЯМ",
            communication_details=CommunicationDetails(phone="79819740258"),
            signing_date="2025-04-22T12:58:00"
        ),
        signatory_person_identity_details=SignatoryPersonIdentityDetails(
            identity_card_code="RU01001",
            identity_card_name="ПАСПОРТ",
            identity_card_series="9999",
            identity_card_number="999999",
            identity_card_date="2099-09-09",
            organization_name="ТП № 99 УФМС РОССИИ ПО СПБ И МУУУ В ВОЛЫНКО_ДЕРЕВЕНСКОМ РАЙОНЕ Г. СПБ",
            doc_arch_id_details=DocArchIdDetails(
                electronic_document_id="a2d9020d-3206-4c9b-9fda-d69ea2ff67ac",
                electronic_arch_id="a5340484-f7dd-4950-a752-5c7f273afe2f",
                document_mode_id="1001204E"
            )
        ),
        power_of_attorney_details=PowerOfAttorneyDetails(
            pr_document_name="ДОВЕРЕННОСТЬ",
            pr_document_number="64",
            pr_document_date="2025-01-09",
            doc_start_date="2025-01-09",
            doc_validity_date="2025-12-31",
            doc_kind_code="11004",
            doc_arch_id_details=DocArchIdDetails(
                electronic_document_id="6a57d59c-47a2-4757-be28-2c60c4fe5cb7",
                electronic_arch_id="3e6fbe94-ae85-42fe-be50-0b7de474c730",
                document_mode_id="1002008E"
            )
        )
    )
    return filled_person


if __name__ == "__main__":
    filled_person = get_filled_person()    
    print(filled_person)