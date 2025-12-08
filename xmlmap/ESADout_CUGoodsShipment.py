from lxml import etree as ET
from typing import List

from .base import IXMLElement


# Вспомогательные классы для адресов и организаций
class SubjectAddressDetails(IXMLElement):
    """Класс для RUScat_ru:SubjectAddressDetails"""
    
    def __init__(self, postal_code: str = None, country_code: str = None,
                 country_name: str = None, region: str = None, city: str = None,
                 street_house: str = None, house: str = None):
        self.postal_code = postal_code
        self.country_code = country_code
        self.country_name = country_name
        self.region = region
        self.city = city
        self.street_house = street_house
        self.house = house
    
    def to_xml(self) -> ET.Element:
        elem = ET.Element('{urn:customs.ru:RUSCommonAggregateTypes:5.24.0}SubjectAddressDetails')
        
        if self.postal_code:
            postal_code_elem = ET.Element('{urn:customs.ru:RUSCommonAggregateTypes:5.24.0}PostalCode')
            postal_code_elem.text = self.postal_code
            elem.append(postal_code_elem)
        
        if self.country_code:
            country_code_elem = ET.Element('{urn:customs.ru:RUSCommonAggregateTypes:5.24.0}CountryCode')
            country_code_elem.text = self.country_code
            elem.append(country_code_elem)
        
        if self.country_name:
            country_name_elem = ET.Element('{urn:customs.ru:RUSCommonAggregateTypes:5.24.0}CounryName')
            country_name_elem.text = self.country_name
            elem.append(country_name_elem)
        
        if self.region:
            region_elem = ET.Element('{urn:customs.ru:RUSCommonAggregateTypes:5.24.0}Region')
            region_elem.text = self.region
            elem.append(region_elem)
        
        if self.city:
            city_elem = ET.Element('{urn:customs.ru:RUSCommonAggregateTypes:5.24.0}City')
            city_elem.text = self.city
            elem.append(city_elem)
        
        if self.street_house:
            street_house_elem = ET.Element('{urn:customs.ru:RUSCommonAggregateTypes:5.24.0}StreetHouse')
            street_house_elem.text = self.street_house
            elem.append(street_house_elem)
        
        if self.house:
            house_elem = ET.Element('{urn:customs.ru:RUSCommonAggregateTypes:5.24.0}House')
            house_elem.text = self.house
            elem.append(house_elem)
        
        return elem


class RFOrganizationFeatures(IXMLElement):
    """Класс для cat_ru:RFOrganizationFeatures"""
    
    def __init__(self, ogrn: str = None, inn: str = None, kpp: str = None):
        self.ogrn = ogrn
        self.inn = inn
        self.kpp = kpp
    
    def to_xml(self) -> ET.Element:
        elem = ET.Element('{urn:customs.ru:CommonAggregateTypes:5.24.0}RFOrganizationFeatures')
        
        if self.ogrn:
            ogrn_elem = ET.Element('{urn:customs.ru:CommonAggregateTypes:5.24.0}OGRN')
            ogrn_elem.text = self.ogrn
            elem.append(ogrn_elem)
        
        if self.inn:
            inn_elem = ET.Element('{urn:customs.ru:CommonAggregateTypes:5.24.0}INN')
            inn_elem.text = self.inn
            elem.append(inn_elem)
        
        if self.kpp:
            kpp_elem = ET.Element('{urn:customs.ru:CommonAggregateTypes:5.24.0}KPP')
            kpp_elem.text = self.kpp
            elem.append(kpp_elem)
        
        return elem


# Классы для участников сделки

class ESADout_CUConsignor(IXMLElement):
    """Класс для ESADout_CUConsignor (Отправитель)"""
    
    def __init__(self, organization_name: str = None, 
                 subject_address_details: SubjectAddressDetails = None):
        self.organization_name = organization_name
        self.subject_address_details = subject_address_details
    
    def to_xml(self) -> ET.Element:
        elem = ET.Element('ESADout_CUConsignor')
        
        if self.organization_name:
            org_name_elem = ET.Element('{urn:customs.ru:CommonAggregateTypes:5.24.0}OrganizationName')
            org_name_elem.text = self.organization_name
            elem.append(org_name_elem)
        
        if self.subject_address_details:
            elem.append(self.subject_address_details.to_xml())
        
        return elem


class ESADout_CUConsignee(IXMLElement):
    """Класс для ESADout_CUConsignee (Получатель)"""
    
    def __init__(self, equal_indicator: str = None):
        self.equal_indicator = equal_indicator
    
    def to_xml(self) -> ET.Element:
        elem = ET.Element('ESADout_CUConsignee')
        
        if self.equal_indicator:
            equal_indicator_elem = ET.Element('{urn:customs.ru:RUDeclCommonAggregateTypesCust:5.24.0}EqualIndicator')
            equal_indicator_elem.text = self.equal_indicator
            elem.append(equal_indicator_elem)
        
        return elem


class ESADout_CUFinancialAdjustingResponsiblePerson(IXMLElement):
    """Класс для ESADout_CUFinancialAdjustingResponsiblePerson"""
    
    def __init__(self, declarant_equal_flag: str = None):
        self.declarant_equal_flag = declarant_equal_flag
    
    def to_xml(self) -> ET.Element:
        elem = ET.Element('ESADout_CUFinancialAdjustingResponsiblePerson')
        
        if self.declarant_equal_flag:
            flag_elem = ET.Element('DeclarantEqualFlag')
            flag_elem.text = self.declarant_equal_flag
            elem.append(flag_elem)
        
        return elem


class ESADout_CUDeclarant(IXMLElement):
    """Класс для ESADout_CUDeclarant (Декларант)"""
    
    def __init__(self, organization_name: str = None,
                 rf_organization_features: RFOrganizationFeatures = None,
                 subject_address_details: SubjectAddressDetails = None):
        self.organization_name = organization_name
        self.rf_organization_features = rf_organization_features
        self.subject_address_details = subject_address_details
    
    def to_xml(self) -> ET.Element:
        elem = ET.Element('ESADout_CUDeclarant')
        
        if self.organization_name:
            org_name_elem = ET.Element('{urn:customs.ru:CommonAggregateTypes:5.24.0}OrganizationName')
            org_name_elem.text = self.organization_name
            elem.append(org_name_elem)
        
        if self.rf_organization_features:
            elem.append(self.rf_organization_features.to_xml())
        
        if self.subject_address_details:
            elem.append(self.subject_address_details.to_xml())
        
        return elem


# Классы для местоположения товаров

class RegisterDocumentIdDetails(IXMLElement):
    """Класс для RegisterDocumentIdDetails"""
    
    def __init__(self, doc_id: str = None):
        self.doc_id = doc_id
    
    def to_xml(self) -> ET.Element:
        elem = ET.Element('RegisterDocumentIdDetails')
        
        if self.doc_id:
            doc_id_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}DocId')
            doc_id_elem.text = self.doc_id
            elem.append(doc_id_elem)
        
        return elem


class ESADout_CUGoodsLocation(IXMLElement):
    """Класс для ESADout_CUGoodsLocation (Местонахождение товаров)"""
    
    def __init__(self, information_type_code: str = None, customs_office: str = None,
                 customs_country_code: str = None, location_name: str = None,
                 register_document_id_details: RegisterDocumentIdDetails = None,
                 address: SubjectAddressDetails = None):
        self.information_type_code = information_type_code
        self.customs_office = customs_office
        self.customs_country_code = customs_country_code
        self.location_name = location_name
        self.register_document_id_details = register_document_id_details
        self.address = address
    
    def to_xml(self) -> ET.Element:
        elem = ET.Element('ESADout_CUGoodsLocation')
        
        if self.information_type_code:
            info_type_elem = ET.Element('InformationTypeCode')
            info_type_elem.text = self.information_type_code
            elem.append(info_type_elem)
        
        if self.customs_office:
            customs_office_elem = ET.Element('CustomsOffice')
            customs_office_elem.text = self.customs_office
            elem.append(customs_office_elem)
        
        if self.customs_country_code:
            customs_country_elem = ET.Element('CustomsCountryCode')
            customs_country_elem.text = self.customs_country_code
            elem.append(customs_country_elem)
        
        if self.location_name:
            location_name_elem = ET.Element('LocationName')
            location_name_elem.text = self.location_name
            elem.append(location_name_elem)
        
        if self.register_document_id_details:
            elem.append(self.register_document_id_details.to_xml())
        
        if self.address:
            address_elem = ET.Element('Address')
            address_child = self.address.to_xml()
            for child in address_child:
                address_elem.append(child)
            elem.append(address_elem)
        
        return elem


# Классы для транспорта

class BorderCustomsOffice(IXMLElement):
    """Класс для catESAD_cu:BorderCustomsOffice"""
    
    def __init__(self, code: str = None, office_name: str = None, 
                 customs_country_code: str = None):
        self.code = code
        self.office_name = office_name
        self.customs_country_code = customs_country_code
    
    def to_xml(self) -> ET.Element:
        elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}BorderCustomsOffice')
        
        if self.code:
            code_elem = ET.Element('{urn:customs.ru:CommonAggregateTypes:5.24.0}Code')
            code_elem.text = self.code
            elem.append(code_elem)
        
        if self.office_name:
            office_name_elem = ET.Element('{urn:customs.ru:CommonAggregateTypes:5.24.0}OfficeName')
            office_name_elem.text = self.office_name
            elem.append(office_name_elem)
        
        if self.customs_country_code:
            country_code_elem = ET.Element('{urn:customs.ru:CommonAggregateTypes:5.24.0}CustomsCountryCode')
            country_code_elem.text = self.customs_country_code
            elem.append(country_code_elem)
        
        return elem


class RUTransportMeans(IXMLElement):
    """Класс для RUTransportMeans"""
    
    def __init__(self, transport_identifier: str = None,
                 transport_means_nationality_code: str = None,
                 active_transport_identifier: str = None):
        self.transport_identifier = transport_identifier
        self.transport_means_nationality_code = transport_means_nationality_code
        self.active_transport_identifier = active_transport_identifier
    
    def to_xml(self) -> ET.Element:
        elem = ET.Element('RUTransportMeans')
        
        if self.transport_identifier:
            transport_id_elem = ET.Element('{urn:customs.ru:CommonAggregateTypes:5.24.0}TransportIdentifier')
            transport_id_elem.text = self.transport_identifier
            elem.append(transport_id_elem)
        
        if self.transport_means_nationality_code:
            nationality_elem = ET.Element('{urn:customs.ru:CommonAggregateTypes:5.24.0}TransportMeansNationalityCode')
            nationality_elem.text = self.transport_means_nationality_code
            elem.append(nationality_elem)
        
        if self.active_transport_identifier:
            active_transport_elem = ET.Element('{urn:customs.ru:CommonAggregateTypes:5.24.0}ActiveTransportIdentifier')
            active_transport_elem.text = self.active_transport_identifier
            elem.append(active_transport_elem)
        
        return elem


class ESADout_CUDepartureArrivalTransport(IXMLElement):
    """Класс для ESADout_CUDepartureArrivalTransport"""
    
    def __init__(self, transport_mode_code: str = None,
                 transport_nationality_code: str = None,
                 transport_means_quantity: str = None,
                 ru_transport_means: List[RUTransportMeans] = None):
        self.transport_mode_code = transport_mode_code
        self.transport_nationality_code = transport_nationality_code
        self.transport_means_quantity = transport_means_quantity
        self.ru_transport_means = ru_transport_means or []
    
    def to_xml(self) -> ET.Element:
        elem = ET.Element('ESADout_CUDepartureArrivalTransport')
        
        if self.transport_mode_code:
            mode_code_elem = ET.Element('{urn:customs.ru:CommonAggregateTypes:5.24.0}TransportModeCode')
            mode_code_elem.text = self.transport_mode_code
            elem.append(mode_code_elem)
        
        if self.transport_nationality_code:
            nationality_elem = ET.Element('{urn:customs.ru:CommonAggregateTypes:5.24.0}TransportNationalityCode')
            nationality_elem.text = self.transport_nationality_code
            elem.append(nationality_elem)
        
        if self.transport_means_quantity:
            quantity_elem = ET.Element('TransportMeansQuantity')
            quantity_elem.text = self.transport_means_quantity
            elem.append(quantity_elem)
        
        for transport_means in self.ru_transport_means:
            elem.append(transport_means.to_xml())
        
        return elem


class ESADout_CUBorderTransport(IXMLElement):
    """Класс для ESADout_CUBorderTransport"""
    
    def __init__(self, transport_mode_code: str = None,
                 transport_means_quantity: str = None):
        self.transport_mode_code = transport_mode_code
        self.transport_means_quantity = transport_means_quantity
    
    def to_xml(self) -> ET.Element:
        elem = ET.Element('ESADout_CUBorderTransport')
        
        if self.transport_mode_code:
            mode_code_elem = ET.Element('{urn:customs.ru:CommonAggregateTypes:5.24.0}TransportModeCode')
            mode_code_elem.text = self.transport_mode_code
            elem.append(mode_code_elem)
        
        if self.transport_means_quantity:
            quantity_elem = ET.Element('TransportMeansQuantity')
            quantity_elem.text = self.transport_means_quantity
            elem.append(quantity_elem)
        
        return elem


class ESADout_CUConsigment(IXMLElement):
    """Класс для ESADout_CUConsigment (Партия товаров)"""
    
    def __init__(self, container_indicator: str = None,
                 dispatch_country_code: str = None, dispatch_country_name: str = None,
                 destination_country_code: str = None, destination_country_name: str = None,
                 border_customs_office: BorderCustomsOffice = None,
                 departure_arrival_transport: ESADout_CUDepartureArrivalTransport = None,
                 border_transport: ESADout_CUBorderTransport = None):
        self.container_indicator = container_indicator
        self.dispatch_country_code = dispatch_country_code
        self.dispatch_country_name = dispatch_country_name
        self.destination_country_code = destination_country_code
        self.destination_country_name = destination_country_name
        self.border_customs_office = border_customs_office
        self.departure_arrival_transport = departure_arrival_transport
        self.border_transport = border_transport
    
    def to_xml(self) -> ET.Element:
        elem = ET.Element('ESADout_CUConsigment')
        
        if self.container_indicator:
            container_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}ContainerIndicator')
            container_elem.text = self.container_indicator
            elem.append(container_elem)
        
        if self.dispatch_country_code:
            dispatch_code_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}DispatchCountryCode')
            dispatch_code_elem.text = self.dispatch_country_code
            elem.append(dispatch_code_elem)
        
        if self.dispatch_country_name:
            dispatch_name_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}DispatchCountryName')
            dispatch_name_elem.text = self.dispatch_country_name
            elem.append(dispatch_name_elem)
        
        if self.destination_country_code:
            dest_code_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}DestinationCountryCode')
            dest_code_elem.text = self.destination_country_code
            elem.append(dest_code_elem)
        
        if self.destination_country_name:
            dest_name_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}DestinationCountryName')
            dest_name_elem.text = self.destination_country_name
            elem.append(dest_name_elem)
        
        if self.border_customs_office:
            elem.append(self.border_customs_office.to_xml())
        
        if self.departure_arrival_transport:
            elem.append(self.departure_arrival_transport.to_xml())
        
        if self.border_transport:
            elem.append(self.border_transport.to_xml())
        
        return elem


# Классы для условий контракта

class CUESADDeliveryTerms(IXMLElement):
    """Класс для catESAD_cu:CUESADDeliveryTerms"""
    
    def __init__(self, delivery_place: str = None, delivery_terms_string_code: str = None):
        self.delivery_place = delivery_place
        self.delivery_terms_string_code = delivery_terms_string_code
    
    def to_xml(self) -> ET.Element:
        elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}CUESADDeliveryTerms')
        
        if self.delivery_place:
            place_elem = ET.Element('{urn:customs.ru:CommonAggregateTypes:5.24.0}DeliveryPlace')
            place_elem.text = self.delivery_place
            elem.append(place_elem)
        
        if self.delivery_terms_string_code:
            terms_elem = ET.Element('{urn:customs.ru:CommonAggregateTypes:5.24.0}DeliveryTermsStringCode')
            terms_elem.text = self.delivery_terms_string_code
            elem.append(terms_elem)
        
        return elem


class ESADout_CUMainContractTerms(IXMLElement):
    """Класс для ESADout_CUMainContractTerms (Условия контракта)"""
    
    def __init__(self, contract_currency_code: str = None,
                 contract_currency_rate: str = None, total_invoice_amount: str = None,
                 trade_country_code: str = None, deal_feature_code: str = None,
                 deal_nature_code: str = None,
                 cu_esad_delivery_terms: CUESADDeliveryTerms = None):
        self.contract_currency_code = contract_currency_code
        self.contract_currency_rate = contract_currency_rate
        self.total_invoice_amount = total_invoice_amount
        self.trade_country_code = trade_country_code
        self.deal_feature_code = deal_feature_code
        self.deal_nature_code = deal_nature_code
        self.cu_esad_delivery_terms = cu_esad_delivery_terms
    
    def to_xml(self) -> ET.Element:
        elem = ET.Element('ESADout_CUMainContractTerms')
        
        if self.contract_currency_code:
            currency_code_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}ContractCurrencyCode')
            currency_code_elem.text = self.contract_currency_code
            elem.append(currency_code_elem)
        
        if self.contract_currency_rate:
            currency_rate_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}ContractCurrencyRate')
            currency_rate_elem.text = self.contract_currency_rate
            elem.append(currency_rate_elem)
        
        if self.total_invoice_amount:
            invoice_amount_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}TotalInvoiceAmount')
            invoice_amount_elem.text = self.total_invoice_amount
            elem.append(invoice_amount_elem)
        
        if self.trade_country_code:
            trade_country_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}TradeCountryCode')
            trade_country_elem.text = self.trade_country_code
            elem.append(trade_country_elem)
        
        if self.deal_feature_code:
            deal_feature_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}DealFeatureCode')
            deal_feature_elem.text = self.deal_feature_code
            elem.append(deal_feature_elem)
        
        if self.deal_nature_code:
            deal_nature_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}DealNatureCode')
            deal_nature_elem.text = self.deal_nature_code
            elem.append(deal_nature_elem)
        
        if self.cu_esad_delivery_terms:
            elem.append(self.cu_esad_delivery_terms.to_xml())
        
        return elem


# Классы для товаров

class GoodsGroupQuantity(IXMLElement):
    """Класс для catESAD_cu:GoodsGroupQuantity"""
    
    def __init__(self, goods_quantity: str = None, measure_unit_qualifier_name: str = None,
                 measure_unit_qualifier_code: str = None):
        self.goods_quantity = goods_quantity
        self.measure_unit_qualifier_name = measure_unit_qualifier_name
        self.measure_unit_qualifier_code = measure_unit_qualifier_code
    
    def to_xml(self) -> ET.Element:
        elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}GoodsGroupQuantity')
        
        if self.goods_quantity:
            quantity_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}GoodsQuantity')
            quantity_elem.text = self.goods_quantity
            elem.append(quantity_elem)
        
        if self.measure_unit_qualifier_name:
            name_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}MeasureUnitQualifierName')
            name_elem.text = self.measure_unit_qualifier_name
            elem.append(name_elem)
        
        if self.measure_unit_qualifier_code:
            code_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}MeasureUnitQualifierCode')
            code_elem.text = self.measure_unit_qualifier_code
            elem.append(code_elem)
        
        return elem


class GoodsGroupInformation(IXMLElement):
    """Класс для catESAD_cu:GoodsGroupInformation"""
    
    def __init__(self, manufacturer: str = None, goods_mark: str = None,
                 goods_model: str = None, goods_marking: str = None,
                 serial_number: str = None, goods_group_quantity: GoodsGroupQuantity = None,
                 invoiced_cost: str = None):
        self.manufacturer = manufacturer
        self.goods_mark = goods_mark
        self.goods_model = goods_model
        self.goods_marking = goods_marking
        self.serial_number = serial_number
        self.goods_group_quantity = goods_group_quantity
        self.invoiced_cost = invoiced_cost
    
    def to_xml(self) -> ET.Element:
        elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}GoodsGroupInformation')
        
        if self.manufacturer:
            manufacturer_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}Manufacturer')
            manufacturer_elem.text = self.manufacturer
            elem.append(manufacturer_elem)
        
        if self.goods_mark:
            mark_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}GoodsMark')
            mark_elem.text = self.goods_mark
            elem.append(mark_elem)
        
        if self.goods_model:
            model_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}GoodsModel')
            model_elem.text = self.goods_model
            elem.append(model_elem)
        
        if self.goods_marking:
            marking_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}GoodsMarking')
            marking_elem.text = self.goods_marking
            elem.append(marking_elem)
        
        if self.serial_number:
            serial_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}SerialNumber')
            serial_elem.text = self.serial_number
            elem.append(serial_elem)
        
        if self.goods_group_quantity:
            elem.append(self.goods_group_quantity.to_xml())
        
        if self.invoiced_cost:
            cost_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}InvoicedCost')
            cost_elem.text = self.invoiced_cost
            elem.append(cost_elem)
        
        return elem


class GoodsGroupDescription(IXMLElement):
    """Класс для catESAD_cu:GoodsGroupDescription"""
    
    def __init__(self, goods_description: str = None,
                 goods_group_information: GoodsGroupInformation = None,
                 group_num: str = None):
        self.goods_description = goods_description
        self.goods_group_information = goods_group_information
        self.group_num = group_num
    
    def to_xml(self) -> ET.Element:
        elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}GoodsGroupDescription')
        
        if self.goods_description:
            desc_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}GoodsDescription')
            desc_elem.text = self.goods_description
            elem.append(desc_elem)
        
        if self.goods_group_information:
            elem.append(self.goods_group_information.to_xml())
        
        if self.group_num:
            num_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}GroupNum')
            num_elem.text = self.group_num
            elem.append(num_elem)
        
        return elem


class Preferencii(IXMLElement):
    """Класс для catESAD_cu:Preferencii"""
    
    def __init__(self, customs_tax: str = None, customs_duty: str = None,
                 excise: str = None, rate: str = None):
        self.customs_tax = customs_tax
        self.customs_duty = customs_duty
        self.excise = excise
        self.rate = rate
    
    def to_xml(self) -> ET.Element:
        elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}Preferencii')
        
        if self.customs_tax:
            tax_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}CustomsTax')
            tax_elem.text = self.customs_tax
            elem.append(tax_elem)
        
        if self.customs_duty:
            duty_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}CustomsDuty')
            duty_elem.text = self.customs_duty
            elem.append(duty_elem)
        
        if self.excise:
            excise_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}Excise')
            excise_elem.text = self.excise
            elem.append(excise_elem)
        
        if self.rate:
            rate_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}Rate')
            rate_elem.text = self.rate
            elem.append(rate_elem)
        
        return elem


class CustomsDocIdDetails(IXMLElement):
    """Класс для RUDECLcat:CustomsDocIdDetails"""
    
    def __init__(self, customs_code: str = None, registration_date: str = None,
                 gtd_number: str = None):
        self.customs_code = customs_code
        self.registration_date = registration_date
        self.gtd_number = gtd_number
    
    def to_xml(self) -> ET.Element:
        elem = ET.Element('{urn:customs.ru:RUSCommonAggregateTypes:5.24.0}CustomsDocIdDetails')
        
        if self.customs_code:
            code_elem = ET.Element('{urn:customs.ru:CommonAggregateTypes:5.24.0}CustomsCode')
            code_elem.text = self.customs_code
            elem.append(code_elem)
        
        if self.registration_date:
            date_elem = ET.Element('{urn:customs.ru:CommonAggregateTypes:5.24.0}RegistrationDate')
            date_elem.text = self.registration_date
            elem.append(date_elem)
        
        if self.gtd_number:
            gtd_elem = ET.Element('{urn:customs.ru:CommonAggregateTypes:5.24.0}GTDNumber')
            gtd_elem.text = self.gtd_number
            elem.append(gtd_elem)
        
        return elem


class DocumentPresentingDetails(IXMLElement):
    """Класс для RUDECLcat:DocumentPresentingDetails"""
    
    def __init__(self, doc_present_kind_code: str = None,
                 presented_document_mode_code: str = None,
                 customs_doc_id_details: CustomsDocIdDetails = None):
        self.doc_present_kind_code = doc_present_kind_code
        self.presented_document_mode_code = presented_document_mode_code
        self.customs_doc_id_details = customs_doc_id_details
    
    def to_xml(self) -> ET.Element:
        elem = ET.Element('{urn:customs.ru:RUSCommonAggregateTypes:5.24.0}DocumentPresentingDetails')
        
        if self.doc_present_kind_code:
            kind_elem = ET.Element('{urn:customs.ru:RUSCommonAggregateTypes:5.24.0}DocPresentKindCode')
            kind_elem.text = self.doc_present_kind_code
            elem.append(kind_elem)
        
        if self.presented_document_mode_code:
            mode_elem = ET.Element('{urn:customs.ru:RUSCommonAggregateTypes:5.24.0}PresentedDocumentModeCode')
            mode_elem.text = self.presented_document_mode_code
            elem.append(mode_elem)
        
        if self.customs_doc_id_details:
            elem.append(self.customs_doc_id_details.to_xml())
        
        return elem


class RFG44PresentedDocId(IXMLElement):
    """Класс для catESAD_cu:RFG44PresentedDocId"""
    
    def __init__(self, electronic_document_id: str = None,
                 electronic_arch_id: str = None, document_mode_id: str = None):
        self.electronic_document_id = electronic_document_id
        self.electronic_arch_id = electronic_arch_id
        self.document_mode_id = document_mode_id
    
    def to_xml(self) -> ET.Element:
        elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}RFG44PresentedDocId')
        
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


class ESADout_CUPresentedDocument(IXMLElement):
    """Класс для ESADout_CUPresentedDocument (Представленные документы)"""
    
    def __init__(self, pr_document_name: str = None, pr_document_number: str = None,
                 pr_document_date: str = None, presented_document_mode_code: str = None,
                 document_begin_actions_date: str = None, document_end_actions_date: str = None,
                 record_id: str = None, rfg44_presented_doc_id: RFG44PresentedDocId = None,
                 document_presenting_details: DocumentPresentingDetails = None):
        self.pr_document_name = pr_document_name
        self.pr_document_number = pr_document_number
        self.pr_document_date = pr_document_date
        self.presented_document_mode_code = presented_document_mode_code
        self.document_begin_actions_date = document_begin_actions_date
        self.document_end_actions_date = document_end_actions_date
        self.record_id = record_id
        self.rfg44_presented_doc_id = rfg44_presented_doc_id
        self.document_presenting_details = document_presenting_details
    
    def to_xml(self) -> ET.Element:
        elem = ET.Element('ESADout_CUPresentedDocument')
        
        if self.pr_document_name:
            name_elem = ET.Element('{urn:customs.ru:CommonAggregateTypes:5.24.0}PrDocumentName')
            name_elem.text = self.pr_document_name
            elem.append(name_elem)
        
        if self.pr_document_number:
            number_elem = ET.Element('{urn:customs.ru:CommonAggregateTypes:5.24.0}PrDocumentNumber')
            number_elem.text = self.pr_document_number
            elem.append(number_elem)
        
        if self.pr_document_date:
            date_elem = ET.Element('{urn:customs.ru:CommonAggregateTypes:5.24.0}PrDocumentDate')
            date_elem.text = self.pr_document_date
            elem.append(date_elem)
        
        if self.presented_document_mode_code:
            mode_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}PresentedDocumentModeCode')
            mode_elem.text = self.presented_document_mode_code
            elem.append(mode_elem)
        
        if self.document_begin_actions_date:
            begin_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}DocumentBeginActionsDate')
            begin_elem.text = self.document_begin_actions_date
            elem.append(begin_elem)
        
        if self.document_end_actions_date:
            end_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}DocumentEndActionsDate')
            end_elem.text = self.document_end_actions_date
            elem.append(end_elem)
        
        if self.record_id:
            record_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}RecordID')
            record_elem.text = self.record_id
            elem.append(record_elem)
        
        if self.rfg44_presented_doc_id:
            elem.append(self.rfg44_presented_doc_id.to_xml())
        
        if self.document_presenting_details:
            elem.append(self.document_presenting_details.to_xml())
        
        return elem


class ESADout_CUCustomsPaymentCalculation(IXMLElement):
    """Класс для ESADout_CUCustomsPaymentCalculation (Расчет платежей)"""
    
    def __init__(self, payment_mode_code: str = None, payment_amount: str = None,
                 payment_currency_code: str = None, tax_base: str = None,
                 tax_base_currency_code: str = None, rate: str = None,
                 rate_type_code: str = None, rate_currency_code: str = None,
                 rate_use_date: str = None, payment_code: str = None):
        self.payment_mode_code = payment_mode_code
        self.payment_amount = payment_amount
        self.payment_currency_code = payment_currency_code
        self.tax_base = tax_base
        self.tax_base_currency_code = tax_base_currency_code
        self.rate = rate
        self.rate_type_code = rate_type_code
        self.rate_currency_code = rate_currency_code
        self.rate_use_date = rate_use_date
        self.payment_code = payment_code
    
    def to_xml(self) -> ET.Element:
        elem = ET.Element('ESADout_CUCustomsPaymentCalculation')
        
        if self.payment_mode_code:
            mode_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}PaymentModeCode')
            mode_elem.text = self.payment_mode_code
            elem.append(mode_elem)
        
        if self.payment_amount:
            amount_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}PaymentAmount')
            amount_elem.text = self.payment_amount
            elem.append(amount_elem)
        
        if self.payment_currency_code:
            currency_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}PaymentCurrencyCode')
            currency_elem.text = self.payment_currency_code
            elem.append(currency_elem)
        
        if self.tax_base:
            base_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}TaxBase')
            base_elem.text = self.tax_base
            elem.append(base_elem)
        
        if self.tax_base_currency_code:
            base_currency_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}TaxBaseCurrencyCode')
            base_currency_elem.text = self.tax_base_currency_code
            elem.append(base_currency_elem)
        
        if self.rate:
            rate_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}Rate')
            rate_elem.text = self.rate
            elem.append(rate_elem)
        
        if self.rate_type_code:
            rate_type_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}RateTypeCode')
            rate_type_elem.text = self.rate_type_code
            elem.append(rate_type_elem)
        
        if self.rate_currency_code:
            rate_currency_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}RateCurrencyCode')
            rate_currency_elem.text = self.rate_currency_code
            elem.append(rate_currency_elem)
        
        if self.rate_use_date:
            date_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}RateUseDate')
            date_elem.text = self.rate_use_date
            elem.append(date_elem)
        
        if self.payment_code:
            code_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}PaymentCode')
            code_elem.text = self.payment_code
            elem.append(code_elem)
        
        return elem


class SupplementaryGoodsQuantity(IXMLElement):
    """Класс для SupplementaryGoodsQuantity"""
    
    def __init__(self, goods_quantity: str = None, measure_unit_qualifier_name: str = None,
                 measure_unit_qualifier_code: str = None):
        self.goods_quantity = goods_quantity
        self.measure_unit_qualifier_name = measure_unit_qualifier_name
        self.measure_unit_qualifier_code = measure_unit_qualifier_code
    
    def to_xml(self) -> ET.Element:
        elem = ET.Element('SupplementaryGoodsQuantity')
        
        if self.goods_quantity:
            quantity_elem = ET.Element('{urn:customs.ru:CommonAggregateTypes:5.24.0}GoodsQuantity')
            quantity_elem.text = self.goods_quantity
            elem.append(quantity_elem)
        
        if self.measure_unit_qualifier_name:
            name_elem = ET.Element('{urn:customs.ru:CommonAggregateTypes:5.24.0}MeasureUnitQualifierName')
            name_elem.text = self.measure_unit_qualifier_name
            elem.append(name_elem)
        
        if self.measure_unit_qualifier_code:
            code_elem = ET.Element('{urn:customs.ru:CommonAggregateTypes:5.24.0}MeasureUnitQualifierCode')
            code_elem.text = self.measure_unit_qualifier_code
            elem.append(code_elem)
        
        return elem


class PackagePalleteInformation(IXMLElement):
    """Класс для catESAD_cu:PackagePalleteInformation"""
    
    def __init__(self, info_kind_code: str = None, pallete_code: str = None,
                 pallete_quantity: str = None, cargo_description_text: str = None):
        self.info_kind_code = info_kind_code
        self.pallete_code = pallete_code
        self.pallete_quantity = pallete_quantity
        self.cargo_description_text = cargo_description_text
    
    def to_xml(self) -> ET.Element:
        elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}PackagePalleteInformation')
        
        if self.info_kind_code:
            kind_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}InfoKindCode')
            kind_elem.text = self.info_kind_code
            elem.append(kind_elem)
        
        if self.pallete_code:
            code_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}PalleteCode')
            code_elem.text = self.pallete_code
            elem.append(code_elem)
        
        if self.pallete_quantity:
            quantity_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}PalleteQuantity')
            quantity_elem.text = self.pallete_quantity
            elem.append(quantity_elem)
        
        if self.cargo_description_text:
            desc_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}CargoDescriptionText')
            desc_elem.text = self.cargo_description_text
            elem.append(desc_elem)
        
        return elem


class ESADGoodsPackaging(IXMLElement):
    """Класс для ESADGoodsPackaging"""
    
    def __init__(self, pakage_quantity: str = None, pakage_type_code: str = None,
                 package_pallete_information: List[PackagePalleteInformation] = None):
        self.pakage_quantity = pakage_quantity
        self.pakage_type_code = pakage_type_code
        self.package_pallete_information = package_pallete_information or []
    
    def to_xml(self) -> ET.Element:
        elem = ET.Element('ESADGoodsPackaging')
        
        if self.pakage_quantity:
            quantity_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}PakageQuantity')
            quantity_elem.text = self.pakage_quantity
            elem.append(quantity_elem)
        
        if self.pakage_type_code:
            type_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}PakageTypeCode')
            type_elem.text = self.pakage_type_code
            elem.append(type_elem)
        
        for pallete_info in self.package_pallete_information:
            elem.append(pallete_info.to_xml())
        
        return elem


class ESADCustomsProcedure(IXMLElement):
    """Класс для ESADCustomsProcedure"""
    
    def __init__(self, main_customs_mode_code: str = None,
                 preceding_customs_mode_code: str = None,
                 goods_transfer_feature: str = None):
        self.main_customs_mode_code = main_customs_mode_code
        self.preceding_customs_mode_code = preceding_customs_mode_code
        self.goods_transfer_feature = goods_transfer_feature
    
    def to_xml(self) -> ET.Element:
        elem = ET.Element('ESADCustomsProcedure')
        
        if self.main_customs_mode_code:
            main_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}MainCustomsModeCode')
            main_elem.text = self.main_customs_mode_code
            elem.append(main_elem)
        
        if self.preceding_customs_mode_code:
            preceding_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}PrecedingCustomsModeCode')
            preceding_elem.text = self.preceding_customs_mode_code
            elem.append(preceding_elem)
        
        if self.goods_transfer_feature:
            transfer_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}GoodsTransferFeature')
            transfer_elem.text = self.goods_transfer_feature
            elem.append(transfer_elem)
        
        return elem


class ESADout_CUGoods(IXMLElement):  
    def __init__(
        self,
        goods_numeric: str = None,
        goods_descriptions: str = None,  #
        gross_weight_quantity: str = None,
        net_weight_quantity: str = None,
        invoiced_cost: str = None,
        customs_cost: str = None,
        statistical_cost: str = None,
        goods_tnved_code: str = None,
        additions_sign: str = None,
        intellect_property_sign: str = None,
        origin_country_code: str = None,
        customs_cost_correct_method: str = None,
        additional_sheet_count: str = None,
        goods_group_description=None,
        preferencii: Preferencii = None,
        language_goods: str = None,
        presented_documents: List[ESADout_CUPresentedDocument] = None,
        customs_payment_calculations: List[ESADout_CUCustomsPaymentCalculation] = None,
        supplementary_goods_quantity: SupplementaryGoodsQuantity = None,
        esad_goods_packaging: ESADGoodsPackaging = None,
        esad_customs_procedure: ESADCustomsProcedure = None,
    ):
        self.goods_numeric = goods_numeric

        if goods_descriptions is None:
            self.goods_descriptions: List[str] = []
        elif isinstance(goods_descriptions, (list, tuple)):
            self.goods_descriptions = list(goods_descriptions)
        else:
            self.goods_descriptions = [str(goods_descriptions)]

        self.gross_weight_quantity = gross_weight_quantity
        self.net_weight_quantity = net_weight_quantity
        self.invoiced_cost = invoiced_cost
        self.customs_cost = customs_cost
        self.statistical_cost = statistical_cost
        self.goods_tnved_code = goods_tnved_code
        self.intellect_property_sign = intellect_property_sign
        self.origin_country_code = origin_country_code
        self.customs_cost_correct_method = customs_cost_correct_method
        self.additional_sheet_count = additional_sheet_count

        if goods_group_description is None:
            self.goods_group_description: List[GoodsGroupDescription] = []
        elif isinstance(goods_group_description, (list, tuple)):
            self.goods_group_description = list(goods_group_description)
        else:
            self.goods_group_description = [goods_group_description]

        self.preferencii = preferencii
        self.language_goods = language_goods
        self.presented_documents = presented_documents or []
        self.customs_payment_calculations = customs_payment_calculations or []
        self.supplementary_goods_quantity = supplementary_goods_quantity
        self.esad_goods_packaging = esad_goods_packaging
        self.esad_customs_procedure = esad_customs_procedure
    
    def to_xml(self) -> ET.Element:
        elem = ET.Element('ESADout_CUGoods')
        
        if self.goods_numeric:
            numeric_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}GoodsNumeric')
            numeric_elem.text = self.goods_numeric
            elem.append(numeric_elem)
        
        for desc in self.goods_descriptions:
            desc_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}GoodsDescription')
            desc_elem.text = desc
            elem.append(desc_elem)
        
        if self.gross_weight_quantity:
            gross_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}GrossWeightQuantity')
            gross_elem.text = self.gross_weight_quantity
            elem.append(gross_elem)
        
        if self.net_weight_quantity:
            net_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}NetWeightQuantity')
            net_elem.text = self.net_weight_quantity
            elem.append(net_elem)
        
        if self.invoiced_cost:
            invoiced_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}InvoicedCost')
            invoiced_elem.text = self.invoiced_cost
            elem.append(invoiced_elem)
        
        if self.customs_cost:
            customs_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}CustomsCost')
            customs_elem.text = self.customs_cost
            elem.append(customs_elem)
        
        if self.statistical_cost:
            stat_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}StatisticalCost')
            stat_elem.text = self.statistical_cost
            elem.append(stat_elem)
        
        if self.goods_tnved_code:
            tnved_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}GoodsTNVEDCode')
            tnved_elem.text = self.goods_tnved_code
            elem.append(tnved_elem)
        
        if self.intellect_property_sign:
            intellect_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}IntellectPropertySign')
            intellect_elem.text = self.intellect_property_sign
            elem.append(intellect_elem)
        
        if self.origin_country_code:
            origin_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}OriginCountryCode')
            origin_elem.text = self.origin_country_code
            elem.append(origin_elem)
        
        if self.customs_cost_correct_method:
            method_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}CustomsCostCorrectMethod')
            method_elem.text = self.customs_cost_correct_method
            elem.append(method_elem)
        
        if self.additional_sheet_count:
            sheet_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}AdditionalSheetCount')
            sheet_elem.text = self.additional_sheet_count
            elem.append(sheet_elem)
        
        for ggd in self.goods_group_description:
            elem.append(ggd.to_xml())
        
        if self.preferencii:
            elem.append(self.preferencii.to_xml())
        
        if self.language_goods:
            lang_elem = ET.Element('LanguageGoods')
            lang_elem.text = self.language_goods
            elem.append(lang_elem)
        
        for doc in self.presented_documents:
            elem.append(doc.to_xml())
        
        for calc in self.customs_payment_calculations:
            elem.append(calc.to_xml())
        
        if self.supplementary_goods_quantity:
            elem.append(self.supplementary_goods_quantity.to_xml())
        
        if self.esad_goods_packaging:
            elem.append(self.esad_goods_packaging.to_xml())
        
        if self.esad_customs_procedure:
            elem.append(self.esad_customs_procedure.to_xml())
        
        return elem



# Классы для платежей

class RFOrganizationFeaturesSimple(IXMLElement):
    """Класс для RFOrganizationFeatures (упрощенный для платежей)"""
    
    def __init__(self, inn: str = None):
        self.inn = inn
    
    def to_xml(self) -> ET.Element:
        elem = ET.Element('RFOrganizationFeatures')
        
        if self.inn:
            inn_elem = ET.Element('{urn:customs.ru:CommonAggregateTypes:5.24.0}INN')
            inn_elem.text = self.inn
            elem.append(inn_elem)
        
        return elem


class ESADout_CUCustomsPayment(IXMLElement):
    """Класс для ESADout_CUCustomsPayment"""
    
    def __init__(self, payment_mode_code: str = None, payment_amount: str = None,
                 payment_currency_code: str = None, currency_rate: str = None,
                 rf_organization_features: RFOrganizationFeaturesSimple = None):
        self.payment_mode_code = payment_mode_code
        self.payment_amount = payment_amount
        self.payment_currency_code = payment_currency_code
        self.currency_rate = currency_rate
        self.rf_organization_features = rf_organization_features
    
    def to_xml(self) -> ET.Element:
        elem = ET.Element('ESADout_CUCustomsPayment')
        
        if self.payment_mode_code:
            mode_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}PaymentModeCode')
            mode_elem.text = self.payment_mode_code
            elem.append(mode_elem)
        
        if self.payment_amount:
            amount_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}PaymentAmount')
            amount_elem.text = self.payment_amount
            elem.append(amount_elem)
        
        if self.payment_currency_code:
            currency_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}PaymentCurrencyCode')
            currency_elem.text = self.payment_currency_code
            elem.append(currency_elem)
        
        if self.currency_rate:
            rate_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}CurrencyRate')
            rate_elem.text = self.currency_rate
            elem.append(rate_elem)
        
        if self.rf_organization_features:
            elem.append(self.rf_organization_features.to_xml())
        
        return elem


class ESADout_CUPayments(IXMLElement):
    """Класс для ESADout_CUPayments"""
    
    def __init__(self, customs_payments: List[ESADout_CUCustomsPayment] = None):
        self.customs_payments = customs_payments or []
    
    def to_xml(self) -> ET.Element:
        elem = ET.Element('ESADout_CUPayments')
        
        for payment in self.customs_payments:
            elem.append(payment.to_xml())
        
        return elem


# Главный класс

class ESADout_CUGoodsShipment(IXMLElement):
    """Класс для ESADout_CUGoodsShipment (Партия товаров в декларации)"""
    
    def __init__(self, origin_country_name: str = None, origin_country_code: str = None,
                 total_goods_number: str = None, total_package_number: str = None,
                 total_sheet_number: str = None, total_cust_cost: str = None,
                 cust_cost_currency_code: str = None,
                 consignor: ESADout_CUConsignor = None,
                 consignee: ESADout_CUConsignee = None,
                 financial_adjusting_responsible_person: ESADout_CUFinancialAdjustingResponsiblePerson = None,
                 declarant: ESADout_CUDeclarant = None,
                 goods_location: ESADout_CUGoodsLocation = None,
                 consigment: ESADout_CUConsigment = None,
                 main_contract_terms: ESADout_CUMainContractTerms = None,
                 goods_list: List[ESADout_CUGoods] = None,
                 payments: ESADout_CUPayments = None):
        self.origin_country_name = origin_country_name
        self.origin_country_code = origin_country_code
        self.total_goods_number = total_goods_number
        self.total_package_number = total_package_number
        self.total_sheet_number = total_sheet_number
        self.total_cust_cost = total_cust_cost
        self.cust_cost_currency_code = cust_cost_currency_code
        self.consignor = consignor
        self.consignee = consignee
        self.financial_adjusting_responsible_person = financial_adjusting_responsible_person
        self.declarant = declarant
        self.goods_location = goods_location
        self.consigment = consigment
        self.main_contract_terms = main_contract_terms
        self.goods_list = goods_list or []
        self.payments = payments
    
    def to_xml(self) -> ET.Element:
        # Определяем namespace map для корневого элемента
        nsmap = {
            None: 'urn:customs.ru:Information:CustomsDocuments:ESADout_CU:5.24.0',
            'RUScat_ru': 'urn:customs.ru:RUSCommonAggregateTypes:5.24.0',
            'catESAD_cu': 'urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0',
            'cat_ru': 'urn:customs.ru:CommonAggregateTypes:5.24.0',
            'RUDECLcat': 'urn:customs.ru:RUDeclCommonAggregateTypesCust:5.24.0',
        }

        elem = ET.Element('ESADout_CUGoodsShipment', nsmap=nsmap)
        
        if self.origin_country_name:
            country_name_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}OriginCountryName')
            country_name_elem.text = self.origin_country_name
            elem.append(country_name_elem)
        
        if self.origin_country_code:
            country_code_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}OriginCountryCode')
            country_code_elem.text = self.origin_country_code
            elem.append(country_code_elem)
        
        if self.total_goods_number:
            goods_num_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}TotalGoodsNumber')
            goods_num_elem.text = self.total_goods_number
            elem.append(goods_num_elem)
        
        if self.total_package_number:
            package_num_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}TotalPackageNumber')
            package_num_elem.text = self.total_package_number
            elem.append(package_num_elem)
        
        if self.total_sheet_number:
            sheet_num_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}TotalSheetNumber')
            sheet_num_elem.text = self.total_sheet_number
            elem.append(sheet_num_elem)
        
        if self.total_cust_cost:
            cost_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}TotalCustCost')
            cost_elem.text = self.total_cust_cost
            elem.append(cost_elem)
        
        if self.cust_cost_currency_code:
            currency_elem = ET.Element('{urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0}CustCostCurrencyCode')
            currency_elem.text = self.cust_cost_currency_code
            elem.append(currency_elem)
        
        if self.consignor:
            elem.append(self.consignor.to_xml())
        
        if self.consignee:
            elem.append(self.consignee.to_xml())
        
        if self.financial_adjusting_responsible_person:
            elem.append(self.financial_adjusting_responsible_person.to_xml())
        
        if self.declarant:
            elem.append(self.declarant.to_xml())
        
        if self.goods_location:
            elem.append(self.goods_location.to_xml())
        
        if self.consigment:
            elem.append(self.consigment.to_xml())
        
        if self.main_contract_terms:
            elem.append(self.main_contract_terms.to_xml())
        
        for goods in self.goods_list:
            elem.append(goods.to_xml())
        
        if self.payments:
            elem.append(self.payments.to_xml())
        
        ET.register_namespace('RUScat_ru', 'urn:customs.ru:RUSCommonAggregateTypes:5.24.0')
        ET.register_namespace('cat_ru', 'urn:customs.ru:CommonAggregateTypes:5.24.0')
        ET.register_namespace('RUDECLcat', 'urn:customs.ru:RUDeclCommonAggregateTypesCust:5.24.0')
        ET.register_namespace('catESAD_cu', 'urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0')
#        ET.register_namespace('xmlns', 'customs.ru:Information:CustomsDocuments:ESADout_CU:5.24.0')
        return elem



def get_goods_shipment():
    # Создаем адрес отправителя
    consignor_address = SubjectAddressDetails(
        postal_code="518111",
        country_code="CN",
        country_name="КИТАЙ",
        region="SHENZHEN",
        city="LONGGANG, PINGHU, CHINA SOUTH CITY",
        street_house="TIEDONG,",
        house="BUILDING 12, NO.510"
    )
    
    # Создаем отправителя
    consignor = ESADout_CUConsignor(
        organization_name="SHENZHEN NEWIN MACHINERY CO., LTD.",
        subject_address_details=consignor_address
    )
    
    # Создаем получателя
    consignee = ESADout_CUConsignee(equal_indicator="1")
    
    # Создаем финансово ответственное лицо
    financial_person = ESADout_CUFinancialAdjustingResponsiblePerson(declarant_equal_flag="1")
    
    # Создаем декларанта
    declarant_rf_features = RFOrganizationFeatures(
        ogrn="1027726002260",
        inn="7726319569",
        kpp="772301001"
    )
    
    declarant_address = SubjectAddressDetails(
        postal_code="109559",
        country_code="RU",
        country_name="РОССИЯ",
        city="ГОРОД МОСКВА",
        street_house="Б-Р ТИХОРЕЦКИЙ",
        house="Д.1, СТР.5, ЭТАЖ 6 КОМНАТА 15"
    )
    
    declarant = ESADout_CUDeclarant(
        organization_name='ООО "ТРЭЙД ИНВЕСТМЕНТС"',
        rf_organization_features=declarant_rf_features,
        subject_address_details=declarant_address
    )
    
    # Создаем местоположение товаров
    location_address = SubjectAddressDetails(
        country_code="RU",
        country_name="РОССИЯ",
        region="МОСКОВСКАЯ ОБЛАСТЬ",
        city="ГОРОД КРАСНОЗНАМЕНСК",
        street_house="УЛ БЕРЕЗОВАЯ АЛЛЕЯ",
        house="Д. 5"
    )
    
    register_doc = RegisterDocumentIdDetails(doc_id="10013/200111/10045/7")
    
    goods_location = ESADout_CUGoodsLocation(
        information_type_code="11",
        customs_office="10013070",
        customs_country_code="RU",
        location_name='ООО "ОСТ-ТЕРМИНАЛ"',
        register_document_id_details=register_doc,
        address=location_address
    )
    
    # Создаем транспорт
    border_office = BorderCustomsOffice(
        code="10719110",
        office_name="Т/П МАПП ЗАБАЙКАЛЬСК",
        customs_country_code="643"
    )
    
    transport_means_1 = RUTransportMeans(
        transport_identifier="B072HM138",
        transport_means_nationality_code="RU"
    )
    
    transport_means_2 = RUTransportMeans(
        transport_identifier="AX262138",
        transport_means_nationality_code="RU",
        active_transport_identifier="B072HM138"
    )
    
    departure_transport = ESADout_CUDepartureArrivalTransport(
        transport_mode_code="31",
        transport_nationality_code="RU",
        transport_means_quantity="2",
        ru_transport_means=[transport_means_1, transport_means_2]
    )
    
    border_transport = ESADout_CUBorderTransport(
        transport_mode_code="31",
        transport_means_quantity="1"
    )
    
    consigment = ESADout_CUConsigment(
        container_indicator="0",
        dispatch_country_code="CN",
        dispatch_country_name="КИТАЙ",
        destination_country_code="RU",
        destination_country_name="РОССИЯ",
        border_customs_office=border_office,
        departure_arrival_transport=departure_transport,
        border_transport=border_transport
    )
    
    # Создаем условия контракта
    delivery_terms = CUESADDeliveryTerms(
        delivery_place="NEWIN FACTORY, HUIZHOU",
        delivery_terms_string_code="EXW"
    )
    
    contract_terms = ESADout_CUMainContractTerms(
        contract_currency_code="CNY",
        contract_currency_rate="11.0648",
        total_invoice_amount="330100",
        trade_country_code="CN",
        deal_feature_code="00",
        deal_nature_code="010",
        cu_esad_delivery_terms=delivery_terms
    )
    
    # Создаем товар (упрощенная версия)
    goods_group_qty = GoodsGroupQuantity(
        goods_quantity="1",
        measure_unit_qualifier_name="ШТ",
        measure_unit_qualifier_code="796"
    )
    
    goods_group_info = GoodsGroupInformation(
        manufacturer="SHENZHEN NEWIN MACHINERY CO., LTD.",
        goods_mark="NEWIN",
        goods_model="NWFL",
        goods_marking="NWFL-750VA",
        serial_number="GW2024130-49-1-23",
        goods_group_quantity=goods_group_qty,
        invoiced_cost="330100"
    )
    
    goods_group_desc = GoodsGroupDescription(
        goods_description=": ГРАДИРНИ ОХЛАЖДАЮЩИЕ, ЗАКРЫТОГО ТИПА",
        goods_group_information=goods_group_info,
        group_num="1"
    )
    
    preferencii = Preferencii(
        customs_tax="ОО",
        customs_duty="ОО",
        excise="-",
        rate="ОО"
    )
    
    # Создаем документ (один пример)
    doc_presenting = DocumentPresentingDetails(
        doc_present_kind_code="0",
        presented_document_mode_code="01402"
    )
    
    presented_doc = ESADout_CUPresentedDocument(
        pr_document_name="ДЕКЛАРАЦИЯ О СООТВЕТСТВИИ ТРЕБОВАНИЯМ ТР ЕАЭС",
        pr_document_number="ЕАЭС N RU Д-CN.РА10.В.35527/23",
        pr_document_date="2023-12-05",
        presented_document_mode_code="01402",
        document_begin_actions_date="2023-12-05",
        document_end_actions_date="2028-12-04",
        record_id="5CC73D96-1F95-4073-BBD5-C12C23538175",
        document_presenting_details=doc_presenting
    )
    
    # Создаем расчет платежей
    payment_calc_1 = ESADout_CUCustomsPaymentCalculation(
        payment_mode_code="1010",
        payment_amount="16524",
        payment_currency_code="643",
        tax_base="3982409.34",
        tax_base_currency_code="643",
        rate="16524",
        rate_type_code="*",
        rate_currency_code="643",
        rate_use_date="2025-04-22",
        payment_code="ИУ"
    )
    
    payment_calc_2 = ESADout_CUCustomsPaymentCalculation(
        payment_mode_code="5010",
        payment_amount="796481.87",
        payment_currency_code="643",
        tax_base="3982409.34",
        tax_base_currency_code="643",
        rate="20",
        rate_type_code="%",
        rate_use_date="2025-04-22",
        payment_code="ИУ"
    )
    
    # Создаем упаковку
    pallete_info_1 = PackagePalleteInformation(
        info_kind_code="0",
        pallete_code="PK",
        pallete_quantity="5"
    )
    
    pallete_info_2 = PackagePalleteInformation(
        info_kind_code="3",
        pallete_code="PX",
        pallete_quantity="5",
        cargo_description_text="."
    )
    
    goods_packaging = ESADGoodsPackaging(
        pakage_quantity="5",
        pakage_type_code="1",
        package_pallete_information=[pallete_info_1, pallete_info_2]
    )
    
    # Создаем таможенную процедуру
    customs_procedure = ESADCustomsProcedure(
        main_customs_mode_code="40",
        preceding_customs_mode_code="00",
        goods_transfer_feature="000"
    )
    
    # Создаем дополнительное количество
    suppl_quantity = SupplementaryGoodsQuantity(
        goods_quantity="1",
        measure_unit_qualifier_name="ШТ",
        measure_unit_qualifier_code="796"
    )
    
    # Создаем товар
    goods = ESADout_CUGoods(
        goods_numeric="1",
        goods_descriptions=[
            "ГРАДИРНИ ОХЛАЖДАЮЩИЕ (ДРАЙКУЛЕРЫ) ПРОМЫШЛЕННОГО НАЗНАЧЕНИЯ, НЕ УКОМПЛЕКТОВАННЫЕ КОНТРОЛЬНОЙ ПАНЕЛЬЮ. ЗАКРЫТОГО ТИПА, РАЗОБРАННЫЕ НА ВРЕМЯ ПЕРЕВОЗКИ. ПРЕДНАЗНАЧЕНЫ ДЛЯ ОХЛАЖДЕНИЯ ВОДЫ, УЧАСТВУЮЩЕЙ В ПРОИЗВОДСТВЕННЫХ ПРОЦЕССАХ, ПРИ ПОМОЩИ ЦИРКУЛИРУЮЩ",
            "ЕГО В ЗАКРЫТОМ КОНТУРЕ ОХЛАЖДАЕМОГО ВЕНТИЛЯТОРАМИ ХЛАДАГЕНТА, КОТОРЫМ ЯВЛЯЕТСЯ ТАКЖЕ ВОДА. ПОСТАВЛЯЮТСЯ В КОМПЛЕКТЕ С ПРИНАДЛЕЖНОСТЯМИ ДЛЯ МОНТАЖА И ВВОДА УСТАНОВОК В ЭКСПЛУАТАЦИЮ И ТЕХНИЧЕСКОЙ ДОКУМЕНТАЦИЕЙ, НЕ ВОЕННОГО НАЗНАЧЕНИЯ, НЕ СОДЕРЖАТ ОЗО",
            "НОРАЗРУШАЮЩИХ ВЕЩЕСТВ, НЕ ПРЕДНАЗНАЧЕНЫ ДЛЯ РАБОТЫ ВО ВЗРЫВООПАСНЫХ СРЕДАХ."
        ],
        gross_weight_quantity="4676",
        net_weight_quantity="4606",
        invoiced_cost="330100",
        customs_cost="3982409.34",
        statistical_cost="49311.84",
        goods_tnved_code="8419891000",
        intellect_property_sign="N",
        origin_country_code="CN",
        customs_cost_correct_method="1",
        additional_sheet_count="1",
        goods_group_description=goods_group_desc,
        preferencii=preferencii,
        language_goods="RU",
        presented_documents=[presented_doc],
        customs_payment_calculations=[payment_calc_1, payment_calc_2],
        supplementary_goods_quantity=suppl_quantity,
        esad_goods_packaging=goods_packaging,
        esad_customs_procedure=customs_procedure
    )
    
    # Создаем платежи
    rf_org_1 = RFOrganizationFeaturesSimple(inn="7726319569")
    rf_org_2 = RFOrganizationFeaturesSimple(inn="7726319569")
    
    customs_payment_1 = ESADout_CUCustomsPayment(
        payment_mode_code="1010",
        payment_amount="16524",
        payment_currency_code="643",
        currency_rate="1",
        rf_organization_features=rf_org_1
    )
    
    customs_payment_2 = ESADout_CUCustomsPayment(
        payment_mode_code="5010",
        payment_amount="796481.87",
        payment_currency_code="643",
        currency_rate="1",
        rf_organization_features=rf_org_2
    )
    
    payments = ESADout_CUPayments(customs_payments=[customs_payment_1, customs_payment_2])
    
    # Создаем главный объект
    goods_shipment = ESADout_CUGoodsShipment(
        origin_country_name="КИТАЙ",
        origin_country_code="CN",
        total_goods_number="1",
        total_package_number="5",
        total_sheet_number="1",
        total_cust_cost="3982409.34",
        cust_cost_currency_code="RUB",
        consignor=consignor,
        consignee=consignee,
        financial_adjusting_responsible_person=financial_person,
        declarant=declarant,
        goods_location=goods_location,
        consigment=consigment,
        main_contract_terms=contract_terms,
        goods_list=[goods],
        payments=payments
    )
    return goods_shipment


if __name__ == "__main__":
    goods_shipment = get_goods_shipment()
    print(goods_shipment)
