from lxml import etree as ET
from lxml.etree import QName

from .base import IXMLElement


class DocumentID(IXMLElement):
    """Класс для cat_ru:DocumentID"""
    
    def __init__(self, document_id: str):
        self.document_id = document_id
    
    def to_xml(self, parent=None) -> ET.Element:
        if parent is not None:
            elem = ET.SubElement(parent, QName('urn:customs.ru:CommonAggregateTypes:5.24.0', 'DocumentID'))
        else:
            elem = ET.Element(QName('urn:customs.ru:CommonAggregateTypes:5.24.0', 'DocumentID'))
        elem.text = self.document_id
        return elem


class SubjectAddressDetails(IXMLElement):
    """Класс для RUScat_ru:SubjectAddressDetails"""
    
    def __init__(self, 
                 postal_code: str = None,
                 country_code: str = None,
                 country_name: str = None,
                 region: str = None,
                 city: str = None,
                 street_house: str = None,
                 house: str = None):
        self.postal_code = postal_code
        self.country_code = country_code
        self.country_name = country_name
        self.region = region
        self.city = city
        self.street_house = street_house
        self.house = house
    
    def to_xml(self, parent=None) -> ET.Element:
        if parent is not None:
            elem = ET.SubElement(parent, QName('urn:customs.ru:RUSCommonAggregateTypes:5.24.0', 'SubjectAddressDetails'))
        else:
            elem = ET.Element(QName('urn:customs.ru:RUSCommonAggregateTypes:5.24.0', 'SubjectAddressDetails'))
        
        if self.postal_code:
            postal_code_elem = ET.SubElement(elem, QName('urn:customs.ru:RUSCommonAggregateTypes:5.24.0', 'PostalCode'))
            postal_code_elem.text = self.postal_code
        
        if self.country_code:
            country_code_elem = ET.SubElement(elem, QName('urn:customs.ru:RUSCommonAggregateTypes:5.24.0', 'CountryCode'))
            country_code_elem.text = self.country_code
        
        if self.country_name:
            country_name_elem = ET.SubElement(elem, QName('urn:customs.ru:RUSCommonAggregateTypes:5.24.0', 'CounryName'))
            country_name_elem.text = self.country_name
        
        if self.region:
            region_elem = ET.SubElement(elem, QName('urn:customs.ru:RUSCommonAggregateTypes:5.24.0', 'Region'))
            region_elem.text = self.region
        
        if self.city:
            city_elem = ET.SubElement(elem, QName('urn:customs.ru:RUSCommonAggregateTypes:5.24.0', 'City'))
            city_elem.text = self.city
        
        if self.street_house:
            street_house_elem = ET.SubElement(elem, QName('urn:customs.ru:RUSCommonAggregateTypes:5.24.0', 'StreetHouse'))
            street_house_elem.text = self.street_house
        
        if self.house:
            house_elem = ET.SubElement(elem, QName('urn:customs.ru:RUSCommonAggregateTypes:5.24.0', 'House'))
            house_elem.text = self.house
        
        return elem


class RFOrganizationFeatures(IXMLElement):
    """Класс для cat_ru:RFOrganizationFeatures"""
    
    def __init__(self, 
                 ogrn: str = None,
                 inn: str = None,
                 kpp: str = None):
        self.ogrn = ogrn
        self.inn = inn
        self.kpp = kpp
    
    def to_xml(self, parent=None) -> ET.Element:
        if parent is not None:
            elem = ET.SubElement(parent, QName('urn:customs.ru:CommonAggregateTypes:5.24.0', 'RFOrganizationFeatures'))
        else:
            elem = ET.Element(QName('urn:customs.ru:CommonAggregateTypes:5.24.0', 'RFOrganizationFeatures'))
        
        if self.ogrn:
            ogrn_elem = ET.SubElement(elem, QName('urn:customs.ru:CommonAggregateTypes:5.24.0', 'OGRN'))
            ogrn_elem.text = self.ogrn
        
        if self.inn:
            inn_elem = ET.SubElement(elem, QName('urn:customs.ru:CommonAggregateTypes:5.24.0', 'INN'))
            inn_elem.text = self.inn
        
        if self.kpp:
            kpp_elem = ET.SubElement(elem, QName('urn:customs.ru:CommonAggregateTypes:5.24.0', 'KPP'))
            kpp_elem.text = self.kpp
        
        return elem


class DTSoutSeller(IXMLElement):
    """Класс для DTSoutSeller"""
    
    def __init__(self, 
                 organization_name: str = None,
                 address: SubjectAddressDetails = None):
        self.organization_name = organization_name
        self.address = address
    
    def to_xml(self, parent=None) -> ET.Element:
        if parent is not None:
            elem = ET.SubElement(parent, 'DTSoutSeller')
        else:
            elem = ET.Element('DTSoutSeller')
        
        if self.organization_name:
            org_name_elem = ET.SubElement(elem, QName('urn:customs.ru:CommonAggregateTypes:5.24.0', 'OrganizationName'))
            org_name_elem.text = self.organization_name
        
        if self.address:
            self.address.to_xml(elem)
        
        return elem


class DTSoutBuyer(IXMLElement):
    """Класс для DTSoutBuyer"""
    
    def __init__(self, 
                 organization_name: str = None,
                 rf_features: RFOrganizationFeatures = None,
                 address: SubjectAddressDetails = None):
        self.organization_name = organization_name
        self.rf_features = rf_features
        self.address = address
    
    def to_xml(self, parent=None) -> ET.Element:
        if parent is not None:
            elem = ET.SubElement(parent, 'DTSoutBuyer')
        else:
            elem = ET.Element('DTSoutBuyer')
        
        if self.organization_name:
            org_name_elem = ET.SubElement(elem, QName('urn:customs.ru:CommonAggregateTypes:5.24.0', 'OrganizationName'))
            org_name_elem.text = self.organization_name
        
        if self.rf_features:
            self.rf_features.to_xml(elem)
        
        if self.address:
            self.address.to_xml(elem)
        
        return elem


class DTSInvoiceDocuments(IXMLElement):
    """Класс для DTSInvoiceDocuments"""
    
    def __init__(self, 
                 document_name: str = None,
                 document_number: str = None,
                 document_date: str = None,
                 presented_document_mode_code: str = None,
                 position_number: str = None):
        self.document_name = document_name
        self.document_number = document_number
        self.document_date = document_date
        self.presented_document_mode_code = presented_document_mode_code
        self.position_number = position_number
    
    def to_xml(self, parent=None) -> ET.Element:
        if parent is not None:
            elem = ET.SubElement(parent, 'DTSInvoiceDocuments')
        else:
            elem = ET.Element('DTSInvoiceDocuments')
        
        if self.document_name:
            doc_name_elem = ET.SubElement(elem, QName('urn:customs.ru:CommonAggregateTypes:5.24.0', 'PrDocumentName'))
            doc_name_elem.text = self.document_name
        
        if self.document_number:
            doc_number_elem = ET.SubElement(elem, QName('urn:customs.ru:CommonAggregateTypes:5.24.0', 'PrDocumentNumber'))
            doc_number_elem.text = self.document_number
        
        if self.document_date:
            doc_date_elem = ET.SubElement(elem, QName('urn:customs.ru:CommonAggregateTypes:5.24.0', 'PrDocumentDate'))
            doc_date_elem.text = self.document_date
        
        if self.presented_document_mode_code:
            mode_code_elem = ET.SubElement(elem, QName('urn:customs.ru:CUESADDTSCommonAggregateTypes:5.24.0', 'PresentedDocumentModeCode'))
            mode_code_elem.text = self.presented_document_mode_code
        
        if self.position_number:
            position_elem = ET.SubElement(elem, QName('urn:customs.ru:CUESADDTSCommonAggregateTypes:5.24.0', 'PositionNumber'))
            position_elem.text = self.position_number
        
        return elem


# НОВЫЕ КЛАССЫ:

class DTSBuyerSellerDependence(IXMLElement):
    """Класс для DTSBuyerSellerDependence"""
    
    def __init__(self, column_7a: str = None):
        self.column_7a = column_7a
    
    def to_xml(self, parent=None) -> ET.Element:
        if parent is not None:
            elem = ET.SubElement(parent, 'DTSBuyerSellerDependence')
        else:
            elem = ET.Element('DTSBuyerSellerDependence')
        
        if self.column_7a:
            column_elem = ET.SubElement(elem, QName('urn:customs.ru:CUESADDTSCommonAggregateTypes:5.24.0', 'Column7A'))
            column_elem.text = self.column_7a
        
        return elem


class DTSSellingLimitation(IXMLElement):
    """Класс для DTSSellingLimitation"""
    
    def __init__(self, column_8a: str = None, column_8b: str = None):
        self.column_8a = column_8a
        self.column_8b = column_8b
    
    def to_xml(self, parent=None) -> ET.Element:
        if parent is not None:
            elem = ET.SubElement(parent, 'DTSSellingLimitation')
        else:
            elem = ET.Element('DTSSellingLimitation')
        
        if self.column_8a:
            column_elem = ET.SubElement(elem, QName('urn:customs.ru:CUESADDTSCommonAggregateTypes:5.24.0', 'Column8A'))
            column_elem.text = self.column_8a
        
        if self.column_8b:
            column_elem = ET.SubElement(elem, QName('urn:customs.ru:CUESADDTSCommonAggregateTypes:5.24.0', 'Column8B'))
            column_elem.text = self.column_8b
        
        return elem


class DTSAdditionalPayments(IXMLElement):
    """Класс для DTSAdditionalPayments"""
    
    def __init__(self, column_9: str = None, column_9a: str = None, column_9b: str = None):
        self.column_9 = column_9
        self.column_9a = column_9a
        self.column_9b = column_9b
    
    def to_xml(self, parent=None) -> ET.Element:
        if parent is not None:
            elem = ET.SubElement(parent, 'DTSAdditionalPayments')
        else:
            elem = ET.Element('DTSAdditionalPayments')
        
        if self.column_9:
            column_elem = ET.SubElement(elem, QName('urn:customs.ru:CUESADDTSCommonAggregateTypes:5.24.0', 'Column9'))
            column_elem.text = self.column_9
        
        if self.column_9a:
            column_elem = ET.SubElement(elem, QName('urn:customs.ru:CUESADDTSCommonAggregateTypes:5.24.0', 'Column9A'))
            column_elem.text = self.column_9a
        
        if self.column_9b:
            column_elem = ET.SubElement(elem, QName('urn:customs.ru:CUESADDTSCommonAggregateTypes:5.24.0', 'Column9B'))
            column_elem.text = self.column_9b
        
        return elem


class ExchangeRate(IXMLElement):
    """Класс для ExchangeRate"""
    
    def __init__(self, currency_rate: str = None, currency_a3_code: str = None, scale_number: str = None):
        self.currency_rate = currency_rate
        self.currency_a3_code = currency_a3_code
        self.scale_number = scale_number
    
    def to_xml(self, parent=None) -> ET.Element:
        if parent is not None:
            elem = ET.SubElement(parent, 'ExchangeRate')
        else:
            elem = ET.Element('ExchangeRate')
        
        if self.currency_rate:
            rate_elem = ET.SubElement(elem, QName('urn:customs.ru:RUSCommonAggregateTypes:5.24.0', 'CurrencyRate'))
            rate_elem.text = self.currency_rate
        
        if self.currency_a3_code:
            code_elem = ET.SubElement(elem, QName('urn:customs.ru:RUSCommonAggregateTypes:5.24.0', 'CurrencyA3Code'))
            code_elem.text = self.currency_a3_code
        
        if self.scale_number:
            scale_elem = ET.SubElement(elem, QName('urn:customs.ru:RUSCommonAggregateTypes:5.24.0', 'ScaleNumber'))
            scale_elem.text = self.scale_number
        
        return elem


class Method1CalculationBasis(IXMLElement):
    """Класс для Method1CalculationBasis"""
    
    def __init__(self, 
                 deal_currency_amount: str = None,
                 deal_currency_code: str = None,
                 deal_national_amount: str = None,
                 deal_currency_date: str = None,
                 deal_currency_rate: str = None,
                 indirect_national_payment: str = None,
                 basis_national_amount: str = None):
        self.deal_currency_amount = deal_currency_amount
        self.deal_currency_code = deal_currency_code
        self.deal_national_amount = deal_national_amount
        self.deal_currency_date = deal_currency_date
        self.deal_currency_rate = deal_currency_rate
        self.indirect_national_payment = indirect_national_payment
        self.basis_national_amount = basis_national_amount
    
    def to_xml(self, parent=None) -> ET.Element:
        if parent is not None:
            elem = ET.SubElement(parent, QName('urn:customs.ru:CUESADDTSCommonAggregateTypes:5.24.0', 'Method1CalculationBasis'))
        else:
            elem = ET.Element(QName('urn:customs.ru:CUESADDTSCommonAggregateTypes:5.24.0', 'Method1CalculationBasis'))
        
        if self.deal_currency_amount:
            amount_elem = ET.SubElement(elem, QName('urn:customs.ru:CUESADDTSCommonAggregateTypes:5.24.0', 'DealCurrencyAmount'))
            amount_elem.text = self.deal_currency_amount
        
        if self.deal_currency_code:
            code_elem = ET.SubElement(elem, QName('urn:customs.ru:CUESADDTSCommonAggregateTypes:5.24.0', 'DealCurrencyCode'))
            code_elem.text = self.deal_currency_code
        
        if self.deal_national_amount:
            amount_elem = ET.SubElement(elem, QName('urn:customs.ru:CUESADDTSCommonAggregateTypes:5.24.0', 'DealNationalAmount'))
            amount_elem.text = self.deal_national_amount
        
        if self.deal_currency_date:
            date_elem = ET.SubElement(elem, QName('urn:customs.ru:CUESADDTSCommonAggregateTypes:5.24.0', 'DealCurrencyDate'))
            date_elem.text = self.deal_currency_date
        
        if self.deal_currency_rate:
            rate_elem = ET.SubElement(elem, QName('urn:customs.ru:CUESADDTSCommonAggregateTypes:5.24.0', 'DealCurrencyRate'))
            rate_elem.text = self.deal_currency_rate
        
        if self.indirect_national_payment:
            payment_elem = ET.SubElement(elem, QName('urn:customs.ru:CUESADDTSCommonAggregateTypes:5.24.0', 'IndirectNationalPayment'))
            payment_elem.text = self.indirect_national_payment
        
        if self.basis_national_amount:
            amount_elem = ET.SubElement(elem, QName('urn:customs.ru:CUESADDTSCommonAggregateTypes:5.24.0', 'BasisNationalAmount'))
            amount_elem.text = self.basis_national_amount
        
        return elem


class Method1AdditionalSum(IXMLElement):
    """Класс для Method1AdditionalSum"""
    
    def __init__(self,
                 agent_bonus: str = None,
                 package_expenses: str = None,
                 store_cost: str = None,
                 production_toolkit_cost: str = None,
                 working_stock_cost: str = None,
                 design_payment: str = None,
                 intellectual_property_payment: str = None,
                 seller_income: str = None,
                 border_transport_charges: str = None,
                 border_place: str = None,
                 load_charges: str = None,
                 insurance_charges: str = None,
                 total_additional_sum: str = None):
        self.agent_bonus = agent_bonus
        self.package_expenses = package_expenses
        self.store_cost = store_cost
        self.production_toolkit_cost = production_toolkit_cost
        self.working_stock_cost = working_stock_cost
        self.design_payment = design_payment
        self.intellectual_property_payment = intellectual_property_payment
        self.seller_income = seller_income
        self.border_transport_charges = border_transport_charges
        self.border_place = border_place
        self.load_charges = load_charges
        self.insurance_charges = insurance_charges
        self.total_additional_sum = total_additional_sum
    
    def to_xml(self, parent=None) -> ET.Element:
        if parent is not None:
            elem = ET.SubElement(parent, QName('urn:customs.ru:CUESADDTSCommonAggregateTypes:5.24.0', 'Method1AdditionalSum'))
        else:
            elem = ET.Element(QName('urn:customs.ru:CUESADDTSCommonAggregateTypes:5.24.0', 'Method1AdditionalSum'))
        
        fields = [
            ('AgentBonus', self.agent_bonus),
            ('PackageExpenses', self.package_expenses),
            ('StoreCost', self.store_cost),
            ('ProductionToolkitCost', self.production_toolkit_cost),
            ('WorkingStockCost', self.working_stock_cost),
            ('DesignPayment', self.design_payment),
            ('IntellectualPropertyPayment', self.intellectual_property_payment),
            ('SellerIncome', self.seller_income),
            ('BorderTransportCharges', self.border_transport_charges),
            ('BorderPlace', self.border_place),
            ('LoadCharges', self.load_charges),
            ('InsuranceCharges', self.insurance_charges),
            ('TotalAdditionalSum', self.total_additional_sum)
        ]
        
        for field_name, field_value in fields:
            if field_value is not None:
                field_elem = ET.SubElement(elem, QName('urn:customs.ru:CUESADDTSCommonAggregateTypes:5.24.0', field_name))
                field_elem.text = field_value
        
        return elem


class Method1Deduction(IXMLElement):
    """Класс для Method1Deduction"""
    
    def __init__(self,
                 building_amount: str = None,
                 union_transport_charge: str = None,
                 union_tax_payment: str = None,
                 total_deduction_amount: str = None):
        self.building_amount = building_amount
        self.union_transport_charge = union_transport_charge
        self.union_tax_payment = union_tax_payment
        self.total_deduction_amount = total_deduction_amount
    
    def to_xml(self, parent=None) -> ET.Element:
        if parent is not None:
            elem = ET.SubElement(parent, QName('urn:customs.ru:CUESADDTSCommonAggregateTypes:5.24.0', 'Method1Deduction'))
        else:
            elem = ET.Element(QName('urn:customs.ru:CUESADDTSCommonAggregateTypes:5.24.0', 'Method1Deduction'))
        
        if self.building_amount:
            amount_elem = ET.SubElement(elem, QName('urn:customs.ru:CUESADDTSCommonAggregateTypes:5.24.0', 'BuildingAmount'))
            amount_elem.text = self.building_amount
        
        if self.union_transport_charge:
            charge_elem = ET.SubElement(elem, QName('urn:customs.ru:CUESADDTSCommonAggregateTypes:5.24.0', 'UnionTransportCharge'))
            charge_elem.text = self.union_transport_charge
        
        if self.union_tax_payment:
            payment_elem = ET.SubElement(elem, QName('urn:customs.ru:CUESADDTSCommonAggregateTypes:5.24.0', 'UnionTaxPayment'))
            payment_elem.text = self.union_tax_payment
        
        if self.total_deduction_amount:
            amount_elem = ET.SubElement(elem, QName('urn:customs.ru:CUESADDTSCommonAggregateTypes:5.24.0', 'TotalDeductionAmount'))
            amount_elem.text = self.total_deduction_amount
        
        return elem


class DTSMethod1(IXMLElement):
    """Класс для DTSMethod1"""
    
    def __init__(self,
                 calculation_basis: Method1CalculationBasis = None,
                 additional_sum: Method1AdditionalSum = None,
                 deduction: Method1Deduction = None):
        self.calculation_basis = calculation_basis
        self.additional_sum = additional_sum
        self.deduction = deduction
    
    def to_xml(self, parent=None) -> ET.Element:
        if parent is not None:
            elem = ET.SubElement(parent, QName('urn:customs.ru:CUESADDTSCommonAggregateTypes:5.24.0', 'DTSMethod1'))
        else:
            elem = ET.Element(QName('urn:customs.ru:CUESADDTSCommonAggregateTypes:5.24.0', 'DTSMethod1'))
        
        if self.calculation_basis:
            self.calculation_basis.to_xml(elem)
        
        if self.additional_sum:
            self.additional_sum.to_xml(elem)
        
        if self.deduction:
            self.deduction.to_xml(elem)
        
        return elem


class DTSCUCustomsCostCalculation(IXMLElement):
    """Класс для DTS_CUCustomsCostCalculation"""
    
    def __init__(self, dts_method1: DTSMethod1 = None):
        self.dts_method1 = dts_method1
    
    def to_xml(self, parent=None) -> ET.Element:
        if parent is not None:
            elem = ET.SubElement(parent, 'DTS_CUCustomsCostCalculation')
        else:
            elem = ET.Element('DTS_CUCustomsCostCalculation')
        
        if self.dts_method1:
            self.dts_method1.to_xml(elem)
        
        return elem


class DTSoutCUGoodsCustomsCost(IXMLElement):
    """Класс для DTSout_CUGoodsCustomsCost"""
    
    def __init__(self,
                 sheet_number: str = None,
                 sheet_goods_serial_number: str = None,
                 gtd_goods_number: str = None,
                 goods_tnved_code: str = None,
                 national_declared_customs_cost: str = None,
                 dollar_declared_customs_cost: str = None,
                 exchange_rate: ExchangeRate = None,
                 currency_date: str = None,
                 method_number_code: str = None,
                 customs_cost_calculation: DTSCUCustomsCostCalculation = None):
        self.sheet_number = sheet_number
        self.sheet_goods_serial_number = sheet_goods_serial_number
        self.gtd_goods_number = gtd_goods_number
        self.goods_tnved_code = goods_tnved_code
        self.national_declared_customs_cost = national_declared_customs_cost
        self.dollar_declared_customs_cost = dollar_declared_customs_cost
        self.exchange_rate = exchange_rate
        self.currency_date = currency_date
        self.method_number_code = method_number_code
        self.customs_cost_calculation = customs_cost_calculation
    
    def to_xml(self, parent=None) -> ET.Element:
        if parent is not None:
            elem = ET.SubElement(parent, 'DTSout_CUGoodsCustomsCost')
        else:
            elem = ET.Element('DTSout_CUGoodsCustomsCost')
        
        if self.sheet_number:
            sheet_elem = ET.SubElement(elem, 'SheetNumber')
            sheet_elem.text = self.sheet_number
        
        if self.sheet_goods_serial_number:
            serial_elem = ET.SubElement(elem, 'SheetGoodsSerialNumber')
            serial_elem.text = self.sheet_goods_serial_number
        
        if self.gtd_goods_number:
            gtd_elem = ET.SubElement(elem, 'GTDGoodsNumber')
            gtd_elem.text = self.gtd_goods_number
        
        if self.goods_tnved_code:
            code_elem = ET.SubElement(elem, 'GoodsTNVEDCode')
            code_elem.text = self.goods_tnved_code
        
        if self.national_declared_customs_cost:
            cost_elem = ET.SubElement(elem, 'NationalDeclaredCustomsCost')
            cost_elem.text = self.national_declared_customs_cost
        
        if self.dollar_declared_customs_cost:
            cost_elem = ET.SubElement(elem, 'DollarDeclaredCustomsCost')
            cost_elem.text = self.dollar_declared_customs_cost
        
        if self.exchange_rate:
            self.exchange_rate.to_xml(elem)
        
        if self.currency_date:
            date_elem = ET.SubElement(elem, 'CurrencyDate')
            date_elem.text = self.currency_date
        
        if self.method_number_code:
            method_elem = ET.SubElement(elem, 'MethodNumberCode')
            method_elem.text = self.method_number_code
        
        if self.customs_cost_calculation:
            self.customs_cost_calculation.to_xml(elem)
        
        return elem


class CommunicationDetails(IXMLElement):
    """Класс для RUScat_ru:CommunicationDetails"""
    
    def __init__(self, phone: str = None):
        self.phone = phone
    
    def to_xml(self, parent=None) -> ET.Element:
        if parent is not None:
            elem = ET.SubElement(parent, QName('urn:customs.ru:RUSCommonAggregateTypes:5.24.0', 'CommunicationDetails'))
        else:
            elem = ET.Element(QName('urn:customs.ru:RUSCommonAggregateTypes:5.24.0', 'CommunicationDetails'))
        
        if self.phone:
            phone_elem = ET.SubElement(elem, QName('urn:customs.ru:CommonAggregateTypes:5.24.0', 'Phone'))
            phone_elem.text = self.phone
        
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
    
    def to_xml(self, parent=None) -> ET.Element:
        if parent is not None:
            elem = ET.SubElement(parent, QName('urn:customs.ru:RUDeclCommonAggregateTypesCust:5.24.0', 'SigningDetails'))
        else:
            elem = ET.Element(QName('urn:customs.ru:RUDeclCommonAggregateTypesCust:5.24.0', 'SigningDetails'))
        
        if self.person_surname:
            surname_elem = ET.SubElement(elem, QName('urn:customs.ru:CommonAggregateTypes:5.24.0', 'PersonSurname'))
            surname_elem.text = self.person_surname
        
        if self.person_name:
            name_elem = ET.SubElement(elem, QName('urn:customs.ru:CommonAggregateTypes:5.24.0', 'PersonName'))
            name_elem.text = self.person_name
        
        if self.person_middle_name:
            middle_name_elem = ET.SubElement(elem, QName('urn:customs.ru:CommonAggregateTypes:5.24.0', 'PersonMiddleName'))
            middle_name_elem.text = self.person_middle_name
        
        if self.person_post:
            post_elem = ET.SubElement(elem, QName('urn:customs.ru:CommonAggregateTypes:5.24.0', 'PersonPost'))
            post_elem.text = self.person_post
        
        if self.communication_details:
            self.communication_details.to_xml(elem)
        
        if self.signing_date:
            date_elem = ET.SubElement(elem, QName('urn:customs.ru:RUSCommonAggregateTypes:5.24.0', 'SigningDate'))
            date_elem.text = self.signing_date
        
        return elem


class SignatoryPersonIdentityDetails(IXMLElement):
    """Класс для RUDECLcat:SignatoryPersonIdentityDetails"""
    
    def __init__(self,
                 identity_card_code: str = None,
                 identity_card_name: str = None,
                 identity_card_series: str = None,
                 identity_card_number: str = None,
                 identity_card_date: str = None,
                 organization_name: str = None):
        self.identity_card_code = identity_card_code
        self.identity_card_name = identity_card_name
        self.identity_card_series = identity_card_series
        self.identity_card_number = identity_card_number
        self.identity_card_date = identity_card_date
        self.organization_name = organization_name
    
    def to_xml(self, parent=None) -> ET.Element:
        if parent is not None:
            elem = ET.SubElement(parent, QName('urn:customs.ru:RUDeclCommonAggregateTypesCust:5.24.0', 'SignatoryPersonIdentityDetails'))
        else:
            elem = ET.Element(QName('urn:customs.ru:RUDeclCommonAggregateTypesCust:5.24.0', 'SignatoryPersonIdentityDetails'))
        
        if self.identity_card_code:
            code_elem = ET.SubElement(elem, QName('urn:customs.ru:RUSCommonAggregateTypes:5.24.0', 'IdentityCardCode'))
            code_elem.text = self.identity_card_code
        
        if self.identity_card_name:
            name_elem = ET.SubElement(elem, QName('urn:customs.ru:RUSCommonAggregateTypes:5.24.0', 'IdentityCardName'))
            name_elem.text = self.identity_card_name
        
        if self.identity_card_series:
            series_elem = ET.SubElement(elem, QName('urn:customs.ru:RUSCommonAggregateTypes:5.24.0', 'IdentityCardSeries'))
            series_elem.text = self.identity_card_series
        
        if self.identity_card_number:
            number_elem = ET.SubElement(elem, QName('urn:customs.ru:RUSCommonAggregateTypes:5.24.0', 'IdentityCardNumber'))
            number_elem.text = self.identity_card_number
        
        if self.identity_card_date:
            date_elem = ET.SubElement(elem, QName('urn:customs.ru:RUSCommonAggregateTypes:5.24.0', 'IdentityCardDate'))
            date_elem.text = self.identity_card_date
        
        if self.organization_name:
            org_elem = ET.SubElement(elem, QName('urn:customs.ru:RUSCommonAggregateTypes:5.24.0', 'OrganizationName'))
            org_elem.text = self.organization_name
        
        return elem


class DTSoutCUFilledPerson(IXMLElement):
    """Класс для DTSout_CUFilledPerson"""
    
    def __init__(self,
                 signing_details: SigningDetails = None,
                 signatory_person_identity: SignatoryPersonIdentityDetails = None):
        self.signing_details = signing_details
        self.signatory_person_identity = signatory_person_identity
    
    def to_xml(self, parent=None) -> ET.Element:
        if parent is not None:
            elem = ET.SubElement(parent, 'DTSout_CUFilledPerson')
        else:
            elem = ET.Element('DTSout_CUFilledPerson')
        
        if self.signing_details:
            self.signing_details.to_xml(elem)
        
        if self.signatory_person_identity:
            self.signatory_person_identity.to_xml(elem)
        
        return elem


class DTSoutDeclarant(IXMLElement):
    """Класс для DTSoutDeclarant"""
    
    def __init__(self, 
                 organization_name: str = None,
                 rf_features: RFOrganizationFeatures = None,
                 address: SubjectAddressDetails = None):
        self.organization_name = organization_name
        self.rf_features = rf_features
        self.address = address
    
    def to_xml(self, parent=None) -> ET.Element:
        if parent is not None:
            elem = ET.SubElement(parent, 'DTSoutDeclarant')
        else:
            elem = ET.Element('DTSoutDeclarant')
        
        if self.organization_name:
            org_name_elem = ET.SubElement(elem, QName('urn:customs.ru:CommonAggregateTypes:5.24.0', 'OrganizationName'))
            org_name_elem.text = self.organization_name
        
        if self.rf_features:
            self.rf_features.to_xml(elem)
        
        if self.address:
            self.address.to_xml(elem)
        
        return elem


class DeliveryTerms(IXMLElement):
    """Класс для DeliveryTerms"""
    
    def __init__(self, delivery_place: str = None, delivery_terms_string_code: str = None):
        self.delivery_place = delivery_place
        self.delivery_terms_string_code = delivery_terms_string_code
    
    def to_xml(self, parent=None) -> ET.Element:
        if parent is not None:
            elem = ET.SubElement(parent, 'DeliveryTerms')
        else:
            elem = ET.Element('DeliveryTerms')
        
        if self.delivery_place:
            place_elem = ET.SubElement(elem, QName('urn:customs.ru:CommonAggregateTypes:5.24.0', 'DeliveryPlace'))
            place_elem.text = self.delivery_place
        
        if self.delivery_terms_string_code:
            code_elem = ET.SubElement(elem, QName('urn:customs.ru:CommonAggregateTypes:5.24.0', 'DeliveryTermsStringCode'))
            code_elem.text = self.delivery_terms_string_code
        
        return elem


class DTSout_CU(IXMLElement):
    """Класс для DTSout_CU"""
    
    def __init__(self, 
                 document_mode_id: str = None,
                 document_id: str = None,
                 additional_sheet_number: str = None,
                 electronic_document_sign: str = None,
                 form_dts: str = None,
                 customs_cost_method_code: str = None,
                 seller: DTSoutSeller = None,
                 buyer: DTSoutBuyer = None,
                 invoice_documents: list = None,
                 buyer_seller_dependence: DTSBuyerSellerDependence = None,
                 selling_limitation: DTSSellingLimitation = None,
                 additional_payments: DTSAdditionalPayments = None,
                 goods_customs_cost: DTSoutCUGoodsCustomsCost = None,
                 filled_person: DTSoutCUFilledPerson = None,
                 declarant: DTSoutDeclarant = None,
                 delivery_terms: DeliveryTerms = None):
        self.document_mode_id = document_mode_id
        self.document_id = document_id
        self.additional_sheet_number = additional_sheet_number
        self.electronic_document_sign = electronic_document_sign
        self.form_dts = form_dts
        self.customs_cost_method_code = customs_cost_method_code
        self.seller = seller
        self.buyer = buyer
        self.invoice_documents = invoice_documents or []
        self.buyer_seller_dependence = buyer_seller_dependence
        self.selling_limitation = selling_limitation
        self.additional_payments = additional_payments
        self.goods_customs_cost = goods_customs_cost
        self.filled_person = filled_person
        self.declarant = declarant
        self.delivery_terms = delivery_terms
    
    def to_xml(self, parent=None) -> ET.Element:
        # Определяем namespace map для корневого элемента
        nsmap = {
            None: 'urn:customs.ru:Information:CustomsDocuments:DTSout_CU:5.24.0',
            'cat_ru': 'urn:customs.ru:CommonAggregateTypes:5.24.0',
            'RUScat_ru': 'urn:customs.ru:RUSCommonAggregateTypes:5.24.0',
            'RUDECLcat': 'urn:customs.ru:RUDeclCommonAggregateTypesCust:5.24.0',
            'cat_EDTS_cu': 'urn:customs.ru:CUESADDTSCommonAggregateTypes:5.24.0',
            'RUScat_ru': 'urn:customs.ru:RUSCommonAggregateTypes:5.24.0',
            'catESAD_cu': 'urn:customs.ru:CUESADCommonAggregateTypesCust:5.24.0',
        }



        if parent is not None:
            elem = ET.SubElement(parent, 'DTSout_CU', nsmap=nsmap)
        else:
            elem = ET.Element('DTSout_CU', nsmap=nsmap)
        
        elem.set('DocumentModeID', self.document_mode_id)
        
        if self.document_id:
            doc_id_elem = ET.SubElement(elem, QName('urn:customs.ru:CommonAggregateTypes:5.24.0', 'DocumentID'))
            doc_id_elem.text = self.document_id
        
        if self.additional_sheet_number:
            sheet_elem = ET.SubElement(elem, 'AdditionalSheetNumber')
            sheet_elem.text = self.additional_sheet_number
        
        if self.electronic_document_sign:
            sign_elem = ET.SubElement(elem, 'ElectronicDocumentSign')
            sign_elem.text = self.electronic_document_sign
        
        if self.form_dts:
            form_elem = ET.SubElement(elem, 'FormDTS')
            form_elem.text = self.form_dts
        
        if self.customs_cost_method_code:
            method_elem = ET.SubElement(elem, 'CustomsCostMethodCode')
            method_elem.text = self.customs_cost_method_code
        
        if self.seller:
            self.seller.to_xml(elem)
        
        if self.buyer:
            self.buyer.to_xml(elem)
        
        for doc in self.invoice_documents:
            doc.to_xml(elem)
        
        if self.buyer_seller_dependence:
            self.buyer_seller_dependence.to_xml(elem)
        
        if self.selling_limitation:
            self.selling_limitation.to_xml(elem)
        
        if self.additional_payments:
            self.additional_payments.to_xml(elem)
        
        if self.goods_customs_cost:
            self.goods_customs_cost.to_xml(elem)
        
        if self.filled_person:
            self.filled_person.to_xml(elem)
        
        if self.declarant:
            self.declarant.to_xml(elem)
        
        if self.delivery_terms:
            self.delivery_terms.to_xml(elem)
        
        return elem


# Пример использования (остается без изменений)
def get_dts_out_cu():
    # Создаем адрес продавца
    seller_address = SubjectAddressDetails(
        postal_code="518111",
        country_code="CN",
        country_name="КИТАЙ",
        region="SHENZHEN",
        city="LONGGANG, PINGHU, CHINA SOUTH CITY",
        street_house="TIEDONG,",
        house="BUILDING 12, NO.510"
    )
    
    # Создаем продавца
    seller = DTSoutSeller(
        organization_name='SHENZHEN NEWIN MACHINERY CO., LTD.',
        address=seller_address
    )
    
    # Создаем реквизиты российского покупателя
    buyer_rf_features = RFOrganizationFeatures(
        ogrn="1027726002260",
        inn="7726319569",
        kpp="772301001"
    )
    
    # Создаем адрес покупателя
    buyer_address = SubjectAddressDetails(
        postal_code="109559",
        country_code="RU",
        country_name="РОССИЯ",
        city="ГОРОД МОСКВА",
        street_house="Б-Р ТИХОРЕЦКИЙ",
        house="Д.1, СТР.5, ЭТАЖ 6 КОМНАТА 15"
    )
    
    # Создаем покупателя
    buyer = DTSoutBuyer(
        organization_name='ООО "ТРЭЙД ИНВЕСТМЕНТС"',
        rf_features=buyer_rf_features,
        address=buyer_address
    )
    
    # Создаем список документов
    invoice_documents = [
        DTSInvoiceDocuments(
            document_name="СВИДЕТЕЛЬСТВО О ГОС. РЕГИСТРАЦИИ ЮЛ",
            document_number="77 006868279",
            document_date="2002-09-18",
            presented_document_mode_code="04011",
            position_number="4"
        ),
        DTSInvoiceDocuments(
            document_name="СЧЕТ-ФАКТУРА (ИНВОЙС) К ДОГОВОРУ",
            document_number="NW-2025032601-6",
            document_date="2025-03-26",
            presented_document_mode_code="04021",
            position_number="4"
        )
    ]
    
    # Создаем дополнительные секции
    buyer_seller_dependence = DTSBuyerSellerDependence(column_7a="0")
    selling_limitation = DTSSellingLimitation(column_8a="0", column_8b="0")
    additional_payments = DTSAdditionalPayments(column_9="0", column_9a="0", column_9b="0")
    
    # Создаем расчет таможенной стоимости
    exchange_rate = ExchangeRate(
        currency_rate="80.7597",
        currency_a3_code="USD",
        scale_number="0"
    )
    
    calculation_basis = Method1CalculationBasis(
        deal_currency_amount="330100",
        deal_currency_code="CNY",
        deal_national_amount="3652490.48",
        deal_currency_date="2025-04-22",
        deal_currency_rate="11.0648",
        indirect_national_payment="0",
        basis_national_amount="3652490.48"
    )
    
    additional_sum = Method1AdditionalSum(
        agent_bonus="0",
        package_expenses="0",
        store_cost="0",
        production_toolkit_cost="0",
        working_stock_cost="0",
        design_payment="0",
        intellectual_property_payment="0",
        seller_income="0",
        border_transport_charges="328400.8",
        border_place="Т/П МАПП ЗАБАЙКАЛЬСК",
        load_charges="0",
        insurance_charges="1518.06",
        total_additional_sum="329918.86"
    )
    
    deduction = Method1Deduction(
        building_amount="0",
        union_transport_charge="0",
        union_tax_payment="0",
        total_deduction_amount="0"
    )
    
    dts_method1 = DTSMethod1(
        calculation_basis=calculation_basis,
        additional_sum=additional_sum,
        deduction=deduction
    )
    
    customs_cost_calculation = DTSCUCustomsCostCalculation(dts_method1=dts_method1)
    
    goods_customs_cost = DTSoutCUGoodsCustomsCost(
        sheet_number="2",
        sheet_goods_serial_number="1",
        gtd_goods_number="1",
        goods_tnved_code="8419891000",
        national_declared_customs_cost="3982409.34",
        dollar_declared_customs_cost="49311.84",
        exchange_rate=exchange_rate,
        currency_date="2025-04-22",
        method_number_code="1",
        customs_cost_calculation=customs_cost_calculation
    )
    
    # Создаем информацию о заполняющем лице
    communication_details = CommunicationDetails(phone="79819740258")
    
    signing_details = SigningDetails(
        person_surname="ЖУКОВ",
        person_name="НИКОЛАЙ",
        person_middle_name="СЕРГЕЕВИЧ",
        person_post="МЕНЕДЖЕР ПО ТАМОЖЕННЫМ ОПЕРАЦИЯМ",
        communication_details=communication_details,
        signing_date="2025-04-22T12:58:00"
    )
    
    signatory_person_identity = SignatoryPersonIdentityDetails(
        identity_card_code="RU01001",
        identity_card_name="ПАСПОРТ",
        identity_card_series="4017",
        identity_card_number="799090",
        identity_card_date="2017-06-09",
        organization_name="ТП № 83 УФМС РОССИИ ПО СПБ И ЛО В ЦЕНТРАЛЬНОМ РАЙОНЕ Г. СПБ"
    )
    
    filled_person = DTSoutCUFilledPerson(
        signing_details=signing_details,
        signatory_person_identity=signatory_person_identity
    )
    
    # Создаем декларанта
    declarant = DTSoutDeclarant(
        organization_name='ООО "ТРЭЙД ИНВЕСТМЕНТС"',
        rf_features=RFOrganizationFeatures(
            ogrn="1027726002260",
            inn="7726319569",
            kpp="772301001"
        ),
        address=SubjectAddressDetails(
            postal_code="109559",
            country_code="RU",
            country_name="РОССИЯ",
            city="ГОРОД МОСКВА",
            street_house="Б-Р ТИХОРЕЦКИЙ",
            house="Д.1, СТР.5, ЭТАЖ 6 КОМНАТА 15"
        )
    )
    
    # Создаем условия поставки
    delivery_terms = DeliveryTerms(
        delivery_place="NEWIN FACTORY, HUIZHOU",
        delivery_terms_string_code="EXW"
    )
    
    # Создаем основной документ
    dts_out_cu = DTSout_CU(
        document_mode_id="1006112E",
        document_id="6B120531-A94A-45F2-AFAF-C0F31AE7D997",
        additional_sheet_number="0",
        electronic_document_sign="ЭД",
        form_dts="1",
        customs_cost_method_code="1",
        seller=seller,
        buyer=buyer,
        invoice_documents=invoice_documents,
        buyer_seller_dependence=buyer_seller_dependence,
        selling_limitation=selling_limitation,
        additional_payments=additional_payments,
        goods_customs_cost=goods_customs_cost,
        filled_person=filled_person,
        declarant=declarant,
        delivery_terms=delivery_terms
    )
    
    return dts_out_cu


if __name__ == "__main__":
    dts_out_cu = get_dts_out_cu()
    xml_tree = dts_out_cu.to_xml()
    
    # Красивое форматирование XML
    xml_str = ET.tostring(xml_tree, encoding='utf-8', pretty_print=True, xml_declaration=True).decode('utf-8')
    print(xml_str)