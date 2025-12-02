from lxml import etree as ET

from .base import IXMLElement


class DocumentID(IXMLElement):
    """Класс для cat_ru:DocumentID"""
    
    def __init__(self, document_id: str):
        self.document_id = document_id
    
    def to_xml(self, parent=None) -> ET.Element:
        if parent is not None:
            elem = ET.SubElement(parent, '{urn:customs.ru:CommonAggregateTypes:5.24.0}DocumentID')
        else:
            elem = ET.Element('{urn:customs.ru:CommonAggregateTypes:5.24.0}DocumentID')
        elem.text = self.document_id
        return elem


class CustomsCode(IXMLElement):
    """Класс для cat_ru:CustomsCode"""
    
    def __init__(self, customs_code: str):
        self.customs_code = customs_code
    
    def to_xml(self, parent=None) -> ET.Element:
        if parent is not None:
            elem = ET.SubElement(parent, '{urn:customs.ru:CommonAggregateTypes:5.24.0}CustomsCode')
        else:
            elem = ET.Element('{urn:customs.ru:CommonAggregateTypes:5.24.0}CustomsCode')
        elem.text = self.customs_code
        return elem


class RegistrationDate(IXMLElement):
    """Класс для cat_ru:RegistrationDate"""
    
    def __init__(self, registration_date: str):
        self.registration_date = registration_date
    
    def to_xml(self, parent=None) -> ET.Element:
        if parent is not None:
            elem = ET.SubElement(parent, '{urn:customs.ru:CommonAggregateTypes:5.24.0}RegistrationDate')
        else:
            elem = ET.Element('{urn:customs.ru:CommonAggregateTypes:5.24.0}RegistrationDate')
        elem.text = self.registration_date
        return elem


class GTDNumber(IXMLElement):
    """Класс для cat_ru:GTDNumber"""
    
    def __init__(self, gtd_number: str):
        self.gtd_number = gtd_number
    
    def to_xml(self, parent=None) -> ET.Element:
        if parent is not None:
            elem = ET.SubElement(parent, '{urn:customs.ru:CommonAggregateTypes:5.24.0}GTDNumber')
        else:
            elem = ET.Element('{urn:customs.ru:CommonAggregateTypes:5.24.0}GTDNumber')
        elem.text = self.gtd_number
        return elem


class DecisionCode(IXMLElement):
    """Класс для catESAD_ru:DecisionCode"""
    
    def __init__(self, decision_code: str):
        self.decision_code = decision_code
    
    def to_xml(self, parent=None) -> ET.Element:
        if parent is not None:
            elem = ET.SubElement(parent, '{urn:customs.ru:RUCommonAggregateTypes:5.24.0}DecisionCode')
        else:
            elem = ET.Element('{urn:customs.ru:RUCommonAggregateTypes:5.24.0}DecisionCode')
        elem.text = self.decision_code
        return elem


class DateInf(IXMLElement):
    """Класс для catESAD_ru:DateInf"""
    
    def __init__(self, date_inf: str):
        self.date_inf = date_inf
    
    def to_xml(self, parent=None) -> ET.Element:
        if parent is not None:
            elem = ET.SubElement(parent, '{urn:customs.ru:RUCommonAggregateTypes:5.24.0}DateInf')
        else:
            elem = ET.Element('{urn:customs.ru:RUCommonAggregateTypes:5.24.0}DateInf')
        elem.text = self.date_inf
        return elem


class TimeInf(IXMLElement):
    """Класс для catESAD_ru:TimeInf"""
    
    def __init__(self, time_inf: str):
        self.time_inf = time_inf
    
    def to_xml(self, parent=None) -> ET.Element:
        if parent is not None:
            elem = ET.SubElement(parent, '{urn:customs.ru:RUCommonAggregateTypes:5.24.0}TimeInf')
        else:
            elem = ET.Element('{urn:customs.ru:RUCommonAggregateTypes:5.24.0}TimeInf')
        elem.text = self.time_inf
        return elem


class LNP(IXMLElement):
    """Класс для catESAD_ru:LNP"""
    
    def __init__(self, lnp: str):
        self.lnp = lnp
    
    def to_xml(self, parent=None) -> ET.Element:
        if parent is not None:
            elem = ET.SubElement(parent, '{urn:customs.ru:RUCommonAggregateTypes:5.24.0}LNP')
        else:
            elem = ET.Element('{urn:customs.ru:RUCommonAggregateTypes:5.24.0}LNP')
        elem.text = self.lnp
        return elem


class FoundationDes(IXMLElement):
    """Класс для FoundationDes"""
    
    def __init__(self, resolution_description: str):
        self.resolution_description = resolution_description
    
    def to_xml(self, parent=None) -> ET.Element:
        if parent is not None:
            elem = ET.SubElement(parent, 'FoundationDes')
        else:
            elem = ET.Element('FoundationDes')
        resolution_elem = ET.SubElement(elem, 'ResolutionDescription')
        resolution_elem.text = self.resolution_description
        return elem


class PersonName(IXMLElement):
    """Класс для PersonName"""
    
    def __init__(self, person_name: str):
        self.person_name = person_name
    
    def to_xml(self, parent=None) -> ET.Element:
        if parent is not None:
            elem = ET.SubElement(parent, 'PersonName')
        else:
            elem = ET.Element('PersonName')
        elem.text = self.person_name
        return elem


class GTDOutGoodsResult(IXMLElement):
    """Класс для GTDOutGoodsResult"""
    
    def __init__(self, decision_code: str, date_inf: str, lnp: str, resolution_description: str, person_name: str):
        self.decision_code = DecisionCode(decision_code)
        self.date_inf = DateInf(date_inf)
        self.lnp = LNP(lnp)
        self.foundation_des = FoundationDes(resolution_description)
        self.person_name = PersonName(person_name)
    
    def to_xml(self, parent=None) -> ET.Element:
        if parent is not None:
            elem = ET.SubElement(parent, 'GTDOutGoodsResult')
        else:
            elem = ET.Element('GTDOutGoodsResult')
        
        self.decision_code.to_xml(elem)
        self.date_inf.to_xml(elem)
        self.lnp.to_xml(elem)
        self.foundation_des.to_xml(elem)
        self.person_name.to_xml(elem)
        return elem


class CustomsCostCorrectMark(IXMLElement):
    """Класс для CustomsCostCorrectMark"""
    
    def __init__(self, customs_cost_correct_mark: str):
        self.customs_cost_correct_mark = customs_cost_correct_mark
    
    def to_xml(self, parent=None) -> ET.Element:
        if parent is not None:
            elem = ET.SubElement(parent, 'CustomsCostCorrectMark')
        else:
            elem = ET.Element('CustomsCostCorrectMark')
        elem.text = self.customs_cost_correct_mark
        return elem


class CustomsCostCorrectMethod(IXMLElement):
    """Класс для CustomsCostCorrectMethod"""
    
    def __init__(self, customs_cost_correct_method: str):
        self.customs_cost_correct_method = customs_cost_correct_method
    
    def to_xml(self, parent=None) -> ET.Element:
        if parent is not None:
            elem = ET.SubElement(parent, 'CustomsCostCorrectMethod')
        else:
            elem = ET.Element('CustomsCostCorrectMethod')
        elem.text = self.customs_cost_correct_method
        return elem


class CustCostMethod(IXMLElement):
    """Класс для CustCostMethod"""
    
    def __init__(self, customs_cost_correct_mark: str, customs_cost_correct_method: str):
        self.customs_cost_correct_mark = CustomsCostCorrectMark(customs_cost_correct_mark)
        self.customs_cost_correct_method = CustomsCostCorrectMethod(customs_cost_correct_method)
    
    def to_xml(self, parent=None) -> ET.Element:
        if parent is not None:
            elem = ET.SubElement(parent, 'CustCostMethod')
        else:
            elem = ET.Element('CustCostMethod')
        
        self.customs_cost_correct_mark.to_xml(elem)
        self.customs_cost_correct_method.to_xml(elem)
        return elem


class GTDOutGoodsResolution(IXMLElement):
    """Класс для GTDOutGoodsResolution"""
    
    def __init__(self, goods_numeric: str, gtd_out_goods_result: GTDOutGoodsResult, cust_cost_method: CustCostMethod):
        self.goods_numeric = goods_numeric
        self.gtd_out_goods_result = gtd_out_goods_result
        self.cust_cost_method = cust_cost_method
    
    def to_xml(self, parent=None) -> ET.Element:
        if parent is not None:
            elem = ET.SubElement(parent, 'GTDOutGoodsResolution')
        else:
            elem = ET.Element('GTDOutGoodsResolution')
        
        goods_numeric_elem = ET.SubElement(elem, 'GoodsNumeric')
        goods_numeric_elem.text = self.goods_numeric
        
        self.gtd_out_goods_result.to_xml(elem)
        self.cust_cost_method.to_xml(elem)
        return elem


class GTDOutResolution(IXMLElement):
    """Класс для GTDOutResolution"""
    
    def __init__(self, decision_code: str, date_inf: str, time_inf: str, lnp: str, resolution_description: str, person_name: str):
        self.decision_code = DecisionCode(decision_code)
        self.date_inf = DateInf(date_inf)
        self.time_inf = TimeInf(time_inf)
        self.lnp = LNP(lnp)
        self.foundation_des = FoundationDes(resolution_description)
        self.person_name = PersonName(person_name)
    
    def to_xml(self, parent=None) -> ET.Element:
        if parent is not None:
            elem = ET.SubElement(parent, 'GTDOutResolution')
        else:
            elem = ET.Element('GTDOutResolution')
        
        self.decision_code.to_xml(elem)
        self.date_inf.to_xml(elem)
        self.time_inf.to_xml(elem)
        self.lnp.to_xml(elem)
        self.foundation_des.to_xml(elem)
        self.person_name.to_xml(elem)
        return elem


class GTDID(IXMLElement):
    """Класс для GTDID"""
    
    def __init__(self, customs_code: str, registration_date: str, gtd_number: str):
        self.customs_code = CustomsCode(customs_code)
        self.registration_date = RegistrationDate(registration_date)
        self.gtd_number = GTDNumber(gtd_number)
    
    def to_xml(self, parent=None) -> ET.Element:
        if parent is not None:
            elem = ET.SubElement(parent, 'GTDID')
        else:
            elem = ET.Element('GTDID')
        
        self.customs_code.to_xml(elem)
        self.registration_date.to_xml(elem)
        self.gtd_number.to_xml(elem)
        return elem


class GTDoutCustomsMark(IXMLElement):
    """Класс для GTDoutCustomsMark"""
    
    def __init__(self, document_mode_id: str, document_id: str, gtd_document_id: str, 
                 gtd_out_resolution: GTDOutResolution, gtd_out_goods_resolution: GTDOutGoodsResolution, 
                 gtd_id: GTDID):
        self.document_mode_id = document_mode_id
        self.document_id = DocumentID(document_id)
        self.gtd_document_id = gtd_document_id
        self.gtd_out_resolution = gtd_out_resolution
        self.gtd_out_goods_resolution = gtd_out_goods_resolution
        self.gtd_id = gtd_id
    
    def to_xml(self, parent=None) -> ET.Element:
        # Определяем namespace map для корневого элемента
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
        }

        elem = ET.Element('GTDoutCustomsMark', nsmap=nsmap)
        elem.set('DocumentModeID', self.document_mode_id)
        
        self.document_id.to_xml(elem)
        
        gtd_document_id_elem = ET.SubElement(elem, 'GTDDocumentID')
        gtd_document_id_elem.text = self.gtd_document_id
        
        self.gtd_out_resolution.to_xml(elem)
        self.gtd_out_goods_resolution.to_xml(elem)
        self.gtd_id.to_xml(elem)
        
        return elem


def get_gtd_out_customs_mark():
    """Создание экземпляра GTDoutCustomsMark"""
    
    # Создаем GTDOutResolution
    gtd_out_resolution = GTDOutResolution(
        decision_code="10",
        date_inf="2025-04-22",
        time_inf="16:30:10",
        lnp="000",
        resolution_description="ВЫПУСК ТОВАРОВ РАЗРЕШЕН",
        person_name="АВТОМАТ"
    )
    
    # Создаем GTDOutGoodsResult
    gtd_out_goods_result = GTDOutGoodsResult(
        decision_code="10",
        date_inf="2025-04-22",
        lnp="000",
        resolution_description="ВЫПУСК ТОВАРОВ РАЗРЕШЕН",
        person_name="АВТОМАТ"
    )
    
    # Создаем CustCostMethod
    cust_cost_method = CustCostMethod(
        customs_cost_correct_mark="0",
        customs_cost_correct_method="1"
    )
    
    # Создаем GTDOutGoodsResolution
    gtd_out_goods_resolution = GTDOutGoodsResolution(
        goods_numeric="1",
        gtd_out_goods_result=gtd_out_goods_result,
        cust_cost_method=cust_cost_method
    )
    
    # Создаем GTDID
    gtd_id = GTDID(
        customs_code="10013160",
        registration_date="2025-04-22",
        gtd_number="5172692"
    )
    
    # Создаем основной объект
    gtd_out_customs_mark = GTDoutCustomsMark(
        document_mode_id="1006078E",
        document_id="B0768CE8-71BC-456A-B133-D1494B92B39F",
        gtd_document_id="C3311921-1FCD-42FB-B038-1786E96F85DE",
        gtd_out_resolution=gtd_out_resolution,
        gtd_out_goods_resolution=gtd_out_goods_resolution,
        gtd_id=gtd_id
    )
    
    return gtd_out_customs_mark


if __name__ == "__main__":
    gtd_out_customs_mark = get_gtd_out_customs_mark()
    print(gtd_out_customs_mark)