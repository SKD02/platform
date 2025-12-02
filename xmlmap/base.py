from lxml import etree as ET
from typing import TypeVar
from abc import ABC, abstractmethod


class IXMLElement(ABC):
    """Общий интерфейс для описания всех XML элементов"""
    
    @abstractmethod
    def to_xml(self) -> ET.Element:
        pass

    
    def __str__(self) -> str:
        """
        Красиво выводит XML элемент используя lxml
        """
        element = self.to_xml()
        xml_str = ET.tostring(
            element, 
            encoding='utf-8', 
            pretty_print=True, 
            xml_declaration=True
        ).decode('utf-8')
        return xml_str


    def save(self, filename: str, extension: str = "xml") -> None:
        """
        Сохраняет XML элемент в файл
        
        Args:
            filename: Имя файла без расширения
            extension: Расширение файла (по умолчанию "xml")
        """
        with open(f"{filename}.{extension}", "w", encoding="utf-8") as file:
            file.write(str(self))


XMLElementType = TypeVar('XMLElementType', bound=IXMLElement)
