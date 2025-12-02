import xml.etree.ElementTree as ET

from wrapper_classes import EDContainer

from ESADout_CU import get_ESADout_CU
from DTSout_CU import get_dts_out_cu
from GTDoutCustomsMark import get_gtd_out_customs_mark


if __name__ == "__main__":
    c = EDContainer()
    c.ESADout_CU = get_ESADout_CU()
    c.DTSout_CU = get_dts_out_cu()
    c.GTDoutCustomsMark = get_gtd_out_customs_mark()
    print(c)

    c.save("file", extension="xml")