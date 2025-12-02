""" Пример заполнения классов """
from .ESADout_CU import DocumentID as ESADout_CU_DocumentID, CustomsProcedure, CustomsModeCode, ElectronicDocumentSign, RecipientCountryCode, EECEDocHeaderAddInfo, ESADout_CU
from .ESADout_CUGoodsShipment import ESADout_CUGoodsShipment, ESADout_CUConsignor, SubjectAddressDetails, ESADout_CUConsignee, ESADout_CUFinancialAdjustingResponsiblePerson, RFOrganizationFeatures, ESADout_CUDeclarant, ESADout_CUConsigment, BorderCustomsOffice, RUTransportMeans, RUTransportMeans, ESADout_CUDepartureArrivalTransport, ESADout_CUBorderTransport, CUESADDeliveryTerms, ESADout_CUMainContractTerms, ESADout_CUGoodsLocation, RegisterDocumentIdDetails, GoodsGroupQuantity, GoodsGroupInformation, GoodsGroupDescription, Preferencii, DocumentPresentingDetails, ESADout_CUPresentedDocument, ESADout_CUCustomsPaymentCalculation, ESADout_CUCustomsPaymentCalculation, PackagePalleteInformation, PackagePalleteInformation, ESADGoodsPackaging, ESADCustomsProcedure, SupplementaryGoodsQuantity, ESADout_CUGoods


def fill_ESADout_CU_with_gt(
        document_id_str="", #SKD C3311921-1FCD-42FB-B038-1786E96F85DE
        declaration_date="",
        g1_1 = "ИМ",
        g1_2 = "40",
        g1_3 = "ЭД",
        g3_1 = 1, 
        g3_2 = 1, # равен g5_1 и равен TotalGoodsNumber
        g5_1 = "",
        g6_1 = "",

        g2_1 ="", # ИНН КПП ОГРН - только рос компании
        g2_2 ="",
        g2_3 ="",
        g2_5 ="",
        g2_4 ="",
        g2_6 ="",
        g2_7 ="",
        g2_8 ="",
        g2_9 ="",
        g2_10 ="",
        g2_11 ="",

        # Декларант
        g14_1="",
        g14_2="",
        g14_3="",
        g14_5="",
        g14_4="",
        g14_6="",
        g14_7="",
        g14_8="",
        g14_9="",
        g14_10="",
        g14_11="",

        g12_1="",
        g11_1="", #g2_4
        
        g17_1="",
        g17_2="",

        g15_1="",
        g15_2="",

        g16_2="",
        g16_1="",

        g18_1="",
        g18_2="", # здесь передаются номера сразу нескольких транспортных средств 
        g18_3="",

        g19_1="",

        g20_2 = "",
        g20_1 = "", 

        g22_1 = "",
        g22_2 = "",

        g26_1 = "",

        g23_1 = "",

        g24_1 = "",
        g24_2 = "", 

        g25_1 = "",

        g30_1 = "",
        g30_2 = "",  # в идеале заполняем только КОД
        g30_3 = "",  # Адрес нужно правильно разбивать, берем из ВЭД-Декларанта
        g30_4 = "",
        g30_5 = "",

        g29_1 = "",  # в идеале тоже брать по коду
        g29_2 = "",


        g31_1 = "",
        # g31_2 = "",
        g31_3 = "",
        g31_4 = "",
        g31_5 = "",
        # g31_6 = "",
        # g31_7 = "",
        # g31_8 = "",
        # g31_9 = "",
        # g31_10 = "",
        # g31_11 = "",
        # g31_12 = "",
        # g31_13 = "",
        # g31_14 = "",
        # g31_15 = "",
        # g31_16 = "",
        # g31_17 = "",
        # g31_18 = "",
        # g31_19 = "",
        # g31_20 = "",
        # g31_21 = "",
        # g31_22 = "",
        # g31_23 = "",
        # g31_24 = "",
        # g31_25 = "",
        # g31_26 = "",
        # g31_27 = "",
        # g31_28 = "",
        # g31_29 = "",
        # g31_30 = "",
        # g31_31 = "",
        # g31_32 = "",
        # g31_33 = "",
        # g31_34 = "",
        # g31_35 = "",

        g32_1 = "",  # подозреваю что здесь GoodsNumeric

        g34_1 = "",
        g34_2 = "",  # должна быть расшифровка, скип

        g35_1 = "",
        g38_1 = "",

        g37_1 = "",
        g40_1 = "",  

        g41_1 = "",
        g41_2 = "",
        g41_3 = "",

        g42_1 = "",

        g33_1 = "",

        g36_1 = "",
        g36_2 = "",
        g36_3 = "",
        g36_4 = "",

        g39_1 = "",
        g39_2 = "",
        g39_3 = "",

        g43 = "",
        g44_1 = "",

        g45_1 = "",
        g46_1 = "",

    ):
    
    def fill_goods_shipment_with_gt(
            TotalGoodsNumber=g5_1,
            TotalPackageNumber=g6_1,
            OrganizationName=g2_3,
            PostalCode=g2_6,
            CountryCode=g2_4,
            CounryName=g2_5,
            Region=g2_7,
            City=g2_8,
            StreetHouse=g2_9,
            House=g2_10,

            declarantOrganizationName=g14_3,
            declarantOGRN=g14_11,
            declarantINN=g14_1,
            declarantKPP=g14_2,
            declarantPostalCode=g14_6,
            declarantRegion=g14_7,
            declarantCountryCode=g14_4,
            declarantCounryName=g14_5,
            declarantCity=g14_8,
            declarantStreetHouse=g14_9,
            declarantHouse=g14_10,

            TotalCustCost=g12_1,
            TradeCountry=g11_1, # равен g2_4

            DestinationCountryCode=g17_1,
            DestinationCountryName=g17_2,
            DispatchCountryCode=g15_1,
            DispatchCountryName=g15_2,
            TransportMeansQuantity=g18_1,
            TransportIdentifier=g18_2, # TODO: корректная обработка строки
            TransportMeansNationalityCode=g18_3,
            ContainerIndicator=g19_1,
            DeliveryPlace=g20_2,
            DeliveryTermsStringCode=g20_1,

            ContractCurrencyCode=g22_1,
            TotalInvoiceAmount=g22_2,

            TransportModeCode=g26_1,
            ContractCurrencyRate=g23_1,

            DealFeatureCode=g24_2,
            DealNatureCode=g24_1,

            TransportModeCodeBorder=g25_1, 

            InformationTypeCode=g30_1,
            CustomsOffice=g30_2,
            Adress=g30_3,  
            DocId=g30_4,
            LicencePeriod=g30_5,


            BorderCustomsOfficeCode=g29_1,
            BorderCustomsOfficeOfficeName=g29_2,

            GoodsDescription = g31_1,           
            # g31_2 = g31_2,                      
            GoodsQuantity = g31_3,
            MeasureUnitQualifierName = g31_4,
            MeasureUnitQualifierCode = g31_5,
            # g31_6 = g31_6,                      # повтор, пропуск
            # g31_7 = g31_7,                      # повтор, пропуск
            # g31_8 = g31_8,                      # повтор, пропуск
            # g31_9 = g31_9,                      # ОИС ???
            # g31_10 = g31_10,                    # месторождение ???
            # PakageQuantity = g31_11, 
            # g31_12 = g31_12,                    # сейчас нет, тег PakagePartQuantity
            # g31_13 = g31_13,
            # g31_14 = g31_14,
            # g31_15 = g31_15,
            # g31_16 = g31_16,
            # g31_17 = g31_17,
            # g31_18 = g31_18,
            # g31_19 = g31_19,
            # g31_20 = g31_20,
            # g31_21 = g31_21,
            # g31_22 = g31_22,
            # g31_23 = g31_23,
            # g31_24 = g31_24,
            # g31_25 = g31_25,
            # g31_26 = g31_26,
            # g31_27 = g31_27,
            # g31_28 = g31_28,
            # g31_29 = g31_29,
            # g31_30 = g31_30,
            # g31_31 = g31_31,
            # g31_32 = g31_32,
            # g31_33 = g31_33,
            # g31_34 = g31_34,
            # g31_35 = g31_35,
            
            GoodsNumeric=g32_1,

            OriginCountryCode = g34_1,
            # g34_2 = g34_2,                    # непонятно где брать, скип

            GrossWeightQuantity = g35_1,
            NetWeightQuantity = g38_1,
            MainCustomsModeCode = g37_1,                     # нет примера,  4000
            g40_1 = g40_1,                     # аналогично, пусто 



            G41_GoodsQuantity = g41_1,           # потенциально путаница с g31_1
            G41_MeasureUnitQualifierName = g41_2,
            G41_MeasureUnitQualifierCode = g41_3,

            InvoicedCost = g42_1,

            GoodsTNVEDCode = g33_1,

            CustomsTax = g36_1,
            CustomsDuty = g36_2,
            Excise = g36_3,
            Rate = g36_4,

            g39_1 = g39_1,                     # квоты нет
            g39_2 = g39_2,
            g39_3 = g39_3,


            CustomsCostCorrectMethod = g43,
            g44_1 = g44_1,                     # непонятно как обрабатывать

            NationalDeclaredCustomsCost = g45_1,
            DollarDeclaredCustomsCost = g46_1,

        ):

        consignor = ESADout_CUConsignor(
            organization_name=OrganizationName,
            subject_address_details=SubjectAddressDetails(
                postal_code=PostalCode,
                country_code=CountryCode,
                country_name=CounryName,
                region=Region,
                city=City,
                street_house=StreetHouse,
                house=House,
            )
        )

        declarant = ESADout_CUDeclarant(
            organization_name=declarantOrganizationName,
            rf_organization_features= RFOrganizationFeatures(
                ogrn=declarantOGRN,
                inn=declarantINN,
                kpp=declarantKPP,

            ),
            subject_address_details=SubjectAddressDetails(
                postal_code=declarantPostalCode,
                country_code=declarantCountryCode,
                country_name=declarantCounryName,
                region=declarantRegion,
                city=declarantCity,
                street_house=declarantStreetHouse,
                house=declarantHouse,
            )
        )


        # consigment
        border_office = BorderCustomsOffice(
            code=BorderCustomsOfficeCode,
            office_name=BorderCustomsOfficeOfficeName,
            customs_country_code="643"  # нет кода страны
        )
        transport_means_1 = RUTransportMeans(
            transport_identifier=TransportIdentifier,
            transport_means_nationality_code=TransportMeansNationalityCode
        )
        transport_means_2 = RUTransportMeans(
            transport_identifier=TransportIdentifier,
            transport_means_nationality_code=TransportMeansNationalityCode,
            active_transport_identifier="" #SKD B072HM138
        )
        departure_transport = ESADout_CUDepartureArrivalTransport(
            transport_mode_code=TransportModeCode,
            transport_nationality_code=TransportMeansNationalityCode,
            transport_means_quantity=TransportMeansQuantity,
            ru_transport_means=[transport_means_1, transport_means_2]
        )
        border_transport = ESADout_CUBorderTransport(
            transport_mode_code=TransportModeCodeBorder,
            transport_means_quantity="1"
        )
        consigment = ESADout_CUConsigment(
            container_indicator=ContainerIndicator,
            dispatch_country_code=DispatchCountryCode,
            dispatch_country_name=DispatchCountryName,
            destination_country_code=DestinationCountryCode,
            destination_country_name=DestinationCountryName,
            border_customs_office=border_office,
            departure_arrival_transport=departure_transport,
            border_transport=border_transport
        )


        #  местоположение товаров
        location_address = SubjectAddressDetails( # TODO корректно разбивать адрес g30_3
            country_code="RU",
            country_name="РОССИЯ",
            region="", #МОСКОВСКАЯ ОБЛАСТЬ
            city="", #ГОРОД КРАСНОЗНАМЕНСК
            street_house="", #УЛ БЕРЕЗОВАЯ АЛЛЕЯ
            house="" #Д. 5
        )
        goods_location = ESADout_CUGoodsLocation(
            information_type_code=InformationTypeCode,
            customs_office=CustomsOffice,
            customs_country_code="RU",
            location_name="", #SKD
            register_document_id_details=RegisterDocumentIdDetails(doc_id=DocId),
            address=location_address
        )

        # условия контракта ESADout_CUMainContractTerms
        delivery_terms = CUESADDeliveryTerms(
            delivery_place=DeliveryPlace,
            delivery_terms_string_code=DeliveryTermsStringCode,
        )
        contract_terms = ESADout_CUMainContractTerms(
            contract_currency_code=ContractCurrencyCode,
            contract_currency_rate=ContractCurrencyRate,
            total_invoice_amount=TotalInvoiceAmount,
            trade_country_code="CN",
            deal_feature_code=DealFeatureCode,
            deal_nature_code=DealNatureCode,
            cu_esad_delivery_terms=delivery_terms
        )



######## Товар
        goods_group_qty = GoodsGroupQuantity(
            goods_quantity=GoodsQuantity,
            measure_unit_qualifier_name=MeasureUnitQualifierName,
            measure_unit_qualifier_code=MeasureUnitQualifierCode
        )
        
        goods_group_info = GoodsGroupInformation(
            manufacturer="",
            goods_mark="ОТСУТСТВУЕТ",
            goods_model="ОТСУТСТВУЕТ",
            goods_marking="ОТСУТСТВУЕТ",
            serial_number="",
            goods_group_quantity=goods_group_qty,
            invoiced_cost=""
        )
        
        goods_group_desc = GoodsGroupDescription(
            goods_description="",
            goods_group_information=goods_group_info,
            group_num="1"
        )
        
        preferencii = Preferencii(
            customs_tax= CustomsTax,
            customs_duty= CustomsDuty,
            excise= Excise,
            rate= Rate,
        )
        
        # Создаем документ (один пример)
        doc_presenting = DocumentPresentingDetails(
            doc_present_kind_code="0",
            presented_document_mode_code="01402"
        )
        
        presented_doc = ESADout_CUPresentedDocument(
            pr_document_name="pr_document_name", #ДЕКЛАРАЦИЯ О СООТВЕТСТВИИ ТРЕБОВАНИЯМ ТР ЕАЭС
            pr_document_number="pr_document_number", #ЕАЭС N RU Д-CN.РА10.В.35527/23
            pr_document_date="pr_document_date", #2023-12-05
            presented_document_mode_code="presented_document_mode_code", #01402
            document_begin_actions_date="document_begin_actions_date", #2023-12-05
            document_end_actions_date="document_end_actions_date", #2028-12-04
            record_id="", #5CC73D96-1F95-4073-BBD5-C12C23538175
            document_presenting_details=doc_presenting
        )
        
        # Создаем расчет платежей
        # payment_calc_1 = ESADout_CUCustomsPaymentCalculation(
        #     payment_mode_code="1010",
        #     payment_amount="16524",
        #     payment_currency_code="643",
        #     tax_base="3982409.34",
        #     tax_base_currency_code="643",
        #     rate="16524",
        #     rate_type_code="*",
        #     rate_currency_code="643",
        #     rate_use_date="2025-04-22",
        #     payment_code="ИУ"
        # )
        
        # payment_calc_2 = ESADout_CUCustomsPaymentCalculation(
        #     payment_mode_code="5010",
        #     payment_amount="796481.87",
        #     payment_currency_code="643",
        #     tax_base="3982409.34",
        #     tax_base_currency_code="643",
        #     rate="20",
        #     rate_type_code="%",
        #     rate_use_date="2025-04-22",
        #     payment_code="ИУ"
        # )
        
        # Создаем упаковку
        pallete_info_1 = PackagePalleteInformation(
            info_kind_code="", #0
            pallete_code="PK",
            pallete_quantity="" #5
        )
        
        # мб несколько
        # pallete_info_2 = PackagePalleteInformation(
        #     info_kind_code="3",
        #     pallete_code="PX",
        #     pallete_quantity="5",
        #     cargo_description_text="."
        # )
        
        goods_packaging = ESADGoodsPackaging(
            pakage_quantity="5",
            pakage_type_code="1",
            package_pallete_information=[pallete_info_1]
        )
        
        # Создаем таможенную процедуру. остальное занулется всегда
        customs_procedure = ESADCustomsProcedure(
            main_customs_mode_code=MainCustomsModeCode,
            preceding_customs_mode_code="00",
            goods_transfer_feature="000"
        )
        
        # Создаем дополнительное количество
        suppl_quantity = SupplementaryGoodsQuantity(
            goods_quantity=G41_GoodsQuantity,
            measure_unit_qualifier_name=G41_MeasureUnitQualifierName,
            measure_unit_qualifier_code=G41_MeasureUnitQualifierCode
        )
        
        # Создаем товар
        goods = ESADout_CUGoods(
            goods_numeric=GoodsNumeric,
            goods_descriptions=GoodsDescription, # должен быть списком по 250 символов
            gross_weight_quantity=GrossWeightQuantity,
            net_weight_quantity=NetWeightQuantity,
            invoiced_cost=InvoicedCost,
            customs_cost=NationalDeclaredCustomsCost,
            statistical_cost=DollarDeclaredCustomsCost,
            goods_tnved_code=GoodsTNVEDCode,
            intellect_property_sign="N",
            origin_country_code=OriginCountryCode,
            customs_cost_correct_method=CustomsCostCorrectMethod,
            additional_sheet_count="1",
            goods_group_description=goods_group_desc,
            preferencii=preferencii,
            language_goods="RU",
            presented_documents=[presented_doc],
            # customs_payment_calculations=[payment_calc_1, payment_calc_2],
            supplementary_goods_quantity=suppl_quantity,
            esad_goods_packaging=goods_packaging,
            esad_customs_procedure=customs_procedure,
            additions_sign = "C"
        )




        return ESADout_CUGoodsShipment(
            origin_country_name="КИТАЙ",  ## Должен быть парсинг в название
            origin_country_code=TradeCountry, # TradeCounrty потенциально
            total_goods_number=TotalGoodsNumber,
            total_package_number=TotalPackageNumber,
            total_sheet_number=TotalGoodsNumber,
            total_cust_cost=TotalCustCost,
            cust_cost_currency_code="",
            consignor = consignor,
            consignee = ESADout_CUConsignee(equal_indicator="1"),
            financial_adjusting_responsible_person = ESADout_CUFinancialAdjustingResponsiblePerson(declarant_equal_flag="1"),
            declarant=declarant,
            consigment=consigment,
            main_contract_terms=contract_terms,
            goods_location=goods_location,
            goods_list=[goods],
        )


    # Создание объекта с данными
    document_id = ESADout_CU_DocumentID(document_id_str)
    customs_procedure = CustomsProcedure(g1_1)
    customs_mode_code = CustomsModeCode(g1_2)
    electronic_document_sign = ElectronicDocumentSign(g1_3)
    recipient_country_code = RecipientCountryCode("RU")

    # не уверен, что он генерируется так
    ece_doc_header_add_info = EECEDocHeaderAddInfo(
        e_doc_code="R.036",
        e_doc_date_time=declaration_date, #SKD
        language_code="RU",
        source_country_code="RU",
        destination_country_code="RU"
    )

    esa_dout_cu = ESADout_CU(
        document_mode_id="1006107E", #Заменил на 1006107E
        document_id=document_id,
        customs_procedure = customs_procedure,
        customs_mode_code = customs_mode_code,
        electronic_document_sign = electronic_document_sign,
        recipient_country_code = recipient_country_code,
        ece_doc_header_add_info = ece_doc_header_add_info,
        goods_shipment = fill_goods_shipment_with_gt(),
        filled_person = "",
        customs_representative = ""
        # filled_person = get_filled_person(),
        # customs_representative=get_CUESADCustomsRepresentative(),
    )
    return esa_dout_cu


if __name__ == "__main__":

######  1 — ДЕКЛАРАЦИЯ

    ### g1_1 = st.text_input("1.1 — Вид ТП", value=default_kind_TP, label_visibility="collapsed")
    ### g1_2 = st.text_input("1.2 — Код ТП", value=default_code_TP, label_visibility="collapsed")
    ### g1_3 = st.text_input("1.3 — Электронное", value="ЭД", label_visibility="collapsed") #По умолчанию
    g1_1 = "ИМ"
    g1_2 = "40"
    g1_3 = "ЭД"


###### 3 — Формы")
    g3_1 = 1 # st.text_input("3.1 — Номер страницы", value="1", label_visibility="collapsed")
    g3_2 = 1 # st.text_input("3.2 — MAX кол-во страниц", 

###### 5 — Всего тов.
    g5_1 = "" # st.text_input("5.1 — Общее кол-во товаров",
###### 6 — Всего мест
    g6_1 = "" # st.text_input("6.1 — Общее кол-во мест", get_total_places(all_data), label_visibility="collapsed")


###### 2 — Отправитель/Экспортёр")
    g2_1 = "" # st.text_input("2.1 — ИНН",
    g2_2 = "" # st.text_input("2.2 — КПП",
    g2_3 = "" # st.text_input("2.3 — Наименование",
    g2_5 = "" # st.text_input("2.5 — Страна", 
    g2_4 = "" # st.text_input("2.4 — Код страны",
    g2_6 = "" # st.text_input("2.6 — Индекс", 
    g2_7 = "" # st.text_input("2.7 — Регион", 
    g2_8 = "" # st.text_input("2.8 — Город", 
    g2_9 = "" # st.text_input("2.9 — Улица",                                 
    g2_10 = "" # st.text_input("2.10 — Дом",                                 
    g2_11 = "" # st.text_input("2.11 — ОГРН", 



###### 8 — Получатель - НЕ ЗАПОЛНЯЕМ, СМ ГРАФУ 14
    g8_1 = "" # st.text_input("8.1 — ИНН",
    g8_2 = "" # st.text_input("8.2 — КПП",
    g8_3 = "" # st.text_input("8.3 — Наименование",
    g8_5 = "" # st.text_input ("8.5 — Страна",
    g8_4 = "" # st.text_input("8.4 — Код страны",
    g8_6 = "" # st.text_input("8.6 — Индекс", 
    g8_7 = "" # st.text_input("8.7 — Город", 
    g8_8 = "" # st.text_input("8.8 — Улица, дом",                                  
    g8_9 = "" # st.text_input("8.9 — Дом",                                  
    g8_10 ="" #  st.text_input("8.10 — ОГРН", 

###### 9 — Лицо, ответственное за финансовое урегулирование - НЕ ЗАПОЛНЯЕМ, СМ ГРАФУ 14
    g9_1 = "" # st.text_input("9.1 — ИНН",
    g9_2 = "" # st.text_input("9.2 — КПП",
    g9_3 = "" # st.text_input("9.3 — Наименование",
    g9_5 = "" # st.text_input("9.5 — Страна",
    g9_4 = "" # st.text_input("9.4 — Код страны",
    g9_6 = "" # st.text_input("9.6 — Индекс", 
    g9_7 = "" # st.text_input("9.7 — Город",
    g9_8 = "" # st.text_input("9.8 — Улица",
    g9_9 = "" # st.text_input("9.9 — Дом",
    g9_10 = "" # st.text_input("9.10 — ОГРН", 

###### 14 — Декларант
    g14_1 = "" # st.text_input("14.1 — ИНН",
    g14_2 = "" # st.text_input("14.2 — КПП",
    g14_3 = "" # st.text_input("14.3 — Наименование",
    g14_5 = "" # st.text_input("14.5 — Страна", 
    g14_4 = "" # st.text_input("14.4 — Код страны",
    g14_6 = "" # st.text_input("14.6 — Индекс", 
    g14_7 = "" # st.text_input("2.7 — Регион", 
    g14_8 = "" # st.text_input("14.7 — Город",
    g14_9 = "" # st.text_input("14.8 — Улица",
    g14_10 = "" # st.text_input("14.10 — Дом",
    g14_11 = "" # st.text_input("14.10 — ОГРН", 


###### 12 — Общая таможенная стоимость")
    g12_1 = "" # st.text_input("12.1 — Сумма", value=get_total_sum_rub(all_data), label_visibility="collapsed")

###### 11 — Торг. страна")
    g11_1 = "" # st.text_input("11.1 — Код страны", value=g2_4, label_visibility="collapsed")

###### 15 — Код и наименование страны отпр./эксп.")
    g15_1 = "" # st.text_input("15.1 — Код", value=get_country_code(g15_2), label_visibility="collapsed")
    g15_2 = "" # st.text_input("15.2 — Страна", value=normalize_country(get_any(all_data, [
 

###### 17 — Код и наименование страны назначения !Доделать коносамент")
    g17_2 = "" # st.text_input("17.2 — Страна", value=normalize_country(get_any(all_data, [
    g17_1 = "" # st.text_input("17.1 — Код", value=get_country_code(g17_2), label_visibility="collapsed")

###### 16 — Код и наименование страны происхождения"
    g16_2 = "" # st.text_input("16.2 — Страна", value=default_g16_2, label_visibility="collapsed")
    g16_1 = "" # st.text_input("16.1 — Код", value=default_g16_1, label_visibility="collapsed")


###### 18 — Идентификация и страна регистрации трансп.средства при отправлении/прибытии")
    g18_1 = "" #st.text_input("18.1 — Количество", value="", label_visibility="collapsed")
    g18_2 = "" #st.text_input("18.2 — Номера ТС (через ; прицеп через /)", value="", label_visibility="collapsed")
    g18_3 = "" #st.text_input("18.3 — Код страны регистрации ТС", value="", label_visibility="collapsed")
        
###### 21 — Идентификация и страна регистрации активного трансп.средства на границе")
    g21_1 = "" #st.text_input("21.1 — Количество", value=get_transport(all_data)[0], label_visibility="collapsed")
    g21_2 = "" #st.text_input("21.2 — Номера ТС (через ; прицеп через /)", value=get_transport(all_data)[1], label_visibility="collapsed")
    g21_3 = "" #st.text_input("21.3 — Код страны регистрации ТС", value=get_transport(all_data)[2], label_visibility="collapsed")

###### 19 — Конт.
    g19_1 = "" #st.text_input("19 — Бинарная", value = "1", label_visibility="collapsed") 

###### 20 Условия поставки")
    g20_2 = "" # st.text_input("20.2 — Место", value=get_incoterms(get_any(all_data, [
    g20_1 = "" # st.text_input("20.1 — Код инкотермс", value=get_incoterms(get_any(all_data, [

###### 22 — Валюта и общая сумма по счету")
    g22_1 = "" # st.text_input("22.1 — Валюта", value=get_currency(all_data), label_visibility="collapsed") 
    g22_2 = "" # st.text_input("22.2 — Сумма по инвойсу", value=get_total_sum_invoice(all_data), label_visibility="collapsed") 

###### 26 — Вид транспорта внутри страны")
    g26_1 = "" # st.text_input("26.1 — Код", value = "", label_visibility="collapsed") 

###### 23 — Курс валюты")
    g23_1 = "" # st.text_input("23.1 — Курс", value=cb_rate(date_declaration, get_currency(all_data)), label_visibility="collapsed") 

###### 24 — Характер сделки")
    g24_1 = "" # st.text_input("24.1 — Код характера сделки", value = "010", label_visibility="collapsed") 
    g24_2 = "" # st.text_input("24.2 — Код особенности сделки", value = nature_transaction(all_data), label_visibility="collapsed") 

###### 25 — Вид транспорта на границе")
    g25_1 = "" # st.text_input("25.1 — Код", value = get_transport_type(all_data, g21_2), label_visibility="collapsed") 


###### 30 - Местонахождение товаров")
    g30_1 = "" # st.text_input("30.1 — Код", value = "11", label_visibility="collapsed")
    g30_2 = "" # st.text_input("30.2 — Код таможенного органа", 
    g30_3 = "" # st.text_input("30.2 — Адрес", value = svh["Адрес"], label_visibility="collapsed")
    g30_4 = "" # st.text_input("30.3 — Номер лицензии", value = svh["Лицензия"].split("действует")[0], label_visibility="collapsed")
    g30_5 = "" # st.text_input("30.4 — Срок лицензии", value = svh["Лицензия"].split("действует")[1], label_visibility="collapsed")


###### 29 - Орган въезда/выезда")
    g29_1 = "" # st.text_input("29.1 — Код", value = g30_2, label_visibility="collapsed") 
    g29_2 = "" # st.text_input("29.2 — Остальная информация", value = svh_name, label_visibility="collapsed") 



###### 31 - Маркировка и кол. -Номера конт. - Кол. и отлич. особенности")
    g31_1 = "" # st.text_area("31.1 — Наименование, характеристики", value = "")
    g31_2 = "" # st.text_area("31.2 — Дополнение", value = "", label_visibility="collapsed")
    g31_3 = "" # st.text_input("31.3 - Количество", value = "", label_visibility="collapsed")
    g31_4 = "" # st.text_input("31.4 - Наименование единицы измерения", value = "", label_visibility="collapsed")
    g31_5 = "" # st.text_input("31.5 - Код единицы измерения", value = "", label_visibility="collapsed")
    g31_6 = "" # st.text_input("31.6 - Количество", value = "", label_visibility="collapsed")
    g31_7 = "" # st.text_input("31.7 - Наименование единицы измерения", value = "", label_visibility="collapsed")
    g31_8 = "" # st.text_input("31.8 - Код единицы измерения", value = "", label_visibility="collapsed")
    g31_9 = ""  #st.text_input("###### 31.9 - Регистрационный номер ОИС",value = "", label_visibility="collapsed")
    g31_10 = "" # st.text_input("###### 31.10 - Сведения о месторождении",value = "", label_visibility="collapsed")
    g31_11 = "" # st.text_input("31.11 - Общее кол-во мест", value = "", label_visibility="collapsed")
    g31_12 = "" # st.text_input("31.12 - Общее кол-во мест занимаемых частично", value = "", label_visibility="collapsed")
    g31_13 = "" # st.text_input("31.13 - Код наличия упаковки", value = "", label_visibility="collapsed")
    g31_14 = "" # st.text_input("31.14 - Вид грузю места", value = "", label_visibility="collapsed")
    g31_15 = "" # st.text_input("31.15 - Сведения о поддонах",value = "", label_visibility="collapsed")
    g31_16 = "" # st.text_input("31.16 - Сведения об индивидуальной таре",value = "", label_visibility="collapsed")
    g31_17 = "" # st.text_input("31.17 - Номера контейнеров",value = "", label_visibility="collapsed")
    g31_18 = "" # st.text_input("31.18 - Сведения об акцизах",value = "", label_visibility="collapsed")
    g31_19 = "" # st.text_input("31.19 - Сведения об условиях доставки",value = "", label_visibility="collapsed")
    g31_20 = "" # st.text_input("31.20 - Сведения о переработке",value = "", label_visibility="collapsed")
    g31_21 = "" # st.text_input("31.21 - Дата начала периода", value = "", label_visibility="collapsed")
    g31_22 = "" # st.text_input("31.22 - Дата окончания периода", value = "", label_visibility="collapsed")
    g31_23 = "" # st.text_input("31.23 - Количество нефтепродуктов", value = "", label_visibility="collapsed")
    g31_24 = "" # st.text_input("31.24 - Единицы измерения", value = "", label_visibility="collapsed")
    g31_25 = "" # st.text_input("31.25 - Сведения об электроенергии" , value = "", label_visibility="collapsed")
    g31_26 = "" # st.text_input("31.26 - Нанесенные контрольные знаки", value = "", label_visibility="collapsed")
    g31_27 = "" # st.text_input("31.27 - Количество нанесенных знаков", value = "", label_visibility="collapsed")
    g31_28 = "" # st.text_input("31.28 - Сведения о нанесенных знаках", value = "", label_visibility="collapsed")
    g31_29 = "" # st.text_input("31.29 - Признак нанесения идентификации", value = "", label_visibility="collapsed")
    g31_30 = "" # st.text_input("31.30 - Количество кодов идентификации", value = "", label_visibility="collapsed")
    g31_31 = "" # st.text_input("31.31 - Сведения о средствах идентификации", value = "", label_visibility="collapsed")
    g31_32 = "" # st.text_input("31.32 - Страна назначения при периодическом декларировании", value = "", label_visibility="collapsed")
    g31_33 = "" # st.text_input("31.33 - Кол-во товаров подлежащие последовательности", value = "", label_visibility="collapsed")
    g31_34 = "" # st.text_input("31.34 - Код единицы измерения ", value = "", label_visibility="collapsed")
    g31_35 = "" #st.text_input("31.35 - Наименвоание единицы измерения", value = "", label_visibility="collapsed")


###### 32 - Товар")
    g32_1= "" # st.text_input("32.1 - Доп информация о товаре", value = "", label_visibility="collapsed")


###### 34 - Код страны происхождения")
    g34_1 = "" # st.text_input("34.1 — Код", value = get_country_code(get_product_country(all_data)), label_visibility="collapsed")
    g34_2 = "" # st.text_input("34.2 - Код страны при преференциях", value = "", label_visibility="collapsed")


###### 35 - Вес брутто")
    g35_1 = "" # st.text_input("35 — Вес", value = get_brutto(all_data), label_visibility="collapsed")

###### 38 - Вес нетто")
    g38_1 = "" # st.text_input("38 — Вес", value = get_netto(all_data), label_visibility="collapsed")


 ###### 37 - Процедура")
    g37_1 = "" # st.text_input("37.1 - Код", value = default_code_TP + "00" + "000", label_visibility="collapsed") #КЛАССИФИКАТОР ОСОБЕННОСТЕЙ ПЕРЕМЕЩЕНИЯ ТОВАРОВ - по умолчанию 000

###### 40 - Общая декларация/Предшествующий документ") 
    g40_1 = "" # st.text_input("40.1 - Предшествующий документ", value = "", label_visibility="collapsed")

###### 41 - Дополнительные единицы")
    g41_1 = "" # st.text_input("41.1 - Количество/Единицы измерения/Код", value=get_product_unit(all_data), label_visibility="collapsed")
    g41_2 = "" # st.text_input("41.2 - ", value = "", label_visibility="collapsed")
    g41_3 = "" # st.text_input("41.3 - ", value = "", label_visibility="collapsed")

###### 42 - Цена товара")
    g42_1 = "" # st.text_input("42 - Цена в иностранной валюте", value=get_total_sum_invoice(all_data), label_visibility="collapsed")


###### 33 - Код товара")
    g33_1 = "" # st.text_input("33.1 — Номер", value = get_tnved(all_data), label_visibility="collapsed")


###### 36 - Преференции") 
    g36_1 = "" # st.text_input("36.1 — Преференция таможенных сборов", value = "OO", label_visibility="collapsed")
    g36_2 = "" # st.text_input("36.2 — Преференция пошлины", value = "OO", label_visibility="collapsed")
    g36_3 = "" # st.text_input("36.3 — Преференция акцизы", value = "-", label_visibility="collapsed")
    g36_4 = "" # st.text_input("36.4 — Преференция НДС", value = "OO", label_visibility="collapsed")


###### 39 - Квота
    g39_1 = "" # st.text_input("39.1 - Квота", value = "", label_visibility="collapsed")
    g39_2 = "" # st.text_input("39.2 - Единицы измерения квоты", value = "", label_visibility="collapsed")
    g39_3 = "" # st.text_input("39.3 - Код единицы", value = "", label_visibility="collapsed")


###### 43 - Код МОС")
    g43 = "" # st.text_input("Код признака", value = ch43["code"])

###### 44 - Дополнительная информация / Предоставленные документы")
    g44_1 = "" # st.text_area("44 - Список всех документов",value=docs_text,height=120, label_visibility="collapsed")

###### 45 - Таможенная стоимость")
    g45_1 = "" # st.text_input("45.1 Таможенная стоимость", value = "", label_visibility="collapsed")

###### 46 - Статистическая стоимость")
    g46_1 = "" # st.text_input("46.1 Таможенная стоимость", value = "", label_visibility="collapsed")


    fill_ESADout_CU_with_gt(
        document_id_str=" ",
        g1_1=g1_1,
        g1_2=g1_2,
        g1_3=g1_3, 
        g3_1=g3_1,
        g3_2=g3_2,
        g5_1=g5_1,
        g6_1=g6_1,

        g2_1=g2_1,
        g2_2=g2_2,
        g2_3=g2_3,
        g2_5=g2_5,
        g2_4=g2_4,
        g2_6=g2_6,
        g2_7=g2_7,
        g2_8=g2_8,
        g2_9=g2_9,
        g2_10= g2_10,
        g2_11=g2_11,

        g14_1=g14_1,
        g14_2=g14_2,
        g14_3=g14_3,
        g14_5=g14_5,
        g14_4=g14_4,
        g14_6=g14_6,
        g14_7=g14_7,
        g14_8=g14_8,
        g14_9=g14_9,
        g14_10=g14_10,
        g14_11=g14_11,

        g12_1=g12_1, 
        g11_1=g2_4,

        g17_2=g17_2,
        g17_1=g17_1,
        
        g15_1=g15_1,
        g15_2=g15_2,

        g16_2=g16_2,
        g16_1=g16_1,

        g18_1=g18_1,
        g18_2=g18_2,
        g18_3=g18_3,

        g19_1=g19_1,

        g20_2=g20_2,
        g20_1=g20_1,
        
        g22_1=g22_1,
        g22_2=g22_2,

        g26_1=g26_1,

        g23_1=g23_1,

        g24_1=g24_1,
        g24_2=g24_2,

        g25_1=g25_1,

        g30_1=g30_1,
        g30_2=g30_2,
        g30_3=g30_3,
        g30_4=g30_4,
        g30_5=g30_5,

        g29_1=g29_1,
        g29_2=g29_2,


        g31_1 = g31_1,
        # g31_2 = g31_2,
        g31_3 = g31_3,
        g31_4 = g31_4,
        g31_5 = g31_5,
        # g31_6 = g31_6,
        # g31_7 = g31_7,
        # g31_8 = g31_8,
        # g31_9 = g31_9,
        # g31_10 = g31_10,
        # g31_11 = g31_11,
        # g31_12 = g31_12,
        # g31_13 = g31_13,
        # g31_14 = g31_14,
        # g31_15 = g31_15,
        # g31_16 = g31_16,
        # g31_17 = g31_17,
        # g31_18 = g31_18,
        # g31_19 = g31_19,
        # g31_20 = g31_20,
        # g31_21 = g31_21,
        # g31_22 = g31_22,
        # g31_23 = g31_23,
        # g31_24 = g31_24,
        # g31_25 = g31_25,
        # g31_26 = g31_26,
        # g31_27 = g31_27,
        # g31_28 = g31_28,
        # g31_29 = g31_29,
        # g31_30 = g31_30,
        # g31_31 = g31_31,
        # g31_32 = g31_32,
        # g31_33 = g31_33,
        # g31_34 = g31_34,
        # g31_35 = g31_35,

        g34_1 = g34_1,
        g34_2 = g34_2,
    
        g35_1 = g35_1,

        g38_1 = g38_1,

        g37_1 = g37_1,

        g40_1 = g40_1,

        g41_1 = g41_1,
        g41_2 = g41_2,
        g41_3 = g41_3,
        
        g42_1 = g42_1,

        g33_1 = g33_1,

        g36_1 = g36_1,
        g36_2 = g36_2,
        g36_3 = g36_3,
        g36_4 = g36_4,
    
        g39_1 = g39_1,
        g39_2 = g39_2,
        g39_3 = g39_3,

        g43 = g43,
        g44_1 = g44_1,
        
        g45_1 = g45_1,
        g46_1 = g46_1,
    
    )



