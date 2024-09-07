# DWH - пересмотр модели данных

## Описание:

В данном учебном задании необходимо осуществить миграцию данных в отдельные логические таблицы, а затем собрать на них витрину данных.

Изначально данные хранятся в одной таблице, shipping, где много дублирующейся и несистематизированной справочной информации. 


## Цель работы:

Из несистематизированной начальной таблицы shipping построить отдельные таблицы с качественными данными. На основании этих таблиц построить винтрину данных shipping_datamart для последующей аналитики. 

## Этапы работы: 

1. Создать справочник стоимости доставки в страны shipping_country_rates из данных, указанных в shipping_country и shipping_country_base_rate.
2. Создать справочник тарифов доставки вендора по договору shipping_agreement из данных строки vendor_agreement_description.
3. Создать справочник о типах доставки shipping_transfer из строки shipping_transfer_description.
4. Создать таблицу shipping_info, справочник комиссий по странам, с уникальными доставками shipping_id и связать ее с созданными справочниками shipping_country_rates, shipping_agreement, shipping_transfer и константной информации о доставке shipping_plan_datetime, payment_amount, vendor_id.
5. Создать таблицу статусов о доставке shipping_status и включить туда информацию из лога shipping.
6. Создать представление shipping_datamart на основании готовых таблиц для аналитики.

## Описание рабочих файлов:

shipping_migration.sql - SQL-запросы на создание и заполнение следующих таблиц: shipping_status, shipping_info, shipping_country_rates, shipping_agreement, shipping_transfer.

shipping_datamart.sql - SQL-запрос на создание представления shipping_datamart.
