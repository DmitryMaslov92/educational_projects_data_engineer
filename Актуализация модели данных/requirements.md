# Витрина RFM

## 1.1. Выясните требования к целевой витрине.

Постановка задачи выглядит достаточно абстрактно - постройте витрину. Первым делом вам необходимо выяснить у заказчика детали. Запросите недостающую информацию у заказчика в чате.

Зафиксируйте выясненные требования. Составьте документацию готовящейся витрины на основе заданных вами вопросов, добавив все необходимые детали.

-----------

Нужно построить витрину `dm_rfm_segments` для RFM-классификации пользователей приложения на основании данных за 2022 год. Таблицы с данными для витрины находятся с схеме `production`. Витрина должна находится в схеме `analysis`. Она должна включать в себя следующие поля:
* `user_id` - идентификатор клиента
* `recency` - классификатор времени, прошедшего с последнего заказа (число от 1 до 5)
* `frequency ` - классифиактор количества заказов (число от 1 до 5)
* `monetary_value` - классификатор суммы затрат клиента (число от 1 до 5)
Для анализа необходимо выбирать только заказы со статусом `Closed`. 


## 1.2. Изучите структуру исходных данных.

Полключитесь к базе данных и изучите структуру таблиц.

Если появились вопросы по устройству источника, задайте их в чате.

Зафиксируйте, какие поля вы будете использовать для расчета витрины.

-----------

Для витрины не нужны персональные данные клиентов, а только их `id`. В таблице `orders` есть поле `user_id`, но так как оно не связано внешним ключом с таблице `users`, в нем могут быть посторонние значения, поэтому для построения витрины лучше использовать поле `id` из таблицы `users`. Кроме того, потребуется использовать только заказы, у которых значение в поле `status` таблицы `orders` равно 4, т.к. согласно таблице `orderstatuses` это значение равно статусу `Closed`.
Для полей `recency` и `frequency`, помимо указанных выше полей, необходимо взять данные из поля `order_ts`, а для `monetary_value` понадобится еще поле `payment`.


## 1.3. Проанализируйте качество данных

Изучите качество входных данных. Опишите, насколько качественные данные хранятся в источнике. Так же укажите, какие инструменты обеспечения качества данных были использованы в таблицах в схеме production.

-----------

Файл data_quality.md заполнен.


## 1.4. Подготовьте витрину данных

Теперь, когда требования понятны, а исходные данные изучены, можно приступить к реализации.

### 1.4.1. Сделайте VIEW для таблиц из базы production.**

Вас просят при расчете витрины обращаться только к объектам из схемы analysis. Чтобы не дублировать данные (данные находятся в этой же базе), вы решаете сделать view. Таким образом, View будут находиться в схеме analysis и вычитывать данные из схемы production. 

Напишите SQL-запросы для создания пяти VIEW (по одному на каждую таблицу) и выполните их. Для проверки предоставьте код создания VIEW.

```SQL
CREATE OR REPLACE VIEW analysis.orderitems AS (SELECT * FROM production.orderitems);
CREATE OR REPLACE VIEW analysis.orders AS (SELECT * FROM production.orders);
CREATE OR REPLACE VIEW analysis.orderstatuses AS (SELECT * FROM production.orderstatuses);
CREATE OR REPLACE VIEW analysis.orderstatuslog AS (SELECT * FROM production.orderstatuslog);
CREATE OR REPLACE VIEW analysis.products AS (SELECT * FROM production.products);
CREATE OR REPLACE VIEW analysis.users AS (SELECT * FROM production.users);
```

### 1.4.2. Напишите DDL-запрос для создания витрины.**

Далее вам необходимо создать витрину. Напишите CREATE TABLE запрос и выполните его на предоставленной базе данных в схеме analysis.

```SQL
CREATE TABLE IF NOT EXISTS analysis.dm_rfm_segments (
	user_id int4 PRIMARY KEY,
	recency int4 CHECK ((recency >= 1) AND (recency <= 5)),
	frequency int4 CHECK ((frequency >= 1) AND (frequency <= 5)),
	monetary_value int4 CHECK ((monetary_value >= 1) AND (monetary_value <= 5))
);


```

### 1.4.3. Напишите SQL запрос для заполнения витрины

Наконец, реализуйте расчет витрины на языке SQL и заполните таблицу, созданную в предыдущем пункте.

Для решения предоставьте код запроса.

```SQL
INSERT INTO analysis.dm_rfm_segments
SELECT trr.*, trf.frequency, trmv.monetary_value
FROM analysis.tmp_rfm_recency trr 
JOIN analysis.tmp_rfm_frequency trf on trr.user_id=trf.user_id 
JOIN analysis.tmp_rfm_monetary_value trmv on trr.user_id=trmv.user_id
ORDER BY user_id;

SELECT *
FROM analysis.dm_rfm_segments
LIMIT 10;



```



