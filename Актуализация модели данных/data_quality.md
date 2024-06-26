# 1.3. Качество данных

## Оцените, насколько качественные данные хранятся в источнике.
Опишите, как вы проверяли исходные данные и какие выводы сделали.
Для проверки качества данных использовались следующие инструменты:
* поиск дублей;
* поиск пропущенных значений;
* проверка типов данных;
* проверка форматов данных.
Дублей и пропусков в таблицах нет. Типы и форматы данных соответсвуют. 
Однако между таблицами `orders` и `users` отсутсвет связь по внешнему ключу. Это может привести к тому, что в таблице `orders` могут появиться заказы от клиентов, которых нет в `users`.
Кроме того, поле `name` таблицы `users` может быть пустым (нет ограничения NOT NULL).

## Укажите, какие инструменты обеспечивают качество данных в источнике.
Ответ запишите в формате таблицы со следующими столбцами:
- `Наименование таблицы` - наименование таблицы, объект которой рассматриваете.
- `Объект` - Здесь укажите название объекта в таблице, на который применён инструмент. Например, здесь стоит перечислить поля таблицы, индексы и т.д.
- `Инструмент` - тип инструмента: первичный ключ, ограничение или что-то ещё.
- `Для чего используется` - здесь в свободной форме опишите, что инструмент делает.

Пример ответа:

| Таблицы             | Объект                      | Инструмент      | Для чего используется |
| ------------------- | --------------------------- | --------------- | --------------------- |
| production.orders | order_id int4 PRIMARY KEY | Первичный ключ  | Обеспечивает уникальность записей о заказах |
| production.orders | ((cost = (payment + bonus_payment))) | Ограничение CHECK  | Проверяет, что значение в поле cost равно сумме полей payment и bonus_payment |
| production.users | id int4 PRIMARY KEY | Первичный ключ  | Обеспечивает уникальность записей о пользователях |
| production.orderstatuses | id int4 PRIMARY KEY | Первичный ключ  | Обеспечивает уникальность записей о пользователях |
| production.orderstatuslog | id int4 PRIMARY KEY | Первичный ключ  | Обеспечивает уникальность записей в логе статусов заказов |
| production.orderstatuslog | order_id int4 NOT NULL FOREIGN KEY | Внешний ключ | Проверка на то, что в таблице orderstatuslog нет заказов, у которых order_id нет в orders |
| production.orderstatuslog | status_id int4  FOREIGN KEY | Внешний ключ | Проверка на то, что в таблице orderstatuslog нет заказов, у которых status_id нет в orderstatuses |
| production.orderstatuslog | order_id status_id UNIQUE KEY | Уникальный ключ  | Обеспечивает уникальность записей в связке номер заказа/статус заказа |
| production.products | id int4  PRIMARY KEY | Первичный ключ  | Обеспечивает уникальность записей о товарах |
| production.products | ((price >= (0)::numeric)) | Ограничение CHECK | Проверяет, что цена неотрицательная и с типом numeric |
| production.orderitems | id int4 PRIMARY KEY | Первичный ключ  | Обеспечивает уникальность записей о товарных позициях в заказе |
| production.orderitems | ((price >= (0)::numeric)) | Ограничение CHECK | Проверяет, что цена неотрицательная и с типом numeric |
| production.orderitems | (((discount >= (0)::numeric) AND (discount <= price))) | Ограничение CHECK | Проверяет, что скидка неотрицательная, не больше цены и с типом numeric |
| production.orderitems | ((quantity > 0)) | Ограничение CHECK | Проверяет, количество товара в заказе больше 0 |
| production.orderitems | order_id product_id UNIQUE KEY | Уникальный ключ  | Обеспечивает уникальность записей в связке номер заказа/товарная позиция |
| production.orderitems | order_id int4 NOT NULL FOREIGN KEY | Внешний ключ | Проверка на то, что в таблице orderitems нет заказов, у которых order_id нет в orders |
| production.orderitems | product_id int4 NOT NULL FOREIGN KEY | Внешний ключ | Проверка на то, что в таблице orderitems нет заказов, у которых product_id нет в products |
