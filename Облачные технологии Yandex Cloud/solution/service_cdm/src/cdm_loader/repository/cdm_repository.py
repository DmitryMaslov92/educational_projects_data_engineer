import uuid
from typing import Any, Dict, List
from pydantic import BaseModel

from lib.pg import PgConnect


class User_Product_Counters(BaseModel):
    user_id: uuid.UUID
    product_id: uuid.UUID
    product_name: str
    order_cnt: int

class User_Category_Counters(BaseModel):
    user_id: uuid.UUID
    category_id: uuid.UUID
    category_name: str
    order_cnt: int


class OrderCdmBuilder:
    def init(self, dict: Dict) -> None:
        self._dict = dict

    def user_product_counters(self) -> List[User_Product_Counters]:
        user_id = self._dict['user_id']
        prod_counter = []
        for prod_dict in self._dict['products']:
            prod_counter.append(
                User_Product_Counters(
                    user_id = user_id,
                    product_id = prod_dict['product_id'],
                    product_name = prod_dict['product_name'],
                    order_cnt = prod_dict['product_cnt']
                )
            )

    def user_category_counters(self) -> List[User_Category_Counters]:
        user_id = self._dict['user_id']
        cat_counter = []
        for prod_dict in self._dict['products']:
            cat_counter.append(
                User_Category_Counters(
                    user_id = user_id,
                    category_id = prod_dict['category_id'],
                    category_name = prod_dict['category_id'],
                    order_cnt = prod_dict['category_cnt']
                )
            )

class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def user_prod_insert(self, user_prod: User_Product_Counters) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO cdm.user_product_counters (user_id, product_id, product_name, order_cnt)
                        VALUES (%(user_id)s, %(product_id)s, %(product_name)s, %(load_src)s)
                        ON CONFLICT (user_id, product_id) DO UPDATE
                        SET
                            order_cnt = order_cnt + EXCLUDED.order_cnt
                    """,
                    {
                        'user_id': user_prod.user_id,
                        'product_id': user_prod.product_id,
                        'product_name': user_prod.product_name,
                        'order_cnt': user_prod.order_cnt
                    }
                )

    def user_cat_insert(self, user_cat: User_Category_Counters) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO cdm.user_category_counters (user_id, category_id, category_name, order_cnt)
                        VALUES (%(user_id)s, %(category_id)s, %(category_name)s, %(load_src)s)
                        ON CONFLICT (user_id, category_id) DO UPDATE
                        SET
                            order_cnt = order_cnt + EXCLUDED.order_cnt
                    """,
                    {
                        'user_id': user_cat.user_id,
                        'category_id': user_cat.category_id,
                        'category_name': user_cat.category_name,
                    }
                )