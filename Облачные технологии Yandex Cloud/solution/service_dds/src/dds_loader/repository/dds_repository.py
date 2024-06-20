import uuid
from datetime import datetime
from typing import Any, Dict, List
from pydantic import BaseModel

from lib.pg import PgConnect

class H_User(BaseModel):
    h_user_pk: uuid.UUID
    user_id: str
    load_dt: datetime
    load_src: str

class H_Restaurant(BaseModel):
    h_restaurant_pk: uuid
    restaurant_id: str
    load_dt: datetime
    load_src: str

class H_Product(BaseModel):
    h_product_pk: uuid.UUID
    product_id: str
    load_dt: datetime
    load_src: str 

class H_Categoty(BaseModel):
    h_category_pk: uuid.UUID
    category_name: str
    load_dt: datetime
    load_src: str 

class H_Order(BaseModel):
    h_order_pk: uuid.UUID
    order_id: str
    order_dt: datetime
    load_dt: datetime
    load_src: str

class L_Order_Product(BaseModel):
    hk_order_product_pk: uuid.UUID
    h_order_pk: uuid.UUID
    h_product_pk: uuid.UUID
    load_dt: datetime
    load_src: str

class L_Order_User(BaseModel):
    hk_order_user_pk: uuid.UUID
    h_order_pk: uuid.UUID
    h_user_pk: uuid.UUID
    load_dt: datetime
    load_src: str

class L_Product_Restaurant(BaseModel):
    hk_product_restaurant_pk: uuid.UUID
    h_restaurant_pk: uuid.UUID
    h_product_pk: uuid.UUID
    load_dt: datetime
    load_src: str

class L_Product_Category(BaseModel):
    hk_product_category_pk: uuid.UUID
    h_category_pk: uuid.UUID
    h_product_pk: uuid.UUID
    load_dt: datetime
    load_src: str

class S_Product_Names(BaseModel):
    h_product_pk: uuid.UUID
    name: str
    load_dt: datetime
    load_src: str
    hk_product_names_hashdiff: uuid.UUID

class S_Restaurant_Names(BaseModel):
    h_restaurant_pk: uuid.UUID
    name: str
    load_dt: datetime
    load_src: str
    hk_restaurant_names_hashdiff: uuid.UUID

class S_User_Names(BaseModel):
    h_user_pk: uuid.UUID
    username: str
    userlogin: str
    load_dt: datetime
    load_src: str
    hk_user_names_hashdiff: uuid.UUID

class S_Order_Status(BaseModel):
    h_order_pk: uuid.UUID
    status: str
    load_dt: datetime
    load_src: str
    hk_order_status_hashdiff: uuid.UUID

class S_Order_Cost(BaseModel):
    h_order_pk: uuid.UUID
    cost: float
    payment: float
    load_dt: datetime
    load_src: str
    hk_order_cost_hashdiff: uuid.UUID


class OrderDdsBuilder:
    def init(self, dict: Dict) -> None:
        self._dict = dict
        self.source_system = "orders-system-kafka"
        self.order_ns_uuid = uuid.UUID('')

    def _uuid(self, obj: Any) -> uuid.UUID:
        return uuid.uuid5(namespace=self.order_ns_uuid, name=str(obj))
    
    def h_user(self) -> H_User:
        user_id = self._dict['user']['id']
        return H_User(
            h_user_pk=self._uuid(user_id),
            user_id=user_id,
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )
    
    def h_restaurant(self) -> H_User:
        restaurant_id = self._dict['restaurant']['id']
        return H_Restaurant(
            h_restaurant_pk=self._uuid(restaurant_id),
            restaurant_id=restaurant_id,
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )
    
    def h_product(self) -> List[H_Product]:
        products = []
        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            products.append(
                H_Product(
                    h_product_pk=self._uuid(prod_id),
                    product_id=prod_id,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )
        return products 
    
    def h_category(self) -> List[H_Categoty]:
        cats = []
        for prod_dict in self._dict['products']:
            cat_name = prod_dict['category']
            cats.append(
                H_Product(
                    h_categroy_pk=self._uuid(cat_name),
                    cat_name=cat_name,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )
        return cats 
    
    def h_order(self) -> H_User:
        order_id = self._dict['object_id']
        return H_Restaurant(
            h_order_pk=self._uuid(order_id),
            order_id=order_id,
            order_dt = self._dict['date'],
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )
    
    def l_order_user(self) -> L_Order_User:
        h_order_pk = self._uuid(self._dict['object_id'])
        h_user_pk = self._uuid(self._dict['user']['id'])
        return L_Order_User(
            hk_order_user_pk = self._uuid([h_order_pk, h_user_pk]),
            h_order_pk = h_order_pk,
            h_user_pk = h_user_pk,
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )
    
    def l_order_proudct(self) -> List[L_Order_Product]:
        h_order_pk = self._uuid(self._dict['object_id'])
        l_ord_prods = []
        for prod_dict in self._dict['products']:
            h_product_pk = self._uuid(prod_dict['id'])
            l_ord_prods.append(
                L_Order_Product(
                    hk_order_product_pk = self._uuid([h_order_pk, h_product_pk]),
                    h_order_pk = h_order_pk,
                    h_product_pk = h_product_pk,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )
        return l_ord_prods
    
    def l_proudct_restaurant(self) -> List[L_Product_Restaurant]:
        h_restaurant_pk = self._uuid(self._dict['restaurant']['id'])
        l_prods_cats = []
        for prod_dict in self._dict['products']:
            h_product_pk = self._uuid(prod_dict['id'])
            l_prods_cats.append(
                L_Product_Restaurant(
                    hk_product_restaurant_pk = self._uuid([h_restaurant_pk, h_product_pk]),
                    h_restaurant_pk = h_restaurant_pk,
                    h_product_pk = h_product_pk,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )
        return l_prods_cats
    
    def l_proudct_category(self) -> List[L_Product_Category]:
        l_prods_cats = []
        for prod_dict in self._dict['products']:
            h_category_pk = self._uuid(prod_dict['category'])
            h_product_pk = self._uuid(prod_dict['id'])
            l_prods_cats.append(
                L_Product_Category(
                    hk_product_category_pk = self._uuid([h_category_pk, h_product_pk]),
                    h_category_pk = h_category_pk,
                    h_product_pk = h_product_pk,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )
        return l_prods_cats
    
    def s_product_names(self) -> List[S_Product_Names]:
        s_prod_names = []
        for prod_dict in self._dict['products']:
            h_product_pk = self._uuid(prod_dict['id'])
            s_prod_names.append(
                S_Product_Names(
                    h_product_pk = h_product_pk,
                    name = prod_dict['name'],
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system,
                    hk_product_names_hashdiff = self._uuid(prod_dict['name'])
                )
            )
        return s_prod_names
    
    def s_restaurant_names(self) -> S_Restaurant_Names:
        h_restaurant_pk = self._uuid(self._dict['restaurant']['id'])
        rest_name = self._dict['restaurant']['name']
        return S_Restaurant_Names(
            h_restaurant_pk = h_restaurant_pk,
            name = rest_name,
            load_dt=datetime.utcnow(),
            load_src=self.source_system,
            hk_restaurant_names_hashdiff = self._uuid(rest_name)
        )

    def s_user_names(self) -> S_User_Names:
        h_user_pk = self._uuid(self._dict['user']['id'])
        username = self._dict['user']['name']
        userlogin = self._dict['user']['login']
        return S_User_Names(
            h_user_pk = h_user_pk,
            username = username,
            userlogin = userlogin,
            load_dt=datetime.utcnow(),
            load_src=self.source_system,
            hk_user_names_hashdiff = self._uuid([username, userlogin])
        )
    
    def s_order_status(self) -> S_Order_Status:
        h_order_pk = self._uuid(self._dict['object_id'])
        status = self._dict['status']
        return S_Restaurant_Names(
            h_order_pk = h_order_pk,
            status = status,
            load_dt=datetime.utcnow(),
            load_src=self.source_system,
            hk_order_status_hashdiff = self._uuid(status)
        )
    
    def s_order_cost(self) -> S_Order_Cost:
        h_order_pk = self._uuid(self._dict['object_id'])
        cost = self._dict['cost']
        payment = self._dict['payment']
        return S_Restaurant_Names(
            h_order_pk = h_order_pk,
            cost = cost,
            payment = payment,
            load_dt=datetime.utcnow(),
            load_src=self.source_system,
            hk_order_cost_hashdiff = self._uuid([cost, payment])
        )
    
    def output_message(self) -> Dict:
        h_user_pk = self._uuid(self._dict['user']['id'])

        products_data = []

        for prod_dict in self._dict['products']:
            h_product_pk = self._uuid(prod_dict['id'])
            h_category_pk = self._uuid(prod_dict['category'])
            products_data.append({
                'product_id': h_product_pk,
                'product_name': prod_dict['name'],
                'product_cnt' : int(prod_dict['quantity']),
                'category_id': h_category_pk,
                'category_name': prod_dict['category'],
                'category_cnt': 1
            })

        output_message = {
            'object_id':self._dict['object_id'],
            'payload': {
                'user_id': h_user_pk,
                'products': products_data
            }
        }
    
        return output_message
    

class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def h_user_insert(self, h_user: H_User) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_user (h_user_pk, user_id, load_dt, load_src)
                        VALUES (%(h_user_pk)s, %(user_id)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT (h_user_pk) DO UPDATE
                        SET
                            user_id = EXCLUDED.user_id,
                            load_dt = EXCLUDED.load_dt,
                            load_src = EXCLUDED.load_src
                    """,
                    {
                        'h_user_pk': h_user.h_user_pk,
                        'user_id': h_user.user_id,
                        'load_dt': h_user.load_dt,
                        'load_src': h_user.load_src
                    }
                )

    def h_restaurant_insert(self, h_restaurant: H_Restaurant) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_restaurant (h_restaurant_pk, restaurant_id, load_dt, load_src)
                        VALUES (%(h_restaurant_pk)s, %(restaurant_id)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT (h_restaurant_pk) DO UPDATE
                        SET
                            restaurant_id = EXCLUDED.restaurant_id,
                            load_dt = EXCLUDED.load_dt,
                            load_src = EXCLUDED.load_src
                    """,
                    {
                        'h_restaurant_pk': h_restaurant.h_restaurant_pk,
                        'restaurant_id': h_restaurant.restaurant_id,
                        'load_dt': h_restaurant.load_dt,
                        'load_src': h_restaurant.load_src
                    }
                )
    
    def h_product_insert(self, h_product: H_Product) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_product (h_product_pk, product_id, load_dt, load_src)
                        VALUES (%(h_product_pk)s, %(product_id)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT (h_product_pk) DO UPDATE
                        SET
                            product_id = EXCLUDED.product_id,
                            load_dt = EXCLUDED.load_dt,
                            load_src = EXCLUDED.load_src
                    """,
                    {
                        'h_product_pk': h_product.h_product_pk,
                        'product_id': h_product.product_id,
                        'load_dt': h_product.load_dt,
                        'load_src': h_product.load_src
                    }
                )

    def h_category_insert(self, h_cat: H_Categoty) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_category (h_category_pk, category_id, load_dt, load_src)
                        VALUES (%(h_category_pk)s, %(category_id)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT (h_category_pk) DO UPDATE
                        SET
                            category_id = EXCLUDED.category_id,
                            load_dt = EXCLUDED.load_dt,
                            load_src = EXCLUDED.load_src
                    """,
                    {
                        'h_category_pk': h_cat.h_category_pk,
                        'category_id': h_cat.category_id,
                        'load_dt': h_cat.load_dt,
                        'load_src': h_cat.load_src
                    }
                )

    def h_order_insert(self, h_order: H_Order) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_order (h_order_pk, order_id, order_dt, load_dt, load_src)
                        VALUES (%(h_order_pk)s, %(order_id)s, %(order_dt)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT (h_order_pk) DO UPDATE
                        SET
                            order_id = EXCLUDED.order_id,
                            order_dt = EXCLUDED.order_dt,
                            load_dt = EXCLUDED.load_dt,
                            load_src = EXCLUDED.load_src
                    """,
                    {
                        'h_order_pk': h_order.h_order_pk,
                        'order_id': h_order.order_id,
                        'order_dt': h_order.order_dt,
                        'load_dt': h_order.load_dt,
                        'load_src': h_order.load_src
                    }
                )

    def l_order_proudct_insert(self, lop:L_Order_Product) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_order_proudct (hk_order_product_pk, h_order_pk, h_product_pk, load_dt, load_src)
                        VALUES (%(hk_order_product_pk)s, %(h_order_pk)s, %(h_product_pk)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT (hk_order_product_pk) DO UPDATE
                        SET
                            h_order_pk = EXCLUDED.h_order_pk,
                            h_product_pk = EXCLUDED.h_product_pk,
                            load_dt = EXCLUDED.load_dt,
                            load_src = EXCLUDED.load_src
                    """,
                    {
                        'hk_order_product_pk': lop.hk_order_product_pk,
                        'h_order_pk': lop.h_order_pk,
                        'h_product_pk': lop.h_product_pk,
                        'load_dt': lop.load_dt,
                        'load_src': lop.load_src
                    }
                )

    def l_order_user_insert(self, lou: L_Order_User) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_order_user (hk_order_user_pk, h_order_pk, h_user_pk, load_dt, load_src)
                        VALUES (%(hk_order_user_pk)s, %(h_order_pk)s, %(h_user_pk)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT (hk_order_user_pk) DO UPDATE
                        SET
                            h_order_pk = EXCLUDED.h_order_pk,
                            h_user_pk = EXCLUDED.h_user_pk,
                            load_dt = EXCLUDED.load_dt,
                            load_src = EXCLUDED.load_src
                    """,
                    {
                        'hk_order_user_pk': lou.hk_order_user_pk,
                        'h_order_pk': lou.h_order_pk,
                        'h_user_pk': lou.h_user_pk,
                        'load_dt': lou.load_dt,
                        'load_src': lou.load_src
                    }
                )

    def l_proudct_restaurant_insert(self, lpr: L_Product_Restaurant) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_proudct_restaurant (hk_product_restaurant_pk, h_restaurant_pk, h_product_pk, load_dt, load_src)
                        VALUES (%(hk_product_restaurant_pk)s, %(h_restaurant_pk)s, %(h_product_pk)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT (hk_product_restaurant_pk) DO UPDATE
                        SET
                            h_restaurant_pk = EXCLUDED.h_restaurant_pk,
                            h_product_pk = EXCLUDED.h_product_pk,
                            load_dt = EXCLUDED.load_dt,
                            load_src = EXCLUDED.load_src
                    """,
                    {
                        'hk_product_restaurant_pk': lpr.hk_product_restaurant_pk,
                        'h_restaurant_pk': lpr.h_restaurant_pk,
                        'h_product_pk': lpr.h_product_pk,
                        'load_dt': lpr.load_dt,
                        'load_src': lpr.load_src
                    }
                )

    def l_proudct_category_insert(self, lpc: L_Product_Category) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_proudct_category (hk_product_category_pk, h_category_pk, h_product_pk, load_dt, load_src)
                        VALUES (%(hk_product_category_pk)s, %(h_category_pk)s, %(h_product_pk)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT (hk_product_category_pk) DO UPDATE
                        SET
                            h_category_pk = EXCLUDED.h_category_pk,
                            h_product_pk = EXCLUDED.h_product_pk,
                            load_dt = EXCLUDED.load_dt,
                            load_src = EXCLUDED.load_src
                    """,
                    {
                        'hk_product_category_pk': lpc.hk_product_category_pk,
                        'h_category_pk': lpc.h_category_pk,
                        'h_product_pk': lpc.h_product_pk,
                        'load_dt': lpc.load_dt,
                        'load_src': lpc.load_src
                    }
                )

    def s_product_names_insert(self, prod_name: S_Product_Names) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_product_names (h_product_pk, name, load_dt, load_src, hk_product_names_hashdiff)
                        VALUES (%(h_product_pk)s, %(name)s, %(load_dt)s, %(load_src)s, %(hk_product_names_hashdiff)s)
                        ON CONFLICT (h_product_pk) DO UPDATE
                        SET
                            name = EXCLUDED.name,
                            load_dt = EXCLUDED.load_dt,
                            load_src = EXCLUDED.load_src,
                            hk_product_names_hashdiff = EXCLUDED.hk_product_names_hashdiff
                    """,
                    {
                        'h_product_pk': prod_name.h_product_pk,
                        'name': prod_name.name,
                        'load_dt': prod_name.load_dt,
                        'load_src': prod_name.load_src, 
                        'hk_product_names_hashdiff': prod_name.hk_product_names_hashdiff
                    }
                )

    def s_restaurant_names_insert(self, rest_name: S_Restaurant_Names) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_restaurant_names (h_restaurant_pk, name, load_dt, load_src, hk_restaurant_names_hashdiff)
                        VALUES (%(h_restaurant_pk)s, %(name)s, %(load_dt)s, %(load_src)s, %(hk_restaurant_names_hashdiff)s)
                        ON CONFLICT (h_restaurant_pk) DO UPDATE
                        SET
                            name = EXCLUDED.name,
                            load_dt = EXCLUDED.load_dt,
                            load_src = EXCLUDED.load_src,
                            hk_restaurant_names_hashdiff = EXCLUDED.hk_restaurant_names_hashdiff
                    """,
                    {
                        'h_restaurant_pk': rest_name.h_restaurant_pk,
                        'name': rest_name.name,
                        'load_dt': rest_name.load_dt,
                        'load_src': rest_name.load_src, 
                        'hk_restaurant_names_hashdiff': rest_name.hk_restaurant_names_hashdiff
                    }
                )

    def s_user_names_insert(self, user_name: S_User_Names) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_user_names (h_user_pk, username, userlogin, load_dt, load_src, hk_user_names_hashdiff)
                        VALUES (%(h_user_pk)s, %(username)s, %(userlogin)s, %(load_dt)s, %(load_src)s, %(hk_user_names_hashdiff)s)
                        ON CONFLICT (h_user_pk) DO UPDATE
                        SET
                            username = EXCLUDED.username,
                            userlogin = EXCLUDED.userlogin,
                            load_dt = EXCLUDED.load_dt,
                            load_src = EXCLUDED.load_src,
                            hk_user_names_hashdiff = EXCLUDED.hk_user_names_hashdiff
                    """,
                    {
                        'h_user_pk': user_name.h_user_pk,
                        'username': user_name.username,
                        'userlogin': user_name.userlogin,
                        'load_dt': user_name.load_dt,
                        'load_src': user_name.load_src, 
                        'hk_user_names_hashdiff': user_name.hk_user_names_hashdiff
                    }
                )

    def s_order_status_insert(self, order_status: S_Order_Status) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_order_status (h_order_pk, status, load_dt, load_src, hk_order_status_hashdiff)
                        VALUES (%(h_order_pk)s, %(status)s, %(load_dt)s, %(load_src)s, %(hk_order_status_hashdiff)s)
                        ON CONFLICT (h_order_pk) DO UPDATE
                        SET
                            status = EXCLUDED.status,
                            load_dt = EXCLUDED.load_dt,
                            load_src = EXCLUDED.load_src,
                            hk_order_status_hashdiff = EXCLUDED.hk_order_status_hashdiff
                    """,
                    {
                        'h_order_pk': order_status.h_order_pk,
                        'status': order_status.status,
                        'load_dt': order_status.load_dt,
                        'load_src': order_status.load_src, 
                        'hk_order_status_hashdiff': order_status.hk_order_status_hashdiff
                    }
                )    

    def s_order_cost_insert(self, order_cost: S_Order_Cost) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_order_cost (h_order_pk, cost, payment, load_dt, load_src, hk_order_cost_hashdiff)
                        VALUES (%(h_order_pk)s, %(cost)s, %(payment)s, %(load_dt)s, %(load_src)s, %(hk_order_cost_hashdiff)s)
                        ON CONFLICT (h_order_pk) DO UPDATE
                        SET
                            cost = EXCLUDED.cost,
                            payment = EXCLUDED.payment,
                            load_dt = EXCLUDED.load_dt,
                            load_src = EXCLUDED.load_src,
                            hk_order_cost_hashdiff = EXCLUDED.hk_order_cost_hashdiff
                    """,
                    {
                        'h_order_pk': order_cost.h_order_pk,
                        'cost': order_cost.cost,
                        'payment': order_cost.payment,
                        'load_dt': order_cost.load_dt,
                        'load_src': order_cost.load_src, 
                        'hk_order_status_hashdiff': order_cost.hk_order_cost_hashdiff
                    }
                )             
        

     

    
