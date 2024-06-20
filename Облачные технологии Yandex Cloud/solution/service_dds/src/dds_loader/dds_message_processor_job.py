from datetime import datetime
from logging import Logger
from lib.kafka_connect import KafkaConsumer, KafkaProducer
from dds_loader.repository.dds_repository import OrderDdsBuilder, DdsRepository


class DdsMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 dds_repo: DdsRepository,
                 logger: Logger) -> None:
        
        self._consumer = consumer
        self._producer = producer
        self._dds_repo = dds_repo


        self._logger = logger

        self._batch_size = 30

        self._load_src = "orders-system-kafka"
       
    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()

            if not msg:
                self._logger.info(f"{datetime.utcnow()}: There is no message")   
                break

            builder = OrderDdsBuilder(msg['payload'])

            self._dds_repo.h_restaurant_insert(builder.h_restaurant())
            self._dds_repo.h_user_insert(builder.h_user())
            self._dds_repo.h_order_insert(builder.h_order())
            self._dds_repo.h_product_insert(builder.h_product())
            self._dds_repo.h_category_insert(builder.h_category())


            self._dds_repo.l_order_user_insert(builder.l_order_user())
            self._dds_repo.l_order_proudct_insert(builder.l_order_proudct())
            self._dds_repo.l_proudct_restaurant_insert(builder.l_proudct_restaurant())
            self._dds_repo.l_proudct_category_insert(builder.l_proudct_category())


            self._dds_repo.s_product_names_insert(builder.s_product_names())
            self._dds_repo.s_restaurant_names_insert(builder.s_restaurant_names())            
            self._dds_repo.s_user_names_insert(builder.s_user_names())            
            self._dds_repo.s_order_status_insert(builder.s_order_status())            
            self._dds_repo.s_order_cost_insert(builder.s_order_cost())
            
            self._producer.produce(builder.out_message())

        self._logger.info(f"{datetime.utcnow()}: FINISH")
