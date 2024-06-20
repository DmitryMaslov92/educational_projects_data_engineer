from datetime import datetime
from logging import Logger

from lib.kafka_connect import KafkaConsumer
from cdm_loader.repository.cdm_repository import OrderCdmBuilder, CdmRepository


class CdmMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 cdm_repo: CdmRepository,
                 logger: Logger,
                 ) -> None:
        
        self._consumer = consumer
        self._cdm_repo = cdm_repo
        self._logger = logger
        self._batch_size = 100

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()

            if not msg:
                self._logger.info(f"{datetime.utcnow()}: There is no message")   
                break

            builder = OrderCdmBuilder(msg['payload'])

            self._cdm_repo.user_prod_insert(builder.user_product_counters())

            self._cdm_repo.user_cat_insert(builder.user_category_counters())

        self._logger.info(f"{datetime.utcnow()}: FINISH")
