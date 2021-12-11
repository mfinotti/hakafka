import json
import logging
from aiokafka import AIOKafkaConsumer

#logger initlialization
_LOGGER = logging.getLogger(__name__)

class KafkaConsumer:

    consumer = None

    def __init__(
        self,
        host,
        port,
        topic,
        consumerGroup,
        onMessageCallback,
        platform = None,
        security = "",
        sslContext = None,
        saslContext = None,
    ):
        """ Initialization """
        self.port           = port
        self.host           = host
        self.topic          = topic
        self.consumerGroup  = consumerGroup
        self.security       = security
        self.sslContext     = sslContext
        self.saslContext    = saslContext
        self.platform       = platform

        self.onMessageCallback  = onMessageCallback

        self._initConsumer()



    def _initConsumer(self):

        try:
            if "" != self.security :
                if "ssl+sasl" == self.security:
                    _LOGGER.debug("Initializing Kafka consumer with ssl+sasl security profile")
                    self.consumer = AIOKafkaConsumer(
                        self.topic,
                        bootstrap_servers       = f"{self.host}:{self.port}",
                        group_id                = self.consumerGroup,
                        security_protocol       = "SSL",
                        ssl_context             = self.sslContext,
                        sasl_mechanism          = self.saslContext["mechanism"],
                        sasl_plain_username     = self.saslContext["plain_username"],
                        sasl_plain_password     = self.saslContext["plain_password"],
                        session_timeout_ms      = 60000,
                        heartbeat_interval_ms   = 25000,
                        fetch_max_wait_ms       = 10000,
                        auto_commit_interval_ms = 10000
                    )
                elif "ssl" == self.security:
                    _LOGGER.debug("Initializing Kafka consumer with ssl security profile")
                    self.consumer = AIOKafkaConsumer(
                        self.topic,
                        bootstrap_servers       = f"{self.host}:{self.port}",
                        group_id                = self.consumerGroup,
                        ssl_context             = self.sslContext,
                        security_protocol       = "SSL",
                        session_timeout_ms      = 60000,
                        heartbeat_interval_ms   = 25000,
                        fetch_max_wait_ms       = 10000,
                        auto_commit_interval_ms = 10000
                    )
                elif "sasl" == self.security:
                    _LOGGER.debug("Initializing Kafka consumer with SASL security profile")
                    self.producer = AIOKafkaConsumer(
                        bootstrap_servers       = f"{self.host}:{self.port}",
                        group_id                = self.consumerGroup,
                        security_protocol       = "PLAINTEXT",
                        sasl_mechanism          = self.saslContext["mechanism"],
                        sasl_plain_username     = self.saslContext["plain_username"],
                        sasl_plain_password     = self.saslContext["plain_password"],
                        session_timeout_ms      = 60000,
                        heartbeat_interval_ms   = 25000,
                        fetch_max_wait_ms       = 10000,
                        auto_commit_interval_ms =10000
                    )
            else:
                # start a consumer without security context
                _LOGGER.debug("Initializing Kafka consumer without security profile")
                self.consumer = AIOKafkaConsumer(
                    self.topic,
                    bootstrap_servers       = f"{self.host}:{self.port}",
                    group_id                = self.consumerGroup,
                    session_timeout_ms      = 60000,
                    heartbeat_interval_ms   = 25000,
                    fetch_max_wait_ms       = 10000,
                    auto_commit_interval_ms = 10000
                )

        except Exception as e:
            _LOGGER.error(e)
            _LOGGER.error("KAFKA CONSUMER NOT INITIALIZED!! BROKER= "+self.host+":"+self.port+" TOPIC= "+self.topic)


        if None == self.consumer:
            raise Exception("KAFKA CONSUMER NOT INITIALIZED!! BROKER= "+self.host+":"+self.port+" TOPIC= "+self.topic)


    async def startConsuming(self, event):

        if "" != event.data and "topic" in event.data:
            if self.topic == event.data["topic"] :
                _LOGGER.debug("REQUESTED STARTING OF A SINGLE CONSUMER ON BROKER: %s:%d topic: %s", self.host, self.port, self.topic)
            else:
                return

        # start all consumers or a single consumer that match self.topic
        await self.startAndConsumeTopic()


    async def stopConsuming(self, event):

        if "" != event.data and "topic" in event.data:
            if self.topic == event.data["topic"] :
                _LOGGER.info("STOP consuming on broker: %s:%d topic: %s", self.host, self.port, self.topic)
            else:
                return

        # stop all consumers or a single consumer that match self.topic
        await self.consumer.stop()


    async def startAndConsumeTopic(self):

        if self.consumer._closed == True:
            _LOGGER.info("RESTART consuming on broker: %s:%d topic: %s", self.host, self.port, self.topic)
            self._initConsumer()
        else:
            _LOGGER.info("START consuming on broker: %s:%d topic: %s", self.host, self.port, self.topic)

        await self.consumer.start()

        try:
            async for msg in self.consumer:
                try:
                    message = (msg.value.decode("utf-8"))
                    _LOGGER.info("Consumed message from topic: [%s] message: [%s]", self.topic, message)
                    try:
                        message = json.loads(message)
                    except Exception as e:
                        _LOGGER.warn("bad json format msg: %s", message)

                    self.onMessageCallback(self.platform, message)
                except Exception as e:
                    _LOGGER.warn("bad message: %s", msg.value.decode("utf-8"))
                    _LOGGER.error(e)

        except Exception as e:
            _LOGGER.error("error given in consuming message, error: %s", e)
