import pytz
import logging
from datetime import datetime
from json import dump, dumps, loads
from aiokafka import AIOKafkaProducer

#logger initlialization
_LOGGER = logging.getLogger(__name__)

def serializer(value):
    return json.dumps(value).encode()

class KafkaProducer:

    producer = None

    def __init__(
        self,
        host,
        port,
        topic,
        platform = None,
        security = "",
        sslContext = None,
        saslContext = None
    ):
        """ Initialization """
        self.port           = port
        self.host           = host
        self.topic          = topic
        self.security       = security
        self.sslContext     = sslContext
        self.saslContext    = saslContext
        self.platform       = platform

        self.local_tz = pytz.timezone('Europe/Rome')

        self._initProducer()

    def _initProducer(self):
        if "" != self.security :
            if "ssl+sasl" == self.security:
                _LOGGER.debug("Initializing Kafka producer with SSL+SASL security profile")

                self.producer = AIOKafkaProducer(
                    bootstrap_servers   = f"{self.host}:{self.port}",
                    compression_type    = "gzip",
                    linger_ms           = 1000,
                    max_batch_size      = 32768,
                    value_serializer    = serializer,
                    security_protocol   = "SSL",
                    ssl_context         = self.sslContext,
                    sasl_mechanism      = self.saslContext["mechanism"],
                    sasl_plain_username = self.saslContext["plain_username"],
                    sasl_plain_password = self.saslContext["plain_password"]
                )
            elif "ssl" == self.security:
                _LOGGER.debug("Initializing Kafka producer with SSL security profile")
                self.producer = AIOKafkaProducer(
                    bootstrap_servers   = f"{self.host}:{self.port}",
                    compression_type    = "gzip",
                    linger_ms           = 1000,
                    max_batch_size      = 32768,
                    # value_serializer    = serializer,
                    security_protocol   = "SSL",
                    ssl_context         = self.sslContext
                )
            elif "sasl" == self.security:
                _LOGGER.debug("Initializing Kafka producer with SASL security profile")
                self.producer = AIOKafkaProducer(
                    bootstrap_servers   = f"{self.host}:{self.port}",
                    compression_type    = "gzip",
                    linger_ms           = 1000,
                    max_batch_size      = 32768,
                    value_serializer    = serializer,
                    security_protocol   = "PLAINTEXT",
                    sasl_mechanism      = self.saslContext["mechanism"],
                    sasl_plain_username = self.saslContext["plain_username"],
                    sasl_plain_password = self.saslContext["plain_password"]
                )
        else:
            # start a producer without security context
            _LOGGER.debug("Initializing Kafka producer without security profile")
            self.producer = AIOKafkaProducer(
                bootstrap_servers   = f"{self.host}:{self.port}"
            )

        if None == self.producer:
            raise Exception("KAFKA PRODUCER NOT INITIALIZED!! BROKER= "+self.host+":"+self.port+" TOPIC= "+self.topic)


    async def startProducing(self, event):

        if "" != event.data and "topic" in event.data:
            if self.topic == event.data["topic"] :
                _LOGGER.debug("REQUESTED STARTING OF A SINGLE PRODUCER ON BROKER: %s:%d topic: %s", self.host, self.port, self.topic)
            else:
                return

        # start all producers or a single producer that match self.topic
        if self.producer._closed == True:
            _LOGGER.info("RESTART producer on broker: %s:%d topic: %s", self.host, self.port, self.topic)
            self._initProducer()
        else:
            _LOGGER.info("START producing on broker: %s:%d topic: %s", self.host, self.port, self.topic)


        await self.producer.start()


    async def stopProducing(self, event):

        if "" != event.data and "topic" in event.data:
            if self.topic == event.data["topic"] :
                _LOGGER.info("STOP producing on broker: %s:%d topic: %s", self.host, self.port, self.topic)
            else:
                return

        # stop all producers or a single producer that match self.topic
        await self.producer.stop()


    async def produceOnEvent(self, event):

        eventData = None
        if "" != event.data :
            eventData = event.data
        else:
            _LOGGER.warn("Empty data! Nothing to produce..")
            return
        
        if "topic" in eventData:
            if self.topic != eventData["topic"] :
                return        

        _LOGGER.info("Producing message on broker: %s:%d topic: %s message: %s", self.host, self.port, self.topic, eventData['message'])
        await self.produce(eventData["message"])


    async def produce(self, msg):

        if None == msg or "" == msg:
            _LOGGER.warn("Attempt to producing an empty message on broker: %s:%d topic: %s", self.host, self.port, self.topic)
            return

        try:
            current_timestamp = datetime.now(tz=self.local_tz)

            payload = None
            if isinstance(msg, list) or isinstance(msg, dict) :
                msg['timestamp'] = current_timestamp.isoformat("T", "seconds")
                jsonData = dumps(msg)
                payload = jsonData.encode()
            else:
                payload = bytearray(msg, "utf-8")
                # payload = msg
            if payload:
                _LOGGER.info("Producing message: [%s] on broker: [%s:%d] and topic: [%s]", payload, self.host, self.port, self.topic)
                await self.producer.send_and_wait(self.topic, payload)
        except Exception as e:
            _LOGGER.error("Error while producing on on broker: %s:%d topic: %s", self.host, self.port, self.topic)
            _LOGGER.error(e)

