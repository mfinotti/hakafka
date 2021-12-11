import logging

#logger initlialization
_LOGGER = logging.getLogger(__name__)

#own consumer / producer
from .kafkaConsumer import (
    KafkaConsumer
)
from .kafkaProducer import (
    KafkaProducer
)

#built in events
EVENT_CONSUMER_START = "HAKAFKA_CONSUMER_START"
EVENT_CONSUMER_STOP = "HAKAFKA_CONSUMER_STOP"
EVENT_CONSUMER_INCOMING_MESSAGE = "INBOUND_EVENT"
EVENT_PRODUCER_START = "HAKAFKA_PRODUCER_START"
EVENT_PRODUCER_STOP = "HAKAFKA_PRODUCER_STOP"
EVENT_PRODUCER_PRODUCE = "HAKAFKA_PRODUCER_PRODUCE"

def consumerStartFn(self, hass):
    hass.bus.async_listen(EVENT_CONSUMER_START, self.startConsuming)

def consumerStopFn(self, hass):
    hass.bus.async_listen(EVENT_CONSUMER_STOP, self.stopConsuming)

def onConsumerMessageCallbackEvent(hass, msg):
    _LOGGER.debug("firing %s with payload %s", EVENT_CONSUMER_INCOMING_MESSAGE, msg)
    hass.bus.fire(EVENT_CONSUMER_INCOMING_MESSAGE, msg)

def producerStartFn(self, hass):
    hass.bus.async_listen(EVENT_PRODUCER_START, self.startProducing)

def producerStopFn(self, hass):
    hass.bus.async_listen(EVENT_PRODUCER_STOP, self.stopProducing)

def producingEventFn(self, hass):
    hass.bus.async_listen(EVENT_PRODUCER_PRODUCE, self.produceOnEvent)

def buildKafkaManager(
        hass,
        consumersConfigurationList,
        producersConfigurationList,
        sslContext = None,
        saslContext = None,
        producerStartFunction = None,
        producerStopFunction = None,
        producerCallbackFunction = None,
        consumerStartFunction = None,
        consumerStopFunction = None, 
        consumerMessageCallbackFunction = None
    ):
        """ Ha-Kafka Manager """
        if None == producersConfigurationList or len(producersConfigurationList) > 0 :
            _LOGGER.debug("found %s producer", len(producersConfigurationList))
            for configuration in producersConfigurationList:

                # injecting producer events
                KafkaProducer.eventStartFn  = producerStartFunction if None != producerStartFunction else producerStartFn
                KafkaProducer.eventStopFn   = producerStopFunction if None != producerStopFunction else producerStopFn
                KafkaProducer.eventProduceFn= producerCallbackFunction if None != producerCallbackFunction else producingEventFn

                producer = KafkaProducer(
                    port                = configuration["port"],
                    host                = configuration["host"],
                    topic               = configuration["topic"],
                    security            = configuration["security"],
                    platform            = hass,
                    sslContext          = sslContext,
                    saslContext         = saslContext
                )

                # producer events pull up
                producer.eventStartFn(hass)
                producer.eventStopFn(hass)
                producer.eventProduceFn(hass)

        else:
            _LOGGER.warn("HA-Kafka - Empty Producer list!")

        ##### CONSUMER INITIALIZATION
        if None == consumersConfigurationList or len(consumersConfigurationList) > 0 :
            _LOGGER.debug("found %s consumer", len(consumersConfigurationList))
            for configuration in consumersConfigurationList :

                # injecting consumer events
                KafkaConsumer.eventStartFn      = consumerStartFunction if None != consumerStartFunction else consumerStartFn
                KafkaConsumer.eventStopFn       = consumerStopFunction if None != consumerStopFunction else consumerStopFn

                consumer = KafkaConsumer(
                        port                = configuration["port"],
                        host                = configuration["host"],
                        topic               = configuration["topic"],
                        consumerGroup       = configuration["consumerGroup"],
                        security            = configuration["security"],
                        platform            = hass,
                        sslContext          = sslContext,
                        saslContext         = saslContext,
                        onMessageCallback   = consumerMessageCallbackFunction if None != consumerMessageCallbackFunction else onConsumerMessageCallbackEvent
                )

                # consumer events pull up
                consumer.eventStartFn(hass)
                consumer.eventStopFn(hass)

                # inject hass
                #consumer._hass = hass
        else:
            _LOGGER.warn("HA-Kafka - Empty Consumer list!")