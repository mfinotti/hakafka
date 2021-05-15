import logging
from json import dumps, loads
import ssl

#own class import
from .kafkaManager import (
    buildKafkaManager
)
# import .kafkaManager as KafkaManager

#component domain keyword
DOMAIN = "hakafka"

#logger initlialization
_LOGGER = logging.getLogger(__name__)

SASL_CONFIGKEY = "sasl_context"
SASL_MECHANINSM_CONFIGKEY = "sasl_mechanism"
SASL_PLAINUSERNAME_CONFIGKEY = "sasl_plain_username"
SASL_PLAINPASSWORD_CONFIGKEY = "sasl_plain_password"
SSL_CONFIGKEY = "ssl_context"
SSL_CERTPATH_CONFIGKEY = "ssl_certpath"
SSL_CAFILE_CONFIGKEY = "ssl_cafile"
SSL_CERTFILE_CONFIGKEY = "ssl_certfile"
SSL_KEYFILE_CONFIGKEY = "ssl_keyfile"
SSL_PASSWORD_CONFIGKEY = "ssl_password"
PRODUCER_CONFIGKEY = "producers"
CONSUMER_CONFIGKEY = "consumers"


def create_sasl_config(saslConfiguration):
    return {
        "mechanism"         : saslConfiguration[SASL_MECHANINSM_CONFIGKEY],
        "plain_username"    : saslConfiguration[SASL_PLAINUSERNAME_CONFIGKEY],
        "plain_password"    : saslConfiguration[SASL_PLAINPASSWORD_CONFIGKEY]
    }

async def async_setup(hass, config):
    """Loading HA-kafka module"""
    conf = config[DOMAIN]
    _LOGGER.debug("HA_Kafka configuration loaded: %s", conf)

    sslContext = None
    saslContext = None
    producersBucket = []
    consumersBucket = []

    #### SASL CONFIGURATION
    if SASL_CONFIGKEY in conf:
        saslConfiguration = conf[SASL_CONFIGKEY]
        _LOGGER.info("Detected SALS Kafka configuration mode")
        _LOGGER.debug("SASL Kafka configuration: %s", saslConfiguration)
        saslContext = create_sasl_config(saslConfiguration)
        _LOGGER.debug("SASL configuration params: %s", saslContext)


    #### SSL CONFIGURATION
    if SSL_CONFIGKEY in  conf:
        sslConfiguration = conf[SSL_CONFIGKEY]
        _LOGGER.info("Detected SSL Kafka configuration mode")
        _LOGGER.debug("SSL Kafka configuration: %s", sslConfiguration)

        try:
            certPath = sslConfiguration[SSL_CERTPATH_CONFIGKEY] + "/"
            caFile = certPath + sslConfiguration[SSL_CAFILE_CONFIGKEY]
            certSignedFile = certPath + sslConfiguration[SSL_CERTFILE_CONFIGKEY]
            certKeyFile = certPath + sslConfiguration[SSL_KEYFILE_CONFIGKEY]
            passwordCerts = sslConfiguration[SSL_PASSWORD_CONFIGKEY]

            _LOGGER.debug("SSL configuration param CERTPATH: %s", certPath)
            _LOGGER.debug("SSL configuration param CAFILE: %s", caFile)
            _LOGGER.debug("SSL configuration param CERTFILE: %s", certSignedFile)
            _LOGGER.debug("SSL configuration param KEYFILE: %s", certKeyFile)
            _LOGGER.debug("SSL configuration param PASSWORD: %s", passwordCerts)


            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH, cafile=caFile, capath=None, cadata=None)
            # context.verify_mode = ssl.CERT_REQUIRED
            context.check_hostname = False
            context.load_cert_chain(certSignedFile, certKeyFile, passwordCerts)

            # context = create_default_context(Purpose.SERVER_AUTH, cafile=caFile)
            # context.check_hostname = False
            # context.load_cert_chain(certSignedFile, certKeyFile, passwordCerts)

            sslContext = context
        except Exception as e:
            _LOGGER.error("ERROR CREATING SECURE CONTEXT!")
            _LOGGER.error(e)
            return False


    #### PRODUCER CONFIGURATION
    producerConfiguration = [{
        "host"          : record['host'],
        "port"          : record['port'],
        "topic"         : record['topic'],
        "security"      : record['security']} for record in conf[PRODUCER_CONFIGKEY]]
    if None != producerConfiguration:
        producersBucket = list(producerConfiguration)
        _LOGGER.debug("producers configuration: %s", producersBucket)

    #### CONSUMER CONFIGURATION
    consumerConfiguration = [{
        "host"          : record2['host'],
        "port"          : record2['port'],
        "topic"         : record2['topic'],
        "security"      : record2['security'],
        "consumerGroup" : record2['group']} for record2 in conf[CONSUMER_CONFIGKEY]]
    if None != consumerConfiguration:
        consumersBucket = list(consumerConfiguration)
        _LOGGER.debug("consumers configuration: %s", consumersBucket)

    buildKafkaManager(hass, consumersBucket, producersBucket, sslContext, saslContext)
    _LOGGER.info("HA-Kafka configuration complete")


    return True
