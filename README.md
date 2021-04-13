# hakafka
HASSIO full kafka integration 


configuration example:
hakafka:
  ssl_context:
    ssl_certpath: "./certs"
    ssl_cafile: "ca-cert"
    ssl_certfile: "cert-signed"
    ssl_keyfile: "cert-key"
    ssl_password: "xxxxxxx"
  sasl_context:
    sasl_mechanism: "PLAIN"
    sasl_plain_username: "xxx"
    sasl_plain_password: "xxx-eee"
  producers:
    - host: localhost
      port: 9094
      topic: "ccc"
      security: "ssl+sasl"
    - host: localhost
      port: 9094
      topic: "eeew"
      security: "ssl+sasl"
  consumers:
    - host: localhost
      port: 9094
      topic: ddfggg
      group: "ggggg"
      security: "ssl+sasl"
