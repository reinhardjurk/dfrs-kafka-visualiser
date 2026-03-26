import time
import requests
from confluent_kafka import Consumer

from sensoris.protobuf.messages import data_pb2
    
def oauth_cb(oauth_config):
    token_url = "https://emea.prod.alpha.sso.bmwgroup.com/auth/oauth2/realms/root/realms/alpha/access_token"
    client_id = "182a00cc-4e35-44cf-80df-73ac4a92657b"
    client_secret = "ScFEEoGvy5KXliZIxH1ugMhBZ4ofUh8pjR4tgPWi"
    scope = "machine2machine"

    r = requests.post(
        token_url,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": scope,
        },
        timeout=10,
    )
    r.raise_for_status()
    token = r.json()

    access_token = token["access_token"]
    expires_in = int(token.get("expires_in", 3600))

    expiry_time = int(time.time()) + expires_in
    principal = "kafka-client"
    extensions = {}  # usually empty unless you need Kafka extensions

    return (access_token, expiry_time, principal, extensions)

conf = {
    "bootstrap.servers": "gateway.catena-dmz-euc-stp.aws.bmw.cloud:9092",
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "OAUTHBEARER",
    "oauth_cb": oauth_cb,
    "group.id": "python-test-consumer",
    "auto.offset.reset": "earliest",
    # optional for debugging:
    # "debug": "security,broker,protocol",
}

consumer = Consumer(conf)
consumer.subscribe(["bmw.cdcerd.CDCHazardsForDfrsPROD.v1"])

try:
    while (True):

        msg = consumer.poll(10.0)

        if msg is None:
            pass
            #print("No message")
        elif msg.error():
            pass
            #print(f"Error: {msg.error()}")
        else:
            pass
            #print(f"Received {len(msg.value())} bytes")

            data_msg = data_pb2.DataMessages()
            data_msg.ParseFromString(msg.value())
            print(data_msg)

except KeyboardInterrupt:
    print("Stopping Consumer...")

finally:
    consumer.close()
    print("Consumer closed!")
