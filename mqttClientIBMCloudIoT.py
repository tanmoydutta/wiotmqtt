# Author : Tanmoy Dutta
# Email : tanmoy.dutta@gmail.
# Description : This code below shows how to connect to Watson IoT
# MQTT server thorugh secure connection and publish data.

import time
import paho.mqtt.client as paho
import ssl

#MQTT Connection configuration
org = "<Org Id of the Watson Iot Platform>"
deviceType = "<Device Type>"
deviceId = "<Device Id as defined at the time of device registration>"
authUser = "use-token-auth" #default user id for authentication
authToken = "<Authentication Token>"
mqttTopic = "iot-2/evt/status/fmt/json" #Data being published using this topic in JSON format
mqttHostFragment = ".messaging.internetofthings.ibmcloud.com"
mqttHost = org + mqttHostFragment
mqttClientId = "d:" + org + ":" + deviceType + ":" + deviceId
mqttPort = 8883 #Secure Port
sampleData = "{\"d\":{\"a\":8}}" #Sample data

#define callbacks
def on_message(client, userdata, message):
  print("received message =",str(message.payload.decode("utf-8")))

def on_log(client, userdata, level, buf):
  print("log: ",buf)

def on_connect(client, userdata, flags, rc):
  print("publishing ")
  client.publish(mqttTopic,sampleData,0)


client=paho.Client(mqttClientId)
client.on_message=on_message
client.on_log=on_log
client.on_connect=on_connect
client.username_pw_set(authUser, authToken)
print("connecting to broker...")
client.tls_set(ca_certs=None, certfile=None, keyfile=None, cert_reqs=ssl.CERT_REQUIRED,
    tls_version=ssl.PROTOCOL_TLSv1_2, ciphers=None)

client.tls_insecure_set(True)
client.connect(mqttHost, mqttPort, 60)

##start loop to process received messages
client.loop_start()
#wait to allow publish and logging and exit
time.sleep(1)
client.disconnect()
