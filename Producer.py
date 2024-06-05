
# Producer
from confluent_kafka import Producer
from time import sleep
import requests
import pandas as pd

df = pd.read_csv("data.csv")
df['y'] = 0



p = Producer({'bootstrap.servers':"localhost:9092"})

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (msg.value().decode()))


import json
from bson import json_util
import datetime

#Option2: Json
for i in range(100):
    #data = requests.get(key).json()
    #show1 if data, error
    #show2 if str(data), works
    #show3 json 
    ct = datetime.datetime.now()
    data = df.iloc[i,].to_json()
    p.produce('raw', key="timestamp", value=data, callback=acked)
    #p.produce('raw', key="timestamp", value=json.dumps(data, default=json_util.default).encode('utf-8'), callback=acked)
    sleep(0.1)
    p.poll(1)
    

#Reference
#1. https://binance-docs.github.io/apidocs/spot/en/#rolling-window-price-change-statistics
#2. Good example of mysql sink config https://aiven.io/docs/products/kafka/kafka-connect/howto/jdbc-sink
#3. Sink enable cmd: 
#       curl -d @"sinkMysql.json" -H "Content-Type: application/json" -X POST http://localhost:8083/connectors
#4. https://github.com/confluentinc/confluent-kafka-python/tree/master
#5. https://docs.confluent.io/cloud/current/connectors/cc-mysql-sink.html