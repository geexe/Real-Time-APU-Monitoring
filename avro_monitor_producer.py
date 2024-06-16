#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# A simple example demonstrating use of AvroSerializer.
from confluent_kafka.admin import AdminClient, NewTopic
import argparse
import os
import requests
import pandas as pd

from time import sleep

from uuid import uuid4

# from six.moves import input

from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer


class User(object):
    """
    User record

    Args:
        name (str): User's name

        favorite_number (int): User's favorite number

        favorite_color (str): User's favorite color

        address(str): User's address; confidential
        {
          "symbol": "BTCUSDT",
          "openPrice": "61729.27000000",
          "highPrice": "61800.00000000",
          "lowPrice": "61319.47000000",
          "lastPrice": "61699.01000000",
          "volume": "814.22297000",
          "quoteVolume": "50138059.82771860",
          "openTime": 1715732880000,
          "closeTime": 1715736489761,
          "firstId": 3599114332,
          "lastId": 3599147596,
          "count": 33265
        }
    """

    def __init__(self, timestamp, TP2, TP3, H1, DV_pressure, Reservoirs, Oil_temperature, Motor_current,
                 COMP, DV_eletric, Towers, MPG, LPS, Pressure_switch, Oil_level, Caudal_impulses, y):
        self.timestamp = timestamp
        self.TP2 = TP2
        self.TP3 = TP3
        self.H1 = H1
        self.DV_pressure = DV_pressure
        self.Reservoirs = Reservoirs
        self.Oil_temperature = Oil_temperature
        self.Motor_current = Motor_current
        self.COMP = COMP
        self.DV_eletric = DV_eletric
        self.Towers = Towers
        self.MPG = MPG
        self.LPS = LPS
        self.Pressure_switch = Pressure_switch
        self.Oil_level = Oil_level
        self.Caudal_impulses = Caudal_impulses
        self.y = y


def user_to_dict(user, ctx):
    
    # User._address must not be serialized; omit from dict
    return dict(timestamp=user.timestamp, TP2=user.TP2, TP3=user.TP3, H1=user.H1, DV_pressure=user.DV_pressure, 
                Reservoirs=user.Reservoirs, Oil_temperature=user.Oil_temperature, Motor_current=user.Motor_current,
                COMP=user.COMP, DV_eletric=user.DV_eletric, Towers=user.Towers, MPG=user.MPG, LPS=user.LPS,
                Pressure_switch=user.Pressure_switch, Oil_level=user.Oil_level, Caudal_impulses=user.Caudal_impulses,
                y=user.y
                )


def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.

        msg (Message): The message that was produced or failed.

    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.
    """

    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def main(args):
    topic = args.topic
    is_specific = args.specific == "true"

    if is_specific:
        schema = "user_specific1.avsc"
    else:
        schema = "user_generic.avsc"

    path = os.path.realpath(os.path.dirname(__file__))
    with open(f"{path}/avro/{schema}") as f:
        schema_str = f.read()

    schema_registry_conf = {'url': args.schema_registry}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    avro_serializer = AvroSerializer(schema_registry_client,
                                     schema_str,
                                     user_to_dict)

    string_serializer = StringSerializer('utf_8')

    producer_conf = {'bootstrap.servers': args.bootstrap_servers}

    producer = Producer(producer_conf)

    print("Producing user records to topic {}. ^C to exit.".format(topic))

    api = "data.csv"
    
    #Create topic with a number of partition and replicas
    admin_client = AdminClient(producer_conf)
    topic_list = []
    topic_list.append(NewTopic(topic, 3, 3))
    admin_client.create_topics(topic_list)
    print(topic_list)

  
    while True:
        # Serve on_delivery callbacks from previous calls to produce()
        
        df = pd.read_csv("data.csv")

        # Add column y with initial value = 0 ("Low")
        df['y'] = 0

        # Add y on rows where y = 1 ("Medium") and y = 2 ("High")
        df.loc[(df['timestamp'] >= '2020-04-12 11:50:00') & (df['timestamp'] <= '2020-04-12 23:30:00'), 'y'] = 2
        df.loc[(df['timestamp'] >= '2020-04-18 00:00:00') & (df['timestamp'] <= '2020-04-18 23:59:00'), 'y'] = 2
        df.loc[(df['timestamp'] >= '2020-04-19 00:00:00') & (df['timestamp'] <= '2020-04-19 01:30:00'), 'y'] = 2
        df.loc[(df['timestamp'] >= '2020-04-29 03:20:00') & (df['timestamp'] <= '2020-04-29 04:00:00'), 'y'] = 2
        df.loc[(df['timestamp'] >= '2020-04-29 22:00:00') & (df['timestamp'] <= '2020-04-29 22:20:00'), 'y'] = 2
        df.loc[(df['timestamp'] >= '2020-05-13 14:00:00') & (df['timestamp'] <= '2020-05-13 23:59:00'), 'y'] = 2
        df.loc[(df['timestamp'] >= '2020-05-18 05:00:00') & (df['timestamp'] <= '2020-05-18 05:30:00'), 'y'] = 2
        df.loc[(df['timestamp'] >= '2020-05-19 10:10:00') & (df['timestamp'] <= '2020-05-19 11:00:00'), 'y'] = 2
        df.loc[(df['timestamp'] >= '2020-05-19 22:10:00') & (df['timestamp'] <= '2020-05-19 23:59:00'), 'y'] = 2
        df.loc[(df['timestamp'] >= '2020-05-20 00:00:00') & (df['timestamp'] <= '2020-05-20 20:00:00'), 'y'] = 2
        df.loc[(df['timestamp'] >= '2020-05-23 09:50:00') & (df['timestamp'] <= '2020-05-23 10:10:00'), 'y'] = 2
        df.loc[(df['timestamp'] >= '2020-05-29 23:30:00') & (df['timestamp'] <= '2020-05-29 23:59:00'), 'y'] = 2
        df.loc[(df['timestamp'] >= '2020-05-30 00:00:00') & (df['timestamp'] <= '2020-05-30 06:00:00'), 'y'] = 2
        df.loc[(df['timestamp'] >= '2020-06-01 15:00:00') & (df['timestamp'] <= '2020-06-01 15:40:00'), 'y'] = 2
        df.loc[(df['timestamp'] >= '2020-06-03 10:00:00') & (df['timestamp'] <= '2020-06-03 11:00:00'), 'y'] = 2
        df.loc[(df['timestamp'] >= '2020-06-05 10:00:00') & (df['timestamp'] <= '2020-06-05 23:59:00'), 'y'] = 2
        df.loc[(df['timestamp'] >= '2020-06-06 00:00:00') & (df['timestamp'] <= '2020-06-06 23:29:00'), 'y'] = 2
        df.loc[(df['timestamp'] >= '2020-06-07 00:00:00') & (df['timestamp'] <= '2020-06-07 14:30:00'), 'y'] = 2
        df.loc[(df['timestamp'] >= '2020-07-08 17:30:00') & (df['timestamp'] <= '2020-07-08 19:00:00'), 'y'] = 2
        df.loc[(df['timestamp'] >= '2020-07-15 14:30:00') & (df['timestamp'] <= '2020-07-15 19:00:00'), 'y'] = 1
        df.loc[(df['timestamp'] >= '2020-07-17 04:30:00') & (df['timestamp'] <= '2020-07-17 05:30:00'), 'y'] = 2
        split = 0.55
        # df_offline = df.iloc[0:int(split*len(df)),]
        # df_online = df.iloc[int(split*len(df)):,]
        # df_online = df_online.loc[df_online["y"] == 2]
        df_online = pd.concat([
            df.loc[(df['timestamp'] >= '2020-06-06 23:58:00') & (df['timestamp'] <= '2020-06-07 00:03:00')],
            df.loc[(df['timestamp'] >= '2020-07-15 14:27:00') & (df['timestamp'] <= '2020-07-15 14:33:00')]
        ])
        for i in range(len(df_online)):
            data = df_online.iloc[i,1:]
            print(data)

            user = User(timestamp=str(data["timestamp"]), TP2=float(data["TP2"]), TP3= float(data["TP3"]),
                        H1=float(data["H1"]),DV_pressure=float(data["DV_pressure"]), 
                        Reservoirs=float(data["Reservoirs"]), Oil_temperature=float(data["Oil_temperature"]), 
                        Motor_current=float(data["Motor_current"]), COMP=int(data["COMP"]), DV_eletric=int(data["DV_eletric"]),
                        Towers=int(data["Towers"]), MPG=int(data["MPG"]), LPS=int(data["LPS"]),
                        Pressure_switch=int(data["Pressure_switch"]), Oil_level=int(data["Oil_level"]), 
                        Caudal_impulses=int(data["Caudal_impulses"]), y=int(data["y"])
            )
            producer.produce(topic=topic,
                                key="timestamp",
                                value=avro_serializer(user, SerializationContext(topic, MessageField.VALUE)),
                                on_delivery=delivery_report)
            
            sleep(1)
            producer.poll(0.0)
        
    print("\nFlushing records...")
    producer.flush()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="AvroSerializer example")
    parser.add_argument('-b', dest="bootstrap_servers", required=True,
                        help="Bootstrap broker(s) (host[:port])")
    parser.add_argument('-s', dest="schema_registry", required=True,
                        help="Schema Registry (http(s)://host[:port]")
    parser.add_argument('-t', dest="topic", default="example_serde_avro",
                        help="Topic name")
    parser.add_argument('-p', dest="specific", default="true",
                        help="Avro specific record")

    main(parser.parse_args())

#Example
# python avro_monitor_producer.py -b "localhost:9092" -t "raw2" -s "http://localhost:8081"