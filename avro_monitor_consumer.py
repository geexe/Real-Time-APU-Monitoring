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


# A simple example demonstrating use of AvroDeserializer.

#pip install confluent_kafka
#pip install pandas
#pip install pycaret
#pip install river
#pip install scikit-learn
#pip install fastavro

import argparse
import os

from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from time import sleep
import pandas as pd
from pycaret.regression import load_model, predict_model
from sklearn.metrics import mean_squared_error
import random
from collections import defaultdict

from river import linear_model
from river import tree
from river import multiclass
from river import neighbors
from river import naive_bayes
from river import datasets
from river import evaluate
from river import metrics
from river import ensemble
from river import preprocessing

model = tree.HoeffdingTreeClassifier(
        grace_period=100,
    )
model2 = tree.HoeffdingTreeClassifier(
        grace_period=100,
    )
model3 = tree.HoeffdingTreeClassifier(
        grace_period=100,
    )
bal_acc_off = metrics.Accuracy()
bal_acc_off2 = metrics.Accuracy()
bal_acc = metrics.Accuracy()
bal_acc2 = metrics.Accuracy()
bal_acc3 = metrics.Accuracy()
saved_model = load_model('model1')
saved_model2 = load_model('model2')

class User(object):

    # Edit to monitoring data
    def __init__(self, timestamp, TP2,TP3,H1,DV_pressure,Reservoirs,Oil_temperature,Motor_current
                 ,COMP,DV_eletric,Towers,MPG,LPS,Pressure_switch,Oil_level,Caudal_impulses,y):
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

def dict_to_user(obj, ctx):
    """
    Converts object literal(dict) to a User instance.

    Args:
        obj (dict): Object literal(dict)

        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
    """

    if obj is None:
        return None

    # Edit to monitoring data
    return User(timestamp=obj["timestamp"], TP2=obj["TP2"], TP3= obj["TP3"],
                H1=obj["H1"],DV_pressure=obj["DV_pressure"], Reservoirs=obj["Reservoirs"]
                ,Oil_temperature=obj["Oil_temperature"], Motor_current=obj["Motor_current"]
                ,COMP=obj['COMP'], DV_eletric=obj['DV_eletric'],Towers=obj['Towers']
                ,MPG=obj['MPG'], LPS=obj['LPS'], Pressure_switch=obj['Pressure_switch'],
                Oil_level=obj['Oil_level'],Caudal_impulses=obj['Caudal_impulses'], y=obj["y"])

def find_majority_element_with_weight(nums, weights):
    n = len(nums)
    
    # Dictionary to store cumulative weights by class
    weighted_counts = defaultdict(int)
    
    # Accumulate weights by class
    for num, weight in zip(nums, weights):
        weighted_counts[num] += weight
    
    # Find the candidate with the maximum weighted count
    candidate, max_count = None, 0
    for num, weighted_count in weighted_counts.items():
        if weighted_count > max_count:
            candidate = num
            max_count = weighted_count
    
    # Verify the candidate
    weighted_threshold = sum(weights) / 2
    if max_count > weighted_threshold:
        return candidate
    else:
        return None


def main(args):
    topic = args.topic
    is_specific = args.specific == "true"

    if is_specific:
        # Edit to new schema
        schema = "user_specific1.avsc"
    else:
        schema = "user_generic.avsc"

    path = os.path.realpath(os.path.dirname(__file__))
    with open(f"{path}/avro/{schema}") as f:
        schema_str = f.read()

    sr_conf = {'url': args.schema_registry}
    schema_registry_client = SchemaRegistryClient(sr_conf)

    avro_deserializer = AvroDeserializer(schema_registry_client,
                                         schema_str,
                                         dict_to_user)

    consumer_conf = {'bootstrap.servers': args.bootstrap_servers,
                     'group.id': args.group,
                     'auto.offset.reset': "latest"}

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    y_true = []
    y_hat = []
    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            user = avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
            # Create a dataframe for the consuming data to feed into the ML model.
            
            data = {'timestamp':user.timestamp, 'TP2':user.TP2,
                    'TP3':user.TP3,'H1':user.H1,
                    'DV_pressure':user.DV_pressure, 'Reservoirs':user.Reservoirs
                    ,'Oil_temperature':user.Oil_temperature, 'Motor_current' : user.Motor_current
                    ,'COMP': user.COMP,'DV_eletric':user.DV_eletric,'Towers' : user.Towers
                    ,'MPG': user.MPG,'LPS':user.LPS, 'Pressure_switch' : user.Pressure_switch
                    ,'Oil_level':user.Oil_level, 'Caudal_impulses':user.Caudal_impulses}
            data2 = {'timstamp':user.timestamp, 'TP2':user.TP2,
                    'TP3':user.TP3,
                    'DV_pressure':user.DV_pressure,
                    'Oil_temperature':user.Oil_temperature, 'Motor_current' : user.Motor_current
                    ,'Towers' : user.Towers
                    ,'LPS':user.LPS, 'Pressure_switch' : user.Pressure_switch
                    ,'Oil_level':user.Oil_level, 'Caudal_impulses':user.Caudal_impulses}
            data3 = {'timstamp':user.timestamp, 'TP2':user.TP2,
                    'TP3':user.TP3,
                    'DV_pressure':user.DV_pressure,
                    'Oil_temperature':user.Oil_temperature, 'Motor_current' : user.Motor_current
                    ,'COMP': user.COMP ,'Towers' : user.Towers
                    ,'LPS':user.LPS, 'Pressure_switch' : user.Pressure_switch
                    ,'Oil_level':user.Oil_level, 'Caudal_impulses':user.Caudal_impulses}
            
            print(data)
            
            # Use offline (Batch) model to predict result from streaming
            # print(type(predictions))
            df = pd.DataFrame(data,index=[user.timestamp])
            predictions = predict_model(saved_model, data=df)
            y_pred_off = predictions.iloc[0]['prediction_label']
            print("Predicted", predictions.iloc[0]['prediction_label']," VS Actual=",user.y)
            bal_acc_off.update(user.y, y_pred_off)
            y_true.append(user.y)
            y_hat.append(y_pred_off)
            print(bal_acc_off)
            print(y_true)
            print(y_hat)
            
            df2 = pd.DataFrame(data2,index=[user.timestamp])
            predictions = predict_model(saved_model2, data=df2)
            y_pred_off2 = predictions.iloc[0]['prediction_label']
            print("Predicted", predictions.iloc[0]['prediction_label']," VS Actual=",user.y)
            print(mean_squared_error([user.y] , [predictions.iloc[0]['prediction_label']] ) )
            bal_acc_off2.update(user.y, y_pred_off)
            print(bal_acc_off2)

            # Online (Real-time) model to predict result from streaming
            y_pred = model.predict_one(data)
            y_prob = model.predict_proba_one(data)
            model.learn_one(data, user.y)
            print("y_pred = ",y_pred)
            print("y_prob = ",y_prob)
            if(y_pred == None):
                pass
            else:
                bal_acc.update(user.y, y_pred)
                print(bal_acc)
            
            y_pred2 = model2.predict_one(data2)
            y_prob2 = model2.predict_proba_one(data2)
            model2.learn_one(data2, user.y)
            print("y_pred2 = ",y_pred2)
            print("y_prob2 = ",y_prob2)
            if(y_pred2 == None):
                pass
            else:
                bal_acc2.update(user.y, y_pred2)
                print(bal_acc2)

            try:
                predicted = [y_pred_off, y_pred_off2, y_pred, y_pred2]
                weight = [bal_acc_off.get(), bal_acc_off2.get(), bal_acc.get(), bal_acc2.get()]
                print(predicted)
                print(weight)

                majority_element = find_majority_element_with_weight(predicted, weight)
                if majority_element is not None:
                    print(f"The majority element is {majority_element}.")
                else:
                    print("There is no majority element.")
            except:
                pass

        except KeyboardInterrupt:
            break

        sleep(1)

    consumer.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="AvroDeserializer example")
    parser.add_argument('-b', dest="bootstrap_servers", required=True,
                        help="Bootstrap broker(s) (host[:port])")
    parser.add_argument('-s', dest="schema_registry", required=True,
                        help="Schema Registry (http(s)://host[:port]")
    parser.add_argument('-t', dest="topic", default="example_serde_avro",
                        help="Topic name")
    parser.add_argument('-g', dest="group", default="example_serde_avro",
                        help="Consumer group")
    parser.add_argument('-p', dest="specific", default="true",
                        help="Avro specific record")

    main(parser.parse_args())

# Example
# python avro_monitor_consumer.py -b "localhost:9092" -s "http://localhost:8081" -t "raw2"