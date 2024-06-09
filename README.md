# DADS6005-Real-Time-APU-Monitoring
DADS6005 Final project to combine real-time and batch ML for APU Monitoring
1. Run Kafka Cluster https://www.youtube.com/watch?v=axUEUVSPnzA&ab_channel=EkaratRattagan
2. Run sink file with  curl -d @"sinkMysql_raw2.json" -H "Content-Type: application/json" -X POST http://localhost:8083/connectors
3. Create folder "avro" and put "user_specific1.asvc" into the folder
4. Open terminal in the directory and run python avro_monitor_producer.py -b "localhost:9092" -t "raw2" -s "http://localhost:8081"
5. Open annother terminal in the directory and run python avro_monitor_consumer.py -b "localhost:9092" -s "http://localhost:8081" -t "raw2"
