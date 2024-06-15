# DADS6005-Real-Time-APU-Monitoring
DADS6005 Final project to combine real-time and batch ML for APU Monitoring
1. Create a working directory (Example: "/monitor")
2. Create a folder "/avro" in the working directory (e.g. "/monitor") and put "user_specific1.asvc" into the folder
3. Run Kafka Cluster according to guide. https://www.youtube.com/watch?v=axUEUVSPnzA&ab_channel=EkaratRattagan You may choose any directory to run this but the recommended method is to create a directory within the working directory (e.g. "/monitor/kafka")
4. Open a terminal and run sink file from the working directory ("/monitor"), not Kafka Cluster directory with  curl -d @"sinkMysql_raw2.json" -H "Content-Type: application/json" -X POST http://localhost:8083/connectors
5. Open another terminal in the working directory and run python avro_monitor_producer.py -b "localhost:9092" -t "raw2" -s "http://localhost:8081"
5. Open another terminal in the directory and run python avro_monitor_consumer.py -b "localhost:9092" -s "http://localhost:8081" -t "raw2"
