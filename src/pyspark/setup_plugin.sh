apk add wget
cd /spark/jars
#kafka
wget https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/2.8.1/kafka-clients-2.8.1.jar
wget https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.0.2/spark-sql-kafka-0-10_2.12-3.0.2.jar
wget https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.0.2/spark-token-provider-kafka-0-10_2.12-3.0.2.jar
wget https://repo1.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-10_2.12/3.0.2/spark-streaming-kafka-0-10_2.12-3.0.2.jar

# es
wget https://repo1.maven.org/maven2/org/elasticsearch/elasticsearch-spark-30_2.12/7.12.0/elasticsearch-spark-30_2.12-7.12.0.jar 

#aws
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.0.2/hadoop-aws-3.0.2.jar
wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.375/aws-java-sdk-bundle-1.11.375.jar
