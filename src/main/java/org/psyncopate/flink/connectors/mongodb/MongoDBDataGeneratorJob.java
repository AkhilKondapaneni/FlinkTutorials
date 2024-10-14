package org.psyncopate.flink.connectors.mongodb;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.model.InsertOneModel;

import org.slf4j.Logger;
import org.bson.BsonDocument;
import org.bson.Document;
import org.psyncopate.flink.connectors.PropertyFilesLoader;

import java.util.Properties;
import java.util.Random;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.mongodb.sink.MongoSink;

public class MongoDBDataGeneratorJob {

    public static void main(String[] args) throws Exception {

		final Logger logger = LoggerFactory.getLogger(MongoDBDataGeneratorJob.class);
		
		LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
		//StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = PropertyFilesLoader.loadProperties("mongodb.properties");
        

        MongoSink<String> sink_raw = MongoSink.<String>builder()
        //.setUri("mongodb://api_user:api1234@container-mongodb:27017/api_dev_db")
        .setUri(properties.getProperty("mongodb.uri"))
        .setDatabase(properties.getProperty("mongodb.database"))
        .setCollection(properties.getProperty("mongodb.collection"))
        .setBatchSize(1000)
        .setBatchIntervalMs(1000)
        .setMaxRetries(3)
        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .setSerializationSchema(
                (input, context) -> new InsertOneModel<>(BsonDocument.parse(input)))
        .build();

        //ds_updated.sinkTo(sink);
        long docsCount = Long.parseLong(properties.getProperty("no.of.documents"));
        DataStream<String> documentStream = env.fromSequence(1, docsCount)
                .map(i -> {
                    Random random = new Random();
                    ObjectMapper objectMapper = new ObjectMapper();
                    String name = "user_" + i;
                    int age = random.nextInt(90) + 10; 
                    boolean isActive = random.nextBoolean();
                    Document doc = new Document();
                    doc.put("name", name);
                    doc.put("age", age);
                    doc.put("email", name + "@example.com");
                    doc.put("isActive", isActive);
                    return objectMapper.writeValueAsString(doc);  
                });


        documentStream.sinkTo(sink_raw);

        
        env.execute("Customer Data Ingestion to Mongo");
        

    }
}


