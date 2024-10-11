package org.psyncopate.flink.connectors.mongodb;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.LoggerFactory;

import com.mongodb.client.model.InsertOneModel;

import org.slf4j.Logger;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.psyncopate.flink.connectors.PropertyFilesLoader;

import java.util.Properties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.mongodb.sink.MongoSink;
import org.apache.flink.connector.mongodb.source.MongoSource;
import org.apache.flink.connector.mongodb.source.enumerator.splitter.PartitionStrategy;
import org.apache.flink.connector.mongodb.source.reader.deserializer.MongoDeserializationSchema;

public class MongoDBDataEnricherJob {

    public static void main(String[] args) throws Exception {

		final Logger logger = LoggerFactory.getLogger(MongoDBDataGeneratorJob.class);
		
		//LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = PropertyFilesLoader.loadProperties("mongodb.properties");
        
         MongoSource<String> source = MongoSource.<String>builder()
        //.setUri("mongodb://api_user:api1234@container-mongodb:27017/api_dev_db")
        .setUri(properties.getProperty("mongodb.uri"))
        .setDatabase(properties.getProperty("mongodb.database"))
        .setCollection(properties.getProperty("mongodb.collection"))
        //.setProjectedFields("_id", "name", "age")
        .setFetchSize(2048)
        .setLimit(10000)
        .setNoCursorTimeout(true)
        .setPartitionStrategy(PartitionStrategy.SAMPLE)
        .setPartitionSize(MemorySize.ofMebiBytes(64))
        //.setSamplesPerPartition(10)
        .setDeserializationSchema(new MongoDeserializationSchema<String>() {
            @Override
            public String deserialize(BsonDocument document) {
                return document.toJson();
            }

            @Override
            public TypeInformation<String> getProducedType() {
                return BasicTypeInfo.STRING_TYPE_INFO;
            }
        })
        .build();
        DataStream<String> ds = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Read Data from MongoDB").setParallelism(1); 

        
        DataStream<String> activeUsersStream = ds
        .filter(document -> {
            // Parse the JSON and check if isActive is true
            BsonDocument bsonDocument = BsonDocument.parse(document);
            return bsonDocument.getBoolean("isActive").getValue();
        })
        .map((document) -> {
            BsonDocument bsonDocument = BsonDocument.parse(document);
            bsonDocument.append("processed", new BsonBoolean(true));
            return bsonDocument.toJson();
        });

        MongoSink<String> sink_enriched = MongoSink.<String>builder()
        //.setUri("mongodb://api_user:api1234@container-mongodb:27017/api_dev_db")
        .setUri(properties.getProperty("mongodb.uri"))
        .setDatabase(properties.getProperty("mongodb.database"))
        .setCollection(properties.getProperty("mongodb.collection")+"_enriched")
        .setBatchSize(1000)
        .setBatchIntervalMs(1000)
        .setMaxRetries(3)
        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .setSerializationSchema(
                (input, context) -> new InsertOneModel<>(BsonDocument.parse(input)))
        .build();

        activeUsersStream.sinkTo(sink_enriched);
        
        env.execute("Enriched Customer Data Ingestion to Mongo");
        

    }
}


