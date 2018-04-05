package com.hdp.registry;

import com.example.Customer;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Created by samgupta0 on 4/4/2018.
 */
public class MyHwProducer {

    public static void main(String[] args){

        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","127.0.0.1:6667");
        prop.setProperty("acks","1");
        prop.setProperty("retries","10");

       /* prop.setProperty("key.serializer", StringSerializer.class.getName());
        prop.setProperty("value.serializer", StringSerializer.class.getName());*/

        prop.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), "http://localhost:7788/api/v1");
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

        KafkaProducer<String,Customer>  kafkaProducer = new KafkaProducer<String, Customer>(prop);
        String topic="customer-avro-2";

        Customer customer = Customer.newBuilder()
                            .setFirstName("john")
                            .setLastName("wick")
                            .setHeight(176)
                            .build();
        ProducerRecord<String,Customer> producerRecord = new ProducerRecord<String,Customer>(
                topic,customer
        );

        kafkaProducer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(e == null){
                    System.out.println("Success!");
                    System.out.println(recordMetadata.toString());
                }else{
                    e.printStackTrace();
                }
            }
        });
        kafkaProducer.flush();
        kafkaProducer.close();

    }
}
