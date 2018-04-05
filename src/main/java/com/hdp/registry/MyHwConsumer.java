package com.hdp.registry;

import com.example.Customer;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collections;
import java.util.Properties;

/**
 * Created by samgupta0 on 4/4/2018.
 */
public class MyHwConsumer {

    public static void main(String[] args){

        //Boolean val = (Boolean) true;
        //System.exit(0);
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","127.0.0.1:6667");
        prop.setProperty("acks","1");
        prop.setProperty("retries","10");

        //prop.setProperty("specific.avro.reader","true");
        prop.setProperty("group.id","my-avro-consumer");
        prop.setProperty("enable.auto.commit","false");
        prop.setProperty("auto.offset.reset","earliest");
        //prop.setProperty("specific.avro.reader","true");

        prop.setProperty("retries","10");


        /*prop.setProperty("key.serializer", StringSerializer.class.getName());
        prop.setProperty("value.serializer", StringSerializer.class.getName());*/

        prop.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), "http://localhost:7788/api/v1");
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());

        //KafkaConsumer<String,Customer> consumer = new KafkaConsumer<String, Customer>(prop);
        KafkaConsumer<String,Object> consumer = new KafkaConsumer<String, Object>(prop);
        String topic="customer-avro-2";

        consumer.subscribe(Collections.singleton(topic));
        System.out.println("waiting for data....");

        while(true){
            //ConsumerRecords<String,Customer> records = consumer.poll(   500);
            ConsumerRecords<String,Object> records = consumer.poll(   500);
            for(ConsumerRecord<String,Object> record:records){
                Customer customer = (Customer)record.value();
                GenericRecord customer_generic = (GenericRecord)record;
                customer_generic.getSchema();
                System.out.println(customer);
                /*System.out.println(customer.getFirstName() + "," +
                        customer.getLastName()+","+customer.getHeight());*/
            }
            consumer.commitAsync();
        }
      // consumer.close();

    }
}
