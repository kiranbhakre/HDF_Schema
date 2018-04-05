package com.hdp.registry;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;


/**
 * Created by samgupta0 on 3/26/2018.
 */
public class HdfsSinkTask extends SinkTask {

    private static Logger log = LogManager.getLogger(HdfsSinkTask.class);
    public String version() {
        return null;
    }
    //private AvroWriter writer;
    static int counter = 0;

    public void start(Map<String, String> map) {

        //writer = new AvroWriter();
        log.info("initialized the HDFS Sink ");
        this.context.timeout(2000);
    }

    public void put(Collection<SinkRecord> collection) {

        try {
           /* Collection<String> recordsAsString = collection.stream().map(r -> String.valueOf(r.value()))
                    .collect(Collectors.toList());*/
            System.out.println("collection size --> "+collection.size());
            //collection.stream().map(r -> String.valueOf(r.value()))
           if(collection.size()>0) {
               System.out.println("before iterator");
               Iterator<SinkRecord> it = collection.iterator();
               System.out.println("after iterator");
               process(it);
           }
        }
        catch (Exception e) {
            log.error("Error while processing records");
            log.error(e.toString());
        }
    }

    public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
        log.trace("Flushing the queue");
    }

    public void stop() {

    }

    public void process(Iterator<SinkRecord> it) {

        log.info("processing the record");

        System.out.println("started processing");

        if(it!=null) {

            Object p = null;
            try {
                System.out.println("has or not" + it.hasNext());
                p = it.next().value();
                if (p != null) {
                    System.out.println("value Obtained");
                }

                log.info("The record is --> " + p);
                log.info("The record is --> " + p.toString());
            }catch (Exception ex){
                ex.printStackTrace();
            }

            try {
                counter++;
                //writer.write("avro_" + counter + ".avro", p.toString());
                System.out.println("after write");
            }  catch (Exception ex) {
                ex.printStackTrace();
            }


        }
        /*while(it.hasNext()){

            counter++;
            try {
                System.out.println("before write");

                System.out.println("after write");
            }catch (IOException io){
                io.printStackTrace();
            }catch (Exception ex){
                ex.printStackTrace();
            }
            System.out.println("The record is --> " +it.next().value());
        }*/


    }

    /*public void process(Collection<String> record){

        log.info("processing the record");
        record.parallelStream().map(rec -> {
            System.out.println("The record is --> " + rec);
            return null;
        });
    }*/
}
