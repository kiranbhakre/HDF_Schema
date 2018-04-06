package com.hdp.registry;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by samgupta0 on 3/26/2018.
 */
public class HdfsSinkConnector extends SinkConnector {

    private Map<String, String> configProperties;

    public String version() {
        return "1";
    }

    public void start(Map<String, String> map) {

        try {
            configProperties = new HashMap<String,String>();
            configProperties.put("prop1","val1");
            configProperties.put("prop2","val2");
            configProperties.put("prop3","val3");


        } catch (ConfigException e) {
            throw new ConnectException("Couldn't start Hdfs Sink Connector due to configuration error",e);
        }
    }

    public Class<? extends Task> taskClass() {
        return HdfsSinkTask.class;
    }

    public List<Map<String, String>> taskConfigs(int maxTasks) {

        List<Map<String,String>> taskConfigs = new ArrayList<Map<String,String>>();
        Map<String,String> taskProps = new HashMap<String,String>();
        taskProps.putAll(configProperties);
        for (int i=0; i < maxTasks; i++){
            taskConfigs.add(taskProps);
        }
        return taskConfigs;
    }

    public void stop() {

    }

    @Override
    public ConfigDef config() {

        ConfigDef defs = new ConfigDef();
        return defs;

    }
}
