package com.dianping.cosmos;

import java.util.Map;

import redis.clients.jedis.Jedis;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class RedisSinkBolt implements IRichBolt {
    private OutputCollector collector;
    private Jedis jedis;
    private Updater updater;
    
    private String redisHost;
    private int redisPort;
    
    public RedisSinkBolt(String redisHost, int redisPort) {
        this.redisHost = redisHost;
        this.redisPort = redisPort;
    }
    
    public void setUpdater(Updater updater) {
        this.updater = updater;
    }
    
    @Override
    public void prepare(Map conf, TopologyContext context,
            OutputCollector collector) {
        this.collector = collector;
        jedis = new Jedis(redisHost, redisPort);
    }

    @Override
    public void execute(Tuple input) {
        byte[] key = input.getBinary(0);
        byte[] value = input.getBinary(1);
        
        if (updater != null) {
            byte[] oldValue = jedis.get(key);
            byte[] newValue = updater.update(oldValue, value);
            jedis.set(key, newValue);
            collector.ack(input);
            return;
        }

        jedis.set(key, value);    
        collector.ack(input);
    }

    @Override
    public void cleanup() {
        jedis.disconnect();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }

}
