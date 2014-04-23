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
    
    public void setUpdater(Updater updater) {
        this.updater = updater;
    }
    
    @Override
    public void prepare(Map stormConf, TopologyContext context,
            OutputCollector collector) {
        this.collector = collector;
        jedis = new Jedis("192.168.213.249", 6381);
    }

    @Override
    public void execute(Tuple input) {
        byte[] key = input.getBinary(0);
        byte[] value = input.getBinary(1);
        
        if (updater != null) {
            byte[] oldValue = jedis.get(key);
            if (oldValue != null) {
                byte[] newValue = updater.update(oldValue, value);
                jedis.set(key, newValue);
                collector.ack(input);
                return;
            }
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
