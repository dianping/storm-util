package com.dianping.cosmos;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.metric.api.CountMetric;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.dianping.cosmos.blackhole.BlackholeMessageId;
import com.dianping.cosmos.blackhole.StormOffsetStrategy;
import com.dianping.cosmos.util.CatMetricUtil;
import com.dianping.cosmos.util.Constants;
import com.dp.blackhole.consumer.MessageStream;
import com.dp.blackhole.consumer.api.Consumer;
import com.dp.blackhole.consumer.api.ConsumerConfig;
import com.dp.blackhole.consumer.api.MessagePack;

@SuppressWarnings({"rawtypes"})
public class BlackholeSpout implements IRichSpout {
    private static final long serialVersionUID = 1L;

    public static final Logger LOG = LoggerFactory.getLogger(BlackholeSpout.class);
    
    private SpoutOutputCollector collector;
    private String topic;
    private String group;
    private MessageStream stream;
    private Consumer consumer;
    //多少条消息后，同步一次到Redis中
    private int syncFrequency;
    
    private transient StormOffsetStrategy offsetStrategy;

    private transient CountMetric _spoutMetric;

    public BlackholeSpout(String topic, String group) {
        this.topic = topic;
        this.group = group;
    }
    
    /**
     * blackhole spout的构造函数
     * @param topic topic名称
     * @param group comsumer的group名称
     * @param syncFrequency 多少条消息后，同步offset到Redis中
     */
    public BlackholeSpout(String topic, String group, int syncFrequency) {
        this.topic = topic;
        this.group = group;
        this.syncFrequency = syncFrequency;
    }
    
    @Override
    public void open(Map conf, TopologyContext context,
            SpoutOutputCollector _collector) {
        collector = _collector;
        _spoutMetric = new CountMetric();
        context.registerMetric(CatMetricUtil.getSpoutMetricName(topic, group),  
                _spoutMetric, Constants.EMIT_FREQUENCY_IN_SECONDS);
        offsetStrategy  = new StormOffsetStrategy();
        offsetStrategy.setConsumerGroup(group);
        offsetStrategy.setSyncFrequency(syncFrequency);
        
        ConsumerConfig config = new ConsumerConfig();
        consumer = new Consumer(topic, group, config);
        consumer.start();
        stream = consumer.getStream();
    }

    @Override
    public void close() {
        
    }

    @Override
    public void activate() {        
    }

    @Override
    public void deactivate() {   
        offsetStrategy.syncOffset();
    }

    @Override
    public void nextTuple() {
        for (MessagePack message : stream) {
            collector.emit(topic, new Values(message.getContent()), 
                    BlackholeMessageId.getMessageId(message.getPartition(), message.getOffset()));
            _spoutMetric.incr();
            offsetStrategy.updateOffset(message);
        }
    }

    @Override
    public void ack(Object msgId) {
        LOG.debug("ack: " + msgId);
        
    }

    @Override
    public void fail(Object msgId) {
        LOG.info("fail: " + msgId);   
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(topic, new Fields("event"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
