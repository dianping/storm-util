package com.dianping.cosmos;

import java.util.HashMap;
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
import backtype.storm.utils.Utils;

import com.dianping.cosmos.blackhole.BlackholeMessageId;
import com.dianping.cosmos.blackhole.MessageFetcher;
import com.dianping.cosmos.blackhole.StormOffsetStrategy;
import com.dianping.cosmos.util.CatMetricUtil;
import com.dianping.cosmos.util.Constants;
import com.dp.blackhole.consumer.MessageStream;
import com.dp.blackhole.consumer.api.Consumer;
import com.dp.blackhole.consumer.api.ConsumerConfig;
import com.dp.blackhole.consumer.api.MessagePack;

@SuppressWarnings({"rawtypes", "unchecked"})
public class BlackholeBlockingQueueSpout implements IRichSpout {
    private static final long serialVersionUID = 386827585122587595L;
    public static final Logger LOG = LoggerFactory.getLogger(BlackholeBlockingQueueSpout.class);
    private SpoutOutputCollector collector;
    private String topic;
    private String group;
    private MessageStream stream;
    private Consumer consumer;
    private MessageFetcher fetchThread;
    private int warnningStep = 0;
    //多少条消息后，同步一次到Redis中
    private int syncFrequency;
    
    private transient CountMetric _spoutMetric;
    
    private transient StormOffsetStrategy offsetStrategy;

    public BlackholeBlockingQueueSpout(String topic, String group) {
        this.topic = topic;
        this.group = group;
    }
    
    /**
     * blackhole spout的构造函数
     * @param topic topic名称
     * @param group comsumer的group名称
     * @param syncFrequency 多少条消息后，同步offset到Redis中
     */
    public BlackholeBlockingQueueSpout(String topic, String group, int syncFrequency) {
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
        
        ConsumerConfig config = new ConsumerConfig();
        
        offsetStrategy  = new StormOffsetStrategy();
        offsetStrategy.setConsumerGroup(group);
        offsetStrategy.setSyncFrequency(syncFrequency);
        
        consumer = new Consumer(topic, group, config, offsetStrategy);

        consumer.start();
        stream = consumer.getStream();
        
        fetchThread = new MessageFetcher(stream);
        new Thread(fetchThread).start();
    }

    @Override
    public void close() {
        fetchThread.shutdown();
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
        MessagePack message = fetchThread.pollMessage();
        if (message != null) {
            collector.emit(topic, new Values(message.getContent()), 
                    BlackholeMessageId.getMessageId(message.getPartition(), message.getOffset()));

            _spoutMetric.incr();
            offsetStrategy.updateOffset(message);
        } else {
            Utils.sleep(100);
            warnningStep++;
            if (warnningStep % 100 == 0) {
                LOG.warn("Queue is empty, cannot poll message.");
            }
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
    public Map getComponentConfiguration(){
         Map<String, Object> conf = new HashMap<String, Object>();
         return conf;
    }
}
