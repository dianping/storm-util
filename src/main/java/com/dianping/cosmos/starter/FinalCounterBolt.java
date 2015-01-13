package com.dianping.cosmos.starter;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

@SuppressWarnings("rawtypes")
public class FinalCounterBolt extends BaseRichBolt{
    public static final Logger LOG = LoggerFactory.getLogger(FinalCounterBolt.class);

    private static final long serialVersionUID = 1L;
    private Map<String, Integer> counts = new HashMap<String, Integer>();
    private OutputCollector collector;
    
    private long totalCount = 0l;
    
    @Override
    public void prepare(Map stormConf, TopologyContext context,
            OutputCollector collector) {
        this.collector = collector;
    }
    @Override
    public void execute(Tuple tuple) {
      String word = tuple.getString(0);
      Integer count = tuple.getInteger(1);
      Integer singleCount = counts.get(word);
      if (singleCount == null){
          singleCount = 0;
      }
      
      singleCount += count;
      counts.put(word, singleCount);
      
      totalCount += count;
      
      LOG.info("word = " + word + ", count = " + count + ", totalCount = " + totalCount);
      collector.emit(new Values(totalCount));
      collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("count"));
    }
}
