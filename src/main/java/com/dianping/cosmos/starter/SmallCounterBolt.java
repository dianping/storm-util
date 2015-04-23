package com.dianping.cosmos.starter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.dianping.cosmos.util.TupleHelpers;

@SuppressWarnings({"unchecked", "rawtypes"})
public class SmallCounterBolt extends BaseRichBolt{
    public static final Logger LOG = LoggerFactory.getLogger(SmallCounterBolt.class);

    private static final long serialVersionUID = 1L;
    
    private List<Tuple> anchors = new ArrayList<Tuple>();
    
    private Map<String, Integer> counters = new HashMap<String, Integer>();
    
    private OutputCollector collector;
    @Override
    public void prepare(Map stormConf, TopologyContext context,
            OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
       if (TupleHelpers.isTickTuple(tuple)) {
           for(Map.Entry<String, Integer> counter : counters.entrySet()){
               LOG.info("word = " + counter.getKey() + ", count = " + counter.getValue());
               collector.emit(anchors, new Values(counter.getKey(), counter.getValue()));
           }
           counters.clear();
           anchors.clear();
           return;
       }

      String word = tuple.getString(0);
      Integer count = counters.get(word);
      if (count == null){
        count = 0;
      }
      count++;
      counters.put(word, count);
      anchors.add(tuple);
      
      collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word", "count"));
    }
    
    @Override
    public Map getComponentConfiguration(){
         Map<String, Object> conf = new HashMap<String, Object>();
         conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 2);
         return conf;
    }

}
