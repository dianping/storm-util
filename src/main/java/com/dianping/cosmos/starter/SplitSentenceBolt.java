package com.dianping.cosmos.starter;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

@SuppressWarnings({"rawtypes"})
public class SplitSentenceBolt  extends BaseRichBolt{

    private static final long serialVersionUID = 1L;
    
    private OutputCollector collector;
    @Override
    public void prepare(Map stormConf, TopologyContext context,
            OutputCollector collector) {
        this.collector = collector;
        
    }

    @Override
    public void execute(Tuple input) {
        String sentence = input.getString(0);
        String[] words = sentence.split(" ");
        for(String word : words){
            if(word.length() >= 5){
                collector.emit("bigger", input, new Values(word));
            }
            else{
                collector.emit("smaller", input,  new Values(word));
            }
        }
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        Fields fileds = new Fields("word"); 
        declarer.declareStream("bigger", fileds);
        declarer.declareStream("smaller", fileds);        
    }

}
