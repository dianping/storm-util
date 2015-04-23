
package com.dianping.cosmos.starter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class WordCountTopology {
 

  public static void main(String[] args) throws Exception {

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("Spout", new RandomSentenceSpout(), 3).setNumTasks(6);
    builder.setBolt("SplitBolt", new SplitSentenceBolt(), 4
            ).shuffleGrouping("Spout").setMaxTaskParallelism(8);
    
    builder.setBolt("BiggerCounter", new BigCounterBolt(), 2
            ).fieldsGrouping("SplitBolt", "bigger", new Fields("word"));
    builder.setBolt("SmallerCounter", new SmallCounterBolt(), 2
            ).fieldsGrouping("SplitBolt", "smaller", new Fields("word"));

    builder.setBolt("FinalCounter", new FinalCounterBolt(), 1).
        noneGrouping("BiggerCounter").noneGrouping("SmallerCounter");

    Config conf = new Config();

    if (args != null && args.length > 0) {
      conf.setNumWorkers(5);
      StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
    }
    else {
      conf.setMaxTaskParallelism(3);
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("word-count", conf, builder.createTopology());

      Thread.sleep(10000);

      cluster.shutdown();
    }
  }
}
