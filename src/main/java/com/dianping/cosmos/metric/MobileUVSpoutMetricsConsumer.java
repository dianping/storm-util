package com.dianping.cosmos.metric;

import java.util.Collection;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.metric.api.IMetricsConsumer;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;

import com.dianping.cosmos.monitor.HttpCatClient;
import com.dianping.cosmos.monitor.TopologyMonitor;
import com.dianping.cosmos.util.CatMetricUtil;

/**
 * Listens for all metrics, dumps them to cat
 *
 * To use, add this to your topology's configuration:
 *   conf.registerMetricsConsumer(com.dianping.cosmos.metric.CatSpoutMetricsConsumer.class, 1);
 *
 * Or edit the storm.yaml config file:
 *
 *   topology.metrics.consumer.register:
 *     - class: "com.dianping.cosmos.metric.CatSpoutMetricsConsumer"
 *       parallelism.hint: 1
 *
 */
@SuppressWarnings("rawtypes")
public class MobileUVSpoutMetricsConsumer implements IMetricsConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(MobileUVSpoutMetricsConsumer.class);
    private TopologyMonitor spoutMonitor;
    private String stormId;
        
    @Override
    public void prepare(Map stormConf, Object registrationArgument, 
            TopologyContext context, IErrorReporter errorReporter) {
        spoutMonitor = new TopologyMonitor();
        stormId = context.getStormId();
    }


    @Override
    public void handleDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {
        for (DataPoint p : dataPoints) {
            try{
                if(CatMetricUtil.isCatMetric(p.name)){
                    spoutMonitor.monitorStatus(stormId, taskInfo, p);
                    HttpCatClient.sendMetric("Spout",
                            "dpods_log_mobile-log-web_MAIN", "sum", String.valueOf(p.value));
                }
            }
            catch(Exception e){
                LOGGER.warn("send metirc 2 cat error.", e);
            }
        }
    }

    @Override
    public void cleanup() { }
}
