package com.dianping.cosmos.monitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpCatClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpClientService.class);

    private static HttpClientService httClientSerivce = new HttpClientService();
    private static String CAT_BASE_URL = "http://cat.dianpingoa.com/cat/r/monitor?timestamp=";
    
    public static void sendMetric(String domain, String key, String op, String value){
        try{
            StringBuilder request = new StringBuilder();
            request.append(CAT_BASE_URL);
            request.append(System.currentTimeMillis());
            request.append("&group=Storm&domain=");
            request.append(domain);
            request.append("&key=");
            request.append(key);
            request.append("&op=");
            request.append(op);
            request.append("&" + op +"=");
            request.append(value);
            httClientSerivce.get(request.toString());
        }
        catch(Exception e){
            LOGGER.error("send to cat error. ",  e);
        }
    }
}
