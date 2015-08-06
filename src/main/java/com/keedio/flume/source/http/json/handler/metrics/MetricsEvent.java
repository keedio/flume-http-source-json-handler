package com.keedio.flume.source.http.json.handler.metrics;

/**
 * Created by Luca Rosellini <lrosellini@keedio.com> on 26/5/15.
 */
public class MetricsEvent {
    public enum EventType{
        JSON_ARRIVED,JSON_ERROR,PARSE_OK,EVENT_GENERATION,EVENT_SIZE,NJSONS_ARRIVED,UNKNOWN;
    }

    private long value;
    private EventType code;

    public MetricsEvent(EventType code){
        this.code = code;
    }


    public MetricsEvent(EventType code, long value) {
        this(code);
        this.value = value;
    }

    public long getValue() {
        return value;
    }

    public EventType getCode() {
        return code;
    }
}
