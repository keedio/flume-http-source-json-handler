package com.keedio.flume.source.http.json.handler.metrics;

/**
 * Definition of event types used for management by the controller metric metric.
 *
 * Created by Luca Rosellini <lrosellini@keedio.com> on 26/5/15.
 */
public interface MetricsMBean {

    /**
     * @return number of received jsons
     */
    long receivedJsonsCount();

    /**
     * @return mean rate of received jsons
     */
    double receivedJsonsMeanRate();

    /**
     * @return one-minute rate of received jsons
     */
    double receivedJsonsOneMinuteRate();

    /**
     * @return fifteen minute rate of received jsons
     */
    double receivedJsonsFifteenMinuteRate();

    /**
     * @return number of wrong incoming jsons.
     */
    long jsonErrorCount();

    /**
     * @return mean rate of wrong incoming jsons.
     */
    double jsonErrorMeanRate();

    /**
     * @return one minute rate of wrong incoming jsons.
     */
    double jsonErrorOneMinuteRate();
    /**
     * @return fifteen rate of wrong incoming jsons.
     */
    double jsonErrorFifteenMinuteRate();

    /**
     * @return mean parse time of incoming requests to jsons.
     */
    double requestParseTimeMean();

    /**
     * @return max parse time of incoming requests to jsons.
     */
    long requestParseTimeMax();

    /**
     * @return min parse time of incoming requests to jsons.
     */
    long requestParseTimeMin();

    /**
     * @return 95th percentile of parse time of incoming requests to jsons.
     */
    double requestParseTime95ThPercentile();

    /**
     * @return 99th percentile of parse time of incoming requests to jsons.
     */
    double requestParseTime99ThPercentile();

    /**
     * @return standard deviation of parse time of incoming requests to jsons.
     */
    double requestParseTimeStdDev();

    /**
     * @return mean time taken to transform a parsed json to a flume Event.
     */
    double eventGenerationTimeMean();

    /**
     * @return max time taken to transform a parsed json to a flume Event.
     */
    long eventGenerationTimeMax();

    /**
     * @return min time taken to transform a parsed json to a flume Event.
     */
    long eventGenerationTimeMin();

    /**
     * @return 95th percentile of the time taken to transform a parsed json to a flume Event.
     */
    double eventGenerationTime95ThPercentile();

    /**
     * @return 99th percentile of the time taken to transform a parsed json to a flume Event.
     */
    double eventGenerationTime99ThPercentile();

    /**
     * @return standard deviation of the time taken to transform a parsed json to a flume Event.
     */
    double eventGenerationTimeStdDev();
}
