package com.keedio.flume.source.http.json.handler.metrics;

import com.codahale.metrics.*;
import org.apache.log4j.Logger;

import java.util.concurrent.TimeUnit;


/**
 * This class represents the controller metrics to publish to the source.
 * Extends MonitoredCounterGroup class to allow the publication of JMX metrics
 * following the mechanism established by Flume.
 *
 * Created by Luca Rosellini <lrosellini@keedio.com> on 26/5/15.
 */
public class MetricsController /*extends MonitoredCounterGroup*/ implements MetricsMBean {
    private static Logger logger = Logger.getLogger(MetricsController.class);

    Meter receivedJsons;
    Meter jsonError;
    Timer requestParseTime;
    Timer eventGenerationTime;
    Histogram eventSize;
    Histogram jsonsPerRequest;

    private MetricRegistry metrics;

    private static final String[] ATTRIBUTES = {
            "httpsourcehandler.meter.receivedJsons.count",
            "httpsourcehandler.meter.receivedJsons.mean-rate",
            "httpsourcehandler.meter.receivedJsons.one-minute-rate",
            "httpsourcehandler.meter.receivedJsons.fifteen-minute-rate",

            "httpsourcehandler.meter.jsonError.count",
            "httpsourcehandler.meter.jsonError.mean-rate",
            "httpsourcehandler.meter.jsonError.one-minute-rate",
            "httpsourcehandler.meter.jsonError.fifteen-minute-rate",

            "httpsourcehandler.meter.requestParseTime.mean",
            "httpsourcehandler.meter.requestParseTime.max",
            "httpsourcehandler.meter.requestParseTime.min",
            "httpsourcehandler.meter.requestParseTime.95ThPercentile",
            "httpsourcehandler.meter.requestParseTime.99ThPercentile",
            "httpsourcehandler.meter.requestParseTime.stddev",

            "httpsourcehandler.meter.eventGenerationTime.mean",
            "httpsourcehandler.meter.eventGenerationTime.max",
            "httpsourcehandler.meter.eventGenerationTime.min",
            "httpsourcehandler.meter.eventGenerationTime.95ThPercentile",
            "httpsourcehandler.meter.eventGenerationTime.99ThPercentile",
            "httpsourcehandler.meter.eventGenerationTime.stddev",

            "httpsourcehandler.meter.eventSize.mean",
            "httpsourcehandler.meter.eventSize.max",
            "httpsourcehandler.meter.eventSize.min",
            "httpsourcehandler.meter.eventSize.95ThPercentile",
            "httpsourcehandler.meter.eventSize.99ThPercentile",
            "httpsourcehandler.meter.eventSize.stddev",

            "httpsourcehandler.meter.jsonsPerRequest.mean",
            "httpsourcehandler.meter.jsonsPerRequest.max",
            "httpsourcehandler.meter.jsonsPerRequest.min",
            "httpsourcehandler.meter.jsonsPerRequest.95ThPercentile",
            "httpsourcehandler.meter.jsonsPerRequest.99ThPercentile",
            "httpsourcehandler.meter.jsonsPerRequest.stddev"
    };


    /**
     * Default constructor.
     */
    public MetricsController() {
        final String domainName = MetricRegistry.name(MetricsController.class);

        metrics = new MetricRegistry();

        receivedJsons = metrics.meter("receivedJsons");
        jsonError = metrics.meter("jsonError");
        requestParseTime = metrics.timer("requestParseTime");
        eventGenerationTime = metrics.timer("eventGenerationTime");
        eventSize = metrics.histogram("eventSize");
        jsonsPerRequest = metrics.histogram("jsonsPerRequest");

        JmxReporter.forRegistry(metrics)
                    .inDomain(domainName)
                    .build().start();
    }

    /**
     * This method manages metric based on events received.
     * <p/>
     * For new metrics will need to create the corresponding event type in
     * MetricsEvent class and then define their behavior here
     *
     * @param event event to manage
     * @return
     * @see
     */
    public void manage(MetricsEvent event) {

        switch (event.getCode()) {
            case JSON_ARRIVED:
                receivedJsons.mark();
                break;
            case JSON_ERROR:
                jsonError.mark();
                break;
            case PARSE_OK:
                requestParseTime.update(event.getValue(), TimeUnit.NANOSECONDS);
                break;
            case EVENT_SIZE:
                eventSize.update(event.getValue());
                break;
            case EVENT_GENERATION:
                eventGenerationTime.update(event.getValue(), TimeUnit.NANOSECONDS);
                break;
            case NJSONS_ARRIVED:
                jsonsPerRequest.update(event.getValue());
                break;
            default:
                logger.warn("EventType '"+event.getCode()+"' not recognized");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long receivedJsonsCount() {
        return receivedJsons.getCount();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double receivedJsonsMeanRate() {
        return receivedJsons.getMeanRate();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double receivedJsonsOneMinuteRate() {
        return receivedJsons.getOneMinuteRate();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double receivedJsonsFifteenMinuteRate() {
        return receivedJsons.getFifteenMinuteRate();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public long jsonErrorCount() {
        return jsonError.getCount();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double jsonErrorMeanRate() {
        return jsonError.getMeanRate();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double jsonErrorOneMinuteRate() {
        return jsonError.getOneMinuteRate();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double jsonErrorFifteenMinuteRate() {
        return jsonError.getFifteenMinuteRate();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double requestParseTimeMean() {
        return requestParseTime.getSnapshot().getMean();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long requestParseTimeMax() {
        return requestParseTime.getSnapshot().getMax();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long requestParseTimeMin() {
        return requestParseTime.getSnapshot().getMin();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double requestParseTime95ThPercentile() {
        return requestParseTime.getSnapshot().get95thPercentile();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double requestParseTime99ThPercentile() {
        return requestParseTime.getSnapshot().get99thPercentile();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double requestParseTimeStdDev() {
        return requestParseTime.getSnapshot().getStdDev();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double eventGenerationTimeMean() {
        return eventGenerationTime.getSnapshot().getMean();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long eventGenerationTimeMax() {
        return eventGenerationTime.getSnapshot().getMax();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long eventGenerationTimeMin() {
        return eventGenerationTime.getSnapshot().getMin();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double eventGenerationTime95ThPercentile() {
        return eventGenerationTime.getSnapshot().get95thPercentile();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double eventGenerationTime99ThPercentile() {
        return eventGenerationTime.getSnapshot().get99thPercentile();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double eventGenerationTimeStdDev() {
        return eventGenerationTime.getSnapshot().getStdDev();
    }

    @Override
    public double eventSizeMean() {
        return eventSize.getSnapshot().getMean();
    }

    @Override
    public long eventSizeMax() {
        return eventSize.getSnapshot().getMax();
    }

    @Override
    public long eventSizeMin() {
        return eventSize.getSnapshot().getMin();
    }

    @Override
    public double eventSize95ThPercentile() {
        return eventSize.getSnapshot().get95thPercentile();
    }

    @Override
    public double eventSize99ThPercentile() {
        return eventSize.getSnapshot().get99thPercentile();
    }

    @Override
    public double eventSizeStdDev() {
        return eventSize.getSnapshot().getStdDev();
    }

/***********************************************************************************************************************/
    @Override
    public double jsonsPerRequestMean() {
    return jsonsPerRequest.getSnapshot().getMean();
}

    @Override
    public long jsonsPerRequestMax() {
        return jsonsPerRequest.getSnapshot().getMax();
    }

    @Override
    public long jsonsPerRequestMin() {
        return jsonsPerRequest.getSnapshot().getMin();
    }

    @Override
    public double jsonsPerRequest95ThPercentile() {
        return jsonsPerRequest.getSnapshot().get95thPercentile();
    }

    @Override
    public double jsonsPerRequest99ThPercentile() {
        return jsonsPerRequest.getSnapshot().get99thPercentile();
    }

    @Override
    public double jsonsPerRequestStdDev() {
        return jsonsPerRequest.getSnapshot().getStdDev();
    }
}
