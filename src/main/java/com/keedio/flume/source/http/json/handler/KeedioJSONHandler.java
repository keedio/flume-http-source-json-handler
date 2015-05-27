package com.keedio.flume.source.http.json.handler;

import com.keedio.flume.source.http.json.handler.metrics.MetricsController;
import com.keedio.flume.source.http.json.handler.metrics.MetricsEvent;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.http.HTTPSourceHandler;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.MappingIterator;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.UnsupportedCharsetException;
import java.util.*;

import static com.keedio.flume.source.http.json.handler.metrics.MetricsEvent.EventType.*;


/**
 * <p>
 * HTTP Source handler that parses the input request and converts incoming jsons to flume Events.
 * HTTP headers are propagated as flume Event headers downstream.
 * </p>
 * <p>
 *     More than one json is allowed per request. If N json arrive in one request, this handler will produce N
 *     flume events, each of them sharing the same set of headers.
 * </p>
 *
 * Created by Luca Rosellini <lrosellini@keedio.com> on 18/5/15.
 */
public class KeedioJSONHandler implements HTTPSourceHandler {
    private static final Logger LOG = LoggerFactory.getLogger(KeedioJSONHandler.class);
    private ObjectMapper mapper;

    private JsonFactory jsonFactory;
    MetricsController metricsController;

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Event> getEvents(HttpServletRequest request) throws IOException {
        BufferedReader reader = request.getReader();

        if (reader == null){
            throw new IllegalArgumentException("Reader obtained from HTTP servlet request cannot be null");
        }

        String charset = request.getCharacterEncoding();

        metricsController.manage(new MetricsEvent(JSON_ARRIVED));

        if (charset == null) {
            LOG.debug("Charset is null, default charset of UTF-8 will be used.");

            charset = "UTF-8";
        } else if (!(charset.equalsIgnoreCase("utf-8")
                || charset.equalsIgnoreCase("utf-16")
                || charset.equalsIgnoreCase("utf-32"))) {

            LOG.error("Unsupported character set in request {}. "
                    + "JSON handler supports UTF-8, "
                    + "UTF-16 and UTF-32 only.", charset);

            metricsController.manage(new MetricsEvent(JSON_ERROR));
            throw new UnsupportedCharsetException("JSON handler supports UTF-8, "
                    + "UTF-16 and UTF-32 only.");
        }

        JsonParser jsonParser = jsonFactory.createJsonParser(reader);

        MappingIterator<Map<String,Object>> eventList;
        try {
            eventList = mapper.readValues(jsonParser, new TypeReference<TreeMap<String,Object>>(){});
        } catch (IOException e) {
            metricsController.manage(new MetricsEvent(JSON_ERROR));
            throw e;
        }

        List<Event> result = new ArrayList<>();

        Map<String, String> httpHeaders = new HashMap<>();

        Enumeration<String> headerNames = request.getHeaderNames();

        while (headerNames.hasMoreElements()) {
            String hName = headerNames.nextElement();
            httpHeaders.put(hName, request.getHeader(hName));
        }

        while (eventList.hasNext()){
            Map<String, Object> event = null;
            try {

                long t0 = System.currentTimeMillis();
                event = eventList.next();
                long t1 = System.currentTimeMillis();

                metricsController.manage(new MetricsEvent(PARSE_OK, t1-t0));
            } catch (Exception ex) {
                metricsController.manage(new MetricsEvent(JSON_ERROR));
                throw ex;
            }

            long t0 = System.currentTimeMillis();
            String asString = mapper.writeValueAsString(event);
            long t1 = System.currentTimeMillis();

            result.add(EventBuilder.withBody(asString.getBytes(charset), httpHeaders));

            metricsController.manage(new MetricsEvent(EVENT_GENERATION, t1-t0));
        }

        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(Context context) {
        mapper = new ObjectMapper();
        jsonFactory = new JsonFactory();
        metricsController.start();
    }

    /**
     * Default constructor.
     */
    public KeedioJSONHandler() {
        metricsController = new MetricsController();
    }
}
