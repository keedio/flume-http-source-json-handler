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
import org.json4s.JsonAST;
import org.json4s.StringInput;
import org.json4s.jackson.JsonMethods$;
import org.keedio.kafka.serializers.JValueEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import java.io.*;
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
    private JValueEncoder encoder = new JValueEncoder();

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

        charset = validateCharset(charset);
        long t0 = System.nanoTime();
        MappingIterator<Map<String, Object>> eventList = getMappingIterator(reader);

        Map<String, String> httpHeaders = extractHTTPHeaders(request);

        List<Event> result = new ArrayList<>();
        int nJsons = 0;
        while (eventList.hasNext()){
            Map<String, Object> event = parseNextEvent(eventList);

            String asString = mapper.writeValueAsString(event);

            LOG.trace(asString);

            JsonAST.JValue jval = JsonMethods$.MODULE$.parse(new StringInput(asString),false);

            result.add(EventBuilder.withBody(encoder.toBytes(jval), httpHeaders));
            metricsController.manage(new MetricsEvent(EVENT_SIZE, asString.length()));
            nJsons++;
        }

        metricsController.manage(new MetricsEvent(NJSONS_ARRIVED,nJsons));

        long t1 = System.nanoTime();
        metricsController.manage(new MetricsEvent(EVENT_GENERATION, t1-t0));

        return result;
    }

    /**
     * Parses a new event extracted from the HTTP request.
     *
     * @param eventList the iterator of parsed JSONs extracted from the HTTP request.
     *
     * @return the parsed event.
     */
    private Map<String, Object> parseNextEvent(MappingIterator<Map<String, Object>> eventList) {
        Map<String, Object> event = null;
        try {

            long t0 = System.nanoTime();
            event = eventList.next();
            long t1 = System.nanoTime();

            metricsController.manage(new MetricsEvent(PARSE_OK, t1-t0));
        } catch (Exception ex) {
            metricsController.manage(new MetricsEvent(JSON_ERROR));
            throw ex;
        }
        return event;
    }

    /**
     * Extracts HTTP headers for later usage.
     *
     * @param request the http servlet request.
     * @return the parsed HTTP header map
     */
    private Map<String, String> extractHTTPHeaders(HttpServletRequest request) {
        Map<String, String> httpHeaders = new HashMap<>();

        Enumeration<String> headerNames = request.getHeaderNames();

        while (headerNames.hasMoreElements()) {
            String hName = headerNames.nextElement();
            httpHeaders.put(hName, request.getHeader(hName));
        }
        return httpHeaders;
    }

    /**
     * Returns an iterator over the jsons contained in the request.
     *
     * @param reader the buffered reader extracted from the HTTP servlet request.
     *
     * @return an iterator over the parsed JSONs.
     * @throws IOException
     */
    private MappingIterator<Map<String, Object>> getMappingIterator(Reader reader) throws IOException {
        JsonParser jsonParser = jsonFactory.createJsonParser(reader);
        MappingIterator<Map<String,Object>> eventList;
        try {
            eventList = mapper.readValues(jsonParser, new TypeReference<TreeMap<String,Object>>(){});

        } catch (IOException e) {
            metricsController.manage(new MetricsEvent(JSON_ERROR));
            throw e;
        }
        return eventList;
    }

    /**
     * Validates the provided charset is supported.
     *
     * @param charset the charset to validate.
     * @return the normilized charset
     * @throws UnsupportedCharsetException if the provided charset could not be used.
     */
    private String validateCharset(String charset) {
        if (charset == null) {
            LOG.debug("Charset is null, default charset of UTF-8 will be used.");

            charset = "UTF-8";
        } else if (!("utf-8".equalsIgnoreCase(charset)
                || "utf-16".equalsIgnoreCase(charset)
                || "utf-32".equalsIgnoreCase(charset))) {

            LOG.error("Unsupported character set in request {}. JSON handler supports UTF-8, UTF-16 and UTF-32 only.",
                    charset);

            metricsController.manage(new MetricsEvent(JSON_ERROR));
            throw new UnsupportedCharsetException("JSON handler supports UTF-8, UTF-16 and UTF-32 only.");
        }
        return charset;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(Context context) {
        mapper = new ObjectMapper();
        jsonFactory = new JsonFactory();
    }

    /**
     * Default constructor.
     */
    public KeedioJSONHandler() {
        metricsController = new MetricsController();
    }
}
