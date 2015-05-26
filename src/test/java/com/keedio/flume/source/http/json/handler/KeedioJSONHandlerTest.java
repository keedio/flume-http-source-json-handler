package com.keedio.flume.source.http.json.handler;

import com.google.common.base.Charsets;
import com.keedio.flume.source.http.json.handler.metrics.MetricsController;
import com.keedio.flume.source.http.json.handler.metrics.MetricsEvent;
import org.apache.commons.io.IOUtils;
import org.apache.flume.Event;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import javax.servlet.http.HttpServletRequest;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.UnsupportedCharsetException;
import java.util.List;
import java.util.Vector;

import static com.keedio.flume.source.http.json.handler.metrics.MetricsEvent.EventType.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Created by Luca Rosellini <lrosellini@keedio.com> on 25/5/15.
 */
public class KeedioJSONHandlerTest {

    private KeedioJSONHandler handler;
    private HttpServletRequest mockRequest;

    private BufferedReader reader;

    @Before
    public void setup() throws IOException {
        reader = new BufferedReader(new FileReader("src/main/resources/schema/example-widget.json"));

        handler = new KeedioJSONHandler();
        handler.metricsController = mock(MetricsController.class);

        handler.configure(null);

        mockRequest = mock(HttpServletRequest.class);
        stubServletRequest();
    }

    @After
    public void tearDown(){
        IOUtils.closeQuietly(reader);
    }

    private void stubServletRequest()throws IOException{
        when(mockRequest.getReader()).thenReturn(reader);

        Vector<String> httpHeaders = new Vector<>();
        httpHeaders.add("Accept-Encoding");
        httpHeaders.add("Accept");
        httpHeaders.add("User-Agent");

        when(mockRequest.getHeaderNames()).thenReturn(httpHeaders.elements());
        when(mockRequest.getHeader("Accept")).thenReturn("text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8");
        when(mockRequest.getHeader("Accept-Encoding")).thenReturn("gzip, deflate");
        when(mockRequest.getHeader("User-Agent")).thenReturn("Mozilla/5.0");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullReader() throws IOException {
        when(mockRequest.getReader()).thenReturn(null);

        handler.getEvents(mockRequest);

        verifyNoMoreInteractions(handler.metricsController);
    }

    @Test(expected = IOException.class)
    public void testInvalidReader() throws IOException {
        when(mockRequest.getReader()).thenThrow(IOException.class);

        handler.getEvents(mockRequest);

        verifyNoMoreInteractions(handler.metricsController);
    }

    @Test(expected = UnsupportedCharsetException.class)
    public void testInvalidCharset() throws IOException {
        when(mockRequest.getCharacterEncoding()).thenReturn(Charsets.ISO_8859_1.displayName());

        handler.getEvents(mockRequest);

        ArgumentCaptor<MetricsEvent> captor =  ArgumentCaptor.forClass(MetricsEvent.class);

        verify(handler.metricsController, times(1)).manage(captor.capture());
        verifyNoMoreInteractions(handler.metricsController);
        assertEquals(JSON_ERROR, captor.capture().getCode());
    }

    @Test(expected = RuntimeException.class)
    public void testInvalidJson() throws IOException {
        reader = new BufferedReader(new FileReader("src/main/resources/schema/example-widget-invalid.json"));

        assertNotNull(reader);

        stubServletRequest();

        handler.getEvents(mockRequest);

        ArgumentCaptor<MetricsEvent> captor =  ArgumentCaptor.forClass(MetricsEvent.class);

        verify(handler.metricsController, times(1)).manage(captor.capture());
        verifyNoMoreInteractions(handler.metricsController);
        assertEquals(JSON_ERROR, captor.capture().getCode());
    }

    @Test
    public void testSingleJson() throws IOException {
        reader = new BufferedReader(new FileReader("src/main/resources/schema/example-widget.json"));

        assertNotNull(reader);

        stubServletRequest();

        List<Event> result = handler.getEvents(mockRequest);
        assertNotNull(result);
        assertEquals(1,result.size());

        Event event = result.iterator().next();

        assertEquals(3, event.getHeaders().size());

        assertEquals("text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",event.getHeaders().get("Accept"));
        assertEquals("gzip, deflate",event.getHeaders().get("Accept-Encoding"));
        assertEquals("Mozilla/5.0",event.getHeaders().get("User-Agent"));

        ArgumentCaptor<MetricsEvent> captor =  ArgumentCaptor.forClass(MetricsEvent.class);
        verify(handler.metricsController, times(3)).manage(captor.capture());

        for (MetricsEvent e: captor.getAllValues()){
            assertTrue(e.getCode() == JSON_ARRIVED || e.getCode() == PARSE_OK | e.getCode() == EVENT_GENERATION);

            if (e.getCode() == EVENT_GENERATION || e.getCode() == PARSE_OK){
                assertNotNull(e.getValue());
                assertTrue(e.getValue() > 0);
            }
        }
    }

    @Test
    public void testMultipleJson() throws IOException {
        reader = new BufferedReader(new FileReader("src/main/resources/schema/example-widget-multiple.json"));

        assertNotNull(reader);

        stubServletRequest();

        List<Event> result = handler.getEvents(mockRequest);
        assertNotNull(result);
        assertEquals(2,result.size());

        for (Event event : result){
            assertEquals(3, event.getHeaders().size());

            assertEquals("text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    event.getHeaders().get("Accept"));
            assertEquals("gzip, deflate",event.getHeaders().get("Accept-Encoding"));
            assertEquals("Mozilla/5.0",event.getHeaders().get("User-Agent"));
        }

        ArgumentCaptor<MetricsEvent> captor =  ArgumentCaptor.forClass(MetricsEvent.class);
        verify(handler.metricsController, times(3+1)).manage(captor.capture());

        for (MetricsEvent e: captor.getAllValues()){
            assertTrue(e.getCode() == JSON_ARRIVED || e.getCode() == PARSE_OK | e.getCode() == EVENT_GENERATION);

            if (e.getCode() == EVENT_GENERATION || e.getCode() == PARSE_OK){
                assertNotNull(e.getValue());
                assertTrue(e.getValue() > 0);
            }
        }
    }

}
