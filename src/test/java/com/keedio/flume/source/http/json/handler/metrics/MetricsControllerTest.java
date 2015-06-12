package com.keedio.flume.source.http.json.handler.metrics;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.TimeUnit;

import static com.keedio.flume.source.http.json.handler.metrics.MetricsEvent.EventType.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 * Created by Luca Rosellini <lrosellini@keedio.com> on 26/5/15.
 */
public class MetricsControllerTest {
    private MetricsController controller;

    @Before
    public void setup(){
        controller = new MetricsController();

        controller.receivedJsons = mock(Meter.class);
        controller.jsonError = mock(Meter.class);
        controller.requestParseTime = mock(Timer.class);
        controller.eventGenerationTime = mock(Timer.class);
    }

    @Test
    public void testJsonArrivedEvent(){
        MetricsEvent event = new MetricsEvent(JSON_ARRIVED);
        controller.manage(event);
        verify(controller.receivedJsons, times(1)).mark();
    }

    @Test
    public void testJsonErrorEvent(){
        MetricsEvent event = new MetricsEvent(JSON_ERROR);
        controller.manage(event);
        verify(controller.jsonError, times(1)).mark();
    }
    @Test
    public void testParseOkEvent(){
        Long processTime = 234L;

        ArgumentCaptor<Long> valueCaptor = ArgumentCaptor.forClass(Long.class);

        MetricsEvent event = new MetricsEvent(PARSE_OK, processTime);
        controller.manage(event);
        verify(controller.requestParseTime, times(1)).update(valueCaptor.capture(), any(TimeUnit.class));

        assertEquals(processTime, valueCaptor.getValue());
    }
    @Test
    public void testEventGenerationTimeEvent(){
        Long processTime = 911L;

        ArgumentCaptor<Long> valueCaptor = ArgumentCaptor.forClass(Long.class);

        MetricsEvent event = new MetricsEvent(EVENT_GENERATION, processTime);
        controller.manage(event);
        verify(controller.eventGenerationTime, times(1)).update(valueCaptor.capture(), any(TimeUnit.class));

        assertEquals(processTime, valueCaptor.getValue());
    }

    @Test
    public void testInvalidEventType(){
        MetricsEvent event = new MetricsEvent(UNKNOWN);
        controller.manage(event);
        verify(controller.receivedJsons, times(0)).mark();
        verify(controller.jsonError, times(0)).mark();
        verify(controller.requestParseTime, times(0)).update(anyLong(), any(TimeUnit.class));
        verify(controller.eventGenerationTime, times(0)).update(anyLong(), any(TimeUnit.class));
    }

    @Test
    public void testGettersDelegation(){
        long receivedJsonsCount = 10;
        double receivedJsonsMeanRate = 10.1;
        double receivedJsonsOneMinRate = 10.2;
        double receivedJsonsFifteenMinRate = 10.3;

        long jsonErrorCount = 20;
        double jsonErrorMeanRate = 20.1;
        double jsonErrorOneMinRate = 20.2;
        double jsonErrorFifteenMinRate = 20.3;

        double requestParseTimeMean = 30.0;
        long requestParseTimeMax = 301;
        long requestParseTimeMin = 302;
        double requestParseTime95ThPercentile = 30.3;
        double requestParseTime99ThPercentile = 30.4;
        double requestParseTimeStdDev = 30.5;

        double eventGenerationTimeMean = 40.0;
        long eventGenerationTimeMax = 401;
        long eventGenerationTimeMin = 402;
        double eventGenerationTime95ThPercentile = 40.3;
        double eventGenerationTime99ThPercentile = 40.4;
        double eventGenerationTimeStdDev = 40.5;

        when(controller.receivedJsons.getCount()).thenReturn(receivedJsonsCount);
        when(controller.receivedJsons.getMeanRate()).thenReturn(receivedJsonsMeanRate);
        when(controller.receivedJsons.getOneMinuteRate()).thenReturn(receivedJsonsOneMinRate);
        when(controller.receivedJsons.getFifteenMinuteRate()).thenReturn(receivedJsonsFifteenMinRate);

        when(controller.jsonError.getCount()).thenReturn(jsonErrorCount);
        when(controller.jsonError.getMeanRate()).thenReturn(jsonErrorMeanRate);
        when(controller.jsonError.getOneMinuteRate()).thenReturn(jsonErrorOneMinRate);
        when(controller.jsonError.getFifteenMinuteRate()).thenReturn(jsonErrorFifteenMinRate);

        Snapshot rptSnapshot = mock(Snapshot.class);
        when(controller.requestParseTime.getSnapshot()).thenReturn(rptSnapshot);
        when(rptSnapshot.getMean()).thenReturn(requestParseTimeMean);
        when(rptSnapshot.getMax()).thenReturn(requestParseTimeMax);
        when(rptSnapshot.getMin()).thenReturn(requestParseTimeMin);
        when(rptSnapshot.get95thPercentile()).thenReturn(requestParseTime95ThPercentile);
        when(rptSnapshot.get99thPercentile()).thenReturn(requestParseTime99ThPercentile);
        when(rptSnapshot.getStdDev()).thenReturn(requestParseTimeStdDev);

        Snapshot egtSnapshot = mock(Snapshot.class);
        when(controller.eventGenerationTime.getSnapshot()).thenReturn(egtSnapshot);
        when(egtSnapshot.getMean()).thenReturn(eventGenerationTimeMean);
        when(egtSnapshot.getMax()).thenReturn(eventGenerationTimeMax);
        when(egtSnapshot.getMin()).thenReturn(eventGenerationTimeMin);
        when(egtSnapshot.get95thPercentile()).thenReturn(eventGenerationTime95ThPercentile);
        when(egtSnapshot.get99thPercentile()).thenReturn(eventGenerationTime99ThPercentile);
        when(egtSnapshot.getStdDev()).thenReturn(eventGenerationTimeStdDev);

        assertEquals(receivedJsonsCount, controller.receivedJsonsCount());
        assertEquals(receivedJsonsMeanRate, controller.receivedJsonsMeanRate(),0);
        assertEquals(receivedJsonsOneMinRate, controller.receivedJsonsOneMinuteRate(),0);
        assertEquals(receivedJsonsFifteenMinRate, controller.receivedJsonsFifteenMinuteRate(),0);

        assertEquals(jsonErrorCount, controller.jsonErrorCount());
        assertEquals(jsonErrorMeanRate, controller.jsonErrorMeanRate(),0);
        assertEquals(jsonErrorOneMinRate, controller.jsonErrorOneMinuteRate(),0);
        assertEquals(jsonErrorFifteenMinRate, controller.jsonErrorFifteenMinuteRate(),0);

        assertEquals(requestParseTimeMean, controller.requestParseTimeMean(),0);
        assertEquals(requestParseTimeMax, controller.requestParseTimeMax());
        assertEquals(requestParseTimeMin, controller.requestParseTimeMin());
        assertEquals(requestParseTime95ThPercentile, controller.requestParseTime95ThPercentile(),0);
        assertEquals(requestParseTime99ThPercentile, controller.requestParseTime99ThPercentile(),0);
        assertEquals(requestParseTimeStdDev, controller.requestParseTimeStdDev(),0);

        assertEquals(eventGenerationTimeMean, controller.eventGenerationTimeMean(),0);
        assertEquals(eventGenerationTimeMax, controller.eventGenerationTimeMax());
        assertEquals(eventGenerationTimeMin, controller.eventGenerationTimeMin());
        assertEquals(eventGenerationTime95ThPercentile, controller.eventGenerationTime95ThPercentile(),0);
        assertEquals(eventGenerationTime99ThPercentile, controller.eventGenerationTime99ThPercentile(),0);
        assertEquals(eventGenerationTimeStdDev, controller.eventGenerationTimeStdDev(),0);
    }
}
