package com.keedio.flume.source.http.filter;


import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by Luca Rosellini <lrosellini@keedio.com> on 28/5/15.
 */
public class GzipRequestWrapper extends HttpServletRequestWrapper {
    private HttpServletRequest origRequest = null;
    private ServletInputStream inStream = null;
    private BufferedReader reader = null;

    public GzipRequestWrapper(HttpServletRequest req) throws IOException {
        super(req);
        this.inStream = new GzipRequestStream(req);
        this.reader = new BufferedReader(new InputStreamReader(this.inStream));
    }

    public ServletInputStream getInputStream() throws IOException {
        return this.inStream;
    }

    public BufferedReader getReader() throws IOException {
        return this.reader;
    }
}
