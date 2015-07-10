package com.keedio.flume.source.http.filter;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Created by Luca Rosellini <lrosellini@keedio.com> on 28/5/15.
 */
public class GzipResponseWrapper extends HttpServletResponseWrapper {
    private HttpServletResponse response = null;
    private GzipResponseStream outStream = null;
    private PrintWriter writer = null;

    public GzipResponseWrapper(HttpServletResponse response) {
        super(response);
        this.response = response;
    }

    public ServletOutputStream getOutputStream() throws IOException {
        if(this.outStream == null) {
            this.outStream = new GzipResponseStream(this.response);
        }

        return this.outStream;
    }

    public PrintWriter getWriter() throws IOException {
        if(this.writer == null) {
            this.writer = new PrintWriter(this.getOutputStream());
        }

        return this.writer;
    }
}
