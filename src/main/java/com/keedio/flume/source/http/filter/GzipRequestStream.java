package com.keedio.flume.source.http.filter;

import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

/**
 * Created by Luca Rosellini <lrosellini@keedio.com> on 28/5/15.
 */
public class GzipRequestStream extends ServletInputStream {
    private HttpServletRequest request = null;
    private ServletInputStream inStream = null;
    private GZIPInputStream in = null;

    public GzipRequestStream(HttpServletRequest request) throws IOException {
        this.request = request;
        this.inStream = request.getInputStream();
        this.in = new GZIPInputStream(this.inStream);
    }

    public int read() throws IOException {
        return this.in.read();
    }

    public int read(byte[] b) throws IOException {
        return this.in.read(b);
    }

    public int read(byte[] b, int off, int len) throws IOException {
        return this.in.read(b, off, len);
    }

    public void close() throws IOException {
        this.in.close();
    }
}
