package com.keedio.flume.source.http.filter;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

/**
 * Created by Luca Rosellini <lrosellini@keedio.com> on 28/5/15.
 */
public class GzipResponseStream extends ServletOutputStream {
    private HttpServletResponse response = null;
    private ServletOutputStream outStream;
    private GZIPOutputStream out;

    public GzipResponseStream(HttpServletResponse response) throws IOException {
        this.response = response;
        this.outStream = response.getOutputStream();
        this.out = new GZIPOutputStream(this.outStream);
        response.addHeader("Content-Encoding", "gzip");
    }

    public void write(int b) throws IOException {
        this.out.write(b);
    }

    public void write(byte[] b) throws IOException {
        this.out.write(b);
    }

    public void write(byte[] b, int off, int len) throws IOException {
        this.out.write(b, off, len);
    }

    public void close() throws IOException {
        this.finish();
        this.out.close();
    }

    public void flush() throws IOException {
        this.out.flush();
    }

    public void finish() throws IOException {
        this.out.finish();
    }
}
