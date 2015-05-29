package com.keedio.flume.source.http.filter;


import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Created by Luca Rosellini <lrosellini@keedio.com> on 28/5/15.
 */
public class GZipFilter implements Filter {
    @Override
    public void init(FilterConfig filterConfig) throws ServletException {

    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        HttpServletRequest req;
        String acceptEncoding;
        if (request instanceof HttpServletRequest) {
            req = (HttpServletRequest) request;
            acceptEncoding = req.getHeader("Content-Encoding");
            if (acceptEncoding != null && acceptEncoding.toLowerCase().indexOf("gzip") > -1) {
                request = new GzipRequestWrapper((HttpServletRequest) request);
            }
        }

        if (response instanceof HttpServletResponse) {
            req = (HttpServletRequest) request;
            acceptEncoding = req.getHeader("Accept-Encoding");
            if (acceptEncoding != null && acceptEncoding.toLowerCase().indexOf("gzip") > -1) {
                response = new GzipResponseWrapper((HttpServletResponse) response);
            }
        }

        chain.doFilter((ServletRequest) request, (ServletResponse) response);
        if (response instanceof GzipResponseWrapper) {
            ((GzipResponseStream) ((ServletResponse) response).getOutputStream()).finish();
        }
    }

    @Override
    public void destroy() {

    }
}
