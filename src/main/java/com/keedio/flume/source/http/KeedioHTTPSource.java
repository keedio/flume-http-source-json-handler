package com.keedio.flume.source.http;

import com.keedio.flume.source.http.filter.GZipFilter;
import org.apache.flume.source.http.HTTPSource;
import org.mortbay.jetty.servlet.Context;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Created by Luca Rosellini <lrosellini@keedio.com> on 28/5/15.
 */
public class KeedioHTTPSource extends HTTPSource{
    @Override
    protected void customizeServletContext(Context context) {
        super.customizeServletContext(context);
        context.addFilter(GZipFilter.class,"/*",0);
    }

    @Override
    protected HttpServlet getServlet() {
        return new KeedioFlumeHTTPServlet();
    }

    protected class KeedioFlumeHTTPServlet extends FlumeHTTPServlet{
        @Override
        protected void customizeServletResponse(HttpServletRequest request, HttpServletResponse response) {
            super.customizeServletResponse(request, response);

            response.addHeader("Accept-Encoding","gzip");
        }
    }
}
