package zcy.flume.httpsource.handler.postfile;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.List;
import javax.servlet.MultipartConfigElement;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.Part;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.http.HTTPBadRequestException;
import org.apache.flume.source.http.HTTPSourceHandler;
import org.eclipse.jetty.server.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpSourcePostFileHanlder implements HTTPSourceHandler {
	private static Logger LOG = LoggerFactory.getLogger(HttpSourcePostFileHanlder.class);
	private static final MultipartConfigElement MULTI_PART_CONFIG = new MultipartConfigElement(System.getProperty("java.io.tmpdir"));
	@Override
	public void configure(Context context) {
		
	}

	@Override
	public List<Event> getEvents(HttpServletRequest request) throws HTTPBadRequestException, Exception {
		request.setAttribute(Request.__MULTIPART_CONFIG_ELEMENT, MULTI_PART_CONFIG);
		List<Event> events = null;
		String contentType = request.getContentType();
		LOG.info(contentType);
		
		if(contentType != null && contentType.startsWith("multipart/")) {
			Collection<Part> parts = request.getParts();
			events = new ArrayList<Event>(parts.size());
			for (Part part : parts) {
			    int l = (int) part.getSize();
			    InputStream input = part.getInputStream();
			    
			    byte[] baFileContent = new byte[l];
			    input.read(baFileContent);
			    events.add(EventBuilder.withBody(baFileContent));
			}
		}else {
			Enumeration<String> e = request.getParameterNames();
			while(e.hasMoreElements()) {
				LOG.info("no multipart: ", e.nextElement());
			}
		}
		return events;
	}
}
