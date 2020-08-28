package zcy.flume.httpsource.handler.postfile;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.security.MessageDigest;
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
import org.apache.flume.source.SpoolDirectorySource;
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
	
	public static String md5(byte[] data) {
		try {
			MessageDigest m = MessageDigest.getInstance("MD5");
			m.update(data);
			byte s[] = m.digest();
			String result = "";
			for (int i = 0; i < s.length; i++) {
				result += Integer.toHexString((0x000000FF & s[i]) | 0xFFFFFF00).substring(6);
			}
			return result;
		} catch (Exception e) {
			e.printStackTrace();
		}

		return "";
	}

	@Override
	public List<Event> getEvents(HttpServletRequest request) throws HTTPBadRequestException, Exception {
		request.setAttribute(Request.__MULTIPART_CONFIG_ELEMENT, MULTI_PART_CONFIG);
		List<Event> events = null;
		String contentType = request.getContentType();
		LOG.debug(contentType);
		
		if(contentType != null && contentType.startsWith("multipart/")) {
			Collection<Part> parts = request.getParts();
			events = new ArrayList<Event>(parts.size());
			for (Part part : parts) {
			    int l = (int) part.getSize();
			    InputStream input = part.getInputStream();
			    
				ByteArrayOutputStream out = new ByteArrayOutputStream(l);
                byte[] doc = new byte[512];
                int n;
                //若没有读到，即读取到末尾，则返回-1
                while((n=input.read(doc,0,512))!=-1)
                {
                    //这就把读取到的n个字节全部都写入到指定路径了
                    out.write(doc,0,n);
                }
                byte[] bb = out.toByteArray();
                LOG.warn(md5(bb));
			    events.add(EventBuilder.withBody(bb));
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
