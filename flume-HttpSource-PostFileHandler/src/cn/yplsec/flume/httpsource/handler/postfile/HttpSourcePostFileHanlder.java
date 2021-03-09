package cn.yplsec.flume.httpsource.handler.postfile;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.http.HTTPBadRequestException;
import org.apache.flume.source.http.HTTPSourceHandler;
import org.eclipse.jetty.server.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.MultipartConfigElement;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.Part;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class HttpSourcePostFileHanlder implements HTTPSourceHandler {
	private static Logger LOG = LoggerFactory.getLogger(HttpSourcePostFileHanlder.class);
	private static final MultipartConfigElement MULTI_PART_CONFIG = new MultipartConfigElement(System.getProperty("java.io.tmpdir"));
	//multipart中单个文件的最大值限制
	private Integer limitFilesizeInMultipart;

	public void configure(Context context) {
		//单文件最大限制，默认30M
		limitFilesizeInMultipart = context.getInteger("limitFilesizeInMultipart", 30*1024*1024);
		LOG.info(String.format("limitFilesizeInMultipart = %s", limitFilesizeInMultipart));
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
		List<Event> events = new ArrayList<Event>(1);
		String contentType = request.getContentType();

		if(contentType != null && contentType.startsWith("multipart/")) {
			//若为表单上传，则依次处理文件
			Collection<Part> parts = request.getParts();
			LOG.info(String.format("multipart num: %s", parts.size()));
			int bufferSize = 4096;
			for (Part part : parts) {
			    int filesize = (int) part.getSize();
			    if(filesize > limitFilesizeInMultipart) {
			    	LOG.warn(String.format("file too large , ignore it, part name = %s, size = %s", part.getName(), filesize));
			    	continue;
			    }
			    InputStream input = part.getInputStream();

				ByteArrayOutputStream out = new ByteArrayOutputStream(filesize);
                byte[] t = new byte[bufferSize];
                int n;
                //若没有读到，即读取到末尾，则返回-1
                while((n=input.read(t,0,bufferSize))!=-1)
                {
                    //这就把读取到的n个字节全部都写入到指定路径了
                    out.write(t,0,n);
                }
                byte[] fileContent = out.toByteArray();
                String sMd5OfPart = md5(fileContent);
                LOG.info(String.format("fn = %s, size = %s, md5 = %s", part.getSubmittedFileName(), filesize, sMd5OfPart));
                //todo: some header, fn, size, md5, datetime, batchno
			    events.add(EventBuilder.withBody(fileContent));
			}
		}else {
			LOG.warn(String.format("abnormal request, host = %s, contentType = %s, url = %s", request.getRemoteHost(), contentType, request.getRequestURI()));
		}
		return events;
	}
}
