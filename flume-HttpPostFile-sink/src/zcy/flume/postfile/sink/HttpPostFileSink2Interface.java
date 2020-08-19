package zcy.flume.postfile.sink;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

public class HttpPostFileSink2Interface extends AbstractSink implements Configurable {
	private static Logger LOG = LoggerFactory.getLogger(HttpPostFileSink2Interface.class);
	private String url;
	private int batchSize;

	public HttpPostFileSink2Interface() {
		LOG.info("HttpSink start....");
	}

	@Override
	public void configure(Context context) {
		batchSize = context.getInteger("batchSize", 100);
		Preconditions.checkArgument(batchSize > 0, "batchSize must be a positive number!");
		url = context.getString("url");
		Preconditions.checkNotNull(url, "url must be set!!");
	}
	
	@Override
	public void start() {
		super.start();
	}
	
	@Override
	public void stop() {
		super.stop();
	}

	@Override
	public Status process() throws EventDeliveryException {
		Status result = Status.READY;
		Channel channel = getChannel();
		Transaction transaction = channel.getTransaction();
		transaction.begin();
		try {
			Event event = null;
			byte[] content = null;

			for (int i = 0; i < batchSize; i++) {
				event = channel.take();
				if (event != null) {
					content = event.getBody();
				} else {
					transaction.rollback();
					result = Status.BACKOFF;
					break;
				}
				HashMap<String, byte[]> hmFileContent = unZip(content);
				for(Entry<String, byte[]> entry : hmFileContent.entrySet()) {
					String key = entry.getKey();
					String[] ss = key.split("/");
					String sDomainMD5 = ss[0];
					String sFileName = ss[ss.length-1];
					byte[] value = entry.getValue();
					//String url, String sDomainMD5, String sFileName, byte[] contents
					JSONObject object = this.postFile(sDomainMD5, sFileName, value);
					int statusCode = object.getIntValue("statusCode");
					if(statusCode!=200) {
						transaction.rollback();
						return Status.BACKOFF;
					}
				}
			}
			if(result.equals(Status.READY)) {
				transaction.commit();
			}
		} catch (Exception e) {
			try {
				transaction.rollback();
			} catch (Exception e2) {
				LOG.error("Exception in rollback. Rollback might not have been" + "successful.", e2);
			}
			LOG.error("Failed to commit transaction." + "Transaction rolled back.", e);
			Throwables.propagate(e);
		} finally {
			if (null != transaction) {
				transaction.close();
				LOG.debug("close Transaction");
			}
		}
		return result;
	}
	
	public HashMap<String, byte[]> unZip(byte[] bs) {
		try {
			InputStream is = new ByteArrayInputStream(bs);
			ZipInputStream zipis = new ZipInputStream(is);
			ZipEntry entry;
			HashMap<String, byte[]> hmFileContent = new HashMap<String, byte[]>();
			while((entry = zipis.getNextEntry()) != null) {
				if(entry.isDirectory()) {
					continue;
				}
				String fname = entry.getName();
				
				long l = entry.getSize();
				byte[] content = new byte[(int) l];
				zipis.read(content);
				
				if(fname.endsWith(".json")) {
					continue;
				}
				hmFileContent.put(fname, content);
			}
			
			return hmFileContent;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	
	/**
	 * post 
	 * 
	 * @param url
	 * @param json
	 * @return
	 */
	private JSONObject postFile(String sDomainMD5, String sFileName, byte[] contents) {
		int statusCode = 200;
		JSONObject object = new JSONObject();
		
		try {
            URL url1 = new URL(url);
            HttpURLConnection conn = (HttpURLConnection) url1.openConnection();
            conn.setRequestProperty("Connection", "Keep-Alive");	//长连接
            DataOutputStream out = null;
            String BOUNDARY = "---------------------------" + System.currentTimeMillis();	//表单边界
            String contentType = "application/octet-stream";
            conn.setDoOutput(true);
            conn.setDoInput(true);
            conn.setUseCaches(false);
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "multipart/form-data; boundary=" + BOUNDARY);
            out = new DataOutputStream(conn.getOutputStream());		//输出流
         
            StringBuffer strBuf = new StringBuffer();	//输出内容缓存
            //第一节，文件信息
            strBuf.append("\r\n--").append(BOUNDARY).append("\r\n");
            strBuf.append("Content-Disposition: form-data; name=\"file\"; filename=\"" + sFileName + "\"\r\n");
            strBuf.append("Content-Type:" + contentType + "\r\n\r\n");
            out.write(strBuf.toString().getBytes());
            out.write(contents);
            out.write("\r\n".getBytes());
            
            //第二节，表单其他信息
            strBuf = new StringBuffer();
            strBuf.append(BOUNDARY).append("\r\n");
            strBuf.append("Content-Disposition: form-data; name=\"md5\"\r\n");
            strBuf.append("\r\n\r\n");
            strBuf.append(sDomainMD5).append("\r\n");
            
            //post结束边界
            byte[] endData = ("--" + BOUNDARY + "--\r\n").getBytes();
            out.write(endData);
            out.flush();
            
            InputStream is = conn.getInputStream();
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            String str = br.readLine();
            object.put("retString", str);
            is.close();
            LOG.debug(str);
            conn.disconnect();
        } catch (Exception e) {
            e.printStackTrace();
            statusCode = 500;
        }
		object.put("statusCode", statusCode);
		
		return object;
	}
}