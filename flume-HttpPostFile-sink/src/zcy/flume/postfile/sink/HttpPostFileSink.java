package zcy.flume.postfile.sink;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
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

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

public class HttpPostFileSink extends AbstractSink implements Configurable {
	private static Logger LOG = LoggerFactory.getLogger(HttpPostFileSink.class);
	private String url;
	private String paramName;
	private int batchSize;
	private String postUrl;
	
	private Connection dbConn;
	private PreparedStatement preparedStatementHtml;
	private PreparedStatement preparedStatementPic;
	private String dbHost;
	private String dbPort;
	private String dbName;
	private String dbUser;
	private String dbPassword;

	public HttpPostFileSink() {
		LOG.info("HttpSink start....");
	}

	@Override
	public void configure(Context context) {
		batchSize = context.getInteger("batchSize", 100);
		Preconditions.checkArgument(batchSize > 0, "batchSize must be a positive number!");
		url = context.getString("url");
		paramName = context.getString("paramName");
		postUrl = url;
		
		dbHost = context.getString("dbHost");
		Preconditions.checkNotNull(dbHost, "hostname must be set!!");
		dbPort = context.getString("dbPort");
		Preconditions.checkNotNull(dbPort, "port must be set!!");
		dbName = context.getString("dbName");
		Preconditions.checkNotNull(dbName, "databaseName must be set!!");
		dbUser = context.getString("dbUser");
		Preconditions.checkNotNull(dbUser, "user must be set!!");
		dbPassword = context.getString("dbPassword");
		Preconditions.checkNotNull(dbPassword, "password must be set!!");
	}
	
	@Override
	public void start() {
		super.start();
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

		String url = "jdbc:postgresql://" + dbHost + ":" + dbPort + "/" + dbName;

		try {
			dbConn = DriverManager.getConnection(url, dbUser, dbPassword);
			dbConn.setAutoCommit(false);
			preparedStatementHtml = dbConn.prepareStatement("INSERT INTO spider_info.network_spider_html_bak(md5, htmlpwd) VALUES (?, ?);");
			preparedStatementPic = dbConn.prepareStatement("INSERT INTO spider_info.network_spider_pic_bak(md5, picpwd) VALUES (?, ?);");
		} catch (SQLException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	@Override
	public void stop() {
		super.stop();
		if (preparedStatementHtml != null) {
			try {
				preparedStatementHtml.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		if (preparedStatementPic != null) {
			try {
				preparedStatementPic.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		if (dbConn != null) {
			try {
				dbConn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
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
					LOG.info("--------length: ", String.valueOf(content.length));
				} else {
					result = Status.BACKOFF;
					break;
				}
				HashMap<String, byte[]> hmFileContent = unZip(content);
				PreparedStatement preparedStatement = null;
				for(Entry<String, byte[]> entry : hmFileContent.entrySet()) {
					String key = entry.getKey();
					String sDomainMD5 = key.split("/")[0];
					byte[] value = entry.getValue();
					JSONObject object = postFile(postUrl, paramName, value);
					int statusCode = object.getIntValue("statusCode");
					LOG.info("statusCode: ", String.valueOf(statusCode));
					if(statusCode!=200) {
						transaction.rollback();
						return Status.BACKOFF;
					}
					String s = object.getString("retString");
					JSONArray ret = (JSONArray) JSONObject.parse(s);
					JSONObject o = (JSONObject) ret.get(0);
					String sFnMd5 = o.getString("fn_md5");
					
					if(key.endsWith(".txt")) {
						preparedStatement = preparedStatementHtml;
					}else {
						preparedStatement = preparedStatementPic;
					}
					preparedStatement.setString(1, sDomainMD5);
					preparedStatement.setString(2, sFnMd5);
					preparedStatement.addBatch();
				}
				preparedStatementHtml.executeBatch();
				preparedStatementPic.executeBatch();
			}
			dbConn.commit();
			transaction.commit();
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
				System.out.println(fname);
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
	private JSONObject postFile(String url, String paramName, byte[] contents) {
		int statusCode = 200;
		JSONObject object = new JSONObject();
		object.put("statusCode", statusCode);
		
		try {
            URL url1 = new URL(url);
            HttpURLConnection conn = (HttpURLConnection) url1.openConnection();
            conn.setRequestProperty("Connection", "Keep-Alive");
            DataOutputStream out = null;
            String BOUNDARY = "---------------------------" + System.currentTimeMillis();
            String contentType = "application/octet-stream";
            
            conn.setRequestProperty("connection", "Keep-Alive");
            
            conn.setDoOutput(true);
            conn.setDoInput(true);
            conn.setUseCaches(false);
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "multipart/form-data; boundary=" + BOUNDARY);
            out = new DataOutputStream(conn.getOutputStream());
         
            StringBuffer strBuf = new StringBuffer();
            strBuf.append("\r\n").append("--").append(BOUNDARY).append("\r\n");
            strBuf.append("Content-Disposition: form-data; name=\"" + paramName + "\"; filename=\"" + paramName + "\"\r\n");
            strBuf.append("Content-Type:" + contentType + "\r\n\r\n");
            out.write(strBuf.toString().getBytes());
            out.write(contents);
            byte[] endData = ("\r\n--" + BOUNDARY + "--\r\n").getBytes();
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
		
		return object;
	}
}