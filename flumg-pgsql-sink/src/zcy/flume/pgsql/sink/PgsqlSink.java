package zcy.flume.pgsql.sink;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class PgsqlSink extends AbstractSink implements Configurable {
	private Logger LOG = LoggerFactory.getLogger(PgsqlSink.class);
	private Connection dbConn;
	private PreparedStatement preparedStatement;
	private ResultSetMetaData resultSetMetaData;
	private String dbHost;
	private String dbPort;
	private String dbName;
	private String dbUser;
	private String dbPassword;
	private int batchSize;
	private String insertSql;
	private String fieldType;
	private List<String> lfieldType = null;
	private String sepChar;
	
	public PgsqlSink() {
		LOG.info("PgsqlSink start...");
	}

	public void configure(Context context) {
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
		
		batchSize = context.getInteger("batchSize", 100);
		Preconditions.checkNotNull(batchSize > 0, "batchSize must be a positive number!!");
		insertSql = context.getString("insertSql");
		Preconditions.checkNotNull(insertSql, "insertSql must be set!!");
		fieldType = context.getString("fieldType");
		Preconditions.checkNotNull(fieldType, "fieldType must be set!!");
		for (String s : fieldType.split(",")) {
			lfieldType.add(s.trim());
		}
		sepChar = context.getString("sepChar");
		Preconditions.checkNotNull(sepChar, "sepChar must be set!!");
		sepChar = sepChar.replace("[tab]", "\t");
		sepChar = sepChar.replace("[space]", " ");
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
			// 鍒涘缓涓�涓猄tatement瀵硅薄
			//preparedStatement = dbConn.prepareStatement("insert into " + dbSqlInsert + " (content) values (?)");
			preparedStatement = dbConn.prepareStatement(insertSql);
			resultSetMetaData = preparedStatement.getMetaData();
		} catch (SQLException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	@Override
	public void stop() {
		super.stop();
		if (preparedStatement != null) {
			try {
				preparedStatement.close();
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

	public Status process() throws EventDeliveryException {
		Status result = Status.READY;
		Channel channel = getChannel();
		Transaction transaction = channel.getTransaction();
		Event event;
		String content;

		List<List<String>> actions = Lists.newArrayList();
		transaction.begin();
		try {
			for (int i = 0; i < batchSize; i++) {
				event = channel.take();
				if (event != null) {
					content = new String(event.getBody());
					String[] ls = content.split(sepChar);
					System.out.println(String.format("line = %s, sep = '%s', split size = %s", content, sepChar, ls.length));
					actions.add(Arrays.asList(ls));
				} else {
					result = Status.BACKOFF;
					break;
				}
			}

			if (actions.size() > 0) {
				//preparedStatement.clearBatch();
				for (List<String> temp : actions) {
					System.out.println(String.format("actions size = %s, temp size = %s", actions.size(), temp.size()));
					for(int i=0;i<temp.size();i++) {
						System.out.println(String.format("i = %s, value = %s", i, temp.get(i)));
						//resultSetMetaData.getColumnType(column)
						preparedStatement.setString(i+1, temp.get(i));
						
					}
					preparedStatement.addBatch();
				}
				preparedStatement.executeBatch();

				dbConn.commit();
			}
			transaction.commit();
		} catch (Throwable e) {
			try {
				transaction.rollback();
			} catch (Exception e2) {
				LOG.error("Exception in rollback. Rollback might not have been" + "successful.", e2);
			}
			LOG.error("Failed to commit transaction." + "Transaction rolled back.", e);
			Throwables.propagate(e);
		} finally {
			transaction.close();
		}

		return result;
	}
	
	public static void main(String[]a) throws Exception {
		try {
			// 璋冪敤Class.forName()鏂规硶鍔犺浇椹卞姩绋嬪簭
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

		String url = "jdbc:postgresql://192.168.1.119:5432/anti_fraud";
		// 璋冪敤DriverManager瀵硅薄鐨刧etConnection()鏂规硶锛岃幏寰椾竴涓狢onnection瀵硅薄
		Connection dbConn = null;
		PreparedStatement preparedStatement = null;
		try {
			dbConn = DriverManager.getConnection(url, "dpi_user", "Yplsec.com");
			dbConn.setAutoCommit(false);
			preparedStatement = dbConn.prepareStatement("insert into zcy.zt_test(c1, c2) values(?, ?)");
		} catch (SQLException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}