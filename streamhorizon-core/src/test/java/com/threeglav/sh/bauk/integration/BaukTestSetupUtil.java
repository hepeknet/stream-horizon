package com.threeglav.sh.bauk.integration;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.io.FileUtils;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.threeglav.sh.bauk.BaukEngineConfigurationConstants;
import com.threeglav.sh.bauk.main.StreamHorizonEngine;
import com.threeglav.sh.bauk.model.FeedTarget;
import com.threeglav.sh.bauk.rpc.InputFeed;
import com.threeglav.sh.bauk.rpc.ProcessingResult;
import com.threeglav.sh.bauk.rpc.SHFeedProcessor;

public class BaukTestSetupUtil {

	private static final String JDBC_URL = "jdbc:h2:mem:bauk_test;DB_CLOSE_DELAY=-1";

	private File tempInputDir;
	private File tempOutDir;
	private File archiveDir;
	private File errorDir;
	private final ExecutorService execService = Executors.newSingleThreadExecutor();

	public boolean setupTestEnvironment(final String configFileName) {
		try {
			System.setProperty(StreamHorizonEngine.CONFIG_FILE_PROP_NAME, configFileName);
			System.setProperty(BaukEngineConfigurationConstants.BAUK_INSTANCE_ID_PARAM_NAME, "0");
			System.setProperty(BaukEngineConfigurationConstants.MULTI_INSTANCE_PARTITION_COUNT_PARAM_NAME, "1");
			System.setProperty(BaukEngineConfigurationConstants.FEED_PROCESSING_THREADS_PARAM_NAME, "3");

			final Path tempInDir = Files.createTempDirectory("bauk_test_in");
			tempInputDir = tempInDir.toFile();
			tempInputDir.deleteOnExit();
			System.setProperty(BaukEngineConfigurationConstants.SOURCE_DIRECTORY_PARAM_NAME, tempInputDir.getAbsolutePath());

			final Path tempOutDirP = Files.createTempDirectory("bauk_test_out");
			tempOutDir = tempOutDirP.toFile();
			tempOutDir.deleteOnExit();
			System.setProperty(FeedTarget.FILE_TARGET_DIRECTORY_PROP_NAME, tempOutDir.getAbsolutePath());

			final Path tempArchiveDir = Files.createTempDirectory("bauk_test_archive");
			archiveDir = tempArchiveDir.toFile();
			archiveDir.deleteOnExit();
			System.setProperty(BaukEngineConfigurationConstants.ARCHIVE_DIRECTORY_PARAM_NAME, archiveDir.getAbsolutePath());

			final Path tempErrorDir = Files.createTempDirectory("bauk_test_error");
			errorDir = tempErrorDir.toFile();
			errorDir.deleteOnExit();
			System.setProperty(BaukEngineConfigurationConstants.ERROR_DIRECTORY_PARAM_NAME, errorDir.getAbsolutePath());

			final Path baukHomeDirP = Files.createTempDirectory("bauk_home");
			final File baukHome = baukHomeDirP.toFile();
			baukHome.deleteOnExit();
			System.setProperty(BaukEngineConfigurationConstants.APP_HOME_SYS_PARAM_NAME, baukHome.getAbsolutePath());
			final Path baukDataDirP = Files.createTempDirectory(baukHomeDirP, "bauk_data");
			final File baukData = baukDataDirP.toFile();
			baukData.deleteOnExit();
			System.setProperty(BaukEngineConfigurationConstants.CONFIG_FOLDER_NAME, baukData.getAbsolutePath());
			dropAndCreateTables();
			insertInitialData();
			return true;
		} catch (final Exception exc) {
			exc.printStackTrace();
			return false;
		}
	}

	public void startBaukInstance() throws Exception {
		execService.submit(new Callable<Boolean>() {
			@Override
			public Boolean call() {
				try {
					StreamHorizonEngine.main(null);
					return Boolean.TRUE;
				} catch (final Exception exc) {
					exc.printStackTrace();
					return Boolean.FALSE;
				}
			}
		});
		Thread.sleep(5000);
	}

	public File createInputFile(final String[] lines) throws Exception {
		final List<String> linesColl = new LinkedList<>();
		for (final String l : lines) {
			linesColl.add(l);
		}
		final Path inputFileP = Files.createTempFile(tempInputDir.toPath(), "input", "bauk_test");
		final File inputFile = inputFileP.toFile();
		inputFile.deleteOnExit();
		FileUtils.writeLines(inputFile, linesColl);
		return inputFile;
	}

	public ProcessingResult sendInputDataOverRPC(final String[] lines, final int port) {
		TTransport transport;
		try {
			transport = new TFramedTransport(new TSocket("localhost", port));
			final TProtocol protocol = new TBinaryProtocol(transport);
			final SHFeedProcessor.Client client = new SHFeedProcessor.Client(protocol);
			transport.open();
			final ProcessingResult pr = client.processFeed(this.createFeed(lines));
			transport.close();
			return pr;
		} catch (final TTransportException e) {
			e.printStackTrace();
		} catch (final TException e) {
			e.printStackTrace();
		}
		return null;
	}

	private InputFeed createFeed(final String[] lines) {
		final InputFeed feed = new InputFeed();
		if (lines != null) {
			feed.setData(Arrays.asList(lines));
		}
		return feed;
	}

	public void stopBaukInstance() throws Exception {
		StreamHorizonEngine.shutdown();
		execService.shutdown();
	}

	private static void insertInitialData() throws Exception {
		final Connection conn = getConnection();
		final Statement stat = conn.createStatement();
		stat.execute("insert into TEST_DIM (id,a,b) values (1,'a1','b1')");
		stat.execute("insert into TEST_DIM (id,a,b) values (2,'a2','b2')");
		stat.execute("insert into TEST_DIM (id,a,b) values (3,'a3','b3')");
		stat.execute("insert into BIG_TEST_DIM (id,a,b,c,d, num_updates) values (1,'a11','b11','c11','d11', 0)");
		stat.execute("insert into BIG_TEST_DIM (id,a,b,c,d, num_updates) values (2,'a22','b22','c22','d22', 0)");
		conn.commit();
		stat.close();
		conn.close();
	}

	public void populateT2Dimension() throws Exception {
		final Connection conn = getConnection();
		final Statement stat = conn.createStatement();
		stat.execute("ALTER TABLE T2_TEST_DIM ALTER COLUMN id RESTART WITH 1");
		stat.execute("insert into T2_TEST_DIM (id,a,b,c,d, valid) values (1,'a11','b11','c11','d11', 'Y')");
		stat.execute("insert into T2_TEST_DIM (id,a,b,c,d, valid) values (2,'a11','b11','c11','d11', 'N')");
		stat.execute("insert into T2_TEST_DIM (id,a,b,c,d, valid) values (3,'a22','b22','c22','d22', 'Y')");
		conn.commit();
		stat.close();
		conn.close();
	}

	public void deleteDataFromBigDimension() throws Exception {
		final Connection conn = getConnection();
		final Statement stat = conn.createStatement();
		stat.execute("delete from BIG_TEST_DIM");
		conn.commit();
		stat.close();
		conn.close();
	}

	public void deleteDataFromT2Dimension() throws Exception {
		final Connection conn = getConnection();
		final Statement stat = conn.createStatement();
		stat.execute("delete from T2_TEST_DIM");
		stat.execute("ALTER TABLE T2_TEST_DIM ALTER COLUMN id RESTART WITH 1");
		conn.commit();
		stat.close();
		conn.close();
	}

	public static Collection<Map<String, String>> getDataFromFactTable() throws Exception {
		final List<Map<String, String>> results = new LinkedList<>();
		final Connection conn = getConnection();
		final Statement stat = conn.createStatement();
		stat.execute("select f1, f2, f3, f4 from TEST_FACT order by f1, f2");
		final ResultSet rs = stat.getResultSet();
		while (rs.next()) {
			final Map<String, String> row = new HashMap<String, String>();
			row.put("f1", rs.getString(1));
			row.put("f2", rs.getString(2));
			row.put("f3", rs.getString(3));
			row.put("f4", rs.getString(4));
			results.add(row);
		}
		stat.close();
		conn.close();
		return results;
	}

	public static Collection<Map<String, String>> getDataFromBigDimension() throws Exception {
		final List<Map<String, String>> results = new LinkedList<>();
		final Connection conn = getConnection();
		final Statement stat = conn.createStatement();
		stat.execute("select id, a, b, c, d, num_updates from BIG_TEST_DIM order by id");
		final ResultSet rs = stat.getResultSet();
		while (rs.next()) {
			final Map<String, String> row = new HashMap<String, String>();
			row.put("id", rs.getString(1));
			row.put("a", rs.getString(2));
			row.put("b", rs.getString(3));
			row.put("c", rs.getString(4));
			row.put("d", rs.getString(5));
			row.put("num_updates", rs.getString(6));
			results.add(row);
		}
		stat.close();
		conn.close();
		return results;
	}

	public static Collection<Map<String, String>> getDataFromT2Dimension() throws Exception {
		final List<Map<String, String>> results = new LinkedList<>();
		final Connection conn = getConnection();
		final Statement stat = conn.createStatement();
		stat.execute("select id, a, b, c, d, valid from T2_TEST_DIM order by id");
		final ResultSet rs = stat.getResultSet();
		while (rs.next()) {
			final Map<String, String> row = new HashMap<String, String>();
			row.put("id", rs.getString(1));
			row.put("a", rs.getString(2));
			row.put("b", rs.getString(3));
			row.put("c", rs.getString(4));
			row.put("d", rs.getString(5));
			row.put("valid", rs.getString(6));
			results.add(row);
		}
		stat.close();
		conn.close();
		return results;
	}

	public static Collection<Map<String, String>> getDataFromFeedRecordTable() throws Exception {
		final List<Map<String, String>> results = new LinkedList<>();
		final Connection conn = getConnection();
		final Statement stat = conn.createStatement();
		stat.execute("select cnt, flag from FEED_REC order by cnt asc");
		final ResultSet rs = stat.getResultSet();
		while (rs.next()) {
			final Map<String, String> row = new HashMap<String, String>();
			row.put("cnt", rs.getString(1));
			row.put("flag", rs.getString(2));
			results.add(row);
		}
		stat.close();
		conn.close();
		return results;
	}

	public static Collection<Map<String, String>> getDataFromBulkRecordTable() throws Exception {
		final List<Map<String, String>> results = new LinkedList<>();
		final Connection conn = getConnection();
		final Statement stat = conn.createStatement();
		stat.execute("select cnt, filepath from BULK_LOAD_REC order by cnt asc");
		final ResultSet rs = stat.getResultSet();
		while (rs.next()) {
			final Map<String, String> row = new HashMap<String, String>();
			row.put("cnt", rs.getString(1));
			row.put("filepath", rs.getString(2));
			results.add(row);
		}
		stat.close();
		conn.close();
		return results;
	}

	public static void deleteDataFromTables() throws Exception {
		final Connection conn = getConnection();
		final Statement stat = conn.createStatement();
		stat.execute("delete from TEST_FACT");
		stat.execute("delete from FEED_REC");
		stat.execute("delete from BULK_LOAD_REC");
		stat.execute("delete from FEED_SOURCE");
		stat.close();
		conn.close();
	}

	public static void populateFeedSourceTable() throws Exception {
		final Connection conn = getConnection();
		final Statement stat = conn.createStatement();
		stat.execute("ALTER TABLE FEED_SOURCE ALTER COLUMN id RESTART WITH 1");
		stat.execute("insert into FEED_SOURCE (id,c1,c2,c3,c4) values (1,'10','100','a1','b1')");
		stat.execute("insert into FEED_SOURCE (id,c1,c2,c3,c4) values (2,'20','200','a1','b1')");
		stat.execute("insert into FEED_SOURCE (id,c1,c2,c3,c4) values (3,'30','300','a1','b1')");
		conn.commit();
		stat.close();
		conn.close();
	}

	private static void dropAndCreateTables() throws Exception {
		final Connection conn = getConnection();
		final Statement stat = conn.createStatement();
		try {
			stat.execute("drop table TEST_DIM");
			stat.execute("drop table BIG_TEST_DIM");
			stat.execute("drop table T2_TEST_DIM");
			stat.execute("drop table FEED_SOURCE");
		} catch (final Exception ignored) {
		}
		try {
			stat.execute("drop table TEST_FACT");
		} catch (final Exception ignored) {
		}
		stat.execute("create table TEST_DIM (id INTEGER NOT NULL AUTO_INCREMENT, a VARCHAR(100),b VARCHAR(100))");
		stat.execute("create table BIG_TEST_DIM (id INTEGER NOT NULL AUTO_INCREMENT, a VARCHAR(100),b VARCHAR(100),c VARCHAR(100),d VARCHAR(100), num_updates INTEGER)");
		stat.execute("create table T2_TEST_DIM (id INTEGER NOT NULL AUTO_INCREMENT, a VARCHAR(100),b VARCHAR(100),c VARCHAR(100),d VARCHAR(100), valid VARCHAR(1))");
		stat.execute("create table TEST_FACT (f1 INTEGER,f2 INTEGER,f3 INTEGER, f4 VARCHAR(100))");
		stat.execute("create table FEED_REC(cnt INTEGER, flag VARCHAR(10))");
		stat.execute("create table BULK_LOAD_REC(cnt INTEGER, filepath VARCHAR(200))");
		stat.execute("create table FEED_SOURCE(id INTEGER NOT NULL AUTO_INCREMENT, c1 VARCHAR(20), c2 VARCHAR(20), c3 VARCHAR(20), c4 VARCHAR(20))");
		stat.close();
		conn.close();
	}

	public static Connection getConnection() {
		try {
			Class.forName("org.h2.Driver");
			final Connection conn = DriverManager.getConnection(JDBC_URL);
			return conn;
		} catch (final Exception exc) {
			exc.printStackTrace();
			return null;
		}
	}

}
