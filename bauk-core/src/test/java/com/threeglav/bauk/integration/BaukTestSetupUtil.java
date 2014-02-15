package com.threeglav.bauk.integration;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.io.FileUtils;

import com.threeglav.bauk.BaukEngineConfigurationConstants;
import com.threeglav.bauk.main.BaukApplication;

public class BaukTestSetupUtil {

	private static final String JDBC_URL = "jdbc:h2:mem:bauk_test;DB_CLOSE_DELAY=-1";

	private File tempInputDir;
	private File tempOutDir;
	private File archiveDir;
	private final ExecutorService execService = Executors.newSingleThreadExecutor();

	public boolean setupTestEnvironment(final String configFileName) {
		try {
			System.setProperty(BaukApplication.CONFIG_FILE_PROP_NAME, configFileName);
			final Path tempInDir = Files.createTempDirectory("bauk_test_in");
			tempInputDir = tempInDir.toFile();
			tempInputDir.deleteOnExit();
			System.setProperty(BaukEngineConfigurationConstants.SOURCE_DIRECTORY_PARAM_NAME, tempInputDir.getAbsolutePath());
			final Path tempOutDirP = Files.createTempDirectory("bauk_test_out");
			tempOutDir = tempOutDirP.toFile();
			tempOutDir.deleteOnExit();
			System.setProperty(BaukEngineConfigurationConstants.OUTPUT_DIRECTORY_PARAM_NAME, tempOutDir.getAbsolutePath());
			final Path tempArchiveDir = Files.createTempDirectory("bauk_test_archive");
			archiveDir = tempArchiveDir.toFile();
			archiveDir.deleteOnExit();
			System.setProperty(BaukEngineConfigurationConstants.ARCHIVE_DIRECTORY_PARAM_NAME, archiveDir.getAbsolutePath());
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
		}
		return false;
	}

	public void startBaukInstance() throws Exception {
		execService.submit(new Callable<Boolean>() {
			@Override
			public Boolean call() {
				try {
					BaukApplication.main(null);
					return Boolean.TRUE;
				} catch (final Exception exc) {
					exc.printStackTrace();
					return Boolean.FALSE;
				}
			}
		});
		Thread.sleep(5000);
	}

	public void createInputFile(final String[] lines) throws Exception {
		final List<String> linesColl = new LinkedList<>();
		for (final String l : lines) {
			linesColl.add(l);
		}
		final Path inputFileP = Files.createTempFile(tempInputDir.toPath(), "input", "bauk_test");
		final File inputFile = inputFileP.toFile();
		inputFile.deleteOnExit();
		FileUtils.writeLines(inputFile, linesColl);
	}

	public void stopBaukInstance() throws Exception {
		BaukApplication.shutdown();
		execService.shutdown();
	}

	private static void insertInitialData() throws Exception {
		final Connection conn = getConnection();
		final Statement stat = conn.createStatement();
		stat.execute("insert into TEST_DIM (id,a,b) values (1,'a1','b1')");
		stat.execute("insert into TEST_DIM (id,a,b) values (2,'a2','b2')");
		stat.execute("insert into TEST_DIM (id,a,b) values (3,'a3','b3')");
		stat.close();
		conn.close();
	}

	public static Collection<Map<String, String>> getDataFromFactTable() throws Exception {
		final List<Map<String, String>> results = new LinkedList<>();
		final Connection conn = getConnection();
		final Statement stat = conn.createStatement();
		stat.execute("select f1, f2, f3, f4 from TEST_FACT order by f1");
		final ResultSet rs = stat.getResultSet();
		if (rs.next()) {
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

	private static void dropAndCreateTables() throws Exception {
		final Connection conn = getConnection();
		final Statement stat = conn.createStatement();
		try {
			stat.execute("drop table TEST_DIM");
		} catch (final Exception ignored) {
		}
		try {
			stat.execute("drop table TEST_FACT");
		} catch (final Exception ignored) {
		}
		stat.execute("create table TEST_DIM (id INTEGER NOT NULL AUTO_INCREMENT, a VARCHAR(100),b VARCHAR(100))");
		stat.execute("create table TEST_FACT (f1 INTEGER,f2 INTEGER,f3 INTEGER, f4 VARCHAR(100))");
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
