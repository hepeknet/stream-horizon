package com.threeglav.sh.bauk.dimension.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.sh.bauk.BaukEngineConfigurationConstants;
import com.threeglav.sh.bauk.ConfigurationProperties;
import com.threeglav.sh.bauk.util.StringUtil;
import com.zaxxer.hikari.IConnectionCustomizer;

public class BaukConnectionCustomizer implements IConnectionCustomizer {

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	private final String programName;
	private static List<Map<String, String>> supportedClientInfoProperties;

	public BaukConnectionCustomizer() {
		programName = ConfigurationProperties.getSystemProperty(BaukEngineConfigurationConstants.JDBC_CLIENT_INFO_PROGRAM_NAME_PARAM_NAME, null);
	}

	@Override
	public void customize(final Connection connection) throws SQLException {
		if (!StringUtil.isEmpty(programName)) {
			try {
				connection.setClientInfo("v$session.program", programName);
				connection.setClientInfo("ApplicationName ", programName);
			} catch (final Exception exc) {
				this.checkSupportedClientInfo(connection);
				log.warn(
						"Was not able to set {} to [{}]. Does your JDBC driver support JDBC 4.0? Client info properties supported by database are {}. Details: {}",
						"v$session.program", programName, supportedClientInfoProperties, exc.getMessage());
			}
		}
	}

	private void checkSupportedClientInfo(final Connection conn) {
		if (supportedClientInfoProperties != null) {
			return;
		}
		try {
			log.info("Trying to find out supported client info");
			final ResultSet rs = conn.getMetaData().getClientInfoProperties();
			supportedClientInfoProperties = new LinkedList<>();
			if (rs != null) {
				while (rs.next()) {
					final String name = rs.getString(1);
					final String length = rs.getString(2);
					final Map<String, String> ci = new HashMap<String, String>();
					ci.put("propertyName", name);
					ci.put("maxPropertyValueLength", length);
					supportedClientInfoProperties.add(ci);
				}
				if (log.isInfoEnabled()) {
					log.info("Supported client info properties for database are {}", supportedClientInfoProperties);
				}
				rs.close();
			}
		} catch (final Exception exc) {
			log.warn("Exception while checking for supported client info properties", exc);
			supportedClientInfoProperties = new LinkedList<>();
		}
	}

}