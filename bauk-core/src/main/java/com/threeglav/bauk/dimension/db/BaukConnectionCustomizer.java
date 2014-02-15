package com.threeglav.bauk.dimension.db;

import java.sql.Connection;
import java.sql.SQLException;

import com.threeglav.bauk.BaukEngineConfigurationConstants;
import com.threeglav.bauk.ConfigurationProperties;
import com.zaxxer.hikari.IConnectionCustomizer;

public class BaukConnectionCustomizer implements IConnectionCustomizer {

	private String programName;

	public BaukConnectionCustomizer() {
	}

	@Override
	public void customize(final Connection connection) throws SQLException {
		if (programName != null) {
			connection.setClientInfo("v$session.program", programName);
		}
		programName = ConfigurationProperties.getSystemProperty(BaukEngineConfigurationConstants.JDBC_CLIENT_INFO_PROGRAM_NAME_PARAM_NAME, null);
	}

}