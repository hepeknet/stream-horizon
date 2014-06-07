package com.threeglav.sh.bauk.dimension.db;

import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.support.TransactionTemplate;

import com.threeglav.sh.bauk.dimension.DimensionKeysPair;

public interface DbHandler {

	public Long executeQueryStatementAndReturnKey(String statement, final String dimensionName);

	public Long executeInsertStatementAndReturnKey(final String statement, String description);

	public int executeInsertOrUpdateStatement(final String statement, String description);

	public List<DimensionKeysPair> queryForDimensionKeys(final String dimensionName, final String statement, int expectedTotalValues,
			RowMapper<DimensionKeysPair> rowMapper);

	public Map<String, String> executeSingleRowSelectStatement(final String statement, String description);

	public TransactionTemplate getTransactionTemplate();

	public List<List<String>> selectAllRowsAsStrings(final String sqlStatement, final DataSource dataSource);

}
