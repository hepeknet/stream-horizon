package com.threeglav.sh.bauk.dimension.db;

import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.RowMapper;

import com.threeglav.sh.bauk.dimension.DimensionKeysPair;

public interface DbHandler {

	public Long executeQueryStatementAndReturnKey(String statement, final String dimensionName);

	public Long executeInsertStatementAndReturnKey(final String statement, String description);

	public int executeInsertOrUpdateStatement(final String statement, String description);

	public List<DimensionKeysPair> queryForDimensionKeys(final String dimensionName, final String statement, int expectedTotalValues,
			RowMapper<DimensionKeysPair> rowMapper);

	public Map<String, String> executeSelectStatement(final String statement, String description);

}
