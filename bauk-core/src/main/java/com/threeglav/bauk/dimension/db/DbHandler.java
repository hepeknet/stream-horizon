package com.threeglav.bauk.dimension.db;

import java.util.List;
import java.util.Map;

public interface DbHandler {

	public Long executeQueryStatementAndReturnKey(String statement, final String dimensionName);

	public Long executeInsertStatementAndReturnKey(final String statement, String description);

	public void executeInsertOrUpdateStatement(final String statement, String description);

	public List<String[]> queryForDimensionKeys(final String dimensionName, final String statement, int numberOfNaturalKeyColumns);

	public Map<String, String> executeSelectStatement(final String statement, String description);

}
