package com.threeglav.bauk.dimension.db;

import java.util.List;
import java.util.Map;

public interface DbHandler {

	public Long executeQueryStatementAndReturnKey(String statement);

	public Long executeInsertStatementAndReturnKey(final String statement);

	public void executeInsertOrUpdateStatement(final String statement);

	public List<String[]> queryForDimensionKeys(final String statement, int numberOfNaturalKeyColumns);

	public Map<String, String> executeSelectStatement(final String statement);

}
