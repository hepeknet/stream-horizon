package com.threeglav.bauk.dimension.db;

import java.util.List;

public interface DbHandler {

	public Long executeQueryStatementAndReturnKey(String statement);

	public Long executeInsertStatementAndReturnKey(final String statement);

	public void executeInsertOrUpdateStatement(final String statement);

	public List<String[]> queryForDimensionKeys(final String statement, int numberOfNaturalKeyColumns);

}
