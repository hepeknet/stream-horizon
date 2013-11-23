package com.threeglav.bauk.dimension.db;

public interface DbHandler {

	public Long executeQueryStatementAndReturnKey(String statement);

	public Long executeInsertStatementAndReturnKey(final String statement);

	public void executeInsertOrUpdateStatement(final String statement);

}
