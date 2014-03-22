package com.threeglav.sh.bauk.dimension.db;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.RowMapper;

import com.threeglav.sh.bauk.BaukConstants;
import com.threeglav.sh.bauk.dimension.DimensionKeysPair;
import com.threeglav.sh.bauk.util.StringUtil;

public final class InsertOnlyDimensionKeysRowMapper implements RowMapper<DimensionKeysPair> {

	private final String dimensionName;
	private final String statement;
	private final int expectedTotalValues;
	private boolean alreadyCheckedForColumnNumber = false;

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	public InsertOnlyDimensionKeysRowMapper(final String dimensionName, final String statement, final int expectedTotalValues) {
		this.dimensionName = dimensionName;
		this.statement = statement;
		this.expectedTotalValues = expectedTotalValues;
	}

	@Override
	public DimensionKeysPair mapRow(final ResultSet rs, final int rowNum) throws SQLException {
		if (!alreadyCheckedForColumnNumber) {
			final ResultSetMetaData rsmd = rs.getMetaData();
			final int columnsNumber = rsmd.getColumnCount();
			if (columnsNumber != expectedTotalValues) {
				log.error("For dimension {} sql statement {} does not return correct number of values", dimensionName, statement);
				throw new IllegalStateException("SQL statement for dimension " + dimensionName
						+ " should return surrogate key and all natural keys (in order as declared in configuration). In total expected "
						+ expectedTotalValues + " columns, but database query returned " + columnsNumber + " values!");
			}
			alreadyCheckedForColumnNumber = true;
		}
		final DimensionKeysPair dkp = new DimensionKeysPair();
		dkp.surrogateKey = rs.getInt(1);
		final StringBuilder sb = new StringBuilder(StringUtil.DEFAULT_STRING_BUILDER_CAPACITY);
		for (int i = 2; i <= expectedTotalValues; i++) {
			if (i != 2) {
				sb.append(BaukConstants.NATURAL_KEY_DELIMITER);
			}
			sb.append(rs.getString(i));
		}
		dkp.lookupKey = sb.toString();
		return dkp;
	}
}