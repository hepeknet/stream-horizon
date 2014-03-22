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

public final class T1DimensionKeysRowMapper implements RowMapper<DimensionKeysPair> {

	private final String dimensionName;
	private final String statement;
	private final int expectedTotalValues;
	private final int numberOfNaturalKeys;
	private boolean alreadyCheckedForColumnNumber = false;
	private final boolean isDebugEnabled;

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	public T1DimensionKeysRowMapper(final String dimensionName, final String statement, final int expectedTotalValues, final int numberOfNaturalKeys) {
		this.dimensionName = dimensionName;
		this.statement = statement;
		this.expectedTotalValues = expectedTotalValues;
		this.numberOfNaturalKeys = numberOfNaturalKeys;
		isDebugEnabled = log.isDebugEnabled();
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
		for (int i = 0; i < numberOfNaturalKeys; i++) {
			if (i != 0) {
				sb.append(BaukConstants.NATURAL_KEY_DELIMITER);
			}
			sb.append(rs.getString(i + 2));
		}
		dkp.naturalKeyOnly = sb.toString();
		sb.append(BaukConstants.NATURAL_NON_NATURAL_DELIMITER);
		final StringBuilder nonNaturalKeyOnly = new StringBuilder(StringUtil.DEFAULT_STRING_BUILDER_CAPACITY);
		for (int i = numberOfNaturalKeys; i < expectedTotalValues - 1; i++) {
			if (i != numberOfNaturalKeys) {
				nonNaturalKeyOnly.append(BaukConstants.NON_NATURAL_KEY_DELIMITER);
			}
			nonNaturalKeyOnly.append(rs.getString(i + 2));
		}
		dkp.nonNaturalKeyOnly = nonNaturalKeyOnly.toString();
		sb.append(nonNaturalKeyOnly);
		dkp.lookupKey = sb.toString();
		if (isDebugEnabled) {
			log.debug("Precaching {}={}. Natural key only = [{}] and non-natural key only = [{}]", new Object[] { dkp.surrogateKey, dkp.lookupKey,
					dkp.naturalKeyOnly, dkp.nonNaturalKeyOnly });
		}
		return dkp;
	}
}