package com.threeglav.sh.bauk.util;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.threeglav.sh.bauk.BaukConstants;

public final class StatefulAttributeReplacer {

	private final String str;
	private final String dbStringLiteral;
	private final String dbStringEscapeLiteral;
	private final boolean hasReplacementToDo;
	private final String[] attributeNamesToReplace;
	private final String[] attributeNamePlaceholdersToReplace;

	public StatefulAttributeReplacer(final String str, final String dbStringLiteral, final String dbStringEscapeLiteral) {
		if (StringUtil.isEmpty(str)) {
			throw new IllegalArgumentException("Unable to replace values in null string");
		}
		this.str = str;
		this.dbStringLiteral = dbStringLiteral;
		this.dbStringEscapeLiteral = dbStringEscapeLiteral;
		final Set<String> allAttributesForReplacement = StringUtil.collectAllAttributesFromString(str);
		hasReplacementToDo = !allAttributesForReplacement.isEmpty();
		if (hasReplacementToDo) {
			final TreeSet<String> sortedAttributes = new TreeSet<>();
			sortedAttributes.addAll(allAttributesForReplacement);
			attributeNamePlaceholdersToReplace = new String[allAttributesForReplacement.size()];
			attributeNamesToReplace = new String[allAttributesForReplacement.size()];
			int i = 0;
			for (final String attrName : sortedAttributes) {
				attributeNamesToReplace[i] = attrName;
				final String attributeNamePlaceholder = BaukConstants.STATEMENT_PLACEHOLDER_DELIMITER_START + attrName
						+ BaukConstants.STATEMENT_PLACEHOLDER_DELIMITER_END;
				attributeNamePlaceholdersToReplace[i] = attributeNamePlaceholder;
				i++;
			}
		} else {
			attributeNamesToReplace = null;
			attributeNamePlaceholdersToReplace = null;
		}
	}

	public String replaceAttributes(final Map<String, String> globalAttributes) {
		if (!hasReplacementToDo) {
			return str;
		} else {
			String replacedStr = str;
			for (int i = 0; i < attributeNamesToReplace.length; i++) {
				final String attrName = attributeNamesToReplace[i];
				final String attrNamePlaceholder = attributeNamePlaceholdersToReplace[i];
				final String val = globalAttributes.get(attrName);
				replacedStr = StringUtil.replaceSingleAttribute(replacedStr, attrNamePlaceholder, val, dbStringLiteral, dbStringEscapeLiteral);
			}
			return replacedStr;
		}
	}

	/*
	 * for testing only
	 */

	String[] getAttributeNamesToReplace() {
		return attributeNamesToReplace;
	}

	String[] getAttributeNamePlaceholdersToReplace() {
		return attributeNamePlaceholdersToReplace;
	}

	boolean isHasReplacementToDo() {
		return hasReplacementToDo;
	}

}
