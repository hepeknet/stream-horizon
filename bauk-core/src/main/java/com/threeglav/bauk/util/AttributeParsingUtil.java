package com.threeglav.bauk.util;

import gnu.trove.map.hash.THashMap;

import java.util.ArrayList;
import java.util.Map;

import com.threeglav.bauk.model.Attribute;

public abstract class AttributeParsingUtil {

	public static String[] getAttributeNames(final ArrayList<Attribute> attributes) {
		if (attributes == null) {
			throw new IllegalArgumentException("Attributes must not be null");
		}
		final String[] attributeNames = new String[attributes.size()];
		for (int i = 0; i < attributeNames.length; i++) {
			attributeNames[i] = attributes.get(i).getName();
		}
		return attributeNames;
	}

	public static Map<String, Integer> getAttributeNamesAndPositions(final ArrayList<Attribute> attributes) {
		if (attributes == null) {
			throw new IllegalArgumentException("Attributes must not be null");
		}
		final Map<String, Integer> attrs = new THashMap<>();
		int counter = 0;
		for (final Attribute at : attributes) {
			attrs.put(at.getName(), counter++);
		}
		return attrs;
	}

}
