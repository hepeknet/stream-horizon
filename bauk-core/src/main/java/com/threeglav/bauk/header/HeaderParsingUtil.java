package com.threeglav.bauk.header;

import java.util.ArrayList;

import com.threeglav.bauk.model.Attribute;

public abstract class HeaderParsingUtil {

	public static String[] getAttributeNames(final ArrayList<Attribute> attributes) {
		if (attributes == null) {
			return null;
		}
		final String[] attributeNames = new String[attributes.size()];
		for (int i = 0; i < attributeNames.length; i++) {
			attributeNames[i] = attributes.get(i).getName();
		}
		return attributeNames;
	}

}
