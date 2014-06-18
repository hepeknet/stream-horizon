package com.threeglav.sh.bauk.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import com.threeglav.sh.bauk.model.BaukProperty;

public abstract class BaukPropertyUtil {

	public static BaukProperty getRequiredUniqueProperty(final ArrayList<BaukProperty> properties, final String propName) {
		if (properties == null || properties.isEmpty()) {
			throw new IllegalArgumentException("Properties must not be null");
		}
		if (StringUtil.isEmpty(propName)) {
			throw new IllegalArgumentException("Property name must not be null");
		}
		BaukProperty found = null;
		for (final BaukProperty bp : properties) {
			if (bp.getName().equalsIgnoreCase(propName)) {
				if (found != null) {
					throw new IllegalStateException("Found more than one property named [" + propName + "]. At most one is allowed!");
				}
				found = bp;
			}
		}
		if (found == null) {
			throw new IllegalStateException("Could not find property named  [" + propName + "]. This property is required!");
		}
		return found;
	}

	public static BaukProperty getUniquePropertyIfExists(final ArrayList<BaukProperty> properties, final String propName) {
		if (properties == null || properties.isEmpty()) {
			throw new IllegalArgumentException("Properties must not be null");
		}
		if (StringUtil.isEmpty(propName)) {
			throw new IllegalArgumentException("Property name must not be null");
		}
		BaukProperty found = null;
		for (final BaukProperty bp : properties) {
			if (bp.getName().equalsIgnoreCase(propName)) {
				if (found != null) {
					throw new IllegalStateException("Found more than one property named [" + propName + "]. At most one is allowed!");
				}
				found = bp;
			}
		}
		return found;
	}

	public static Collection<String> getAllPropertyValuesByName(final ArrayList<BaukProperty> properties, final String propName) {
		if (properties == null || properties.isEmpty()) {
			throw new IllegalArgumentException("Properties must not be null");
		}
		if (StringUtil.isEmpty(propName)) {
			throw new IllegalArgumentException("Property name must not be null");
		}
		final List<String> vals = new LinkedList<>();
		for (final BaukProperty bp : properties) {
			if (bp.getName().equalsIgnoreCase(propName)) {
				vals.add(bp.getValue());
			}
		}
		return vals;
	}

}
