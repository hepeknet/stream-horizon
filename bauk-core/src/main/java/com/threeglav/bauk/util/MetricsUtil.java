package com.threeglav.bauk.util;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.threeglav.bauk.SystemConfigurationConstants;

public abstract class MetricsUtil {

	private static final Map<String, AtomicInteger> USED_NAMES = new HashMap<String, AtomicInteger>();

	private static final boolean metricsOff;

	private static final MetricRegistry registry;

	static {
		metricsOff = "true".equals(System.getProperty(SystemConfigurationConstants.METRICS_OFF_SYS_PARAM_NAME));
		registry = new MetricRegistry();
		final JmxReporter reporter = JmxReporter.forRegistry(registry).build();
		reporter.start();
	}

	public static final boolean isMetricsOff() {
		return metricsOff;
	}

	private synchronized static String getUniqueName(final String name) {
		AtomicInteger usedCounter = USED_NAMES.get(name);
		if (usedCounter == null) {
			usedCounter = new AtomicInteger(1);
			USED_NAMES.put(name, usedCounter);
		} else {
			usedCounter.incrementAndGet();
			USED_NAMES.put(name, usedCounter);
		}
		return "(" + usedCounter.get() + ") " + name;
	}

	public static Meter createMeter(final String name) {
		if (!metricsOff) {
			return registry.meter(getUniqueName(name));
		}
		return null;
	}

	public static Counter createCounter(final String name, final boolean uniqueName) {
		if (!metricsOff) {
			String counterName = name;
			if (uniqueName) {
				counterName = getUniqueName(name);
			}
			return registry.counter(counterName);
		}
		return null;
	}

	public static Histogram createHistogram(final String name) {
		if (!metricsOff) {
			return registry.histogram(getUniqueName(name));
		}
		return null;
	}

}
