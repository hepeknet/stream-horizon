package com.threeglav.bauk.util;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

public abstract class MetricsUtil {

	private static final String METRICS_OFF_SYS_PARAM_NAME = "metrics.off";

	private static final Map<String, AtomicInteger> USED_NAMES = new HashMap<String, AtomicInteger>();

	private static boolean metricsOff;

	private static MetricRegistry registry;

	static {
		metricsOff = "true".equals(System.getProperty(METRICS_OFF_SYS_PARAM_NAME));
		registry = new MetricRegistry();
		final JmxReporter reporter = JmxReporter.forRegistry(registry).build();
		reporter.start();
	}

	public static boolean isMetricsOff() {
		return metricsOff;
	}

	private synchronized static String getUniqueName(final String name) {
		final AtomicInteger usedCounter = USED_NAMES.get(name);
		if (usedCounter == null) {
			USED_NAMES.put(name, new AtomicInteger(0));
			return name;
		} else {
			final String newName = name + "_" + usedCounter.incrementAndGet();
			USED_NAMES.put(name, usedCounter);
			return newName;
		}
	}

	public static Meter createMeter(final String name) {
		if (!metricsOff) {
			return registry.meter(getUniqueName(name));
		}
		return null;
	}

	public static Counter createCounter(final String name) {
		if (!metricsOff) {
			return registry.counter(getUniqueName(name));
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
