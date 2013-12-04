package com.threeglav.bauk.dynamic;

import java.util.Arrays;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaCompiler.CompilationTask;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.ToolProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.bauk.util.StringUtil;

public class ClassResolver<T> {

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	private final String fullClassName;
	private final Class<T> expectedType;

	public ClassResolver(final String fullClassName, final Class<T> expectedType) {
		if (StringUtil.isEmpty(fullClassName)) {
			throw new IllegalArgumentException("Full class name must not be null or empty!");
		}
		if (expectedType == null) {
			throw new IllegalArgumentException("Expected type must not be null");
		}
		this.fullClassName = fullClassName;
		this.expectedType = expectedType;
	}

	public T createInstanceFromClasspath() {
		log.debug("Trying to find {} in classpath", fullClassName);
		try {
			final Class clazz = Thread.currentThread().getContextClassLoader().loadClass(fullClassName);
			log.debug("Successfully found {} in classpath", fullClassName);
			if (expectedType.isAssignableFrom(clazz)) {
				log.debug("{} is subtype of {}. Trying to create new instance", clazz, expectedType);
				try {
					final Object obj = clazz.newInstance();
					log.debug("Successfully create instance {} of {}", obj, clazz);
					return (T) obj;
				} catch (InstantiationException | IllegalAccessException e) {
					log.warn("Was not able to create instance of {}", clazz);
					log.warn("Details", e);
				}
			} else {
				throw new IllegalStateException(clazz + " is not of expected type " + expectedType);
			}
		} catch (final ClassNotFoundException e) {
			log.warn("Was not able to find class {} in classpath", fullClassName);
		}
		return null;
	}

	public T createInstanceFromSource(final String sourceCode) {
		if (StringUtil.isEmpty(sourceCode)) {
			throw new IllegalArgumentException("Source must not be null or empty");
		}
		log.debug("Trying to create new instance of {} from source \n {}", expectedType, sourceCode);
		final JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
		if (compiler == null) {
			final String javaHome = System.getProperty("java.home");
			log.error(
					"Unable to find java compiler. Make sure you have JDK installed and JAVA_HOME points to the right JDK installation folder! Currently JAVA_HOME={}",
					javaHome);
			return null;
		}
		log.debug("Compiler is {}, source versions {}", compiler, compiler.getSourceVersions());
		final DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<JavaFileObject>();
		final JavaFileManager fileManager = new ClassFileManager(compiler.getStandardFileManager(null, null, null));
		final JavaFileObject file = new JavaSourceFromString(fullClassName, sourceCode);

		final Iterable<? extends JavaFileObject> compilationUnits = Arrays.asList(file);
		final CompilationTask task = compiler.getTask(null, fileManager, diagnostics, null, null, compilationUnits);
		final boolean success = task.call();
		for (final Diagnostic diagnostic : diagnostics.getDiagnostics()) {
			log.error("cd code: {}, cd: kind {}", diagnostic.getCode(), diagnostic.getKind());
			log.error("cd position: {}, start {}, end {}", diagnostic.getPosition(), diagnostic.getStartPosition(), diagnostic.getEndPosition());
			log.error("cd source {}", diagnostic.getSource());
			log.error("cd message: {}", diagnostic.getMessage(null));
		}
		if (success) {
			log.debug("Successfully compiled {}", sourceCode);
			try {
				final Object instance = fileManager.getClassLoader(null).loadClass(fullClassName).newInstance();
				log.debug("Successfully created new instance {} of class {}", instance, instance.getClass());
				if (!expectedType.isAssignableFrom(instance.getClass())) {
					log.error("{} is not of expected type {}", instance, expectedType);
					throw new IllegalStateException(instance + " is not of expected type " + expectedType);
				}
				return (T) instance;
			} catch (final InstantiationException e) {
				log.error("Error ", e);
			} catch (final IllegalAccessException e) {
				log.error("Error ", e);
			} catch (final ClassNotFoundException e) {
				log.error("Error ", e);
			}
		}
		return null;
	}

}
