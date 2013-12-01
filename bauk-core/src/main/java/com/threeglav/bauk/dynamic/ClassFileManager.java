package com.threeglav.bauk.dynamic;

import java.security.SecureClassLoader;

import javax.tools.FileObject;
import javax.tools.ForwardingJavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.JavaFileObject.Kind;
import javax.tools.StandardJavaFileManager;

class ClassFileManager extends ForwardingJavaFileManager {

	private JavaClassObject jclassObject;

	ClassFileManager(final StandardJavaFileManager standardManager) {
		super(standardManager);
	}

	@Override
	public ClassLoader getClassLoader(final Location location) {
		return new SecureClassLoader() {
			@Override
			protected Class<?> findClass(final String name) throws ClassNotFoundException {
				final byte[] b = jclassObject.getBytes();
				return super.defineClass(name, jclassObject.getBytes(), 0, b.length);
			}
		};
	}

	@Override
	public JavaFileObject getJavaFileForOutput(final Location location, final String className, final Kind kind, final FileObject sibling) {
		jclassObject = new JavaClassObject(className, kind);
		return jclassObject;
	}
}