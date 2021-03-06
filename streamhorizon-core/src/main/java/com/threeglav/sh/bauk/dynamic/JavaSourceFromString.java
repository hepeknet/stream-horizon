package com.threeglav.sh.bauk.dynamic;

import java.net.URI;

import javax.tools.SimpleJavaFileObject;

class JavaSourceFromString extends SimpleJavaFileObject {

	private final String code;

	JavaSourceFromString(final String name, final String code) {
		super(URI.create("string:///" + name.replace('.', '/') + Kind.SOURCE.extension), Kind.SOURCE);
		this.code = code;
	}

	@Override
	public CharSequence getCharContent(final boolean ignoreEncodingErrors) {
		return code;
	}

	@Override
	public String toString() {
		return "[code=" + code + "]";
	}

}