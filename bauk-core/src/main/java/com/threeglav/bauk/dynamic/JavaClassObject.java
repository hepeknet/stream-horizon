package com.threeglav.bauk.dynamic;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import javax.tools.SimpleJavaFileObject;

class JavaClassObject extends SimpleJavaFileObject {

	private final ByteArrayOutputStream bos = new ByteArrayOutputStream();

	JavaClassObject(final String name, final Kind kind) {
		super(URI.create("string:///" + name.replace('.', '/') + kind.extension), kind);
	}

	byte[] getBytes() {
		return bos.toByteArray();
	}

	@Override
	public OutputStream openOutputStream() throws IOException {
		return bos;
	}
}