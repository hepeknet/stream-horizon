package com.threeglav.bauk.files;

import java.io.File;
import java.io.IOException;
import java.nio.file.attribute.BasicFileAttributes;

public interface FileProcessor {

	public void process(File file, BasicFileAttributes attributes) throws IOException;

}
