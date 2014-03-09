package com.threeglav.sh.bauk.examples;

import com.threeglav.sh.bauk.io.BulkOutputWriter;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * This is not dynamic plugin and it has to be recompiled and packaged into jar before it can be used by StreamHorizon engine.
 * 
 * Implementations of BulkOutputWriter must follow rules defined here http://docs.oracle.com/javase/7/docs/api/java/util/ServiceLoader.html
 * 
 * All required hadoop and hdfs dependency jar files must be placed in $ENGINE_HOME/ext-lib/
 * 
 * This class (together with appropriate META-INF/services/com.threeglav.sh.bauk.io.BulkOutputWriter file) must be packaged in a jar
 * and also copied to $ENGINE_HOME/ext-lib/
 * 
 * $ENGINE_HOME/config/engine-config.xml should use outputType="hdfs" so that it matches URI defined below.
 * 
 * It is your responsibility to configure hadoop properly and create all required processes so that this writer can find them.
 * 
 */
public class HDFSOutputWriter implements BulkOutputWriter{
    
    private final Configuration hdfsConfig;
    private final FileSystem hdfsFileSystem;
    private FSDataOutputStream fsDataOut;
        
    public HDFSOutputWriter(){
    	hdfsConfig = new Configuration();
    	hdfsConfig.addResource(new Path("/path/to/your/hadoop/hadoop-2.3.0/etc/hadoop/core-site.xml"));
        try {
        	hdfsFileSystem = FileSystem.get(hdfsConfig);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    public void init(Map<String, String> engineProperties){
    	// do initialized some resources
    }

    @Override
    public void startWriting(Map<String, String> globalAttributes) {
        final String bulkFileName = globalAttributes.get("bulkFileName");
        final Path filenamePath = new Path("hdfs://localhost/streamhorizon/bulk-files/" + bulkFileName);
        System.out.println("Writing to HDFS file " + bulkFileName);
        try {
            if (hdfsFileSystem.exists(filenamePath)) {
                //remove the file
            	hdfsFileSystem.delete(filenamePath, true);
            }
            fsDataOut = hdfsFileSystem.create(filenamePath);
        } catch (IOException ie) {
            throw new RuntimeException(ie);
        }
    }

    @Override
    public void doWriteOutput(Object[] processedData, Map<String, String> globalAttributes) {
        final StringBuilder sb = new StringBuilder(100);
        for(int i=0;i<processedData.length;i++){
            if(i != 0){
                sb.append(",");
            }
            sb.append(processedData[i]);
        }
        try {
            fsDataOut.writeChars(sb.toString());
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    @Override
    public void closeResourcesAfterWriting(Map<String, String> globalAttributes, boolean success) {
        if(fsDataOut != null){
            try {
                fsDataOut.close();
                System.out.println("Closed HDFS resources");
            } catch (IOException ignored) {
            	// ignored
            }
        }
    }

    @Override
    public boolean understandsURI(String uri) {
    	// this is for StreamHorizon to load this class when outputType="hdfs" in engine-config.xml
        return uri != null && uri.toLowerCase().startsWith("hdfs");
    }
    
}
