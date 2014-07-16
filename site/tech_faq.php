<!DOCTYPE html>
<html lang="en">
<head>
<title>StreamHorizon | Technical FAQ</title>
<?php include 'header.php' ?>
<div class="servicess center part clearfix">
  <header class="title">
    <p class="fleft"></p>
  </header>
   <ol class="readable">
	  <li><a href="#a5" class="more">Integration with OLAP (Real Time & Batch)</a></li>
	  <li><a href="#a4" class="more">StreamHorizon JDBC mode of operation</a></li>
	  <li><a href="#a1" class="more">StreamHorizon BULK MODE mode of operation</a><br /></li>
      <li><a href="#a2" class="more">Refreshing OLAP via SQL procedures & functions</a></li>
	  <li><a href="#a6" class="more">Batch oriented OLAP integration</a></li>
	  <li><a href="#a7" class="more">Real Time OLAP integration</a></li>
    </ol>
  <p/>
  <section class="content">
	<article id="a5" class="detail">
      <h3>Integration with OLAP (Real Time & Batch)</h3>
	  <p>
		StreamHorizon supports generic events which allow you to seamlessly integrate with any OLAP server. 
	  </p>
	  <p>
By deploying StreamHorizon demo at adequate hardware you will get idea of power of the platform in terms of data throughput and processing efficiency.
Quick browse of XML tutorial provided in <i>config</i> folder of the StreamHorizon distribution will give you good idea about source and target connectors you could use and all tuning parameters of the platform which are designed to bring maximum performance out of your hardware.
Upon decision to proceed with purchase of StreamHorizon platform our technical staff will be in touch with you do discuss best deployment strategy, licensed copy of software including guides and manuals will be provided. 
	  </p>
    </article>
	<article id="a4" class="detail">
      <h3>StreamHorizon JDBC mode of operation</h3>
	  <p>
		If you use JDBC mode of operation of StreamHorizon all you need to do is to implement event &lt;afterFeedProcessingCompletion&gt; which is called after every successful load/processing of data (if data comes from files event will be invoked after every file is loaded into target database, if data comes from sql queries or message queues same logic is valid).
		All that is required is to implement event &lt;afterFeedProcessingCompletion&gt; to invoke your executable (please see below) which will accept parameter like feedName or/and feedID (or any other) variable which will identify which data need be loaded from your database table into the OLAP cube. 
<pre>
&lt;events&gt;
&lt;afterFeedProcessingCompletion&gt;
&lt;command type="shell"&gt;
d:/olapCubeUploder.bat ${feedName} ${feedID} ${cubeName} ${serverName}
&lt;/command&gt;
&lt;/afterFeedProcessingCompletion&gt;
&lt;/events&gt;
</pre>
	  </p>
    </article>
    <article id="a1" class="detail">
      <h3>StreamHorizon BULK MODE mode of operation</h3>
      <p>
	  IN bulk mode operation instructions are the same as for JDBC mode operation. Only difference is that as database threadpool (rather than etl threadpool) will be executing load of data into the database event which needs overriding is &lt;afterBulkLoadSuccess&gt;
<pre>
&lt;events&gt;
&lt;afterBulkLoadSuccess&gt;
&lt;command type="shell"&gt;
d:/olapCubeUploder.bat ${feedName} ${feedID} ${cubeName} ${serverName}
&lt;/command&gt;
&lt;/afterBulkLoadSuccess&gt;
&lt;/events&gt;
</pre>
	  </p>
    </article>
	<article id="a2" class="detail">
      <h3>Refreshing OLAP via SQL procedures & functions</h3>
      <p>
	  Rather than pushing data into OLAP server via external executable or OS you can supply stored procedures or functions which achieve same goal. Only difference is that &lt;command&gt; element type will need be changed to <b>sql</b> (from <b>shell</b>) and suitable procedure name will need be supplied. Bulk mode OLAP refresh given above would become:
<pre>
&lt;events&gt;
&lt;afterBulkLoadSuccess&gt;
&lt;command type="sql"&gt;
CALL proc_olapCubeUploader ( ${feedName} ${feedID} ${cubeName} ${serverName} )
&lt;/command&gt;
&lt;/afterBulkLoadSuccess&gt;
&lt;/events&gt;
</pre>
Same logic applies for JDBC mode of operation.
	  </p>
    </article>
	<article id="a6" class="detail">
      <h3>Batch oriented OLAP integration</h3>
	  <p>
		If your Data Warehouse/Business Intelligence stack does not have to operate in real time fashion simplest and most robust in terms of stability is way of operation to refresh all cube dimensions after ETL processes have loaded all batch data into the data mart. This ensures that all dimensions are updated only once, speaking in terms of Analysis Services this is time to execute “ProcessUpdate” against all changed dimensions. Dimensional processing should be followed by data upload into the cube which can be designed to load all data (preferably in batches if data volumes are significant [volumes too big for a single upload]).
	  </p>
    </article>
	<article id="a7" class="detail">
      <h3>Real Time OLAP integration</h3>
	  <p>
		Real time OLAP integration requires constant uploading of data from database into the cube as data gets loaded into the Data Warehouse. To make OLAP processing as efficient as possible it is recommended that <b>ProcessAdd</b> (addition of new dimensional records) is used rather than updating of whole dimensions (<b>ProcessUpdate</b>). Terminology used is Analysis Services terminology, however, principle is same for all OLAP vendors.
	  </p>
	  <p>
ProcessUpdate can be time consuming when dimensions are large; it can also cause <b>validation</b> of all cube partitions (not only partition that data is loaded into). The bigger the cube and bigger the dimension in question more time delay is introduced by <b>ProcessUpdate</b> execution.
	</p>
	<p>
If you configure your StreamHorizon dimensions to call stored procedure while inserting new records into database, you could easily implement stored procedure to:
<ul>
<li>- Insert/update new record into dimension table</li>
<li>- Insert/update record with key of new dimensional record created at <i>control</i> table or a queue</li>
</ul>
</p>
<p>
Idea is that process which executes <b>ProcessAdd</b> (adding new records into the dimension) will have a list of which identifiers need to be passed on to the OLAP cube.
Alternative ways of achieving the same is:
<ul>
<li>- You may want to code <pre>select * from database dimension</pre> via SQL statement and perform MINUS in your logic against what is in your cube dimension (via MDX statement). This approach will work well if your dimensions are small to medium in size</li>
<li>- Using triggers to create <i>to do</i> list for <b>ProcessAdd</b> dimensional updating process</li>
</ul>
</p>
<p>
After all dimensions are refreshed by <b>ProcessAdd</b> then upload of data into the cube can commence. Note that you must ensure that all surrogate keys (dimensional keys) contained in cube data upload (data from fact table) are already in dimensions added by either <b>ProcessAdd</b> or <b>ProcessUpdate</b> (or any other processing type like ProcessData or ProcessFull for that matter).
</p>
<p>
Considering large volumes (100+ million records) OLAP data push should be implemented as a process which batches multiple data entities (multiple files for example) and uploads them to fact table in one single upload. Ideal size of such batch should be tested and is deployment specific.
</p>
<p>
Using <b>parallel processing</b> upload of data into the cube will parallelize data uploads at OLAP server side. Note that process uploading data into the  cube is single process in such case as parallelism is achieved by <b>parallel processing</b> XMLA statement. The parallel processing is terminology used in Microsoft Analysis Services, however, same paradigm applies for all OLAP vendors in terms of architecture and design of ETL processes.
</p>
<p>
Indexing underlying tables in your database schema may improve data retrieval performance. This is valid for large dimensions and especially fact tables. 
</p>
<p>
OLAP server should be set up to issue single query per dimension rather than one query per dimensional attribute during refresh (which may be default in some OLAP engines). Later retrieval mechanism may cause refresh failures due to inconsistent data snapshots, this issue manifests with Real Time Data Warehouses which have very high data throughput.
	  </p>
    </article>
  </section>
</div>
<?php include 'footer.php' ?>