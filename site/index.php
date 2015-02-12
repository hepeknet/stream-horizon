<!DOCTYPE html>
<html lang="en">
<head>
<title>StreamHorizon - Data Processing Platform</title>
<?php include 'header.php' ?>
</script>
<div class="main center">
  <section class="part clearfix">
    <header class="title clearfix">
      <p><h2>Next Generation Data Processing and Data Integration Platform</h2></p>
	</header>
	<p>
StreamHorizon is the next generation ETL Big Data processing platform. 
StreamHorizon is hardware efficient, scalable and extensible replacement for existing legacy stove-pipe ETL platforms. 
We have named our novel approach to ETL architecture <b>adaptiveETL</b>. 
Testimony to performance of adaptiveETL is a fact that processing throughput of 1+ million records per second is achievable on a single commodity server. Download and test StreamHorizon Demo Data Warehousing project in <a href="./request_download.php">one hour</a>.
	</p>
	<br />
	<section class="part clearfix">
	<article class="column3 mright services">
      <h5>Easy to develop</h5>
      <div class="sepmini"></div>
	  <p>
      Rather than custom code ETL logic, StreamHorizon enables you to fully configure your ETL logic within single & intuitive XML configuration file. 
	  </p>
	  <br />
	<p>
	More than 90% of ETL code is needlessly developed (coding of dimensional ELT logic, fact table inserts/updates, data quality transformations). StreamHorizon eliminates the need for custom coding of such ETL transformations, instead, entire ETL stream is configurable via single, intuitive XML file.
	</p>
    </article>
    <article class="column3 mright services">
      <h5>Built in ETL functionality</h5>
      <div class="sepmini"></div>
      <p>
	  Forget about coding custom code Type0,1,2,3... dimensional logic, deriving surrogate keys for Fact table or coding any other common ETL transformations!
StreamHorizon simply enables you to configure XML element with table name and dimension type. 
</p>
<br />
<p>
ETL logic is thereby transparently generated & executed by StreamHorizon engine based on your configuration.
</p>
    </article>
    <article class="column3 services">
      <h5>Scalable ETL</h5>
      <div class="sepmini"></div>
      <p>
		Scaling number of your ETL processes and making them run in parallel is achieved by simply specifying number of parallel instances (or threads) you wish to run.
	</p>
	<br />
	<p>
There is no need to setup <i>parameter files</i> (or equivalents) before ETL execution. 
</p>
<br />
<p>
There is no need to create numerous copies of the same ETL flow in order to achieve ETL flow parallelism.
	  </p>
    </article>
	</section>
	<section class="part clearfix">
	<article class="column3 mright services">
      <h5>Customizable</h5>
      <div class="sepmini"></div>
      <p>
		If you wish to implement specific ETL logic rather than use available XML configurable options you may simply supply Java class and thereby override default 
behaviour of StreamHorizon engine.
	  </p>
	  <br />
	  <p>
	  Rich plugin ecosystem enables you to <i>hook-into</i> different part of StreamHorizon execution and become a part of processing pipeline.
	  </p>
    </article>
	
	<article class="column3 mright services">
      <h5>Quick Time to Market</h5>
      <div class="sepmini"></div>
      <p>
		Customizing XML configuration rather than developing ETL code enables you to deliver fully functional Data Marts/Data Warehouses in matter of days.
	  </p>
	  <br />
	  <p>
<b>Demo & Sample Data Mart</b> - StreamHorizon Sample <a href="request_download.php">Demo Data Mart</a> can be used as starting base for your project. It comes with full configuration implementation of ETL logic for Oracle, MSSQL and MySQL databases.
	  </p>
    </article>
	
	<article class="column3 services">
     
      <h5>Minimize I/O operations</h5>
      <div class="sepmini"></div>
      <p>
		StreamHorizon performs all data transformation steps on the fly, within single ETL process. Thereby, you can eliminate Staging area from your ETL processes if it fits your data processing design.
	  </p>
    </article>
	</section>
	<section class="part clearfix">
	<article class="column3 mright services">
      <h5>Clusterable, Virtualizable & Hadoop ready</h5>
      <div class="sepmini"></div>
      <p>
		StreamHorizon can run on Hadoop Ecosystem, is cloud-ready and can be 100% virtualized. 
	  </p>
	  <br />
	  <p>
		StreamHorizon enables easy integration with major big-data frameworks like Storm, Spark, Samza or Hadoop (HDFS and HBase).
	  </p>
    </article>
	
	<article class="column3 mright services">
      <h5>Run anywhere</h5>
      <div class="sepmini"></div>
      <p>
		StreamHorizon is implemented as highly efficient Java library. It can process millions of events per second on standard laptop.
	  </p>
	  <br />
	  <p>
		It has low hardware footprint and can be deployed on workstations, commodity servers and compute and data 
clusters, on all major operative systems which run Java.
	  </p>
    </article>
	
	<article class="column3 services">
      <h5>Encapsulating In Memory Data Grid</h5>
      <div class="sepmini"></div>
      <p>
		StreamHorizon ETL processes utilize embedded Infinispan (or Hazelcast) data grids which are internally used by StreamHorizon engine during data processing. This is one of the reasons
		why StreamHorizon can achieve very high throughput and horizontal scalability.
	  </p>
    </article>
  </section>
  <br /><br />
  <section class="part clearfix">
  <h2>StreamHorizon in more detail</h2>
	<div class="column2 mright">
      <p>
		StreamHorizon is the next generation ETL Big Data processing platform. It is highly efficient and very extensible replacement for legacy stove-pipe ETL platforms. We call this <b>adaptiveETL</b>.
		<ul>
			<li>Data Processing throughput of <b>1+ Million Records per second</b> (single commodity server). Download and test StreamHorizon Demo Data Warehousing project in <a href="./request_download.php">one hour</a></li>
			<li>Quick Time to Market – deploy StreamHorizon & <b>deliver</b> your Data integration project <b>in a single week</b></li>
		</ul>
		<br>
		<ul>
			<li>Fully Configurable via XML</li>
			<li>Requires no ETL platform specific knowledge</li>
			<li>Shift from <b>coding</b> to <b>XML configuration</b> and reduce IT skills required to deliver, manage, run & outsource projects</li>
			<li>Eliminated 90+% manual coding</li>
			<li>Flexible & Customizable – override any default behaviour of StreamHorizon platform with custom Java, OS script or SQL implementation</li>
			<li>No vendor lock-in (all custom written code runs outside StreamHorizon platform - there is no need to re-develop code if migrating your existing solution)</li>
			<li>No ETL tool specific language</li>
			<li>Out of the box features like Type 0,1,2, Custom dimensions, dynamic In Memory Cache formation transparent to developer</li>
		</ul>
		<br>
		<ul>
			<li>Delivering performance critical Big Data projects</li>
			<li>Massively parallel data streaming engine</li>
			<li>Transparently backed with In Memory Data Grid (Coherence, Infinispan, Hazelcast, any other.)</li>
			<li>ETL processes run in memory and interact with cache (In Memory Data Grid)</li>
			<li>Unnecessary Staging (I/O expensive) ETL steps are eliminated</li>
		</ul>
		<br>
		<ul>
			<li>Lambda Architecture - Hadoop (& non-Hadoop) real time & batch oriented data streaming/processing architecture</li>
			<li>Data Streaming & Micro batch Architecture</li>
			<li>Massively parallel conventional ETL Architecture</li>
			<li>Batch oriented conventional ETL Architecture</li>
		</ul>
		<ul>
			<li>1 Hour Proof of Concept – <a href="resources.php">download</a> and test-run StreamHorizon's demo Data Warehousing project</li>
		</ul>
		<ul>
			<li>Runs on Big Data clusters: Hadoop, HDFS, <a href="http://kafka.apache.org/">Kafka</a>, <a href="https://spark.apache.org/">Spark</a>, <a href="https://storm.incubator.apache.org/">Storm</a>, Hive, Impala and more...</li>
			<li>Run your StreamHorizon Data Processing Cluster (ETL grid)</li>
			<li>Runs on Compute Grid (alongside grid libraries like Quant Library or any other)</li>
		</ul>
		<ul>
			<li>Horizontally & Vertically scalable, Highly Available (HA), Clusterable</li>
			<li>Running on Linux, Solaris, Windows, Compute Clouds (EC2 & others)</li>
		</ul>
		<ul>
			<li>See StreamHorizon Data Processing Platform in action on our <a href="https://www.youtube.com/channel/UCdKt8NUmGauq6COhqjJcxhQ">YouTube channel</a></li>
			<li>Read more in <a href="./downloads/Streamhorizon_Overview_CIO_Information_Pack.pdf">StreamHorizon C-Level roles overview (PDF)</a>, <a href="./downloads/StreamHorizon_Overview.pdf">StreamHorizon overview (PDF)</a> or <a href="faq.php">Product FAQ section</a></li>
		</ul>
	  </p>
    </div>
	<div id="slides" class="slider column2">
      <div class="slides_container">
        <div class="slide">
          <figure> 
			<img src="img/one.png" alt="" width="270">
          </figure>
        </div>
        <div class="slide">
          <figure> 
			<img src="img/two.png" alt="">
          </figure>
        </div>
        <div class="slide">
          <figure> 
			<img src="img/three.png" alt="" id="idThree">
          </figure>
        </div>
		<div class="slide">
          <figure> 
			<img src="img/four.png" alt="" id="idFour">
          </figure>
        </div>
		<div class="slide">
          <figure> 
			<img src="img/five.png" alt="" id="idFive">
          </figure>
        </div>
      </div>
    </div>
	<div>
		Read In More detail about:
	<ul>
		<li>Cost-Effectiveness Analyses – <a href="./faq.php#img1">Analysis I</a> & <a href="./faq.php#img2">Analysis II</a> & <a href="./faq.php#img3">Analysis III</a></li>
		<li><a href="./faq.php#img4">Functional & Hardware Profile</a></li>
		<li><a href="./faq.php#img5">Reduced Workforce demand</a></li>
		<li><a href="./faq.php#img6">Environment Risk Management</a></li>
		<li><a href="./faq.php#img7">Targeted Program & Project profiles</a></li>
		<li><a href="./usecases.php">Applicable industries</a></li>
		<li><a href="./faq.php#img8">About StreamHorizon</a></li>
		<li><a href="./faq.php#img9">Connectivity Map</a></li>
	</ul>
	</div>
	</section>
</div>
<?php include 'footer.php' ?>