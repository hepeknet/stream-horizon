<!DOCTYPE html>
<html lang="en">
<head>
<title>StreamHorizon | Product FAQ</title>
<?php include 'header.php' ?>
<div class="servicess center part clearfix">
  <header class="title">
    <p class="fleft"></p>
  </header>
   <ol class="readable">
	  <li><a href="#a1" class="more">When is the StreamHorizon the right choice for your project?</a><br /></li>
      <li><a href="#a2" class="more">When is the StreamHorizon not the right choice for your project?</a></li>
	  <li><a href="#a6" class="more">What StreamHorizon platform brings to your existing Data Integration and Business Intelligence stack?</a></li>
	  <li><a href="#a7" class="more">What is the difference between StreamHorizon and Leading Market ETL/Data Integration vendors?</a></li>
	  <li><a href="#a3" class="more">How is StreamHorizon licensed?</a></li>
	  <li><a href="#a5" class="more">What is the recommended way to start using StreamHorizon?</a></li>
	  <li><a href="#a4" class="more">What are minimum system requirements for running StreamHorizon?</a></li>
    </ol>
  <p/>
  <section class="content">
	<article id="a1" class="detail">
      <h3>When is the StreamHorizon the right choice for your project?</h3>
      <p>If you need to deliver robust project quickly. StreamHorizon single XML configuration file customization is powerful method od rapid development which saves development time and significantly reduces possibility of bugs occurring in your ETL.</p>
	  <p>When your solution need be highly parallel and you wish not to dedicate time to solve multithreading, synchronization & database locking issues.</p>
	  <p>When your solution must scale up in data volumes, scale vertically and/or horizontally.</p>
	  <p>When 90% of your ETL logic can simply be configured (fact loads, parallelism, Type 1, Type 2 and other dimension types, mappings, lookups etc.) and other 10% can be implemented by your own executables or OS scripts or SQL procedures which will be simply invoked by provided StreamHorizon Event architecture. All you need to specify is when your customised logic should be invoked by ShtreamHorizon engine. This is done by simply creating XML element in configuration file which is pointing to the location of your executable logic, script or SQL procedure.</p>
	  <p>100% of ability to override default behaviour of Streamhorizon engine by injecting your data/header/footer/cache processing Java classes which will be invoked by StreamHorizon accordingly.</p>
	  <p>Ability to control/embed your ETL logic by invoking Pre, onSuccess, onFailure, Finally (Post) events for every data entity processed (data entity equates to message, file, sql query).</p>
	  <p>You need a solution that can be clustered simply by installing & copying of XML configuration files</p>
    </article>
	<article id="a2" class="detail">
      <h3>When is the StreamHorizon not the right choice for your project?</h3>
      <p>If you require mainframe connectors (EBCDIC) or any other type of legacy technology connectivity</p>
	  <p>If you already have established teams with expertise in ETL tool of your choice which aren’t high burden for your project budget (assumption made is that you do not require massively parallel database loading functionality)</p>
    </article>
	<article id="a6" class="detail">
      <h3>What StreamHorizon platform brings to your existing Data Integration and Business Intelligence stack?</h3>
	  <p>
		StreamHorizon platform enables SQL developers and programmers to benefit from ETL framework which does not require intimate knowledge of complex ETL tools and yet offers all functionality of the same. This positions StreamHorizon as ideal candidate for strategic ETL solution.  
	  </p>
	  <p>
		In case that you already have existing Data Integration & Business Intelligence stack you could use StreamHorizon as:
		<ul>
			<li>Platform for massively parallel bulk loading of data into the database (avoiding unnecessary database locking)</li>
			<li>OLAP Integration - Platform for massively parallel data loading into OLAP server in both Real Time and Batch oriented architectures</li>
			<li>Hadoop to Non-Hadoop ETL bridge (and vice versa)</li>
			<li>Platform for scheduled massively parallel data extraction from any data source into any other data source of your choice</li>
			<li>Framework for massively parallel file2file, message2file, db2db, db2file, file2db, Thrift2db, Thrift2file, file2Hadoop, messaage2Hadoop, dabase2Hadoop, Hadoop2Hadoop, Hadoop2file, Hadoop2database... and any other possible connector (source/target) permutation ... </li>
			<li>StreamHorizon should be used as a simple to configure, robust ETL platform which will enable existing IT staff (with database and/or programming background) to become productive extremely quickly. No intimate knowledge of ETL tool is required as it is XML configuration based. Framework can be extended as desired by using OS scripts (all platforms supported), Java or SQL (statements and procedures)</li>
			<li>StreamHorizon is simpler to use than Market Leading ETL tools. StreamHorizon platform offers unrivalled performance and parallelism. This is not to say that StreamHorizon cannot be used in conjuction with other ETL frameworks. For example, any ETL framework can be used as a tool (to transform data) and create bulk files while StreamHorizon can be used as massively parallel loading framework into any database. Something that market lading ETL tools cannot natively do as efficiently as StreamHorizon is able to</li>
		</ul>
	  </p>
    </article>
	<article id="a7" class="detail">
      <h3>What is the difference between StreamHorizon and Leading Market ETL/Data Integration vendors?</h3>
	  <p>
		In simple terms, if you require optimal hardware utilization, simple setup (xml configuration), cheaper (but not less functional) alternative to leading ETL & Data Integration tools 
		with high parallelism & high data processing throughput, without any need for ETL tool specific language & knowledge, StreamHorizon is the right ETL tool for your project.
	  </p>
	  <p>
	  <ul>
		<li>High throughput, massively parallel ETL platform capable of processing very large data volumes</li>
		<li>Data throughput of 1+ million records per second (running on a single commodity server)</li>
		<li>Running on BigData, cloud deployable, available as <b>Software as a Service</b></li>
		<li>Quick time to market (enables you to deliver pilot project within single week)</li>
		<li>Hardware efficient - Unrivalled efficiency ratio of data processing throughput achievable on low specification hardware</li>
		<li>No specific ETL knowledge of platform is required as StreamHorizon is fully configurable via single configuration XML file</li>
		<li>IT staff with basic IT knowledge can configure, setup & deliver Data Warehousing & Data Integration projects</li>
		<li>Ideal outsourcing candidate due to low level of skill required to operate StreamHorizon platform</li>
		<li>Simple Environment, Release/Revert and Upgrade Risk management, fully supporting Agile methodologies</li>
		<li>Low cost of ownership (low Business As Usual costs)</li>
		</ul>
	  </p>
    </article>
	<article id="a3" class="detail">
      <h3>How is StreamHorizon licensed?</h3>
      <p>
	  StreamHorizon licensing is based on:
	  <ul>
		<li>Number of CPU’s, CPU type and memory resources of your hardware</li>
		<li>Number of total number of StreamHorizon instances and total number of threads used for each instance</li>
	  </ul>
		Client makes a choice which option of licensing will be signed. Usually that is cheaper licence type of the two listed above. 
		Large corporate clients can be offered <b>Bulk licence agreement</b> which is effectively payment of agreed fixed amount which allows client to deploy agreed number of StreamHorizon deployments.
		Please <a href="contact.php">contact us</a> with details of your CPU, CPU type, memory details and number of instances of StreamHorizon you wish to run. We will reply to you with pricing options to choose from.
		</p>
    </article>
	<article id="a5" class="detail">
      <h3>What is the recommended way to start using StreamHorizon?</h3>
	  <p>
		We recommend that you first download our <a href="./trial_download.php" target="_blank">demo</a> and test deploy StreamHorizon. Demo sample configurations provided will give you solid idea how simple StreamHorizon is to configure and operate as a platform.
	  </p>
	  <p>
By deploying StreamHorizon demo at adequate hardware you will get idea of power of the platform in terms of data throughput and processing efficiency.
Quick browse of XML tutorial provided in <i>config</i> folder of the StreamHorizon distribution will give you good idea about source and target connectors you could use and all tuning parameters of the platform which are designed to bring maximum performance out of your hardware.
Upon decision to proceed with purchase of StreamHorizon platform our technical staff will be in touch with you do discuss best deployment strategy, licensed copy of software including guides and manuals will be provided. 
	  </p>
    </article>
	<article id="a4" class="detail">
      <h3>What are minimum system requirements for running StreamHorizon?</h3>
	  <p>
		Minimal requirements are:
		<ul>
			<li>Any mainstream operating system (Linux, Windows, Solaris)</li>
			<li>100MB of disk space</li>
			<li>Decent CPU</li>
			<li>500MB of RAM</li>
			<li>Java 1.7 or higher</li>
		</ul>
	  </p>
      <p>
		We recommend heap allocation of 4-8GB per StreamHorizon instance. Number of CPU’s and memory is down to limitation of your server and required throughput of your deployment.
		In <i>scavenging mode</i> of operation, in which workstations are utilized to act as ETL processing engines, StreamHorizon can be run with allocated 500MB RAM and single thread.
		For highest efficiency we recommend 64 bit JDK.
	  </p>
    </article>
  </section>
</div>
<?php include 'footer.php' ?>