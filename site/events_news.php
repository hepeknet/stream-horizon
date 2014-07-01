<!DOCTYPE html>
<html lang="en">
<head>
<title>StreamHorizon | Events/News</title>
<?php include 'header.php' ?>
<div class="about center part clearfix">
  <header class="title">
    <p class="fleft"></p>
	<p class="fright"><a href="contact.php" class="more arrow">Contact Us</a></p>
  </header>
  <section class="columnthird content">

	<article class="post">
      <h3><a href="#" class="post-title">Infinite Dimensional collections - Hadoop & Non-Hadoop platforms</a></h3>
      <div class="meta">
		<!--
        <p>Posted on <span class="time">June 28, 2014</span></p>
		-->
      </div>
      <div class="entry">
        <p>
			StreamHorizon supports Hadoop (HBase) and non-Hadoop (Redis)
frameworks which enable StreamHorizon users to manipulate infinitely
large key-value collections (Dimensional 'caches') for extremely high
cardinality lookups/dimensions. This feature comes in addition to
already supported In Memory Data Grid solutions like Infinispan &
Hazelcast.
		</p>
      </div>
    </article>
  
	<article class="post">
      <h3><a href="#" class="post-title">Apache Thrift Connectivity</a></h3>
      <div class="meta">
		<!--
        <p>Posted on <span class="time">June 28, 2014</span></p>
		-->
      </div>
      <div class="entry">
        <p>
			StreamHorizon delivers Thrift connector in an effort to deliver
scalable cross-language services development connectivity. Thrift
enables StreamHorizon to be seamlessly integrated with  Java, C#, C++,
Python, PHP, Perl, Haskell, Smalltalk, JavaScript, OCamel, Delphi,
Node.js and other languages.
		</p>
      </div>
    </article>
  
	<article class="post">
      <h3><a href="#" class="post-title">Hadoop Ecosystem - Full Data Processing Integration</a></h3>
      <div class="meta">
		<!--
        <p>Posted on <span class="time">June 28, 2014</span></p>
		-->
      </div>
      <div class="entry">
        <p>
			StreamHorizon has positioned itself as Data Processing Bridge between
Non-Hadoop and Hadoop platforms by implementation of Hadoop specific
connectivity for Read & Write functionality. Apart from being able to
talk to both Non-Hadoop & Hadoop platforms within single ETL
processes, StreamHorizon can fully operate on Hadoop
Ecosystem.
		</p>
      </div>
    </article>
  
	<article class="post">
      <h3><a href="#" class="post-title">StreamHorizon I/O Fine Tuning Framework</a></h3>
      <div class="meta">
		<!--
        <p>Posted on <span class="time">June 28, 2014</span></p>
		-->
      </div>
      <div class="entry">
        <p>
			Quest to overcome I/O bottlenecks of a typical deployment has lead
StreamHorizon development teams to extend tuning configuration settings which
will help Development and Infrastructure teams to deliver maximum
performance given constraints of their I/O system.
		</p>
		<p>
			Following two modes of I/O bottleneck testing enable Development teams
to estimate performance of their ETL stack prior to investing money
into new I/O infrastructure.
		</p>
		<h4>I/O Tuning Approach 1: Exclusion of I/O from your ETL processes</h4>
		<p>
		 StreamHorizon enables you to test your
ETL stream by simply configuring ETL processes to read source data
which is cached internally within StreamHorizon JVM (this effectively eliminates Read
I/O related latency). Data output of StreamHorizon ETL
framework can also be discarded which effectively eliminates write I/O from your ETL pipeline (as data is
transformed but not persisted).
		</p>
		<h4>I/O Tuning Approach 2: Fine Read & Write I/O buffer tuning</h4>
		<p>
			Ability to set size of your ETL Read and Write buffers which in
combination with number of parallel ETL processes configured in
StreamHorizon derives maximum performance from your current I/O
system. Buffer tuning combined with ETL process parallelism enables
developers to come up with I/O utilization patterns which are optimal
for a given I/O system.

		</p>
      </div>
    </article>
  
    <article class="post">
      <h3><a href="#" class="post-title">Increasing JDBC bulk load performance by 50%</a></h3>
      <div class="meta">
		<!--
        <p>Posted on <span class="time">June 28, 2014</span></p>
		-->
      </div>
      <div class="entry">
        <p>
			StreamHorizon R&D team has developed & filed patent for design
which effectively increases throughput of single JDBC connection by
50%. StreamHorizon ETL threads are able to share single JDBC
connection which operates in bulk mode which boosts the throughput
without creating additional locks against the target table.
		</p>
      </div>
    </article>
	
	<article class="post">
      <h3><a href="#" class="post-title">Shield against Non-Atomic I/O Operations</a></h3>
      <div class="meta">
		<!--
        <p>Posted on <span class="time">June 28, 2014</span></p>
		-->
      </div>
      <div class="entry">
        <p>
			StreamHorizon releases feed and bulk file acceptance delay
configuration settings. Settings are crucial for Non-Atomic I/O
operations, this functionality effectively delivers atomicity of I/O
operations.
		</p>
      </div>
    </article>
	
	<article class="post">
      <h3><a href="#" class="post-title">No I/O ETL</a></h3>
      <div class="meta">
		<!--
        <p>Posted on <span class="time">June 28, 2014</span></p>
		-->
      </div>
      <div class="entry">
        <p>
			StreamHorizon delivers connector which utilize Unix/Linux pipes which
enable StreamHorizon ETL streams to deliver data to the target
database via "bulk load" concepts (like External
Tables or SQL*Loader) without persisting bulk files as middle step
(thereby, fully eliminating I/O from ETL processing pipeline).
		</p>
      </div>
    </article>
	
	<article class="post">
      <h3><a href="#" class="post-title">In Memory Data Grid & StreamHorizon</a></h3>
      <div class="meta">
		<!--
        <p>Posted on <span class="time">June 28, 2014</span></p>
		-->
      </div>
      <div class="entry">
        <p>
			After extensive performance testing StreamHorizon incorporates
Infinispan and Hazelcast as preferred embedded In Memory Data Grid
(IMDG) solutions. Both Infinispan and Hazelcast are integrated, packaged and distributed
with official StreamHorizon releases. StreamHorizon architecture allows
embedding of other (custom) IMDG providers.
		</p>
      </div>
    </article>
  </section>
</div>
<?php include 'footer.php' ?>