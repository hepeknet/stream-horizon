<!DOCTYPE html>
<html lang="en">
<head>
<title>StreamHorizon - Data Processing Platform</title>
<?php include 'header.php' ?>
</script>
<div class="main center">
  <section class="part clearfix">
    <header class="title clearfix">
      <p class="fleft"><h2>Data Processing & Data Integration Platform</h2></p>
      <a href="faq.php" class="arrow more fright">Find out more about StreamHorizon</a> 
	</header>
    <div class="column2 mright">
      <p>
		<ul>
			<li>Data Processing throughput of <b>1+ Million Records per second</b> (single commodity server)</li>
			<li>Quick Time to Market – deploy StreamHorizon & <b>deliver</b> your Data integration project <b>in a single week</b></li>
		</ul>
		<br>
		<ul>
			<li>Fully <b>Configurable</b> via XML – making a project <b>ideal outsourcing candidate</b></li>
			<li><b>Utilizes skills of your IT staff</b> - requires no ETL platform specific knowledge</li>
			<li>Read more in <a href="faq.php">Product FAQ section</a></li>
		</ul>
		<br>
		<ul>
			<li><b>Total Project Cost / Data Throughput</b> ratio = 0.2 (<b>20% of budget required</b> in comparison with Market Leaders)</li>
			<li><b>1 Hour Proof of Concept</b> – <a href="./resources.php">download</a> and test-run StreamHorizon’s demo Data Warehousing project</li>
		</ul>
		<br>
		<ul>
			<li>Runs on <b>Big Data</b> clusters: <a href="http://hadoop.apache.org/">Hadoop</a>, HDFS, <a href="http://kafka.apache.org/">Kafka</a>, <a href="https://storm.incubator.apache.org/">Storm</a>, Hive, <a href="http://www.cloudera.com/content/cloudera/en/products-and-services/cdh/impala.html">Impala</a> and more…</li>
			<li>Run your <b>StreamHorizon Data Processing Cluster</b> (ETL grid)</li>
			<li>Runs on <b>Compute Grid</b> (alongside grid libraries like Quant Library or any other)</li>
		</ul>
		<ul>
			<li>Horizontally & Vertically scalable, Highly Available (HA), Clusterable</li>
			<li>Running on Linux, Solaris, Windows, Compute Clouds (EC2 & others)</li>
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
  </section>
  <section class="part clearfix">
    <header class="title clearfix">
      <p class="fleft"><h3>The Problem</h3></p>
      <a href="contact.php" class="arrow more fright">Contact Us</a> </header>
    <article class="column3 mright services">
      
      <h5>Complexity</h5>
      <div class="sepmini"></div>
	  <p>
      <ul>
	   <li>Data Integration can typically be characterized as a set of large (in numbers), highly coupled and moderately complex units of ETL transformations/code</li>
	  </ul>
	  </p>
    </article>
    <article class="column3 mright services">
      
      <h5>Performance</h5>
      <div class="sepmini"></div>
      <p>
	  <ul>
		<li>Long loading time windows</li>
		<li>Frequent SLA breaks</li>
		<li><b>Domino Effect</b> execution & dependencies (Waterfall paradigm)</li>
	  </ul>
			...above are consequence of batch oriented rather than <i>Real Time</i> & <i>Data Streaming</i> design paradigms
	  </p>
    </article>
    <article class="column3 services">
     
      <h5>Query Latency</h5>
      <div class="sepmini"></div>
      <p>
		<ul>
			<li>Longer than desired query response times</li>
			<li>Inadequate Ad-Hoc query capability</li>
		</ul>
	  </p>
    </article>
  </section>
  <section class="part clearfix">
    <header class="title clearfix">
      <p class="fleft"><h3>Our Solution</h3></p>
	</header>
	<p><h4>Data Throughput of StreamHorizon platform enables:</h4></p>
    <article class="services">
	<p>
		<ul>
      <li>Indexing of Data Warehouses to extent previously unimaginable (4+ indexes per single fact table)</li>
	  <li>Extensively indexed database delivers load throughput of 50-80% compared to model without indexes, however, it reduces query latency (intentional sacrifice of load latency for query performance)</li>
	  <li>No need to utilize OLAP cubes or equivalents (In-Memory solutions) acting as <b>query accelerators</b>. Such solution are dependent on available memory and thereby impose limit to data volumes system can handle.</li>
	  <li>StreamHorizon fully supports <b>OLAP integration</b> (please refer to <a href="faq.php">Product FAQ page</a>). OLAP delivers slice & dice and drill-down/data pivoting capability via Excel or any other user front end tool.</li>
		</ul>
	</p>
    </article>
	<br>
	<article class="services">
	<p>
		<ul>
      <li>Horizontal scaling of In-Memory software comes with a price (increased latency) as queried data is collated from multiple servers into single resultset</li>
	  <li>No need to purchase exotic or specialist hardware appliances</li>
	  <li>No need to purchase In-Memory/OLAP hardware & licence</li>
		</ul>
	</p>
	</article>
	<br>
	<article class="services">
	  <p class="center">
		<table class="centeredTable noBorders">
			<caption>Run simplified software stack: <br /><br /></caption>
			<thead>
				<tr class="noBorders">
					<th class="noBorders">StreamHorizon + Vanilla Database</th>
				</tr>
			</thead>
			<tbody>
				<tr class="noBorders">
					<th class="noBorders">vs</th>
				</tr>
				<tr>
					<th class="noBorders">ETL Tool + Database (usually exotic) + OLAP/In-Memory solution (usually clustered)</th>
				</tr>
			</tbody>
		</table>
	  </p>
	  <br /><br />
    </article>
	<br>
	<article class="services">
	<p>
		<ul>
      <li>Time to market of project delivery - measured in days rather than months.</li>
	  <li>IT Skills required for development are reduced to basic IT knowledge.</li>
	  <li>Manage data volumes typical only for Financial Exchanges, Telecom blue chips and ISP’s.</li>
	  <li>Desktops running with StreamHorizon platform have more data processing bandwidth than commodity servers with state of the art (read complex and expensive) ETL tools and dozen of CPU’s.</li>
		</ul>
	</p>
    </article>
	<br>
	<article class="services">
	<p>
		<ul>
      <li>Generic & Adaptable ETL platform</li>
	  <li>Simple to setup (XML configuration)</li>
	  <li>Platform geared to deliver projects of high complexity (low latency or batch oriented fashion)</li>
		</ul>
	</p>
    </article>
  </section>
</div>
<?php include 'footer.php' ?>