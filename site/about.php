<!DOCTYPE html>
<html lang="en">
<head>
<title>Threeglav | About</title>
<?php include 'header.php' ?>
<div class="about center part clearfix">
  <header class="title">
    <p class="fleft"></p>
	<p class="fright"><a href="contact.php" class="more arrow">Contact Us</a></p>
  </header>
  <aside class="column4 mright">
    <p class="mbottom">Strive not to be a success, but rather to be of value.</p>
    <menu>
    <ul>
      <li><a href="#navteam" class="arrow more">Our team</a></li>
      <li><a href="#navphilo" class="arrow more">The problem</a></li>
      <li><a href="#navplace" class="arrow more">Our response</a></li>
    </ul>
    </menu> </aside>
  <section class="columnthird content"> <img src="img/team.jpg" alt="" />
    <article id="navteam" class="detail">
      <h2>Our team</h2>
      <p>We are small team of engineers focused on application of technology in Data Processing, Data Integration and Business Intelligence domain.</p>
	  <p>Our industry backgrounds are Investment Banking, Insurance and Telecommunication businesses.</p>
	  <p>Our aim is to simplify Data Processing & Data Integration to a level where low skilled IT staff will be able to deliver IT projects without trade-off in elegance, simplicity and most importantly performance of the applications.</p>
	  <p>Ability to deliver on such ambitious objectives comes from in depth experience gained from numerous Data Warehousing & Data Integration projects we have been part of.</p>
    </article>
    <article id="navphilo" class="detail">
      <h2>The Problem</h2>
      <p>- Data Integration can typically be characterised as a set of large (in numbers), interdependent and usually trivial units of ETL transformations/code. Above stated spells 'high complexity' for majority of Data Integration, Business Intelligence and Data Processing projects.</p>
	  <p>- Data Processing commonly faces performance issues, long load times, SLA breaks, long processing windows. Consequence of batch oriented rather than real time (low latency) design paradigms & thinking.</p>
	  <p>- Business Intelligence users often require much smaller query latencies from the ones delivered.</p>
    </article>
    <article id="navplace" class="detail">
      <h2>Our Solution</h2>
      <p>- We have created generic platform which is simple to setup and deliver. Time to market of project delivery is being measured in day(s) rather than months. Skills required for delivery are reduced almost to the one of common sense and truly basic 101 IT knowledge.</p>
	  <p>- We have delivered highly performing ETL platform, which eliminates performance issues even for volumes typical only for Financial Exchanges, Time Series Analysis & Real Time Risk Valuations. Desktops running on our platform have more processing bandwidth than commodity servers with state of the art (read complex and expensive) ETL tools and couple of dozens of CPU’s.</p>
	  <p>- Performance of our platform (throughput measured in millions of records per second) allows database experts to index their Data Mart to extent previously unimaginable (5+ indexes per single fact table) while still having real time ETL processing in place. </p>
	  <p>This ability further simplifies Business Intelligence stack by eliminating the need to have MOLAP or other alternatives acting as query accelerators behind their relational databases (aggregate tables or equivalents). It also reduces complexity of the Business Intelligence stack and further reduces budget spend on hardware, licences and most importantly complexity of solution overall (reduction of man-day costs).</p>
      <p>By bringing together simplicity of implementation (by reducing development to simple customisation) and processing performance we aim to make impact in Data Processing & Data Integration IT Industry.</p>
    </article>
  </section>
</div>
<?php include 'footer.php' ?>