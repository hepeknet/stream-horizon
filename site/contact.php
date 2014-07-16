<!DOCTYPE html>
<html lang="en">
<head>
<title>StreamHorizon | Contact</title>
<?php include 'header.php' ?>
<div class="contact center part clearfix">
  <header class="title">
    <p class="fleft"></p>
	<p class="fright"><a href="resources.php" class="more arrow">Try StreamHorizon now</a></p>
  </header>
  <aside class="column4 mright">
    <p class="mbottom">We would be happy to talk to you and answer questions you may have...</p>
    <p class="mbottom"> 
	  <a href="mailto:info@stream-horizon.com">info@stream-horizon.com</a><br >
      <a href="mailto:support@stream-horizon.com">support@stream-horizon.com</a><br >
      <a href="mailto:sales@stream-horizon.com">sales@stream-horizon.com</a><br >
	</p>
	<!--
    <div class="map mbottom"><a href="#"><img src="img/map.jpg" alt="" /></a></div>
	-->
  </aside>
  <section class="columnthird content">
   <?php
if (!isset($_POST["email"])){
  ?>
    <h2>Drop us a message</h2>
    <p class="more"></p>
    <form id="contact_form" class="contact_form" action="contact.php" method="post" name="contact_form">
      <ul class="contact_ie9">
        <li>
          <label for="name">Your name:</label>
          <input type="text" name="name" id="name" required class="required" >
        </li>
        <li>
          <label for="email">Your corporate email:</label>
          <input type="email" name="email" id="email" required class="required email">
        </li>
        <li>
          <label for="message">Message:</label>
          <textarea name="message" id="message" cols="40" rows="6" required  class="required" ></textarea>
        </li>
        <li>
          <button type="submit" id="submit" class="button fright">Send it</button>
		  <div class="small">
			Your details will not be used for Sales & Marketing purposes and will not be passed to affiliates and partners of StreamHorizon Group
		  </div>
        </li>
      </ul>
    </form>
 <?php 
  } else {
  // the user has submitted the form
  // Check if the "from" input field is filled out
  if (isset($_POST["email"])){
    $from = $_POST["email"]; // sender
    $name = $_POST["name"];
    $message = $_POST["message"];
    // message lines should not exceed 70 characters (PHP rule), so wrap it
    $message = wordwrap($message, 70);
    // send mail
    mail("borisha.zivkovic@gmail.com","Web message from $name",$message,"From: $from\n");
    echo "Thank you for contacting us. One of our staff members will contact you as soon as possible!";
    }
  }
?>
  </section>
</div>
<?php include 'footer.php' ?>