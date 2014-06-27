<!DOCTYPE html>
<html lang="en">
<head>
<title>StreamHorizon | Request Download</title>
<?php include 'header.php' ?>
<div class="contact center part clearfix">
  <header class="title">
    <p class="fleft"></p>
	<p class="fright"><a href="contact.php" class="more arrow">Get Commercial Support</a></p>
  </header>
  
  <section class="columnthird content">
   <?php
if (!isset($_POST["email"])){
  ?>
    <h2>Get the latest version (<?php echo SH_LATEST_VERSION ?>) of StreamHorizon platform</h2>
	<p>Trial version of StreamHorizon binary can be downloaded <a href="./trial_download.php" target="_blank">here</a></p>
    <p class="more">Even better, fill in the form below and we will provide you with one free hour of consultation how to use StreamHorizon in your particular case!</p>
    <form id="contact_form" class="contact_form" action="request_download.php" method="post" name="contact_form">
      <ul class="contact_ie9">
	    <li>
          <label for="username">Full name:</label>
          <input type="text" name="username" id="username" required class="required">
        </li>
		<li>
          <label for="jobtitle">Job title:</label>
          <input type="text" name="jobtitle" id="jobtitle" required class="required">
        </li>
		<li>
          <label for="company">Company name:</label>
          <input type="text" name="company" id="company" required class="required">
        </li>
        <li>
          <label for="email">Corporate email:</label>
          <input type="email" name="email" id="email" required class="required email">
        </li>
		<li>
          <label for="country">Country:</label>
          <input type="text" name="country" id="country" required class="required">
        </li>
		<li>
          <label for="telephone">Telephone number:</label>
          <input type="text" name="telephone" id="telephone" required class="required">
        </li>
		<li>
          <label for="projectinfo">We would like to know more about your project (optional):</label>
          <textarea name="projectinfo" id="projectinfo" cols="40" rows="6" ></textarea>
        </li>
        <li>
          <button type="submit" id="submit" class="button fleft">Get Me The Link!</button>
        </li>
		<li>
			<br /><br /><br /><br />
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
  if (isset($_POST["email"]) && isset($_POST["username"])){
    $from = $_POST["email"]; // sender
	$username = $_POST["username"];
	$project = $_POST["projectinfo"];
	$phone = $_POST["telephone"];
	$company = $_POST["company"];
	$country = $_POST["country"];
	$title = $_POST["jobtitle"];
    // message lines should not exceed 70 characters (PHP rule), so wrap it
	$message = "Thank you for expressing interest in StreamHorizon Data Processing Platform. \n\rYour link to download trial version of StreamHorizon \n\r http://stream-horizon.com/downloads/qw324a8902jlsd4lnljolkjdsgsd232/stream-horizon-" . SH_LATEST_VERSION . "-dist.zip";
	$message .= "\n\r or \n\r http://stream-horizon.com/downloads/qw324a8902jlsd4lnljolkjdsgsd232/stream-horizon-" . SH_LATEST_VERSION . "-dist.tar.gz";
	$message .= "\n\rDo not hesitate to contact us in case you have any questions or suggestions. \n\r We are looking forward to further cooperation.\n\r\n\r StreamHorizon Team \n\r support@stream-horizon.com \n\r www.stream-horizon.com";
	$notifyMessage = "$from asked for trial version of StreamHorizon DPP and download link was sent. \n\r Name: $username \n\r Company: $company \n\r Job title: $title \n\r Phone: $phone \n\r Country: $country \n\r Project info: $project";
    $message = wordwrap($message, 70);
    // send mail
    // mail($from,"Your trial version of StreamHorizon Data Processing Platform",$message,"From: support@stream-horizon.com\n");
	mail("borisha.zivkovic@gmail.com","Someone asked for trial version of StreamHorizon DataProcessingPlatform",$notifyMessage,"From: support@stream-horizon.com\n");
	mail("mladen.golubovic@gmail.com","Someone asked for trial version of StreamHorizon DataProcessingPlatform",$notifyMessage,"From: support@stream-horizon.com\n");
    echo "<p>Thank you for expressing interest in StreamHorizon Data Processing Platform. An email will be sent to you with download link.</p><p>Do not hesitate to contact us in case you have any questions at <b>support@stream-horizon.com</b>!</p>";
    }
  }
?>
  </section>
</div>
<?php include 'footer.php' ?>