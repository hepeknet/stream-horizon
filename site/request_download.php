<!DOCTYPE html>
<html lang="en">
<head>
<title>Threeglav | Request Download</title>
<?php include 'header.php' ?>
<div class="contact center part clearfix">
  <header class="title">
    <p class="fleft">Request Download</p>
	<p class="fright"><a href="contact.php" class="more arrow">Get Commercial Support</a></p>
  </header>
  
  <section class="columnthird content">
   <?php
if (!isset($_POST["email"])){
  ?>
    <h2>Get the latest version (<?php echo SH_LATEST_VERSION ?>) of StreamHorizon platform</h2>
    <p class="more">Fill in the form below and we will email you download link to try out StreamHorizon.</p>
    <form id="contact_form" class="contact_form" action="request_download.php" method="post" name="contact_form">
      <ul class="contact_ie9">
        <li>
          <label for="email">Your email:</label>
          <input type="email" name="email" id="email" required class="required email">
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
  if (isset($_POST["email"])){
    $from = $_POST["email"]; // sender
    // message lines should not exceed 70 characters (PHP rule), so wrap it
	$message = "Dear potential customer, \n\r here is your link to download trial version of StreamHorizon \n\r http://threeglav.com/downloads/qw324a8902jlsd4lnljolkjdsgsd232/stream-horizon-" . SH_LATEST_VERSION . "-dist.zip";
	$message .= "\n\r Do not hesitate to contact us in case you have any questions \n\r looking forward to cooperate with you \n\r StreamHorizon team \n\r support@threeglav.com";
	$notifyMessage = "$from asked for trial version of StreamHorizon DPP and download link was sent";
    $message = wordwrap($message, 70);
    // send mail
    mail($from,"Your trial version of StreamHorizon DataProcessingPlatform",$message,"From: support@threeglav.com\n");
	mail("borisha.zivkovic@gmail.com","Someone asked for trial version of StreamHorizon DataProcessingPlatform",$notifyMessage,"From: support@threeglav.com\n");
	mail("mladen.golubovic@gmail.com","Someone asked for trial version of StreamHorizon DataProcessingPlatform",$notifyMessage,"From: support@threeglav.com\n");
    echo "<p>Thank you for your interest in StreamHorizon. An email has been sent to you with download link.</p><p>Do not hesitate to contact us in case you have any questions at support@threeglav.com!</p>";
    }
  }
?>
  </section>
</div>
<?php include 'footer.php' ?>