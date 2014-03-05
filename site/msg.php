<!DOCTYPE html>
<html lang="en">
<title>StreamHorizon | MSG</title>
<head>
<?php include 'header.php' ?>
<div class="contact center part clearfix">
  <header class="title">
    <p class="fleft"></p>
  </header>
  <section class="columnthird content">
   <?php
if (!isset($_POST["message"])){
  ?>
    <p class="more"></p>
    <form id="contact_form" class="contact_form" action="msg.php" method="post" name="contact_form">
      <ul class="contact_ie9">
        <li>
          <label for="message">Message:</label>
          <textarea name="message" id="message" cols="40" rows="6" required  class="required" ></textarea>
        </li>
        <li>
          <button type="submit" id="submit" class="button fright">Send it</button>
        </li>
      </ul>
    </form>
 <?php 
  } else {
  // the user has submitted the form
  // Check if the "from" input field is filled out
  if (isset($_POST["message"])){
    $message = $_POST["message"];
    // message lines should not exceed 70 characters (PHP rule), so wrap it
    $message = wordwrap($message, 70);
    // send mail
    mail("borisha.zivkovic@gmail.com","Boreas",$message,"From: contact_web@stream-horizon.com\n");
    echo "Sent!";
    }
  }
?>
  </section>
</div>
<?php include 'footer.php' ?>