<?php include 'constants.php' ?>
<?php
$realFileName = "stream-horizon-" . SH_LATEST_VERSION . "-dist-trial.zip";
if(!isset($_GET["test"])){
	mail("mladen.golubovic@gmail.com","Someone just downloaded trial version of StreamHorizon DataProcessingPlatform",$notifyMessage,"From: real-download@stream-horizon.com\n");
	mail("borisha.zivkovic@gmail.com","Someone just downloaded trial version of StreamHorizon DataProcessingPlatform",$notifyMessage,"From: real-download@stream-horizon.com\n");
} else {
	mail("mladen.golubovic@gmail.com","B/M are testing download of trial version of StreamHorizon DataProcessingPlatform",$notifyMessage,"From: test-download@stream-horizon.com\n");
	mail("borisha.zivkovic@gmail.com","B/M are testing download of trial version of StreamHorizon DataProcessingPlatform",$notifyMessage,"From: test-download@stream-horizon.com\n");
}
$file = "downloads/trial_download/".$realFileName;
$fp = fopen($file, 'rb');

header("Content-Type: application/octet-stream");
header("Content-Disposition: attachment; filename=$realFileName");
header("Content-Length: " . filesize($file));
fpassthru($fp);
?>