<?php include 'constants.php' ?>
<?php
$realFileName = "stream-horizon-" . SH_LATEST_VERSION . "-dist-trial.zip";
mail("borisha.zivkovic@gmail.com","Someone just downloaded trial version of StreamHorizon DataProcessingPlatform",$notifyMessage,"From: support@stream-horizon.com\n");
//mail("mladen.golubovic@gmail.com","Someone asked for trial version of StreamHorizon DataProcessingPlatform",$notifyMessage,"From: support@stream-horizon.com\n");
$file = "downloads/trial_download/".$realFileName;
$fp = fopen($file, 'rb');

header("Content-Type: application/octet-stream");
header("Content-Disposition: attachment; filename=$realFileName");
header("Content-Length: " . filesize($file));
fpassthru($fp);
?>