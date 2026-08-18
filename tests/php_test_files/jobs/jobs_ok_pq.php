<?php

ini_set("display_errors", "stderr");
require dirname(__DIR__) . "/vendor/autoload.php";

$consumer = new Spiral\RoadRunner\Jobs\Consumer();

while ($task = $consumer->waitTask()) {
    try {
        sleep(2);
        $task->complete();
    } catch (\Throwable $e) {
        $task->error((string)$e);
    }
}
