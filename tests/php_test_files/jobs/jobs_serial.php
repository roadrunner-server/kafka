<?php

ini_set("display_errors", "stderr");
require dirname(__DIR__) . "/vendor/autoload.php";

$consumer = new Spiral\RoadRunner\Jobs\Consumer();

while ($task = $consumer->waitTask()) {
    try {
        $payload = $task->getPayload();
        [, $seq] = explode(":", $payload);

        usleep(((int)$seq % 2 === 0) ? 50000 : 0);

        if ($payload === "p1:5" && $task->getHeaderLine("attempts") === "") {
            $task->withHeader("attempts", "1")->withDelay(1)->fail("retry", true);
            continue;
        }

        $task->complete();
    } catch (\Throwable $e) {
        $task->error((string)$e);
    }
}
