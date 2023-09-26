<?php

/**
 * @var Goridge\RelayInterface $relay
 */

use Spiral\Goridge;
use Spiral\RoadRunner;
use Spiral\Goridge\StreamRelay;
use Spiral\RoadRunner\Jobs\Consumer;
use Spiral\RoadRunner\Jobs\Serializer\JsonSerializer;

ini_set('display_errors', 'stderr');
require dirname(__DIR__) . "/vendor/autoload.php";

$consumer = new Spiral\RoadRunner\Jobs\Consumer();

while ($task = $consumer->waitTask()) {
    try {
        $headers = $task->getHeaders();
        $total_attempts = (int)$task->getHeaderLine("attempts") + 1;

        if ($total_attempts > 3) {
            $task->complete();
        } else {
            $task->withHeader("attempts",$total_attempts)->withDelay(5)->fail("failed", true);
        }
    } catch (\Throwable $e) {
        $task->error((string)$e);
    }
}
