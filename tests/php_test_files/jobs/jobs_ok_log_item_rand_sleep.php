<?php

/**
 * @var Goridge\RelayInterface $relay
 */

use Spiral\Goridge;
use Spiral\RoadRunner;
use Spiral\Goridge\StreamRelay;
use Spiral\RoadRunner\Jobs\Consumer;
use Spiral\RoadRunner\Jobs\Serializer\JsonSerializer;

error_reporting(E_ERROR);
ini_set('display_errors', 'stderr');
require dirname(__DIR__) . "/vendor/autoload.php";

$consumer = new Spiral\RoadRunner\Jobs\Consumer();

while ($task = $consumer->waitTask()) {
    try {
        $val = random_int(10000, 100000);
        usleep($val);

        error_log("php consumed:".$task->getPayload());
        $task->complete();
    } catch (\Throwable $e) {
        $task->error((string)$e);
    }
}
