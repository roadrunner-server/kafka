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
        for ($i = 0; $i < 5; $i++) {
            $val = random_int(0, 2);
            if ($val % 3 == 0) {
                $task->complete();
            } else if ($val % 3 == 1) {
                $task->fail("failed");
            } else {
                $rr->error("error");
            }
        }

        $task->complete();
    } catch (\Throwable $e) {
        $task->error((string)$e);
    }
}
