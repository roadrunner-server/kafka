<?php

use Spiral\Goridge\RPC\RPC;
use Spiral\RoadRunner\Jobs\Jobs;
use Spiral\RoadRunner\Jobs\KafkaOptions;
use Spiral\RoadRunner\Jobs\Queue\Kafka\PartitionOffset;

ini_set('display_errors', 'stderr');
require dirname(__DIR__) . "/vendor/autoload.php";

$jobs = new Jobs(RPC::create('tcp://127.0.0.1:6001'));
$queue = $jobs->connect('test-1');

$queue->dispatch(
    $queue->create(
        'my-name',
        ['foo' => 'bar'],
        new KafkaOptions(
            topic: 'mytopic',
            offset: PartitionOffset::OFFSET_NEWEST
        )
    )
);
