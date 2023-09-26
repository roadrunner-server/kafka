<?php

use Spiral\RoadRunner\Jobs\Jobs;
use Spiral\Goridge\RPC\RPC;
use Spiral\RoadRunner\Jobs\Queue\MemoryCreateInfo;
use Spiral\RoadRunner\Jobs\Consumer;
use Spiral\RoadRunner\Jobs\Task\ReceivedTaskInterface;

ini_set('display_errors', 'stderr');
require dirname(__DIR__) . "/vendor/autoload.php";

// Jobs service
$jobs = new Jobs(RPC::create('tcp://127.0.0.1:6001'));

$queue1 = $jobs->create(new MemoryCreateInfo(
    name: 'example',
    priority: 10,
    prefetch: 100,
));

$queue1->resume();

// Create task prototype with default headers
$task1 = $queue1->create('ping', '{"site": "https://example.com"}') // Create task with "echo" name
    ->withHeader('attempts', 4) // Number of attempts to execute the task
    ->withHeader('retry-delay', 10); // Delay between attempts

// Push "echo" task to the queue
$task1 = $queue1->dispatch($task1);

// Select "local" pipeline from jobs
$queue2 = $jobs->connect('local');
$queue2->resume();

// Create task prototype with default headers
$task = $queue2->create('ping', '{"site": "https://example.com"}') // Create task with "echo" name
    ->withHeader('attempts', 4) // Number of attempts to execute the task
    ->withHeader('retry-delay', 10); // Delay between attempts

// Push "echo" task to the queue
$task = $queue2->dispatch($task);

$consumer = new Spiral\RoadRunner\Jobs\Consumer();

while ($task = $consumer->waitTask()) {
    $task->complete();
}
