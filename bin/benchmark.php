<?php
/**
 * Copyright © EcomDev B.V. All rights reserved.
 * See LICENSE for license details.
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use EcomDev\BenchmarkMySQLBatchInsert\BenchmarkApplication;
use EcomDev\BenchmarkMySQLBatchInsert\MultiValueInsertMethod;
use EcomDev\BenchmarkMySQLBatchInsert\PreparedSingleValueInsertMethod;
use EcomDev\BenchmarkMySQLBatchInsert\PreparedStatementMultiValueInsert;
use League\CLImate\CLImate;

$console = new CLImate();
$console->description('Set of benchmarks to validate performance of import');
$console->arguments->add(
    [
        'database-host' => [
            'description' => 'MySQL host name',
            'prefix' => 'h',
            'defaultValue' => '127.0.0.1',
        ],
        'database-user' => [
            'description' => 'MySQL user',
            'prefix' => 'u',
            'defaultValue' => 'root'
        ],
        'database-password' => [
            'description' => 'MySQL password',
            'prefix' => 'p',
            'defaultValue' => ''
        ],
        'data-size' => [
            'description' => 'Dataset Size',
            'prefix' => 's',
            'castTo' => 'int',
            'defaultValue' => 10000
        ],
        'batch-size' => [
            'description' => 'Batch Size',
            'prefix' => 'b',
            'castTo' => 'int',
            'defaultValue' => 1000
        ]
    ]
);

$console->arguments->parse($_SERVER['argv']);

try {
    $app = BenchmarkApplication::create(
        $console->arguments->get('database-host'),
        $console->arguments->get('database-user'),
        $console->arguments->get('database-password')
    );

    $batchSize = (int)$console->arguments->get('batch-size');
    $dataSetSize = (int)$console->arguments->get('data-size');

    $console->out(sprintf('<yellow>Starting benchmark for <green>%s</green> records with batch <green>%s</green></yellow>', $dataSetSize, $batchSize));

    $console->table(
        $app->benchmark(
            $batchSize,
            $dataSetSize,
            new MultiValueInsertMethod(),
            new PreparedStatementMultiValueInsert()
        )
    );
} catch (Exception $exception) {
    $console->error($exception->getMessage());
    $console->tab(1);
    $console->out($exception->getTraceAsString());

    $console->usage();
}





