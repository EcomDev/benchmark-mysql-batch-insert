<?php
/**
 * Copyright © EcomDev B.V. All rights reserved.
 * See LICENSE for license details.
 */

declare(strict_types=1);

namespace EcomDev\BenchmarkMySQLBatchInsert;

use PDO;
use Zend\Db\Adapter\Adapter;

class BenchmarkApplication
{
    /**
     * @var Adapter
     */
    private $adapter;

    public function __construct(Adapter $adapter)
    {
        $this->adapter = $adapter;
    }

    public function createTable(): void
    {
        $this->adapter->query(
            'CREATE TABLE bench_data (
                    id INT UNSIGNED AUTO_INCREMENT, 
                    unique_value VARCHAR(255), 
                    some_text_field VARCHAR(1000), 
                    some_int INT UNSIGNED NOT NULL,
                    UNIQUE KEY(unique_value),
                    PRIMARY KEY(id)
                );',
            'execute'
        );
    }

    public function benchmark(int $batchSize, int $dataSize, InsertMethod ... $methods): array
    {
        $results = [];
        foreach ($methods as $method) {
            $this->adapter->query('DROP DATABASE IF EXISTS benchmark', 'execute');
            $this->adapter->query('CREATE DATABASE benchmark', 'execute');
            $this->adapter->query('USE benchmark', 'execute');
            $this->createTable();


            $results[get_class($method)]['new'] = $this->executeMethod($batchSize, $method,
                ['unique_value', 'some_text_field', 'some_int'],
                $this->uniqueDataGenerator($dataSize)
            );

            $results[get_class($method)]['update'] = $this->executeMethod($batchSize, $method,
                ['id', 'unique_value', 'some_text_field', 'some_int'],
                $this->collidingDataGenerator($dataSize)
            );

            $this->adapter->query('USE information_schema',  'execute');
        }

        $info = [
            ['Algorithm', 'Insert (s)', 'Update (s)']
        ];

        foreach ($results as $key => $typeOfTests) {
            $info[] = [$key, sprintf('%0.5f s', $typeOfTests['new']), sprintf('%0.5f s', $typeOfTests['update'])];
        }

        return $info;
    }

    private function uniqueDataGenerator(int $size): iterable
    {
        for ($i = 1; $i <= $size; $i ++) {
            yield [
                'SKU' . $i,
                'SOME TEXT at row ' . $i,
                rand(0, 10)
            ];
        }
    }

    private function collidingDataGenerator(int $size): iterable
    {
        for ($i = 1; $i <= $size; $i ++) {
            $index = rand(1, $i);
            yield [
                $index,
                'SKU' . $index,
                'SOME TEXT at row ' . $index,
                rand(0, 10)
            ];
        }
    }

    public static function create($mysqlHost, $mysqlUser, $mysqlPassword): self
    {
        return new self(new Adapter([
            'driver' => 'Pdo_Mysql',
            'hostname' => $mysqlHost,
            'username' => $mysqlUser,
            'password' => $mysqlPassword,
            'driver_options' => [
                PDO::MYSQL_ATTR_USE_BUFFERED_QUERY => 0
            ]
        ]));
    }

    private function executeMethod(int $batchSize, InsertMethod $method, array $columns, iterable $data): float
    {
        $result = microtime(true);
        $method->import(
            $this->adapter,
            'bench_data',
            $columns,
            $batchSize,
            $data
        );
        $result = microtime(true) - $result;

        return $result;
    }
}
