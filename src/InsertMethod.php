<?php
declare(strict_types=1);

/**
 * Copyright © EcomDev B.V. All rights reserved.
 * See LICENSE for license details.
 */

namespace EcomDev\BenchmarkMySQLBatchInsert;

use Zend\Db\Adapter\Adapter;

interface InsertMethod
{
    public function import(Adapter $adapter, string $tableName, array $columns, int $batchSize, iterable $rows): void;
}
