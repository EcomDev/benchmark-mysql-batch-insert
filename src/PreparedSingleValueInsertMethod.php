<?php
/**
 * Copyright © EcomDev B.V. All rights reserved.
 * See LICENSE for license details.
 */

declare(strict_types=1);

namespace EcomDev\BenchmarkMySQLBatchInsert;

use Zend\Db\Adapter\Adapter;

class PreparedSingleValueInsertMethod implements InsertMethod
{
    public function import(Adapter $adapter, string $tableName, array $columns, int $batchSize, iterable $rows): void
    {
        $baseSql = sprintf(
            'INSERT IGNORE INTO %s (%s) VALUES (%s) -- ON DUPLICATE KEY UPDATE %s',
            $adapter->platform->quoteIdentifier($tableName),
            implode(',', array_map([$adapter->platform,'quoteIdentifier'], $columns)),
            rtrim(str_repeat('?,', count($columns)), ','),
            implode(',', array_map(function ($column) use ($adapter) {
                return sprintf('%1$s = VALUES(%1$s)', $adapter->platform->quoteIdentifier($column));
            }, $columns))
        );

        $statement = $adapter->createStatement($baseSql);
        $statement->prepare();

        $adapter->driver->getConnection()->beginTransaction();
        $rowCount = 0;
        foreach ($rows as $row) {
            $statement->execute($row);
            $rowCount ++;

            if ($rowCount === $batchSize) {
                $adapter->driver->getConnection()->commit();
                $adapter->driver->getConnection()->beginTransaction();
            }
        }

        $adapter->driver->getConnection()->commit();
    }
}
