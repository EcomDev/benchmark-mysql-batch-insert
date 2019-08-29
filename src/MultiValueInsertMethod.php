<?php
/**
 * Copyright © EcomDev B.V. All rights reserved.
 * See LICENSE for license details.
 */

declare(strict_types=1);

namespace EcomDev\BenchmarkMySQLBatchInsert;


use Zend\Db\Adapter\Adapter;

class MultiValueInsertMethod implements InsertMethod
{

    public function import(Adapter $adapter, string $tableName, array $columns, int $batchSize, iterable $rows): void
    {
        $baseSql = sprintf(
            'INSERT INTO %s (%s) VALUES (%%s) ON DUPLICATE KEY UPDATE %s',
            $adapter->platform->quoteIdentifier($tableName),
            implode(',', array_map([$adapter->platform,'quoteIdentifier'], $columns)),
            implode(',', array_map(function ($column) use ($adapter) {
                return sprintf('%1$s = VALUES(%1$s)', $adapter->platform->quoteIdentifier($column));
            }, $columns))
        );

        $adapter->driver->getConnection()->beginTransaction();
        $batchValues = [];
        foreach ($rows as $row) {
            $batchValues[] = $adapter->platform->quoteValueList($row);

            if (count($batchValues) === $batchSize) {
                $adapter->query('BEGIN');
                $adapter->query(sprintf($baseSql, implode('),(', $batchValues)), 'execute');
                $adapter->query('END');
                $batchValues = [];
            }
        }
        $adapter->driver->getConnection()->commit();
    }
}
