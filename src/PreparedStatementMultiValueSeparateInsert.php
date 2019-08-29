<?php
/**
 * Copyright © EcomDev B.V. All rights reserved.
 * See LICENSE for license details.
 */

declare(strict_types=1);

namespace EcomDev\BenchmarkMySQLBatchInsert;


use Zend\Db\Adapter\Adapter;

class PreparedStatementMultiValueSeparateInsert implements InsertMethod
{
    public function import(Adapter $adapter, string $tableName, array $columns, int $batchSize, iterable $rows): void
    {
        $baseSql = sprintf(
            'INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s',
            $adapter->platform->quoteIdentifier($tableName),
            implode(',', array_map([$adapter->platform,'quoteIdentifier'], $columns)),
            implode(',', array_fill(0, $batchSize, sprintf('(%s)', rtrim(str_repeat('?,', count($columns)), ',')))),
            implode(',', array_map(function ($column) use ($adapter) {
                return sprintf('%1$s = VALUES(%1$s)', $adapter->platform->quoteIdentifier($column));
            }, $columns))
        );

        $columnCount = count($columns);

        $adapter->driver->getConnection()->beginTransaction();
        $parameters = [];
        $batchId = 1;
        foreach ($rows as $row) {
            $parameters[] = $adapter->platform->quoteValueList($row);

            if ((count($parameters) / $columnCount) === $batchSize) {
                $adapter->query('BEGIN');
                $adapter->createStatement(sprintf($baseSql . ' -- batch %s', $batchId))->execute($parameters);
                $adapter->query('END');
            }

            $batchId++;

            $parameters = [];
        }

        $adapter->driver->getConnection()->commit();
    }
}
