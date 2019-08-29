<?php
/**
 * Copyright © EcomDev B.V. All rights reserved.
 * See LICENSE for license details.
 */

declare(strict_types=1);

namespace EcomDev\BenchmarkMySQLBatchInsert;


use Zend\Db\Adapter\Adapter;

class PreparedStatementMultiValueInsert implements InsertMethod
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

        $statement = $adapter->createStatement($baseSql);
        $statement->prepare();

        $adapter->driver->getConnection()->beginTransaction();
        $parameters = [];
        foreach ($rows as $row) {
            $parameters[] = $adapter->platform->quoteValueList($row);

            if ((count($parameters) / $columnCount) === $batchSize) {
                $adapter->query('BEGIN');
                $statement->execute($parameters);
                $adapter->query('END');
            }

            $parameters = [];
        }

        $adapter->driver->getConnection()->commit();
    }
}
