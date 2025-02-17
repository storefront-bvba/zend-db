<?php
/**
 * Zend Framework
 *
 * LICENSE
 *
 * This source file is subject to the new BSD license that is bundled
 * with this package in the file LICENSE.txt.
 * It is also available through the world-wide-web at this URL:
 * http://framework.zend.com/license/new-bsd
 * If you did not receive a copy of the license and are unable to
 * obtain it through the world-wide-web, please send an email
 * to license@zend.com so we can send you a copy immediately.
 *
 * @category   Zend
 * @package    Zend_Db
 * @subpackage Adapter
 * @copyright  Copyright (c) 2005-2015 Zend Technologies USA Inc. (http://www.zend.com)
 * @license    http://framework.zend.com/license/new-bsd     New BSD License
 * @version    $Id$
 */


/**
 * @see Zend_Db_Adapter_Pdo_Abstract
 */


/**
 * Class for connecting to MySQL databases and performing common operations.
 *
 * @category   Zend
 * @package    Zend_Db
 * @subpackage Adapter
 * @copyright  Copyright (c) 2005-2015 Zend Technologies USA Inc. (http://www.zend.com)
 * @license    http://framework.zend.com/license/new-bsd     New BSD License
 */
class Zend_Db_Adapter_Pdo_Mysql extends Zend_Db_Adapter_Pdo_Abstract
{

    const array TYPES_INTEGER = ['tinyint', 'int', 'smallint', 'mediumint', 'bigint'];
    const array TYPES_DECIMAL = ['float', 'double', 'real', 'decimal'];
    const array TYPES_TIMESTAMP = ['timestamp', 'datetime'];
    const array TYPES_NUMERIC = ['tinyint', 'int', 'smallint', 'mediumint', 'bigint', 'float', 'double', 'real', 'decimal'];
    const array TYPES_TEXT = ['tinytext', 'text', 'smalltext', 'mediumtext', 'bigtext', 'char', 'varchar'];

    /**
     * PDO type.
     *
     * @var string
     */
    protected $_pdoType = 'mysql';

    /**
     * Keys are UPPERCASE SQL datatypes or the constants
     * Zend_Db::INT_TYPE, Zend_Db::BIGINT_TYPE, or Zend_Db::FLOAT_TYPE.
     *
     * Values are:
     * 0 = 32-bit integer
     * 1 = 64-bit integer
     * 2 = float or decimal
     *
     * @var array Associative array of datatypes to values 0, 1, or 2.
     */
    protected $_numericDataTypes = [
        Zend_Db::INT_TYPE => Zend_Db::INT_TYPE,
        Zend_Db::BIGINT_TYPE => Zend_Db::BIGINT_TYPE,
        Zend_Db::FLOAT_TYPE => Zend_Db::FLOAT_TYPE,
        'INT' => Zend_Db::INT_TYPE,
        'INTEGER' => Zend_Db::INT_TYPE,
        'MEDIUMINT' => Zend_Db::INT_TYPE,
        'SMALLINT' => Zend_Db::INT_TYPE,
        'TINYINT' => Zend_Db::INT_TYPE,
        'BIGINT' => Zend_Db::BIGINT_TYPE,
        'SERIAL' => Zend_Db::BIGINT_TYPE,
        'DEC' => Zend_Db::FLOAT_TYPE,
        'DECIMAL' => Zend_Db::FLOAT_TYPE,
        'DOUBLE' => Zend_Db::FLOAT_TYPE,
        'DOUBLE PRECISION' => Zend_Db::FLOAT_TYPE,
        'FIXED' => Zend_Db::FLOAT_TYPE,
        'FLOAT' => Zend_Db::FLOAT_TYPE
    ];

    /**
     * Override _dsn() and ensure that charset is incorporated in mysql
     * @see Zend_Db_Adapter_Pdo_Abstract::_dsn()
     */
    protected function _dsn()
    {
        $dsn = parent::_dsn();
        if (isset($this->_config['charset'])) {
            $dsn .= ';charset=' . $this->_config['charset'];
        }
        return $dsn;
    }

    /**
     * Creates a PDO object and connects to the database.
     *
     * @return void
     */
    protected function _connect()
    {
        if ($this->_connection) {
            return;
        }
        parent::_connect();
    }

    /**
     * @return string
     */
    public function getQuoteIdentifierSymbol()
    {
        return "`";
    }

    /**
     * Returns a list of the tables in the database.
     *
     * @return array
     */
    public function listTables()
    {
        return $this->fetchCol('SHOW TABLES');
    }

    /**
     * Returns the column descriptions for a table.
     *
     * The return value is an associative array keyed by the column name,
     * as returned by the RDBMS.
     *
     * The value of each array element is an associative array
     * with the following keys:
     *
     * SCHEMA_NAME      => string; name of database or schema
     * TABLE_NAME       => string;
     * COLUMN_NAME      => string; column name
     * COLUMN_POSITION  => number; ordinal position of column in table
     * DATA_TYPE        => string; SQL datatype name of column
     * DEFAULT          => string; default expression of column, null if none
     * NULLABLE         => boolean; true if column can have nulls
     * LENGTH           => number; length of CHAR/VARCHAR
     * SCALE            => number; scale of NUMERIC/DECIMAL
     * PRECISION        => number; precision of NUMERIC/DECIMAL
     * UNSIGNED         => boolean; unsigned property of an integer type
     * PRIMARY          => boolean; true if column is part of the primary key
     * PRIMARY_POSITION => integer; position of column in primary key
     * IDENTITY         => integer; true if column is auto-generated with unique values
     * COMMENT          => string
     */
    public function describeTable(string $tableName, string $schemaName = null): array
    {
        if ($schemaName) {
            $sql = 'SHOW FULL COLUMNS FROM ' . $this->quoteIdentifier("$schemaName.$tableName", true);
        } else {
            $sql = 'SHOW FULL COLUMNS FROM ' . $this->quoteIdentifier($tableName, true);
        }
        $stmt = $this->query($sql);
        $result = $stmt->fetchAll(Zend_Db::FETCH_NUM);

        $field = 0;
        $type = 1;
        $collation = 2;
        $null = 3;
        $key = 4;
        $default = 5;
        $extra = 6;
        $comment = 8;

        $desc = array();
        $i = 1;
        $p = 1;
        foreach ($result as $row) {
            list(
                $length, $scale, $precision, $unsigned, $primary, $primaryPosition, $identity
                ) = array(null, null, null, null, false, null, false);
            if (preg_match('/unsigned/', $row[$type])) {
                $unsigned = true;
            }
            if (preg_match('/^((?:var)?char)\((\d+)\)/', $row[$type], $matches)) {
                $row[$type] = $matches[1];
                $length = $matches[2];
            } elseif (preg_match('/^decimal\((\d+),(\d+)\)/', $row[$type], $matches)) {
                $row[$type] = 'decimal';
                $precision = $matches[1];
                $scale = $matches[2];
            } elseif (preg_match('/^float\((\d+),(\d+)\)/', $row[$type], $matches)) {
                $row[$type] = 'float';
                $precision = $matches[1];
                $scale = $matches[2];
            } elseif (preg_match('/^((?:big|medium|small|tiny)?int)\((\d+)\)/', $row[$type], $matches)) {
                $row[$type] = $matches[1];
                // The optional argument of a MySQL int type is not precision
                // or length; it is only a hint for display width.

                // WOUTER: This line is the only change we made in this method
                $length = $matches[2];
            }
            if (strtoupper($row[$key]) === 'PRI') {
                $primary = true;
                $primaryPosition = $p;
                if ($row[$extra] === 'auto_increment') {
                    $identity = true;
                } else {
                    $identity = false;
                }
                ++$p;
            }
            $commentValue = null;
            if ($row[$comment]) {
                $commentValue = $row[$comment];
            }
            $collationValue = null;
            if ($row[$collation]) {
                $collationValue = $row[$collation];
            }

            $extraValue = $row[$extra];

            $desc[$this->foldCase($row[$field])] = array(
                'SCHEMA_NAME' => null, // @todo
                'TABLE_NAME' => $this->foldCase($tableName),
                'COLUMN_NAME' => $this->foldCase($row[$field]),
                'COLUMN_POSITION' => $i,
                'DATA_TYPE' => $row[$type],
                'DEFAULT' => $row[$default],
                'NULLABLE' => (bool)($row[$null] == 'YES'),
                'LENGTH' => $length,
                'SCALE' => $scale,
                'PRECISION' => $precision,
                'UNSIGNED' => $unsigned,
                'PRIMARY' => $primary,
                'PRIMARY_POSITION' => $primaryPosition,
                'IDENTITY' => $identity,
                'EXTRA' => $extraValue,
                'COMMENT' => $commentValue,
                'COLLATION' => $collationValue
            );
            ++$i;
        }
        return $desc;
    }

    public function describeColumn(string $tableName, string $columnName, string $schemaName = null): Zend_Db_Table_Column_Describe
    {
        $describe = $this->describeTable($tableName, $schemaName);
        $describe = $describe[$columnName] ?? null;

        if ($describe === null) {
            throw new Zend_Db_Adapter_Exception('Column "' . $columnName . '" does not exist in table "' . $tableName . '".');
        } else {
            return new Zend_Db_Table_Column_Describe($describe);
        }
    }

    /**
     * @return Zend_Db_Table_Column_Describe[]
     */
    public function describeTableAsObjects(string $tableName, string $schemaName = null): array
    {
        $r = [];
        $describe = $this->describeTable($tableName, $schemaName);

        foreach($describe as $column){
            $obj = new Zend_Db_Table_Column_Describe($column);
            $r[$obj->getColumnName()] = $obj;
        }
        return $r;
    }


    /**
     * Adds an adapter-specific LIMIT clause to the SELECT statement.
     *
     * @param string $sql
     * @param integer $count
     * @param integer $offset OPTIONAL
     * @return string
     */
    public function limit($sql, $count, $offset = 0)
    {
        $count = intval($count);
        if ($count <= 0) {
            /** @see Zend_Db_Adapter_Exception */

            throw new Zend_Db_Adapter_Exception("LIMIT argument count=$count is not valid");
        }

        $offset = intval($offset);
        if ($offset < 0) {
            /** @see Zend_Db_Adapter_Exception */

            throw new Zend_Db_Adapter_Exception("LIMIT argument offset=$offset is not valid");
        }

        $sql .= " LIMIT $count";
        if ($offset > 0) {
            $sql .= " OFFSET $offset";
        }

        return $sql;
    }

    public function insertOnDuplicate(string $table, array $data, array $fields = []): ?int
    {
        // extract and quote col names from the array keys
        $row = reset($data); // get first element from data array
        $bind = []; // SQL bind array
        $values = [];

        if (is_array($row)) { // Array of column-value pairs
            $canReturnAutoIncrementId = false;
            $cols = array_keys($row);
            foreach ($data as $row) {
                if (array_diff($cols, array_keys($row))) {
                    throw new Zend_Db_Exception('Invalid data for insert');
                }
                $values[] = $this->_prepareInsertData($row, $bind);
            }
            unset($row);
        } else { // Column-value pairs
            $cols = array_keys($data);
            $values[] = $this->_prepareInsertData($data, $bind);
            $canReturnAutoIncrementId = true;
        }

        if (empty($fields)) {
            $fields = $cols;
        }

        // prepare ON DUPLICATE KEY conditions
        $pkColumnName = $this->getAutoIncrementColumnName($table);
        $updateFields = [];
        foreach ($fields as $k => $v) {
            $field = $value = null;
            if (!is_numeric($k)) {
                $field = $this->quoteIdentifier($k);
                if ($v instanceof Zend_Db_Expr) {
                    $value = $v->__toString();
                } elseif (is_string($v)) {
                    $value = sprintf('VALUES(%s)', $this->quoteIdentifier($v));
                } elseif (is_numeric($v)) {
                    $value = $this->quoteInto('?', $v);
                }
            } elseif (is_string($v)) {
                $value = sprintf('VALUES(%s)', $this->quoteIdentifier($v));
                $field = $this->quoteIdentifier($v);
            }
            if ($field === $pkColumnName) {
                throw new \RuntimeException('Updating PK column is not supported yet. This needs to be implemented still.');
            }
            if ($field && $value) {
                $updateFields[] = sprintf('%s = %s', $field, $value);
            }
        }

        if ($canReturnAutoIncrementId && $pkColumnName) {
            $updateFields[] = '`' . $pkColumnName . '` = LAST_INSERT_ID(`' . $pkColumnName . '`)';
        }

        $insertSql = $this->_getInsertSqlQuery($table, $cols, $values);
        if ($updateFields) {
            $insertSql .= ' ON DUPLICATE KEY UPDATE ' . implode(', ', $updateFields);
        }
        // execute the statement and return the number of affected rows
        $stmt = $this->query($insertSql, array_values($bind));

        $affectedRows = $stmt->rowCount();
        $affectedRowId = $this->lastInsertId($table);

        if (is_numeric($affectedRowId)) {
            return (int)$affectedRowId;
        }
        return null;
    }

    /**
     * Return insert sql query
     *
     * @param string $tableName
     * @param array $columns
     * @param array $values
     * @return string
     */
    protected function _getInsertSqlQuery($tableName, array $columns, array $values)
    {
        $tableName = $this->quoteIdentifier($tableName, true);
        $columns = array_map([$this, 'quoteIdentifier'], $columns);
        $columns = implode(',', $columns);
        $values = implode(', ', $values);

        $insertSql = sprintf('INSERT INTO %s (%s) VALUES %s', $tableName, $columns, $values);

        return $insertSql;
    }

    /**
     * Prepare insert data
     *
     * @param mixed $row
     * @param array $bind
     * @return string
     */
    protected function _prepareInsertData($row, &$bind)
    {
        if (is_array($row)) {
            $line = [];
            foreach ($row as $value) {
                if ($value instanceof Zend_Db_Expr) {
                    $line[] = $value->__toString();
                } else {
                    $line[] = '?';
                    $bind[] = $value;
                }
            }
            $line = implode(', ', $line);
        } elseif ($row instanceof Zend_Db_Expr) {
            $line = $row->__toString();
        } else {
            $line = '?';
            $bind[] = $row;
        }

        return sprintf('(%s)', $line);
    }


    public function getAutoIncrementColumnName(string $tableName): ?string
    {
        $describe = $this->describeTable($tableName);
        foreach ($describe as $column) {
            if ($column['EXTRA'] === 'auto_increment') {
                return $column['COLUMN_NAME'];
            }
        }
        return null;
    }


    public function getMaxLength(string $table, string $column): ?int
    {
        $describe = $this->describeTable($table);
        $describe = $describe[$column];
        $length = $describe['length'];
        return $length;
    }

    public function getMaxValue(string $table, string $column): ?int
    {
        // TODO implement
        $describe = $this->describeTable($table);
        $describe = $describe[$column];
        $type = $describe['type'];
        switch ($type) {
            case 'int':
                return 123;
            default:
                throw new \RuntimeException('Not implemented yet');
        }
    }

    public function getMinValue(string $table, string $column): ?int
    {
        // TODO implement
        $describe = $this->describeTable($table);
        $describe = $describe[$column];
        $type = $describe['type'];
        switch ($type) {
            case 'int':
                return 123;
            default:
                throw new \RuntimeException('Not implemented yet');
        }
    }
}
