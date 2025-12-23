<?php

declare(strict_types=1);

class Zend_Db_Table_Column_Describe
{

    public const int MIN_TINYINT_VALUE = -128;
    public const int MAX_TINYINT_VALUE = 127;
    public const int MIN_SMALLINT_VALUE = -32768;
    public const int MAX_SMALLINT_VALUE = 32767;
    public const MIN_MEDIUMINT_VALUE = -8388608;
    public const MAX_MEDIUMINT_VALUE = 8388607;
    public const int MIN_INT_VALUE = -2147483648;
    public const int MAX_INT_VALUE = 2147483647;
//    public const int MIN_BIGINT_VALUE = -;
//    public const int MAX_BIGINT_VALUE = ;
//    public const int MAX_BIGINT_UNSIGNED_VALUE = ;
    public const int MAX_TINYINT_UNSIGNED_VALUE = 255;
    public const int MAX_SMALLINT_UNSIGNED_VALUE = 65535;
    public const int MAX_MEDIUMINT_UNSIGNED_VALUE = 16777215;
    public const int MAX_INT_UNSIGNED_VALUE = 4294967295;

    public const int MAX_TEXT_LENGTH = 65535;
    public const int MAX_MEDIUMTEXT_LENGTH = 16_777_215;

    private array $describe;

    public function __construct(array $describe)
    {
        $this->describe = $describe;
    }

    public function getTableName(): string
    {
        return $this->describe['TABLE_NAME'];
    }

    public function getColumnName(): string
    {
        return $this->describe['COLUMN_NAME'];
    }

    public function getDataType(): string
    {
        $r = $this->getDataTypeFull();
        $r = str_replace(' unsigned', '', $r);
        return $r;
    }

    public function getDataTypeFull(): string
    {
        $r = $this->describe['DATA_TYPE'];
        return $r;
    }

    public function getMaxTextLength(): ?int
    {
        if ($this->isTextField()) {
            if (isset($this->describe['LENGTH']) && is_numeric($this->describe['LENGTH'])) {
                return (int)$this->describe['LENGTH'];
            } else {
                $type = $this->getDataType();
                switch ($type) {
                    case 'text':
                        return self::MAX_TEXT_LENGTH;
                    case 'mediumtext':
                        return self::MAX_MEDIUMTEXT_LENGTH;
                    default:
                        throw new \RuntimeException('Unsupported case: ' . $type);
                }
            }
        } else {
            return null;
        }
    }

    public function getMinNumericValue(): int
    {
        if ($this->isNumericField()) {
            if ($this->isUnsigned()) {
                return 0;
            } else {
                $type = $this->getDataType();
                switch ($type) {
                    // TODO: PHP may not be able to handle bigint. Might need to be presented as string?
//                    case 'bigint':
//                        return self::MIN_BIGINT_VALUE;
                    case 'int':
                        return self::MIN_INT_VALUE;
                    case 'mediumint':
                        return self::MIN_MEDIUMINT_VALUE;
                    case 'smallint':
                        return self::MIN_SMALLINT_VALUE;
                    case 'tinyint':
                        return self::MIN_TINYINT_VALUE;
                    default:
                        throw new \RuntimeException('Unsupported case.');
                }
            }
        } else {
            throw new \RuntimeException('Column "' . $this->getColumnName() . '" is not numeric.');
        }
    }

    public function getMaxNumericValue(): int
    {
        if ($this->isNumericField()) {
            $type = $this->getDataType();
            if ($this->isUnsigned()) {
                switch ($type) {
                    // TODO: PHP may not be able to handle bigint. Might need to be presented as string?
//                    case 'bigint':
//                        return self::MAX_BIGINT_UNSIGNED_VALUE;
                    case 'int':
                        return self::MAX_INT_UNSIGNED_VALUE;
                    case 'mediumint':
                        return self::MAX_MEDIUMINT_UNSIGNED_VALUE;
                    case 'smallint':
                        return self::MAX_SMALLINT_UNSIGNED_VALUE;
                    case 'tinyint':
                        return self::MAX_TINYINT_UNSIGNED_VALUE;
                    default:
                        throw new \RuntimeException('Unsupported case.');
                }
            } else {
                switch ($type) {
                    // TODO: PHP may not be able to handle bigint. Might need to be presented as string?
//                    case 'bigint':
//                        return self::MIN_BIGINT_VALUE;
                    case 'int':
                        return self::MAX_INT_VALUE;
                    case 'mediumint':
                        return self::MAX_MEDIUMINT_VALUE;
                    case 'smallint':
                        return self::MAX_SMALLINT_VALUE;
                    case 'tinyint':
                        return self::MAX_TINYINT_VALUE;
                    default:
                        throw new \RuntimeException('Unsupported case.');
                }
            }
        } else {
            throw new \RuntimeException('Column "' . $this->getColumnName() . '" is not numeric.');
        }
    }

    public function isTextField(): bool
    {
        $dt = $this->getDataType();
        if (in_array($dt, Zend_Db_Adapter_Pdo_Mysql::TYPES_TEXT, true)) {
            return true;
        }
        return false;
    }

    public function isNumericField(): bool
    {
        return $this->isIntegerField() || $this->isDecimalField();
    }

    public function isIntegerField(): bool
    {
        $dt = $this->getDataType();
        if (in_array($dt, Zend_Db_Adapter_Pdo_Mysql::TYPES_INTEGER, true)) {
            return true;
        }
        return false;
    }

    public function isDecimalField(): bool
    {
        $dt = $this->getDataType();
        if (in_array($dt, Zend_Db_Adapter_Pdo_Mysql::TYPES_DECIMAL, true)) {
            return true;
        }
        return false;
    }

    public function isTimestamp(): bool
    {
        $dt = $this->getDataType();
        if (in_array($dt, Zend_Db_Adapter_Pdo_Mysql::TYPES_TIMESTAMP, true)) {
            return true;
        }
        return false;
    }

    public function isUnsigned(): bool
    {
        return str_contains($this->getDataTypeFull(), ' unsigned');
    }

    public function isNullable(): bool
    {
        return $this->describe['NULLABLE'];
    }

    public function isBoolean(): bool
    {
        $type = $this->getDataType();
        return $type === 'tinyint';
    }

    public function isPrimaryKey(): bool
    {
        return $this->describe['IDENTITY'] === true;
    }

    public function isGenerated(): bool
    {
        return str_contains($this->describe['EXTRA'], 'GENERATED');
    }

}