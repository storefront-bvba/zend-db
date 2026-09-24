# Storefront's adaptation of Zend Framework DB 1
## Changes made vs the original `zf1/zend-db` package.
1. Minimum PHP version increased to 7.4
2. We only support MySQL (via `PDO` and not `mysqli`). The MsSQL (`Pdo_Mssql`, `Sqlsrv`), PostgreSQL and SQLite adapters
   were removed: they could not load anymore, because they do not implement `insertOnDuplicate()`.
3. Depencancies removed:
  - Zend_Loader
  - Zend_Registry
4. We improved the `quoteInto()` method
5. We improved the `describeTable()` method, so it returns the length for integer columns
6. Added function `insertOnDuplicate()`