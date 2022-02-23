# Storefront's adaptation of Zend Framework DB 1
## Changes made vs the original `zf1/zend-db` package.
1. Minimum PHP version increased to 7.4
2. We only support:
  - MySQL (via `PDO` and not `mysqli`)
  - MsSQL
  - PostgreSQL
  - SQLite
3. Depencancies removed:
  - Zend_Loader
  - Zend_Registry


