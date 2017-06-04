# mysqlcut
Утилита mysqlcut позволяет вырезать из дампа mysql базы данных не нужные данные.

###### Пример использования:

```bash
unxz -c somedb.sql.xz | mysqlcut -e="logs,logs_extra" | mysql -u root -p somedb
```
