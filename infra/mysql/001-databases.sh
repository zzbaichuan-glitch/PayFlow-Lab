#!/bin/sh
set -eu

case "${MYSQL_USER:-}" in
  ''|*[!a-zA-Z0-9_]* )
    echo "MYSQL_USER must contain only letters, digits, and underscore" >&2
    exit 1
    ;;
esac

mysql --protocol=socket -uroot -p"${MYSQL_ROOT_PASSWORD}" <<SQL
CREATE DATABASE IF NOT EXISTS payflow CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
CREATE DATABASE IF NOT EXISTS dtm CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
CREATE DATABASE IF NOT EXISTS dtm_barrier CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
GRANT ALL PRIVILEGES ON payflow.* TO '${MYSQL_USER}'@'%';
GRANT ALL PRIVILEGES ON dtm.* TO '${MYSQL_USER}'@'%';
GRANT ALL PRIVILEGES ON dtm_barrier.* TO '${MYSQL_USER}'@'%';
FLUSH PRIVILEGES;
SQL
