package infrastructure

import (
	"fmt"
	"log"

	"github.com/jmoiron/sqlx"
	// _ "github.com/lib/pq"
	_ "github.com/go-sql-driver/mysql"
)

var (
	username = "root"
	password = "0000"
	protocol = "tcp"
	ip       = "127.0.0.1"
	dbPort   = "3306"
	dbName   = "demo"

	db *sqlx.DB
)

var schema = `CREATE TABLE IF NOT EXISTS file_upload_infos (
	id INT AUTO_INCREMENT PRIMARY KEY,
	file_id BIGINT NOT NULL UNIQUE,
	file_size BIGINT UNSIGNED,
	file_name VARCHAR(255),
	ext VARCHAR(255),
	mime_type VARCHAR(255),
	created_time INT(11) UNSIGNED NOT NULL,
	updated_time INT(11) UNSIGNED NOT NULL,
	created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
	updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);`

func loadDatabase() {
	var err error
	db, err = sqlx.Connect("mysql", fmt.Sprintf("%v:%v@%v(%v:%v)/%v", username, password, protocol, ip, dbPort, dbName))
	if err != nil {
		log.Fatalln("Unable to connect database: ", err)
	}

	db.MustExec(schema)
}

func GetDB() *sqlx.DB {
	return db
}
