package service

import (
	"context"
	"crawlweb/infrastructure"
	"crawlweb/model"
	"crawlweb/utils"
	"log"
	"time"
)

func Insert(info model.FileUploadInfo) error {
	db := infrastructure.GetDB()

	now := time.Now()
	info.CreatedAt = &now
	info.CreatedTime = now.Unix()
	info.FileId = utils.GenUuid()

	ctxTimeout, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()
	_, err := db.NamedExecContext(ctxTimeout, `INSERT INTO file_upload_infos (file_size, file_name, ext, mime_type) VALUES (:file_size, :file_name, :ext, :mime_type)`, &info)
	if err != nil {
		log.Println(err)
	}
	return nil
}
