package model

import "time"

type OpenGraphModel struct {
	Type        string
	Title       string
	SiteName    string
	Description string
	Author      string
	Image       string
	Url         string
	Filename    string
	Etag        string
}

type FileUploadInfo struct {
	Id          int        `json:"id" db:"id"`
	FileId      int64      `json:"fileId" db:"file_id"`
	FileSize    int64      `json:"fileSize" db:"file_size"`
	FileName    string     `json:"fileName" db:"file_name"`
	Ext         string     `json:"ext" db:"ext"`
	MimeType    string     `json:"mimeType" db:"mime_type"`
	CreatedTime int64      `json:"createdTime" db:"created_time"`
	UpdateTime  int64      `json:"updateTime" db:"updated_time"`
	CreatedAt   *time.Time `json:"createdAt" db:"created_at"`
	UpdatedAt   *time.Time `json:"updatedAt" db:"updated_at"`
}
