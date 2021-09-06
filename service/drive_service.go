package service

import (
	"io"
	"log"
	"net/http"
	"os"

	"google.golang.org/api/drive/v3"
)

const (
	DownloadURL = "https://docs.google.com/uc?export=download&id="
	ViewURL     = "https://docs.google.com/open?export=download&id="
)

// CreateFile Create new file to drive
func CreateFile(service *drive.Service, name string, mineType string, content io.Reader, parentId string) (*drive.File, error) {
	f := &drive.File{
		MimeType: mineType,
		Name:     name,
		Parents:  []string{parentId},
	}
	file, err := service.Files.Create(f).Media(content).Do()

	if err != nil {
		log.Println("createFile: Could not create file: ", err.Error())
		return nil, err
	}

	return file, nil
}

// CreateFolder Create new folder to drive
func CreateFolder(service *drive.Service, name string, parentID string) (*drive.File, error) {
	d := &drive.File{
		Name:     name,
		MimeType: "application/vnd.google-apps.folder",
		Parents:  []string{parentID},
	}

	file, err := service.Files.Create(d).Do()

	if err != nil {
		log.Println("Could not create dir: " + err.Error())
		return nil, err
	}

	return file, nil
}

// Delete folder or file
func DeleteFile(service *drive.Service, fileId string) error {
	err := service.Files.Delete(fileId).Do()
	if err != nil {
		return err
	}
	return nil
}

// DownloadFile from drive
func DownloadFileFromDrive(fileId, fileName string) error {
	output, err := os.Create(fileName)
	if err != nil {
		log.Println("cannot open file: ", err)
		return err
	}
	defer output.Close()

	c := http.Client{
		CheckRedirect: func(r *http.Request, via []*http.Request) error {
			r.URL.Opaque = r.URL.Path
			return nil
		},
	}
	response, err := c.Get(DownloadURL + fileId)
	if err != nil {
		log.Println("Error while downloading: ", err)
		return err
	}
	defer response.Body.Close()

	_, err = io.Copy(output, response.Body)
	if err != nil {
		log.Println("Error while copy file: ", err)
		return err
	}
	return nil
}
