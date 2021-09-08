package service

import (
	"crawlweb/infrastructure"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/lithammer/shortuuid"
)

func DownloadFile(URL, fileName string) error {
	//Get the response bytes from the url
	response, err := http.Get(URL)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode != 200 {
		return errors.New("Received non 200 response code")
	}
	//Create a empty file
	file, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer file.Close()

	//Write the bytes to the fiel
	_, err = io.Copy(file, response.Body)
	if err != nil {
		return err
	}

	return nil
}

func CreateFileAndSave(url string) (driveId string) {
	if url == "" {
		return
	}
	fileName := GenCode()
	filePath := "./temp/" + fileName
	err := DownloadFile(url, filePath)
	if err != nil {
		log.Println("Error:", err.Error())
		return
	}
	tempFile, err := os.Open("./" + filePath)
	if err != nil {
		log.Printf("Read file error: %+v\n", err)
		return
	}
	defer tempFile.Close()
	tmp, err := CreateFile(infrastructure.GetDriveService(), fileName, infrastructure.MIME_File, tempFile, infrastructure.GetRootFolderDrive())
	if err != nil {
		log.Println("error when create file to drive:", err)
		return
	}
	driveId = tmp.Id
	return
}

func GenCode() string {
	id := shortuuid.New()
	return strings.ToUpper(id[0:10])
}
