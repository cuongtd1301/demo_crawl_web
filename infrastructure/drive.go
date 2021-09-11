package infrastructure

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"

	"golang.org/x/oauth2/google"
	"golang.org/x/oauth2/jwt"
	"google.golang.org/api/drive/v3"
)

const (
	MIME_Folder = "application/vnd.google-apps.folder"
	MIME_File   = "application/octet-stream"
	// MIME_Image_PNG = "image/png"
)

var driveService *drive.Service
var rootFolderDrive = "1PjsulGwMg2TuTwFJyqUWlElIjd0xf0YJ"

func loadDriveService() {
	var err error
	client := getClientDrive("./infrastructure/drive_secret.json")

	driveService, err = drive.New(client)

	if err != nil {
		log.Fatal("Unable to retrieve drive client: ", err)
	}
}

func getClientDrive(secretFile string) *http.Client {
	b, err := ioutil.ReadFile(secretFile)
	if err != nil {
		log.Fatal("error while reading the credential file", err)
	}
	var s = struct {
		Email      string `json:"client_email"`
		PrivateKey string `json:"private_key"`
	}{}

	json.Unmarshal(b, &s)
	config := &jwt.Config{
		Email:      s.Email,
		PrivateKey: []byte(s.PrivateKey),
		Scopes: []string{
			drive.DriveScope,
		},
		TokenURL: google.JWTTokenURL,
	}

	client := config.Client(context.Background())

	return client
}

// GetDriveService export drive service
func GetDriveService() *drive.Service {
	return driveService
}

// GetRootDrive export root drive string
func GetRootFolderDrive() string {
	return rootFolderDrive
}

func init() {
	loadDriveService()
	loadAwsService()
	loadDatabase()
	log.Println("Load parameters succussful!")
}
