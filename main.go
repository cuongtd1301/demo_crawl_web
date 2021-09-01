package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/PuerkitoBio/goquery"
)

type OpenGraphModel struct {
	Type        string
	Title       string
	SiteName    string
	Description string
	Author      string
	Image       string
	Url         string
}

// var contentsTag = cascadia.MustCompile("p, h1, h2, h3, h4, h5, h6")

func main() {
	fmt.Println("---------------- Start crawl website--------------------")
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Vui lòng nhập URL: ")
	url, err := reader.ReadString('\n')
	if err != nil {
		log.Fatal(err)
	}
	url = strings.TrimSpace(url)
	// Crawl website using http and goquery
	res, err := http.Get(url)
	if err != nil {
		log.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		log.Fatalf("status code error: %d %s", res.StatusCode, res.Status)
	}
	// Load the HTML document
	doc, err := goquery.NewDocumentFromReader(res.Body)
	if err != nil {
		log.Fatal(err)
	}
	openGraphModel := ParseDoc(doc)

	// Write to data to output.json
	file, _ := json.MarshalIndent(openGraphModel, " ", " ")
	// log.Println(string(file))
	_ = ioutil.WriteFile("output.json", file, 0644)
}

func ParseDoc(doc *goquery.Document) (openGraphModel OpenGraphModel) {
	metaAttr := findMetaAttr(doc)
	if metaAttr == "" {
		return
	}
	doc.Find("meta").Each(func(i int, el *goquery.Selection) {
		// type
		value, _ := el.Attr(metaAttr)
		if strings.EqualFold(value, "og:type") {
			openGraphModel.Type, _ = el.Attr("content")
		}
		// Title
		if strings.EqualFold(value, "og:title") {
			openGraphModel.Title, _ = el.Attr("content")
		}
		// siteName
		if metaAttr == "name" {
			if strings.Contains(value, ":site") {
				openGraphModel.SiteName, _ = el.Attr("content")
			}
		} else if metaAttr == "property" {
			if strings.EqualFold(value, "og:site_name") {
				openGraphModel.SiteName, _ = el.Attr("content")
			}
		}
		// description
		if strings.EqualFold(value, "og:description") {
			openGraphModel.Description, _ = el.Attr("content")
		}
		// author
		if strings.Contains(value, "author") {
			openGraphModel.Author, _ = el.Attr("content")
		}
		// image
		if strings.EqualFold(value, "og:image") {
			openGraphModel.Image, _ = el.Attr("content")
		}
		// url
		if strings.EqualFold(value, "og:url") {
			openGraphModel.Url, _ = el.Attr("content")
		}
	})
	return
}

func findMetaAttr(doc *goquery.Document) (metaAttr string) {
	// name
	doc.Find("meta").Each(func(i int, el *goquery.Selection) {
		value, exists := el.Attr("name")
		if exists {
			if strings.Contains(value, "og:") {
				metaAttr = "name"
			}
		}
	})
	// property
	doc.Find("meta").Each(func(i int, el *goquery.Selection) {
		value, exists := el.Attr("property")
		if exists {
			if strings.Contains(value, "og:") {
				metaAttr = "property"
			}
		}
	})
	return
}
