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
	"golang.org/x/net/html"
)

type CrawlModel struct {
	Text      []string
	ImgSrc    []string
	ImgSrcset []string
}

// var contentsTag = cascadia.MustCompile("p, h1, h2, h3, h4, h5, h6")
var keyword = []string{"Goal", "Hậu trường bóng đá tuần qua:", "Hậu trường bóng đá tuần qua"}

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
	// Remove script and style
	doc.Find("script").Each(func(i int, el *goquery.Selection) {
		el.Remove()
	})
	doc.Find("style").Each(func(i int, el *goquery.Selection) {
		el.Remove()
	})
	crawlModel := CrawlModel{}
	// Find text contains keyword
	crawlModel.Text = findText(doc.Selection)
	// Find src and srcset of img tag
	doc.Find("img").Each(func(i int, el *goquery.Selection) {
		src, exists := el.Attr("src")
		if exists {
			crawlModel.ImgSrc = append(crawlModel.ImgSrc, src)
		}
		srcset, exists := el.Attr("srcset")
		if exists {
			crawlModel.ImgSrcset = append(crawlModel.ImgSrcset, srcset)
		}
	})
	// Write to data to output.json
	file, _ := json.MarshalIndent(crawlModel, " ", " ")
	_ = ioutil.WriteFile("output.json", file, 0644)
}

func findText(s *goquery.Selection) []string {
	var list []string
	var f func(*html.Node)
	f = func(n *html.Node) {
		if n.Type == html.TextNode {
			if checkContainsText(n.Data, keyword) {
				list = append(list, n.Data)
			}
		}
		if n.FirstChild != nil {
			for c := n.FirstChild; c != nil; c = c.NextSibling {
				f(c)
			}
		}
	}
	for _, n := range s.Nodes {
		f(n)
	}
	return list
}

func checkContainsText(str string, list []string) bool {
	for i := range list {
		if strings.Contains(str, list[i]) {
			return true
		}
	}
	return false
}
