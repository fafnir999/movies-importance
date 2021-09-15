package main

import (
	"archive/zip"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/PuerkitoBio/goquery"
	"github.com/gocolly/colly/v2/proxy"
	"io"
	"io/ioutil"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/gocolly/colly/v2"
)

type Movie struct {
	Link       string
	References []Reference
}

type Reference struct {
	Subject     string
	Description string
}

const filesInACacheDir = 5000
const cachePath = "imdb_cache"

func main() {
	c := colly.NewCollector(
		colly.Async(true),
	)

	c.Limit(&colly.LimitRule{DomainGlob: "*", Parallelism: 5})
	content, err := ioutil.ReadFile("var/proxies.txt")
	if err != nil {
		panic(err)
	}

	proxies := strings.Split(string(content), "\n")

	rp, err := proxy.RoundRobinProxySwitcher(proxies...)
	if err != nil {
		panic(err)
	}
	c.SetProxyFunc(rp)

	parseLogFile, err := os.OpenFile("var/references.txt", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0777)
	if err != nil {
		panic(err)
	}

	defer parseLogFile.Close()

	c.OnHTML("#connections_content > .list", func(element *colly.HTMLElement) {
		movieHtml, err := element.DOM.Html()
		if err != nil {
			fmt.Printf("Error during getting html: %v\n", err)
		}
		movieUrl := element.Request.URL.String()

		fmt.Printf("Parsing content of %s...\n", movieUrl)

		saveDataToCache(movieUrl, movieHtml)

		movie := Movie{Link: movieUrl}

		refsStarted := false
		element.DOM.Children().Each(func(i int, refSelection *goquery.Selection) {
			if refsStarted && refSelection.HasClass("soda") {
				html, err := refSelection.Html()
				if err != nil {
					fmt.Printf("Error during getting html: %v\n", err)
				}

				htmlParts := strings.Split(html, "<br/>")

				var ref Reference
				if len(htmlParts) > 1 {
					ref = Reference{Description: htmlParts[1]}
				} else {
					ref = Reference{Description: ""}
				}

				refSelection.Children().Each(func(i int, refElementSelection *goquery.Selection) {
					if val, exists := refElementSelection.Attr("href"); exists == true {
						ref.Subject = val
					}
				})
				movie.References = append(movie.References, ref)
			}

			if refSelection.Text() == "References " && refSelection.HasClass("li_group") {
				refsStarted = true
				return
			}

			if refsStarted == true && !refSelection.HasClass("soda") {
				refsStarted = false
			}
		})

		if len(movie.References) > 0 {
			saveMovieDataToFile(movieUrl, movie)
			if _, err = parseLogFile.WriteString(movieUrl + "\n"); err != nil {
				panic(err)
			}
		}
	})

	c.OnRequest(func(r *colly.Request) {
		fmt.Println("Visiting", r.URL.String())
	})

	parsedCounter := 0
	err = parseTsv("imdb-data/imdb-ids.tsv", 9000000, func(row []string, rowNumber int) {
		if parsedCounter%50000 == 0 {
			c.Wait()
		}
		imdbId := row[0]
		if _, err := os.Stat(createCacheFileFullPath(createCacheFilePath(imdbId, cachePath))); os.IsNotExist(err) {
			c.Visit(fmt.Sprintf("https://www.imdb.com/title/%s/movieconnections/", imdbId))
			parsedCounter = parsedCounter + 1
		} else {
			fmt.Printf("For movie %s cache exists\n", imdbId)
		}
	})

	if err == io.EOF {
		c.Wait()
	} else if err != nil {
		panic(err)
	}

	c.OnError(func(r *colly.Response, err error) {
		fmt.Println("Request URL:", r.Request.URL, "failed with response:", string(r.Body), "\nError:", err)
	})

	//spew.Dump(movies)
}

func createCacheFilePath(movieUrl string, prefix string) (string, string) {
	r, err := regexp.Compile("tt(\\d+)")
	if err != nil {
		fmt.Printf("%v\n", err)
	}

	submatch := r.FindAllStringSubmatch(movieUrl, 1)
	imdbId := submatch[0][0]
	movieNumber, err := strconv.Atoi(submatch[0][1])
	if err != nil {
		fmt.Printf("%v\n", err)
	}

	dirName := movieNumber / filesInACacheDir
	filePath := fmt.Sprintf("%s/%d/", prefix, dirName)

	return filePath, imdbId
}

func createCacheFileFullPath(dirPath string, fileName string) string {
	return fmt.Sprintf("%s %s", dirPath, fileName) + ".zip"
}

func saveDataToCache(movieUrl string, movieHtml string) {
	filePath, imdbId := createCacheFilePath(movieUrl, cachePath)

	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		err := os.Mkdir(filePath, 0777)
		if err != nil {
			fmt.Printf("%v\n", err)
		}
	}

	archive, err := os.Create(createCacheFileFullPath(filePath, imdbId))
	if err != nil {
		panic(err)
	}
	defer archive.Close()
	zipWriter := zip.NewWriter(archive)

	w1, err := zipWriter.Create("data.txt")
	defer zipWriter.Close()

	if err != nil {
		panic(err)
	}
	if _, err := io.Copy(w1, strings.NewReader(movieHtml)); err != nil {
		panic(err)
	}
}

func saveMovieDataToFile(movieUrl string, movie Movie) {
	filePath, imdbId := createCacheFilePath(movieUrl, "reference_data")

	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		err := os.Mkdir(filePath, 0777)
		if err != nil {
			fmt.Printf("%v\n", err)
		}
	}
	movieFilePath := fmt.Sprintf("%s %s", filePath, imdbId) + ".json"

	movieDataFile, _ := json.MarshalIndent(movie, "", " ")

	err := ioutil.WriteFile(movieFilePath, movieDataFile, 0777)
	if err != nil {
		panic(err)
	}
}

func parseTsv(file string, numRows int, action func(row []string, rowNumber int)) error {
	f, err := os.Open(file)
	if err != nil {
		return err
	}
	defer f.Close()

	csvr := csv.NewReader(f)
	csvr.Comma = '\t'
	csvr.LazyQuotes = true

	//var imdbIds []string
	rowNumber := 0
	for {
		row, err := csvr.Read()

		if rowNumber > numRows {
			fmt.Printf("Max number of rows to parse achevied. Last row number: %d", rowNumber)
			return nil
		}

		if rowNumber == 0 {
			rowNumber = rowNumber + 1
			continue
		}

		if err != nil {
			if err == io.EOF {
				fmt.Printf("End of file. Last row number: %d", rowNumber)
			}
			return err
		}

		//imdbIds = append(imdbIds, row[0])
		action(row, rowNumber)
		rowNumber = rowNumber + 1
		fmt.Printf("%d ", rowNumber)
	}
}
