package main

import (
	"encoding/csv"
	"fmt"
	"github.com/PuerkitoBio/goquery"
	"github.com/gocolly/colly/v2"
	"github.com/gocolly/colly/v2/proxy"
	"io"
	"io/ioutil"
	"os"
	"strings"
)

type BaseMovie struct {
	movieId               string
	rating                string
	countries             []string
	runtime               string
	budget                string
	grossWorldWide        string
	numberOfUserReviews   string
	numberOfCriticReviews string
}

func main() {
	c := colly.NewCollector(
		colly.Async(true),
		colly.CacheDir("./colly_cache/base_movie_data"),
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

	c.OnHTML(".dDUGgO", func(element *colly.HTMLElement) {
		urlParts := strings.Split(element.Request.URL.String(), "/")
		movieId := urlParts[len(urlParts)-2]
		if movieId == "title" {
			movieId = urlParts[len(urlParts)-1]
		}
		movie := BaseMovie{movieId: movieId}

		fmt.Printf("Parsing content of %s...\n", movie.movieId)

		element.DOM.Find(".iTLWoV").Each(func(i int, selection *goquery.Selection) {
			movie.rating = selection.Text()
		})
		element.DOM.Find("[data-testid=\"title-details-origin\"] .ipc-metadata-list-item__list-content-item--link").Each(func(i int, selection *goquery.Selection) {
			movie.countries = append(movie.countries, selection.Text())
		})
		element.DOM.Find("[data-testid=\"title-techspec_runtime\"] .ipc-metadata-list-item__list-content-item").Each(func(i int, selection *goquery.Selection) {
			movie.runtime = selection.Text()
		})
		element.DOM.Find("[data-testid=\"title-boxoffice-budget\"] .ipc-metadata-list-item__list-content-item").Each(func(i int, selection *goquery.Selection) {
			movie.budget = strings.Replace(selection.Text(), " (estimated)", "", -1)
		})
		element.DOM.Find("[data-testid=\"title-boxoffice-cumulativeworldwidegross\"] .ipc-metadata-list-item__list-content-item").Each(func(i int, selection *goquery.Selection) {
			movie.grossWorldWide = selection.Text()
		})
		element.DOM.Find(".bUNAEL .label").Each(func(i int, selection *goquery.Selection) {
			if selection.Text() == "User reviews" {
				movie.numberOfUserReviews = selection.Parent().Find(".score").Text()
			} else if selection.Text() == "Critic reviews" {
				movie.numberOfCriticReviews = selection.Parent().Find(".score").Text()
			}
		})

		err := addRecordToCsvFile("data/base_movies_data.csv", []string{
			movie.movieId,
			movie.rating,
			strings.Join(movie.countries, ","),
			movie.runtime,
			movie.budget,
			movie.grossWorldWide,
			movie.numberOfUserReviews,
			movie.numberOfCriticReviews,
		})
		if err != nil {
			panic(err)
		}
	})

	c.OnRequest(func(r *colly.Request) {
		fmt.Println("Visiting", r.URL.String())
	})

	parsedCounter := 0
	err = parseCsv("data/movies_with_references.csv", func(row []string, rowNumber int) {
		if parsedCounter%100 == 0 {
			c.Wait()
		}
		imdbId := row[0]
		c.Visit(fmt.Sprintf("https://www.imdb.com/title/%s", imdbId))
		parsedCounter += 1
	})

	if err == io.EOF {
		c.Wait()
	} else if err != nil {
		panic(err)
	}

	c.OnError(func(r *colly.Response, err error) {
		fmt.Println("Request URL:", r.Request.URL, "failed with response:", string(r.Body), "\nError:", err)
	})
}

func addRecordToCsvFile(fileName string, record []string) error {
	f, err := os.OpenFile(fileName, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0777)
	defer f.Close()

	if err != nil {
		return err
	}

	w := csv.NewWriter(f)
	defer w.Flush()

	if err := w.Write(record); err != nil {
		return err
	}

	return nil
}

func parseCsv(file string, action func(row []string, rowNumber int)) error {
	f, err := os.Open(file)
	if err != nil {
		return err
	}
	defer f.Close()

	csvr := csv.NewReader(f)
	csvr.Comma = ','
	csvr.LazyQuotes = true

	//var imdbIds []string
	rowNumber := 0
	for {
		row, err := csvr.Read()

		if err != nil {
			if err == io.EOF {
				fmt.Printf("End of file. Last row number: %d", rowNumber)
			}
			return err
		}

		action(row, rowNumber)
		rowNumber = rowNumber + 1
		fmt.Printf("%d ", rowNumber)
	}
}
