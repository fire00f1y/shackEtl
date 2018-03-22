package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/lib/pq"
	"io/ioutil"
	"net/http"
	"os"
	"time"
)

const (
	DB_HOST          = "localhost"
	DB_PORT          = "5432"
	DB_USER          = "postgres"
	DB_PASSWORD      = "postgres"
	DB_NAME          = "REPLACE THIS WITH THE DATABASE NAME"
	UNSTRUCTURED_URL = "http://localhost:8983/solr/unstructured_posts/update"
)

type Post struct {
	Id             int    `json:"id"`
	ThreadId       string `json:"threadId"`
	ParentThreadId string `json:"parentThreadId"`
	ParentAuthor   string `json:"parentAuthor"`
	Author         string `json:"author"`
	Category       string `json:"category"`
	Date           string `json:"postDate"`
	Body           string `json:"body_md"`
	AuthorC        string `json:"author_normalized"`
	BodyC          string `json:"body_normalized"`
}

func postPosts(posts []Post, recurse bool) int {
	json, err := json.Marshal(posts)
	if err != nil {
		fmt.Printf("Error while posting to solr: %v\n", err)
		return len(posts)
	}
	buf := bytes.NewBuffer(json)
	resp, err := http.Post(UNSTRUCTURED_URL, "application/json", buf)
	if err != nil {
		if recurse {
			fmt.Println("Error during update. Commiting and sleeping for 30 seconds...")
			_, err := http.Get(UNSTRUCTURED_URL + "?commit=true")
			if err != nil {
				fmt.Println("It really doesn't want to call solr right now")
			}
			time.Sleep(30 * time.Second)
			return postPosts(posts, false)
		}
		fmt.Printf("Error while posting update: %v\n", err)
		return len(posts)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := ioutil.ReadAll(resp.Body)
		fmt.Println(string(body))
	}
	return 0
}

func main() {
	dbinfo := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME)
	fmt.Printf("Connecting using details: %s\n", dbinfo)
	db, err := sql.Open("postgres", dbinfo)
	if err != nil {
		fmt.Printf("Error while opening postgres db with creds [%s]: %v\n", dbinfo, err)
		os.Exit(2)
	}
	start := time.Now()
	defer db.Close()

	fmt.Println("# Starting indexing")

	failed := 0
	count := 0
	found := true
	offset := 0
	limit := 1000
	whereId := -1
	for found {
		roundStart := time.Now()
		found = false
		rows, err := db.Query(fmt.Sprintf("SELECT first.*, second.author as parent_author FROM post as first left join post as second on first.parent_id = second.id and first.id != second.id where first.id > %d order by first.id asc limit %d;", whereId, limit))
		offset += 1

		if err != nil {
			fmt.Printf("Error querying the database: %v\n", err)
			os.Exit(3)
		}

		posts := make([]Post, 0)
		for rows.Next() {
			found = true
			var post Post
			err = rows.Scan(&post.Id, &post.ThreadId, &post.ParentThreadId, &post.Author, &post.Category, &post.Date, &post.Body, &post.AuthorC, &post.BodyC, &post.ParentAuthor)
			if err != nil {
				fmt.Printf("Error scanning row for post[%d]: %v\n", post.Id, err)
				if post.Id == 0 && post.ThreadId == "" {
					failed += 1
					continue
				}
			}
			posts = append(posts, post)
			count += 1
			whereId = post.Id
		}
		fmt.Printf("Index count: %d, next id: %d. Took %s\n", offset*limit, whereId, time.Since(roundStart).String())
		failed += postPosts(posts, true)
	}
	fmt.Printf("updated %d posts, %d updates failed. Entire ETL took %s\n", count, failed, time.Since(start).String())
}
