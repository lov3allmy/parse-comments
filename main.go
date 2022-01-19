package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"io"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"
)

const (
	url      = "https://jsonplaceholder.typicode.com"
	username = "root"
	password = "qwerty"
	protocol = "tcp"
	address  = "127.0.0.1:55695"
	dbname   = "parse_db"
)

type Post struct {
	UserId int    `json:"userId"`
	Id     int    `json:"id"`
	Title  string `json:"title"`
	Body   string `json:"body"`
}

type Comment struct {
	PostId int    `json:"postId"`
	Id     int    `json:"id"`
	Name   string `json:"name"`
	Email  string `json:"email"`
	Body   string `json:"body"`
}

func main() {
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@%s(%s)/%s", username, password, protocol, address, ""))
	if err != nil {
		log.Printf("Ошибка %s при открытии СУБД", err)
		return
	}
	exec, err := db.Exec(fmt.Sprintf(`create database if not exists %s`, dbname))
	if err != nil {
		return
	}
	rows, err := exec.RowsAffected()
	if err != nil {
		return
	}
	fmt.Printf("При создании БД %s было затронуто %d строк", dbname, rows)
	if err := db.Close(); err != nil {
		return
	}

	db, err = sql.Open("mysql", fmt.Sprintf("%s:%s@%s(%s)/%s", username, password, protocol, address, dbname))

	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			panic(err.Error())
		}
	}(db)

	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)

	fmt.Println("БД подключена")

	query := `create table if not exists posts
		(
			id int unique not null,
			user_id int not null,
			title varchar(250) not null,
			body text not null,
			primary key (id)
		);`
	exec, err = db.Exec(query)
	if err != nil {
		log.Println(err)
		return
	}
	rows, err = exec.RowsAffected()
	if err != nil {
		log.Println(err)
		return
	}
	fmt.Printf("При создании таблицы posts было затронуто %d строк", rows)

	query = `create table if not exists comments
		(
			id int unique not null,
			post_id int not null,
			name varchar(250) not null,
			email varchar(250) not null,
			body text not null,
			primary key (id),
			foreign key (post_id) references posts (id)
		);`
	exec, err = db.Exec(query)
	if err != nil {
		log.Println(err)
		return
	}
	rows, err = exec.RowsAffected()
	if err != nil {
		log.Println(err)
		return
	}
	fmt.Printf("При создании таблицы comments было затронуто %d строк", rows)

	posts, err := parsePosts()
	if err != nil {
		log.Printf("Ошибка %s при парсе постов", err)
		panic(err.Error())
	}

	var wg sync.WaitGroup

	for i := range posts {
		p := posts[i]
		wg.Add(1)
		go func() {
			query = fmt.Sprintf(`insert into posts(id, user_id, title, body) values (%d, %d, '%s', '%s');`, p.Id, p.UserId, p.Title, p.Body)
			_, err := db.Exec(query)
			if err != nil {
				log.Println(err)
				return
			}
			log.Printf("В posts добавлена строка")
			comments, err := parseComments(p.Id)
			if err != nil {
				log.Printf("Ошибка %s при парсе комментариев", err)
				panic(err.Error())
			}
			for j := range comments {
				c := comments[j]
				wg.Add(1)
				go func() {
					query = fmt.Sprintf(`insert into comments(id, post_id, name, email, body) values (%d, %d, '%s', '%s', '%s');`, c.Id, c.PostId, c.Name, c.Email, p.Body)
					_, err := db.Exec(query)
					if err != nil {
						log.Println(err)
						return
					}
					log.Printf("В comments добавлена строка")
					wg.Done()
				}()
			}
			wg.Done()
		}()
	}

	wg.Wait()
}

func parsePosts() ([]Post, error) {
	response, err := http.Get(url + "/posts?userId=7")
	if err != nil {
		log.Printf("Ошибка %s при http запросе на сайт", err)
		return nil, err
	}

	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			panic(err.Error())
		}
	}(response.Body)

	posts := make([]Post, 10)

	dec := json.NewDecoder(response.Body)

	if err := dec.Decode(&posts); err != nil {
		log.Printf("Ошибка %s при расшифровке полученного JSON", err)
		return nil, err
	}

	return posts, nil
}

func parseComments(postId int) ([]Comment, error) {
	commentsPath := "/comments?postId=" + strconv.Itoa(postId)

	response, err := http.Get(url + commentsPath)
	if err != nil {
		log.Printf("Ошибка %s при http запросе на сайт", err)
		return nil, err
	}

	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			panic(err.Error())
		}
	}(response.Body)

	comments := make([]Comment, 5)

	dec := json.NewDecoder(response.Body)

	if err := dec.Decode(&comments); err != nil {
		log.Printf("Ошибка %s при расшифровке полученного JSON", err)
		return nil, err
	}

	return comments, nil
}
