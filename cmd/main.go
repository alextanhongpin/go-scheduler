package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/alextanhongpin/go-scheduler"
	"github.com/alextanhongpin/go-scheduler/pkg/server"
	"github.com/alextanhongpin/uow"
	"github.com/go-chi/chi"
	_ "github.com/lib/pq"
)

type PublishProductRequest struct {
	ProductIDs  []int64   `json:"productIds"`
	PublishedAt time.Time `json:"publishedAt"`
}

type JobRequest struct {
	Name        string          `json:"name"`
	Type        string          `json:"type"`
	Data        json.RawMessage `json:"data"`
	ScheduledAt time.Time       `json:"scheduledAt"`
}

func main() {
	connStr := "postgres://john:123456@127.0.0.1:5432/test?sslmode=disable"

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		panic(err)
	}

	if err := db.Ping(); err != nil {
		panic(err)
	}

	unit := uow.New(db)
	repo := scheduler.NewStore(unit)
	sch := scheduler.NewPostgresScheduler(repo, unit)
	sch.AddCronFunc("publish_product", PublishProduct)

	ctx := context.Background()
	if err := sch.Start(ctx); err != nil {
		panic(err)
	}

	router := chi.NewRouter()
	router.Get("/jobs", func(w http.ResponseWriter, r *http.Request) {
		entries := sch.List()

		w.Header().Set("content-type", "application/json;charset=utf8;")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(entries); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)

			return
		}
	})

	router.Post("/jobs", func(w http.ResponseWriter, r *http.Request) {
		var req JobRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)

			return
		}

		// Override for testing.
		req.ScheduledAt = time.Now().Add(1 * time.Minute).Round(time.Second)
		job := scheduler.NewStagedJob(req.Name, req.Type, req.Data, req.ScheduledAt)
		if err := sch.Schedule(ctx, job); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)

			return
		}

		w.Header().Set("content-type", "application/json;charset=utf8;")
		w.WriteHeader(http.StatusCreated)
	})

	portEnv := os.Getenv("PORT")
	if portEnv == "" {
		portEnv = "8080"
	}

	port, err := strconv.Atoi(portEnv)
	if err != nil {
		panic(err)
	}

	server.New(router, port)
}

func PublishProduct(ctx context.Context, job scheduler.StagedJob, dryRun bool) error {
	dec := json.NewDecoder(bytes.NewReader(job.Data))
	dec.DisallowUnknownFields()

	var req PublishProductRequest
	if err := dec.Decode(&req); err != nil {
		return err
	}

	fmt.Printf("got req: %+v\n", req)

	if dryRun {
		fmt.Println("dry run only")

		return nil
	}

	fmt.Println("published products")

	return nil
}
