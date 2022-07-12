package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
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
	defer db.Close()

	if err := db.Ping(); err != nil {
		panic(err)
	}

	unit := uow.New(db)

	ctx := context.Background()

	repo := scheduler.NewCronRepository(unit)
	if err := repo.Migrate(ctx); err != nil {
		log.Fatalf("migration failed: %v", err)
	}

	sch := scheduler.NewCronScheduler(unit, repo)

	// Register the cron jobs.
	sch.AddFunc("publish_product", PublishProduct)

	// Schedule existing pending jobs.
	if err := sch.Init(ctx); err != nil {
		panic(err)
	}

	router := chi.NewRouter()
	router.Get("/jobs", func(w http.ResponseWriter, r *http.Request) {
		jobs, err := sch.List(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnprocessableEntity)

			return
		}

		w.Header().Set("content-type", "application/json;charset=utf8;")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(jobs); err != nil {
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
		job := scheduler.NewCronJob(req.Name, req.Type, req.Data, req.ScheduledAt)
		if err := sch.Schedule(ctx, job); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)

			return
		}

		w.Header().Set("content-type", "application/json;charset=utf8;")
		w.WriteHeader(http.StatusCreated)
	})

	router.Delete("/jobs/{name}", func(w http.ResponseWriter, r *http.Request) {
		name := chi.URLParam(r, "name")

		if err := sch.Unschedule(ctx, name); err != nil {
			http.Error(w, err.Error(), http.StatusUnprocessableEntity)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	})

	fmt.Printf("listening to port *:%d. press ctrl + c to cancel\n", getPort())
	server.New(router, getPort())
}

func PublishProduct(ctx context.Context, job *scheduler.CronJob, dryRun bool) error {
	dec := json.NewDecoder(bytes.NewReader(job.Data))
	dec.DisallowUnknownFields()

	var req PublishProductRequest
	if err := dec.Decode(&req); err != nil {
		return err
	}

	if dryRun {
		fmt.Println("dry-run:", job.Name)

		return nil
	}

	fmt.Println("published products:", job.Name)

	return nil
}

func getPort() int {
	portEnv := os.Getenv("PORT")
	if portEnv == "" {
		return 8080
	}

	port, err := strconv.Atoi(portEnv)
	if err != nil {
		panic(err)
	}

	return port
}
