package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/alextanhongpin/go-scheduler"
	"github.com/alextanhongpin/uow"
	_ "github.com/lib/pq"
)

type PublishProductRequest struct {
	ProductIDs  []int64   `json:"productIds"`
	PublishedAt time.Time `json:"publishedAt"`
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

	req := PublishProductRequest{
		ProductIDs:  []int64{1, 2, 3},
		PublishedAt: time.Now().Add(1 * time.Minute),
	}

	b, err := json.Marshal(req)
	if err != nil {
		panic(err)
	}

	data := json.RawMessage(b)
	job := scheduler.NewStagedJob("merchant_products", "publish_product", data, req.PublishedAt)

	ctx := context.Background()
	if err := sch.Schedule(ctx, job, PublishProduct); err != nil {
		panic(err)
	}

	time.Sleep(2 * time.Minute)
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
