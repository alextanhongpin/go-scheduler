package scheduler

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/alextanhongpin/uow"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
)

type repository interface {
	Create(ctx context.Context, job *StagedJob) error
	Delete(ctx context.Context, name string) error
	FindAndLockPendingByName(ctx context.Context, name string) (*StagedJob, error)
	UpdateSuccess(ctx context.Context, job *StagedJob) error
	UpdateFailed(ctx context.Context, job *StagedJob) error
}

type PostgresScheduler struct {
	repo repository
	jobs sync.Map
	unit uow.UOW
	log  *log.Logger
}

func NewPostgresScheduler(repo repository, unit uow.UOW) *PostgresScheduler {
	return &PostgresScheduler{
		repo: repo,
		unit: unit,
		log:  NewLogger("[scheduler] "),
	}
}

type StagedJob struct {
	ID            uuid.UUID
	Name          string
	Type          string
	Data          json.RawMessage
	Status        Status
	FailureReason string
	ScheduledAt   time.Time
	RunAt         sql.NullTime
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

func NewStagedJob(name, typ string, data json.RawMessage, scheduledAt time.Time) *StagedJob {
	return &StagedJob{
		Name:        name,
		Type:        typ,
		Data:        data,
		ScheduledAt: scheduledAt,
		Status:      Pending,
	}
}

type ScheduleFunc func(ctx context.Context, job StagedJob, dryRun bool) error

func (s *PostgresScheduler) Unschedule(ctx context.Context, name string) error {
	j, loaded := s.jobs.LoadAndDelete(name)
	if loaded {
		fmt.Println("loaded job locally")
		c, ok := j.(*cron.Cron)
		if ok {
			fmt.Println("stopped local job")
			_ = c.Stop()
		}

		// Delete from db
		// Discard no row error
		if err := s.repo.Delete(ctx, name); err != nil && !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("failed to unschedule job %q: %w", name, err)
		}
	}

	return nil
}

func (s *PostgresScheduler) Schedule(ctx context.Context, job *StagedJob, fn ScheduleFunc) error {
	crontab := Crontab(job.ScheduledAt)
	s.log.Println("crontab", crontab)

	tab, err := cron.ParseStandard(crontab)
	if err != nil {
		return fmt.Errorf("failed to parse cron: %s, %w", crontab, err)
	}

	s.log.Println("executing next in", tab.Next(time.Now()))

	dryRun := true
	if err := fn(ctx, *job, dryRun); err != nil {
		return fmt.Errorf("failed during validation: %w", err)
	}

	return s.unit.AtomicFnContext(ctx, func(txCtx context.Context) error {
		fmt.Println("running in transaction")

		if err := s.Unschedule(txCtx, job.Name); err != nil {
			return err
		}

		// cron.WithLocation(job.ScheduledAt.Location())
		c := cron.New()
		c.AddFunc(crontab, func() {
			s.log.Printf("executing job: %+v\n", job)

			job.RunAt = sql.NullTime{
				Time:  time.Now(),
				Valid: true,
			}

			if err := s.newScheduledFunc(ctx, job, fn); err != nil {
				s.log.Printf("failed to schedule func: %v\n", err)

				return
			}
		})
		c.Start()

		// Create locally.
		s.jobs.Store(job.Name, c)
		s.log.Println("stored the job locally")

		s.list()

		// Insert into db.
		if err = s.repo.Create(txCtx, job); err != nil {
			s.log.Printf("failed to create: %v\n", err)

			return s.Unschedule(txCtx, job.Name)
		}

		return nil
	})
}

func (s *PostgresScheduler) list() {
	s.jobs.Range(func(key, value any) bool {
		c, ok := value.(*cron.Cron)
		if !ok {
			return ok
		}

		for _, entry := range c.Entries() {
			s.log.Printf("got cron entry: %+v\n", entry)
		}

		return true
	})
}

func (s *PostgresScheduler) newScheduledFunc(ctx context.Context, job *StagedJob, fn ScheduleFunc) error {
	return s.unit.AtomicFnContext(ctx, func(ctx context.Context) error {
		dbJob, err := s.repo.FindAndLockPendingByName(ctx, job.Name)
		if err != nil {
			return err
		}

		// Check job and dbJob to see if the changes still matches.
		if diff := cmp.Diff(job, dbJob, cmpopts.IgnoreFields(StagedJob{}, "ID", "Data", "RunAt", "CreatedAt", "UpdatedAt")); diff != "" {
			return fmt.Errorf("%s: %w", diff, errors.New("payload has changed"))
		}
		if diff := diffJsonRawMessage(job.Data, dbJob.Data); diff != "" {
			return fmt.Errorf("%s: %w", diff, errors.New("payload has changed"))
		}

		dryRun := false
		if err := fn(ctx, *dbJob, dryRun); err != nil {
			job.Status = Failed
			job.FailureReason = err.Error()

			return s.repo.UpdateFailed(ctx, job)
		}

		job.Status = Success

		return s.repo.UpdateSuccess(ctx, job)
	})
}

func diffJsonRawMessage(a, b json.RawMessage) string {
	var m, n map[string]any

	if err := json.Unmarshal(a, &m); err != nil {
		return err.Error()
	}

	if err := json.Unmarshal(b, &n); err != nil {
		return err.Error()
	}

	return cmp.Diff(m, n)
}
