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
	log  *log.Logger // Potentially nil logger.
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
		c, ok := j.(*cron.Cron)
		if ok {
			_ = c.Stop()
		}
	}

	if err := s.repo.Delete(ctx, name); err != nil {
		// Entry has not been created, skip.
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}

		return fmt.Errorf("failed to delete job %q: %w", name, err)
	}

	return nil
}

func (s *PostgresScheduler) Schedule(ctx context.Context, job *StagedJob, fn ScheduleFunc) error {
	crontab := Crontab(job.ScheduledAt)

	_, err := cron.ParseStandard(crontab)
	if err != nil {
		return fmt.Errorf("failed to parse cron: %s, %w", crontab, err)
	}

	dryRun := true
	if err := fn(ctx, *job, dryRun); err != nil {
		return fmt.Errorf("failed during validation: %w", err)
	}

	return s.unit.AtomicFnContext(ctx, func(txCtx context.Context) error {
		if err := s.Unschedule(txCtx, job.Name); err != nil {
			return err
		}

		if err = s.repo.Create(txCtx, job); err != nil {
			return fmt.Errorf("failed to create: %w", err)
		}

		cron, err := s.newCron(ctx, crontab, job, fn)
		if err != nil {
			return err
		}

		cron.Start()

		s.jobs.Store(job.Name, cron)

		s.list()

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

func (s *PostgresScheduler) newCron(ctx context.Context, crontab string, job *StagedJob, fn ScheduleFunc) (*cron.Cron, error) {
	crn := cron.New()
	_, err := crn.AddFunc(crontab, func() {
		job.RunAt = sql.NullTime{
			Time:  time.Now(),
			Valid: true,
		}

		if err := s.atomicSchedule(ctx, job, fn); err != nil {
			err = fmt.Errorf("failed to execute schedule func %q: %w", job.Name, err)
			s.log.Println(err)

			return
		}
	})

	if err != nil {
		return nil, fmt.Errorf("failed to add cron func: %w", err)
	}

	return crn, nil
}

func (s *PostgresScheduler) atomicSchedule(ctx context.Context, job *StagedJob, fn ScheduleFunc) error {
	return s.unit.AtomicFnContext(ctx, func(ctx context.Context) error {
		dbJob, err := s.repo.FindAndLockPendingByName(ctx, job.Name)
		if err != nil {
			return fmt.Errorf("failed to find and lock pending job %q: %w", job.Name, err)
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

			if err := s.repo.UpdateFailed(ctx, job); err != nil {
				return fmt.Errorf("failed to update job %q status to fail: %w", job.Name, err)
			}

			return nil
		}

		job.Status = Success

		if err := s.repo.UpdateSuccess(ctx, job); err != nil {
			return fmt.Errorf("failed to update job %q status to success: %w", job.Name, err)
		}

		return nil
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
