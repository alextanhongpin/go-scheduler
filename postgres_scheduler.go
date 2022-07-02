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
	"github.com/robfig/cron/v3"
)

type repository interface {
	Create(ctx context.Context, job *StagedJob) error
	Delete(ctx context.Context, name string) error
	FindPending(ctx context.Context) ([]StagedJob, error)
	FindAndLockPendingByName(ctx context.Context, name string) (*StagedJob, error)
	UpdateSuccess(ctx context.Context, job *StagedJob) error
	UpdateFailed(ctx context.Context, job *StagedJob) error
}

type ScheduleFunc func(ctx context.Context, job StagedJob, dryRun bool) error

type PostgresScheduler struct {
	repo  repository
	jobs  sync.Map
	tasks sync.Map
	unit  uow.UOW
	log   *log.Logger // Potentially nil logger.
}

func NewPostgresScheduler(repo repository, unit uow.UOW) *PostgresScheduler {
	return &PostgresScheduler{
		repo: repo,
		unit: unit,
		log:  NewLogger("[scheduler] "),
	}
}

func (s *PostgresScheduler) Register(name string, fn ScheduleFunc) error {
	_, loaded := s.tasks.LoadOrStore(name, fn)
	if loaded {
		return errors.New("task exists")
	}

	return nil
}

func (s *PostgresScheduler) Start(ctx context.Context) error {
	if err := s.unit.AtomicFnContext(ctx, func(ctx context.Context) error {
		jobs, err := s.repo.FindPending(ctx)
		if err != nil {
			return err
		}

		log.Println("jobs scheduled:", len(jobs))

		for _, job := range jobs {
			_ = s.unschedule(job.Name)

			taskFn, err := s.loadTask(job.Type)
			if err != nil {
				return err
			}

			crontab := Crontab(job.ScheduledAt)

			_, err = cron.ParseStandard(crontab)
			if err != nil {
				return fmt.Errorf("failed to parse cron: %s, %w", crontab, err)
			}

			cron, err := s.schedule(ctx, crontab, &job, taskFn)
			if err != nil {
				return err
			}

			cron.Start()

			s.jobs.Store(job.Name, cron)
		}

		return nil
	}); err != nil {
		return fmt.Errorf("failed to schedule pending tasks: %w", err)
	}

	return nil
}

func (s *PostgresScheduler) unschedule(name string) bool {
	j, loaded := s.jobs.LoadAndDelete(name)
	if loaded {
		c, ok := j.(*cron.Cron)
		if ok {
			_ = c.Stop()
		}
	}

	return loaded
}

func (s *PostgresScheduler) Unschedule(ctx context.Context, name string) error {
	_ = s.unschedule(name)

	if err := s.repo.Delete(ctx, name); err != nil {
		// Entry has not been created, skip.
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}

		return fmt.Errorf("failed to delete job %q: %w", name, err)
	}

	return nil
}

func (s *PostgresScheduler) Schedule(ctx context.Context, job *StagedJob) error {
	crontab := Crontab(job.ScheduledAt)

	_, err := cron.ParseStandard(crontab)
	if err != nil {
		return fmt.Errorf("failed to parse cron: %s, %w", crontab, err)
	}

	taskFn, err := s.loadTask(job.Type)
	if err != nil {
		return err
	}

	dryRun := true
	if err := taskFn(ctx, *job, dryRun); err != nil {
		return fmt.Errorf("failed during validation: %w", err)
	}

	return s.unit.AtomicFnContext(ctx, func(txCtx context.Context) error {
		if err := s.Unschedule(txCtx, job.Name); err != nil {
			return err
		}

		if err = s.repo.Create(txCtx, job); err != nil {
			return fmt.Errorf("failed to create: %w", err)
		}

		cron, err := s.schedule(ctx, crontab, job, taskFn)
		if err != nil {
			return err
		}

		cron.Start()

		s.jobs.Store(job.Name, cron)

		return nil
	})
}

type StagedJobInfo struct {
	Name    string    `json:"name"`
	CronID  int       `json:"cronId"`
	Next    time.Time `json:"next"`
	Elapsed string    `json:"elapsed"`
}

func (s *PostgresScheduler) List() (res []StagedJobInfo) {
	s.jobs.Range(func(key, value any) bool {
		name, ok := key.(string)
		if !ok {
			return ok
		}

		c, ok := value.(*cron.Cron)
		if !ok {
			return ok
		}

		for _, entry := range c.Entries() {
			res = append(res, StagedJobInfo{
				Name:    name,
				CronID:  int(entry.ID),
				Next:    entry.Next,
				Elapsed: entry.Next.Sub(time.Now()).String(),
			})
		}

		return true
	})

	return
}

func (s *PostgresScheduler) loadTask(name string) (ScheduleFunc, error) {
	task, loaded := s.tasks.Load(name)
	if !loaded {
		return nil, errors.New("task not registered")
	}

	taskFn, ok := task.(ScheduleFunc)
	if !ok {
		return nil, errors.New("not a valid task")
	}

	return taskFn, nil
}

func (s *PostgresScheduler) schedule(ctx context.Context, crontab string, job *StagedJob, fn ScheduleFunc) (*cron.Cron, error) {
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
