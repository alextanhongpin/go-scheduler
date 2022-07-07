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
	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
)

var (
	ErrCronFuncExists   = errors.New("scheduler: cron func exists")
	ErrCronFuncNotFound = errors.New("scheduler: cron func not found")
	ErrCronCompleted    = errors.New("scheduler: cron completed")
	ErrCronReassigned   = errors.New("scheduler: cron has been reassigned")
	ErrCronJobCompleted = errors.New("scheduler: cron job is completed")
)

type CronJob struct {
	ID            uuid.UUID       `json:"id"`
	WorkerID      uuid.UUID       `json:"workerId"`
	Name          string          `json:"name"`
	Type          string          `json:"type"`
	Status        Status          `json:"status"`
	Data          json.RawMessage `json:"data"`
	FailureReason string          `json:"failureReason"`
	ScheduledAt   time.Time       `json:"scheduledAt"`
	CreatedAt     time.Time       `json:"createdAt"`
	UpdatedAt     time.Time       `json:"updatedAt"`
}

func NewCronJob(name, typ string, data json.RawMessage, scheduledAt time.Time) *CronJob {
	return &CronJob{
		Name:        name,
		Type:        typ,
		Data:        data,
		ScheduledAt: scheduledAt,
		Status:      Pending,
	}
}

type CronInfo struct {
	Job    *CronJob     `json:"job"`
	CronID cron.EntryID `json:"cronId"`
	Next   time.Time    `json:"next"`
	Left   string       `json:"left"`
}

type CronFunc func(ctx context.Context, job *CronJob, dryRun bool) error

type cronRepository interface {
	Create(ctx context.Context, job *CronJob) error
	Delete(ctx context.Context, name string) error
	UpdateStatus(ctx context.Context, name string, status Status, failureReason string) error
	BulkUpdateStatus(ctx context.Context, names []string, status Status, failureReason string) error
	BulkUpdateWorkerID(ctx context.Context, workerID uuid.UUID, names []string) error
	FindAndLockByName(ctx context.Context, name string) (*CronJob, error)
	FindAndLockForWorkerByName(ctx context.Context, workerID uuid.UUID, name string) (*CronJob, error)
	FindAndLockPending(ctx context.Context) ([]CronJob, error)
	FindPendingByWorkerID(ctx context.Context, workerID uuid.UUID) ([]CronJob, error)
}

type CronScheduler struct {
	mu   sync.Mutex
	cron *cron.Cron

	id    uuid.UUID
	funcs *AtomicMap[string, CronFunc]
	crons *AtomicMap[string, cron.EntryID]
	unit  uow.UOW
	repo  cronRepository
}

func NewCronScheduler(unit uow.UOW, repo cronRepository) *CronScheduler {
	crn := cron.New()
	crn.Start()

	id := uuid.New()
	log.Printf("running cron: worker_id=%s\n", id)

	return &CronScheduler{
		cron:  crn,
		id:    id,
		funcs: NewAtomicMap[string, CronFunc](),
		crons: NewAtomicMap[string, cron.EntryID](),
		unit:  unit,
		repo:  repo,
	}
}

func (s *CronScheduler) AddFunc(name string, fn CronFunc) error {
	if !s.funcs.Add(name, fn) {
		return fmt.Errorf("%w: %s", ErrCronFuncExists, name)
	}

	return nil
}

func (s *CronScheduler) Init(ctx context.Context) error {
	return s.unit.AtomicFnContext(ctx, func(ctx context.Context) error {
		jobs, err := s.repo.FindAndLockPending(ctx)
		if err != nil {
			return err
		}

		names := make([]string, len(jobs))
		for i := range jobs {
			names[i] = jobs[i].Name

			if err := s.schedule(ctx, &jobs[i]); err != nil {
				return err
			}
		}

		return s.repo.BulkUpdateWorkerID(ctx, s.id, names)
	})
}

func (s *CronScheduler) List(ctx context.Context) ([]CronInfo, error) {
	jobs, err := s.repo.FindPendingByWorkerID(ctx, s.id)
	if err != nil {
		return nil, err
	}

	res := make([]CronInfo, 0, len(jobs))

	for _, job := range jobs {
		cronEntryID, found := s.crons.Get(job.Name)
		if !found {
			continue
		}

		entry := s.cron.Entry(cronEntryID)

		res = append(res, CronInfo{
			Job:    &job,
			Next:   entry.Next,
			Left:   entry.Next.Sub(time.Now()).String(),
			CronID: entry.ID,
		})
	}

	return res, nil
}

func (s *CronScheduler) Schedule(ctx context.Context, job *CronJob) error {
	job.WorkerID = s.id

	return s.unit.AtomicFnContext(ctx, func(txCtx context.Context) error {
		cronJob, err := s.repo.FindAndLockByName(txCtx, job.Name)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}

		if cronJob != nil && cronJob.Status.IsSuccess() {
			return fmt.Errorf("%w: %s", ErrCronJobCompleted, job.Name)
		}

		if err := s.repo.Create(txCtx, job); err != nil {
			return fmt.Errorf("%w: failed to create: %s", err, job.Name)
		}

		// This ctx does not contains the db transaction.
		return s.schedule(ctx, job)
	}, nil)
}

func (s *CronScheduler) Unschedule(ctx context.Context, name string) error {
	if err := s.repo.Delete(ctx, name); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: %s", err, name)
		}
	}

	s.unschedule(name)

	return nil
}

func (s *CronScheduler) schedule(ctx context.Context, job *CronJob) error {
	cronFunc, ok := s.funcs.Get(job.Type)
	if !ok {
		return fmt.Errorf("%w: %s", ErrCronFuncNotFound, job.Type)
	}

	dryRun := true
	if err := cronFunc(ctx, job, dryRun); err != nil {
		return fmt.Errorf("%w: failed to exec dry-run", err)
	}

	entryID, err := s.cron.AddFunc(CronSpec(job.ScheduledAt), func() {
		defer s.crons.Remove(job.Name)

		if err := s.unit.AtomicFnContext(ctx, func(ctx context.Context) error {
			cronJob, err := s.repo.FindAndLockForWorkerByName(ctx, s.id, job.Name)
			if err != nil {
				return fmt.Errorf("%w: failed to find cron: name=%s worker_id=%s", err, job.Name, s.id)
			}

			if cronJob.Status.IsSuccess() {
				return fmt.Errorf("%w: %s", ErrCronCompleted, job.Name)
			}

			dryRun := false
			if err := cronFunc(ctx, cronJob, dryRun); err != nil {
				if err := s.repo.UpdateStatus(ctx, job.Name, Failed, err.Error()); err != nil {
					return fmt.Errorf("%w: failed to update status to failed: %s", err, job.Name)
				}

				return err
			}

			if err := s.repo.UpdateStatus(ctx, job.Name, Success, ""); err != nil {
				return fmt.Errorf("%w: failed to update status to success: %s", err, job.Name)
			}

			return nil
		}, nil); err != nil {
			log.Println("failed to execute cron func:", err)
		}
	})
	if err != nil {
		return fmt.Errorf("%w: failed to schedule cron job", err)
	}

	s.unschedule(job.Name)
	s.crons.Set(job.Name, entryID)

	return nil
}

func (s *CronScheduler) unschedule(name string) {
	cronEntryID, found := s.crons.Get(name)
	if !found {
		return
	}

	s.cron.Remove(cronEntryID)
	s.crons.Remove(name)
}

func CronSpec(t time.Time) string {
	t = t.In(time.Local).Round(time.Second)

	return fmt.Sprintf("%d %d %d %d %d",
		t.Minute(),
		t.Hour(),
		t.Day(),
		t.Month(),
		t.Weekday(),
	)
}
