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

var (
	ErrScheduleFuncNotFound = errors.New("scheduler: ScheduleFunc not found")
	ErrScheduleFuncExists   = errors.New("scheduler: ScheduleFunc exists")
	ErrScheduleFuncInvalid  = errors.New("scheduler: invalid ScheduleFunc")
	ErrInconsistentJobData  = errors.New("scheduler: inconsistent job data")
	ErrJobCompleted         = errors.New("scheduler: job already complete")
)

type repository interface {
	Create(ctx context.Context, job *StagedJob) error
	Delete(ctx context.Context, name string) error
	FindPending(ctx context.Context) ([]StagedJob, error)
	FindAndLockByName(ctx context.Context, name string) (*StagedJob, error)
	UpdateSuccess(ctx context.Context, job *StagedJob) error
	UpdateFailed(ctx context.Context, job *StagedJob) error
}

type ScheduleFunc func(ctx context.Context, job StagedJob, dryRun bool) error

// 1. Everytime the server starts, register a new server token

type PostgresScheduler struct {
	mu        sync.Mutex
	cron      *cron.Cron
	cronJobs  sync.Map
	cronFuncs sync.Map
	repo      repository
	unit      uow.UOW
}

func NewPostgresScheduler(repo repository, unit uow.UOW) *PostgresScheduler {
	crn := cron.New()
	crn.Start()

	return &PostgresScheduler{
		repo: repo,
		unit: unit,
		cron: crn,
	}
}

func (s *PostgresScheduler) AddCronFunc(jobType string, fn ScheduleFunc) error {
	_, loaded := s.cronFuncs.LoadOrStore(jobType, fn)
	if loaded {
		return fmt.Errorf("%w: %s", ErrScheduleFuncExists, jobType)
	}

	return nil
}

func (s *PostgresScheduler) LoadCronFunc(jobType string) (ScheduleFunc, error) {
	task, loaded := s.cronFuncs.Load(jobType)
	if !loaded {
		return nil, fmt.Errorf("%w: %s", ErrScheduleFuncNotFound, jobType)
	}

	taskFn, ok := task.(ScheduleFunc)
	if !ok {
		return nil, fmt.Errorf("%w: got type %T for %q", ErrScheduleFuncInvalid, task, jobType)
	}

	return taskFn, nil
}

func (s *PostgresScheduler) Start(ctx context.Context) error {
	if err := s.unit.AtomicFnContext(ctx, func(ctx context.Context) error {
		jobs, err := s.repo.FindPending(ctx)
		if err != nil {
			return fmt.Errorf("%w: failed to find pending tasks", err)
		}

		for _, job := range jobs {
			job := job
			if err := s.schedule(ctx, &job); err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		return fmt.Errorf("%w: failed to schedule pending cronFuncs", err)
	}

	return nil
}

// Unschedule deletes the entry from the db and remove the job from being run.
func (s *PostgresScheduler) Unschedule(ctx context.Context, name string) error {
	_ = s.unschedule(name)

	return s.deleteJob(ctx, name)
}

// Schedule creates a new entry in the db and schedules a new job.
func (s *PostgresScheduler) Schedule(ctx context.Context, job *StagedJob) error {
	if err := s.validateJob(ctx, job); err != nil {
		return err
	}

	return s.upsertJob(ctx, job)
}

type StagedJobInfo struct {
	StagedJob *StagedJob `json:"job"`
	CronID    int        `json:"cronId"`
	Next      time.Time  `json:"next"`
	Elapsed   string     `json:"elapsed"`
}

func (s *PostgresScheduler) List() (res []StagedJobInfo) {
	jobByCronEntryID := make(map[cron.EntryID]*StagedJob)

	s.cronJobs.Range(func(key, value any) bool {
		job, ok := value.(*StagedJob)
		if !ok {
			return ok
		}

		jobByCronEntryID[job.cronEntryID] = job

		return true
	})

	for _, entry := range s.cron.Entries() {
		res = append(res, StagedJobInfo{
			StagedJob: jobByCronEntryID[entry.ID],
			CronID:    int(entry.ID),
			Next:      entry.Next,
			Elapsed:   entry.Next.Sub(time.Now()).String(),
		})
	}

	return
}

func (s *PostgresScheduler) unschedule(name string) bool {
	unk, loaded := s.cronJobs.LoadAndDelete(name)
	if loaded {
		job, ok := unk.(*StagedJob)
		if !ok {
			panic("scheduler: invalid job type")
		}

		s.cron.Remove(job.cronEntryID)
	}

	return loaded
}

func (s *PostgresScheduler) schedule(ctx context.Context, job *StagedJob) error {
	job = s.loadOrStore(job)

	scheduleFn, err := s.LoadCronFunc(job.Type)
	if err != nil {
		return err
	}

	crontab := NewCronTab(job.ScheduledAt)
	if err := crontab.Validate(); err != nil {
		return err
	}

	s.mu.Lock()
	entryID, err := s.cron.AddFunc(crontab.String(), func() {
		// Copy the values to avoid mutating the original.
		// We only copy the value after the job.cronEntryID is set.
		localJob := new(StagedJob)
		*localJob = *job

		if err := s.atomicSchedule(ctx, localJob, scheduleFn); err != nil {
			err = fmt.Errorf("%w: failed to execute schedule func: %s", err, job.Name)
			log.Println(err)

			return
		}
	})
	job.cronEntryID = entryID
	s.mu.Unlock()

	if err != nil {
		return fmt.Errorf("%w: failed to add cron func", err)
	}

	return nil
}

func (s *PostgresScheduler) loadOrStore(job *StagedJob) *StagedJob {
	unk, loaded := s.cronJobs.LoadOrStore(job.Name, job)
	if loaded {
		storedJob, ok := unk.(*StagedJob)
		if !ok {
			panic("scheduler: invalid job type")
		}

		s.cron.Remove(storedJob.cronEntryID)

		// Copy all the other properties of job.
		*storedJob = *job

		return storedJob
	}

	return job
}

func (s *PostgresScheduler) atomicSchedule(ctx context.Context, job *StagedJob, fn ScheduleFunc) error {
	defer s.cron.Remove(job.cronEntryID)

	return s.unit.AtomicFnContext(ctx, func(ctx context.Context) error {
		dbJob, err := s.repo.FindAndLockByName(ctx, job.Name)
		if err != nil {
			return fmt.Errorf("%w: failed to find and lock pending job: %s", err, job.Name)
		}
		if dbJob.Status.IsSuccess() {
			return fmt.Errorf("%w: %s", ErrJobCompleted, job.Name)
		}

		// Check job and dbJob to see if the changes still matches.
		// When running in multiple instances, we do not prevent the job from being
		// scheduled on multiple instances.
		// However, only the last registered job on the instance will execute, due
		// to the data being stale.
		if diff := diffJob(*job, *dbJob); diff != "" {
			return fmt.Errorf("%w: %s", ErrInconsistentJobData, diff)
		}

		dryRun := false
		if err := fn(ctx, *dbJob, dryRun); err != nil {
			job.Status = Failed
			job.FailureReason = err.Error()

			if err := s.repo.UpdateFailed(ctx, job); err != nil {
				return fmt.Errorf("%w: failed to update job status to failed: %s", err, job.Name)
			}

			return nil
		}

		job.Status = Success

		if err := s.repo.UpdateSuccess(ctx, job); err != nil {
			return fmt.Errorf("%w: failed to update job status to success: %s", err, job.Name)
		}

		return nil
	})
}

func (s *PostgresScheduler) deleteJob(ctx context.Context, name string) error {
	if err := s.repo.Delete(ctx, name); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}

		return fmt.Errorf("%w: failed to delete job: %s", err, name)
	}

	return nil
}

func (s *PostgresScheduler) upsertJob(ctx context.Context, job *StagedJob) error {
	if job == nil {
		panic("scheduler: no job to create")
	}

	return s.unit.AtomicFnContext(ctx, func(txCtx context.Context) error {
		dbJob, err := s.repo.FindAndLockByName(txCtx, job.Name)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: failed to find and lock pending: %s", err, job.Name)
		}

		if dbJob != nil && dbJob.Status.IsSuccess() {
			return fmt.Errorf("%w: %s", ErrJobCompleted, job.Name)
		}

		if err := s.deleteJob(txCtx, job.Name); err != nil {
			return err
		}

		if err := s.repo.Create(txCtx, job); err != nil {
			return fmt.Errorf("%w: failed to create job: %+v", err, job)
		}

		if err := s.schedule(ctx, job); err != nil {
			return err
		}

		return nil
	})
}

func (s *PostgresScheduler) validateJob(ctx context.Context, job *StagedJob) error {
	if err := NewCronTab(job.ScheduledAt).Validate(); err != nil {
		return err
	}

	taskFn, err := s.LoadCronFunc(job.Type)
	if err != nil {
		return err
	}

	dryRun := true
	if err := taskFn(ctx, *job, dryRun); err != nil {
		return fmt.Errorf("%w: failed during validation", err)
	}

	return nil
}

var fieldsToExcludeDiffing = []string{"ID", "Data", "RunAt", "CreatedAt", "UpdatedAt"}

func diffJob(lhs, rhs StagedJob) string {
	if diff := cmp.Diff(lhs, rhs,
		cmpopts.IgnoreFields(StagedJob{}, fieldsToExcludeDiffing...),
		cmpopts.IgnoreUnexported(StagedJob{}),
	); diff != "" {
		return diff
	}

	return diffJsonRawMessage(lhs.Data, rhs.Data)
}

func diffJsonRawMessage(a, b json.RawMessage) string {
	var lhs, rhs map[string]any

	if err := json.Unmarshal(a, &lhs); err != nil {
		return err.Error()
	}

	if err := json.Unmarshal(b, &rhs); err != nil {
		return err.Error()
	}

	return cmp.Diff(lhs, rhs)
}
