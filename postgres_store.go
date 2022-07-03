package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/alextanhongpin/uow"
)

var ErrNotUpdated = errors.New("scheduler: not updated")

func NewLogger(prefix string) *log.Logger {
	logger := log.New(os.Stderr, prefix, log.LstdFlags|log.Lmsgprefix)

	return logger
}

var _ repository = (*Store)(nil)

type Store struct {
	uow uow.IDB
}

func NewStore(uow uow.IDB) *Store {
	return &Store{
		uow: uow,
	}
}

func (s *Store) DB(ctx context.Context) uow.IDB {
	db, ok := uow.UowContext.Value(ctx)
	if ok {

		return db
	}

	return s.uow
}

func (s *Store) Delete(ctx context.Context, name string) error {
	db := s.DB(ctx)

	_, err := db.ExecContext(ctx, `
		DELETE FROM staged_jobs 
		WHERE name = $1 
		AND status = $2 
		AND run_at IS NULL
	`, name, Pending)
	if err != nil {
		return fmt.Errorf("%w: failed to delete staged job", err)
	}

	return nil
}

func (s *Store) Create(ctx context.Context, job *StagedJob) error {
	db := s.DB(ctx)

	if err := db.QueryRowContext(ctx, `
			INSERT INTO staged_jobs(
				name, 
				type, 
				data, 
				status, 
				scheduled_at
			) VALUES (
				$1,
				$2,
				$3,
				$4,
				$5
			) 
			ON CONFLICT (name)
			DO UPDATE SET
				data = EXCLUDED.data,
				type = EXCLUDED.type,
				status = EXCLUDED.status,
				failure_reason = NULL,
				scheduled_at = EXCLUDED.scheduled_at,
				run_at = NULL
			RETURNING id
		`,
		job.Name,
		job.Type,
		job.Data,
		job.Status,
		job.ScheduledAt,
	).Scan(&job.ID); err != nil {
		return fmt.Errorf("%w: failed to insert staged job", err)
	}

	return nil
}

func (s *Store) FindPending(ctx context.Context) ([]StagedJob, error) {
	db := s.DB(ctx)

	rows, err := db.QueryContext(ctx, `
		SELECT 
			id, 
			name, 
			type,
			data, 
			status,
			scheduled_at,
			run_at
		FROM staged_jobs 
		WHERE status = $1
		AND run_at IS NULL
		FOR UPDATE NOWAIT
	`, Pending)
	if err != nil {
		return nil, fmt.Errorf("%w: select query error", err)
	}
	defer rows.Close()

	var jobs []StagedJob
	for rows.Next() {
		var job StagedJob
		if err := rows.Scan(
			&job.ID,
			&job.Name,
			&job.Type,
			&job.Data,
			&job.Status,
			&job.ScheduledAt,
			&job.RunAt,
		); err != nil {
			return nil, fmt.Errorf("%w: failed to scan job", err)
		}

		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: failed to process rows", err)
	}

	return jobs, nil
}

func (s *Store) FindAndLockByName(ctx context.Context, name string) (*StagedJob, error) {
	db := s.DB(ctx)

	var job StagedJob
	if err := db.QueryRowContext(ctx, `
			SELECT 
				id, 
				name, 
				type,
				data, 
				status,
				scheduled_at,
				run_at
			FROM staged_jobs 
			WHERE name = $1
			FOR UPDATE NOWAIT
		`,
		name,
	).
		Scan(
			&job.ID,
			&job.Name,
			&job.Type,
			&job.Data,
			&job.Status,
			&job.ScheduledAt,
			&job.RunAt,
		); err != nil {
		return nil, fmt.Errorf("%w: failed to find pending staged jobs", err)
	}

	return &job, nil
}

func (s *Store) UpdateSuccess(ctx context.Context, job *StagedJob) error {
	db := s.DB(ctx)

	res, err := db.ExecContext(ctx, `
			UPDATE staged_jobs
			SET 
				status = $1,
				run_at = now(),
				failure_reason = NULL
			WHERE name = $2
		`,
		job.Status,
		job.Name,
	)
	if err != nil {
		return fmt.Errorf("%w: failed to update to success", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("%w: failed to get updated rows affected count", err)
	}

	if rows != 1 {
		return fmt.Errorf("%w: jobName=%s", ErrNotUpdated, job.Name)
	}

	return nil
}

func (s *Store) UpdateFailed(ctx context.Context, job *StagedJob) error {
	db := s.DB(ctx)

	res, err := db.ExecContext(ctx, `
			UPDATE staged_jobs
			SET 
				status = $1,
				failure_reason = $2,
				run_at = now()
				failure_reason = NULL
			WHERE name = $3
		`,
		job.Status,
		job.FailureReason,
		job.Name,
	)
	if err != nil {
		return fmt.Errorf("%w: failed to update to failed", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("%w: failed to get updated rows affected count", err)
	}

	if rows != 1 {
		return fmt.Errorf("%w: jobName=%s", ErrNotUpdated, job.Name)
	}

	return nil
}
