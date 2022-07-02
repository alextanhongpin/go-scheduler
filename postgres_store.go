package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/alextanhongpin/uow"
)

func NewLogger(prefix string) *log.Logger {
	logger := log.New(os.Stderr, prefix, log.LstdFlags|log.Lmsgprefix)

	return logger
}

var _ repository = (*Store)(nil)

type Store struct {
	uow uow.IDB
	log *log.Logger
}

func NewStore(uow uow.IDB) *Store {
	return &Store{
		uow: uow,
		log: NewLogger("[store] "),
	}
}

func (s *Store) DB(ctx context.Context) uow.IDB {
	db, ok := uow.UowContext.Value(ctx)
	if ok {
		s.log.Println("found uow context. is tx?", db.IsTx())

		return db
	}

	return s.uow
}

func (s *Store) Delete(ctx context.Context, name string) error {
	s.log.Println("deleting", name)

	db := s.DB(ctx)

	res, err := db.ExecContext(ctx, `
		DELETE FROM staged_jobs 
		WHERE name = $1 
		AND status = $2 
		AND run_at IS NULL
	`, name, Pending)
	if err != nil {
		return fmt.Errorf("failed to delete staged job: %w", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("no rows deleted: %w", err)
	}

	if rows == 0 {
		return errors.New("no rows deleted")
	}

	return nil
}

func (s *Store) Create(ctx context.Context, job *StagedJob) error {
	s.log.Printf("creating: %+v\n", job)

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
				scheduled_at = EXCLUDED.scheduled_at
			RETURNING id
		`,
		job.Name,
		job.Type,
		job.Data,
		job.Status,
		job.ScheduledAt,
	).Scan(&job.ID); err != nil {
		return fmt.Errorf("failed to insert staged job: %w", err)
	}

	return nil
}

func (s *Store) FindAndLockPendingByName(ctx context.Context, name string) (*StagedJob, error) {
	s.log.Printf("find and lock pending by name: %+v\n", name)

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
			AND status = $2
			AND run_at IS NULL
			FOR UPDATE NOWAIT
		`,
		name,
		Pending,
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
		return nil, fmt.Errorf("failed to find pending staged jobs: %w", err)
	}

	return &job, nil
}

func (s *Store) UpdateSuccess(ctx context.Context, job *StagedJob) error {
	db := s.DB(ctx)

	res, err := db.ExecContext(ctx, `
			UPDATE staged_jobs
			SET 
				status = $1,
				run_at = $2,
				failure_reason = NULL
			WHERE name = $3
		`,
		job.Status,
		job.RunAt,
		job.Name,
	)
	if err != nil {
		return fmt.Errorf("failed to update to success: %w", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get updated rows affected count: %w", err)
	}

	if rows != 1 {
		return errors.New("not updated")
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
				run_at = $3,
				failure_reason = NULL
			WHERE name = $4
		`,
		job.Status,
		job.FailureReason,
		job.RunAt,
		job.Name,
	)
	if err != nil {
		return fmt.Errorf("failed to update to failed: %w", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get updated rows affected count: %w", err)
	}

	if rows != 1 {
		return errors.New("not updated")
	}

	return nil
}
