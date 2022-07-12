package scheduler

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/alextanhongpin/uow"

	_ "embed"
)

//go:embed migration.sql
var migration string

var ErrNotUpdated = errors.New("store: not updated")

var _ cronRepository = (*CronRepository)(nil)

type CronRepository struct {
	uow uow.UOW
}

func NewCronRepository(uow uow.UOW) *CronRepository {
	return &CronRepository{
		uow: uow,
	}
}

func (s *CronRepository) DB(ctx context.Context) uow.DB {
	db, ok := uow.UowContext.Value(ctx)
	if ok {

		return db
	}

	return s.uow.(*uow.UnitOfWork)
}

func (s *CronRepository) Migrate(ctx context.Context) error {
	return s.uow.RunInTx(ctx, func(uow *uow.UnitOfWork) error {
		_, err := uow.ExecContext(ctx, migration)

		return err
	}, nil)
}

func (s *CronRepository) Delete(ctx context.Context, name string) error {
	db := s.DB(ctx)

	_, err := db.ExecContext(ctx, `
		DELETE FROM cron_jobs 
		WHERE name = $1 
	`, name)
	if err != nil {
		return fmt.Errorf("%w: failed to delete cron job", err)
	}

	return nil
}

func (s *CronRepository) Create(ctx context.Context, job *CronJob) error {
	db := s.DB(ctx)

	if err := db.QueryRowContext(ctx, `
			INSERT INTO cron_jobs(
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
				type = EXCLUDED.type,
				data = EXCLUDED.data,
				status = EXCLUDED.status,
				failure_reason = NULL,
				scheduled_at = EXCLUDED.scheduled_at
			RETURNING id
		`,
		job.Name,
		job.Type,
		job.Data,
		job.Status,
		job.ScheduledAt,
	).Scan(&job.ID); err != nil {
		return fmt.Errorf("%w: failed to insert cron job", err)
	}

	return nil
}

// FindAndLockPendingByName locks a pending job by name.
// If there are more than one attempt to lock the row,
// only the first succeeds.
func (s *CronRepository) FindAndLockPendingByName(ctx context.Context, name string) (*CronJob, error) {
	db := s.DB(ctx)

	var job CronJob
	if err := db.QueryRowContext(ctx, `
			SELECT 
				id, 
				name, 
				type,
				data, 
				status,
				scheduled_at
			FROM cron_jobs 
			WHERE name = $1
			AND status = $2
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
		); err != nil {
		return nil, fmt.Errorf("%w: failed to cron job by name", err)
	}

	return &job, nil
}

// FindAndLockByName locks the given row that matches the condition. If there
// are more than one attempt to lock the row, only the first succeeds.
func (s *CronRepository) FindAndLockByName(ctx context.Context, name string) (*CronJob, error) {
	db := s.DB(ctx)

	var job CronJob
	if err := db.QueryRowContext(ctx, `
			SELECT 
				id, 
				name, 
				type,
				data, 
				status,
				scheduled_at
			FROM cron_jobs 
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
		); err != nil {
		return nil, fmt.Errorf("%w: failed to cron job by name", err)
	}

	return &job, nil
}

// FindPending find pending jobs.
func (s *CronRepository) FindPending(ctx context.Context) ([]CronJob, error) {
	db := s.DB(ctx)

	rows, err := db.QueryContext(ctx, `
		SELECT 
			id, 
			name, 
			type,
			data, 
			status,
			scheduled_at
		FROM cron_jobs 
		WHERE status = $1
	`, Pending)
	if err != nil {
		return nil, fmt.Errorf("%w: select query error", err)
	}
	defer rows.Close()

	var jobs []CronJob
	for rows.Next() {
		var job CronJob
		if err := rows.Scan(
			&job.ID,
			&job.Name,
			&job.Type,
			&job.Data,
			&job.Status,
			&job.ScheduledAt,
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

func (s *CronRepository) UpdateStatus(ctx context.Context, name string, status Status, failureReason string) error {
	db := s.DB(ctx)

	res, err := db.ExecContext(ctx, `
			UPDATE cron_jobs
			SET 
				status = $1,
				failure_reason = $2
			WHERE name = $3
		`,
		status,
		sql.NullString{
			String: failureReason,
			Valid:  len(failureReason) > 0,
		},
		name,
	)
	if err != nil {
		return fmt.Errorf("%w: failed to update cron job status", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("%w: failed to get updated rows affected count", err)
	}

	if rows != 1 {
		return fmt.Errorf("%w: name=%s", ErrNotUpdated, name)
	}

	return nil
}
