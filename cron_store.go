package scheduler

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/alextanhongpin/uow"
	"github.com/google/uuid"
	"github.com/lib/pq"
)

var ErrNotUpdated = errors.New("store: not updated")

var _ cronRepository = (*CronRepository)(nil)

type CronRepository struct {
	uow uow.IDB
}

func NewCronRepository(uow uow.IDB) *CronRepository {
	return &CronRepository{
		uow: uow,
	}
}

func (s *CronRepository) DB(ctx context.Context) uow.IDB {
	db, ok := uow.UowContext.Value(ctx)
	if ok {

		return db
	}

	return s.uow
}

func (s *CronRepository) Delete(ctx context.Context, name string) error {
	db := s.DB(ctx)

	_, err := db.ExecContext(ctx, `
		DELETE FROM staged_jobs 
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
			INSERT INTO staged_jobs(
				name, 
				type, 
				data, 
				status, 
				scheduled_at,
				worker_id
			) VALUES (
				$1,
				$2,
				$3,
				$4,
				$5,
				$6
			) 
			ON CONFLICT (name)
			DO UPDATE SET
				type = EXCLUDED.type,
				data = EXCLUDED.data,
				status = EXCLUDED.status,
				failure_reason = NULL,
				scheduled_at = EXCLUDED.scheduled_at,
				worker_id = EXCLUDED.worker_id
			RETURNING id
		`,
		job.Name,
		job.Type,
		job.Data,
		job.Status,
		job.ScheduledAt,
		job.WorkerID,
	).Scan(&job.ID); err != nil {
		return fmt.Errorf("%w: failed to insert cron job", err)
	}

	return nil
}

func (s *CronRepository) FindPending(ctx context.Context) ([]CronJob, error) {
	db := s.DB(ctx)

	rows, err := db.QueryContext(ctx, `
		SELECT 
			id, 
			name, 
			type,
			data, 
			status,
			scheduled_at,
			worker_id
		FROM staged_jobs 
		WHERE status = $1
		FOR UPDATE NOWAIT
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
			&job.WorkerID,
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

func (s *CronRepository) FindByName(ctx context.Context, name string) (*CronJob, error) {
	db := s.DB(ctx)

	var job CronJob
	if err := db.QueryRowContext(ctx, `
			SELECT 
				id, 
				name, 
				type,
				data, 
				status,
				scheduled_at,
				worker_id
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
			&job.WorkerID,
		); err != nil {
		return nil, fmt.Errorf("%w: failed to cron job by name", err)
	}

	return &job, nil
}

func (s *CronRepository) UpdateStatus(ctx context.Context, name string, status Status, failureReason string) error {
	db := s.DB(ctx)

	res, err := db.ExecContext(ctx, `
			UPDATE staged_jobs
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

func (s *CronRepository) BulkUpdateStatus(ctx context.Context, names []string, status Status, failureReason string) error {
	db := s.DB(ctx)

	_, err := db.ExecContext(ctx, `
			UPDATE staged_jobs
			SET 
				status = $1,
				failure_reason = $2
			WHERE name IN ($3)
		`,
		status,
		sql.NullString{
			String: failureReason,
			Valid:  len(failureReason) > 0,
		},
		pq.StringArray(names),
	)
	if err != nil {
		return fmt.Errorf("%w: failed to bulk update cron job status", err)
	}

	return nil
}

func (s *CronRepository) BulkUpdateWorkerID(ctx context.Context, workerID uuid.UUID, names []string) error {
	db := s.DB(ctx)

	_, err := db.ExecContext(ctx, `
			UPDATE staged_jobs
			SET worker_id = $1
			WHERE name IN ($2)
		`,
		workerID,
		pq.StringArray(names),
	)
	if err != nil {
		return fmt.Errorf("%w: failed to update worker id", err)
	}

	return nil
}