package scheduler

import (
	"database/sql"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
)

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

	cronEntryID cron.EntryID
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
