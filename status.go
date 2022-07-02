package scheduler

type Status string

const (
	Success Status = "SUCCESS"
	Failed  Status = "FAILED"
	Pending Status = "PENDING"
)

func (s Status) String() string {
	return string(s)
}

func (s Status) Valid() bool {
	switch s {
	case
		Success,
		Pending,
		Failed:
		return true
	default:
		return false
	}
}

func (s Status) IsPending() bool {
	return s == Pending
}

func (s Status) IsFailed() bool {
	return s == Failed
}

func (s Status) IsSuccess() bool {
	return s == Success
}
