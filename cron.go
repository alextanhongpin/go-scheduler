package scheduler

import (
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
)

type CronTab struct {
	time.Time
}

func NewCronTab(t time.Time) CronTab {
	return CronTab{t.Round(time.Second)}
}

func (c CronTab) String() string {
	return fmt.Sprintf("%d %d %d %d %d",
		c.Minute(),
		c.Hour(),
		c.Day(),
		c.Month(),
		c.Weekday(),
	)
}

func (c CronTab) Validate() error {
	_, err := cron.ParseStandard(c.String())

	return err
}
