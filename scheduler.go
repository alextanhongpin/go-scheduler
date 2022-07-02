package scheduler

import (
	"fmt"
	"log"
	"sort"
	"sync"
	"time"
)

type Task struct {
	Name           string
	ScheduledAt    time.Time
	ScheduledTimer *time.Timer
}

type TimerScheduler struct {
	tasks sync.Map
}

func NewScheduler() *TimerScheduler {
	return &TimerScheduler{}
}

func (s *TimerScheduler) Flush() {
	s.tasks.Range(func(key, value any) bool {
		task, ok := value.(*Task)
		if !ok {
			return ok
		}

		stopped := task.ScheduledTimer.Stop()
		fmt.Printf("stopped: %+v, %t", task, stopped)

		return true
	})
}

func (s *TimerScheduler) List(n int) []Task {
	var tasks []Task

	s.tasks.Range(func(key, value any) bool {
		if n == 0 {
			return false
		}

		task, ok := value.(*Task)
		if !ok {
			return ok
		}

		tasks = append(tasks, Task{
			Name:        task.Name,
			ScheduledAt: task.ScheduledAt,
		})

		n--

		return n != 0
	})

	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].ScheduledAt.Before(tasks[j].ScheduledAt)
	})

	return tasks
}

func (s *TimerScheduler) Schedule(name string, scheduledAt time.Time, scheduledFn func()) bool {
	if name == "" {
		panic("scheduler: missing name")
	}

	if scheduledFn == nil {
		panic("scheduler: missing func")
	}

	// If the time is negative, it will execute immediately.
	after := scheduledAt.Sub(time.Now())
	if after < 0 {
		scheduledFn()

		return true
	}

	task := &Task{
		Name:        name,
		ScheduledAt: scheduledAt,
		ScheduledTimer: time.AfterFunc(after, func() {
			scheduledFn()

			// Clear tasks after completed.
			s.tasks.Delete(name)
		}),
	}

	if exists := s.remove(task.Name); exists {
		log.Println("removed existing task", task.Name)
	}

	fmt.Printf("scheduled: %+v\n", task)

	// Schedule the new task.
	_, loaded := s.tasks.LoadOrStore(task.Name, task)

	return !loaded
}

func (s *TimerScheduler) Unschedule(name string) bool {
	return s.remove(name)
}

func (s *TimerScheduler) remove(name string) bool {
	if name == "" {
		panic("scheduler: missing name")
	}

	val, found := s.tasks.LoadAndDelete(name)
	if !found {
		return false
	}

	task, ok := val.(*Task)
	if !ok {
		return ok
	}

	return task.ScheduledTimer.Stop()
}

func Crontab(t time.Time) string {
	return fmt.Sprintf("%d %d %d %d %d",
		t.Minute(),
		t.Hour(),
		t.Day(),
		t.Month(),
		t.Weekday(),
	)
}
