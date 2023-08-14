package scheduling

import (
	"github.com/google/wire"
	"github.com/robfig/cron/v3"
)

// ProviderSet is scheduling providers.
var ProviderSet = wire.NewSet(NewScheduledTask)

type ScheduledTask struct {
	c *cron.Cron
}

var Task *ScheduledTask

func NewScheduledTask() *ScheduledTask {
	c := cron.New()
	Task = &ScheduledTask{
		c: c,
	}
	return Task
}

// AddTask 添加task
func (scheduledTask *ScheduledTask) AddTask(cron string, job cron.Job) (cron.EntryID, error) {
	entryID, err := scheduledTask.c.AddJob(cron, job)
	return entryID, err
}

// Start 启动所有的task
func (scheduledTask *ScheduledTask) Start() {
	scheduledTask.c.Start()
}
