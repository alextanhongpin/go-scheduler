# go-scheduler
REST-based cron scheduler


## User story

```
As a developer,
I want to schedule Cron jobs without redeploying the server,
In order to execute job at a later time
```

Requirements
- create new job, but delete old job first
- delete a job
- list the pending jobs, and their next execution date


## Scenario

Background: single instance
- user schedule a job
  - the job exists
  - the job is new
  - the job data is different
- user unschedule a job
  - the job exists
  - the job do not exist

Background: multiple instances


## Other considerations

- what happens when the server restarts before the Cron task is completed
- what happens when there are multiple instances running?
  - we can assign a server id to the server when upserting the record to the db
  - when executing the cron, we can then check if the server id matches the registered server id in the database
  - since there can only be one server id, we can avoid all the complicated locking mechanism
