# Design

## Using worker id

- assign a unique worker id whenever a server starts
- when scheduling the pending jobs, update all of them to use the server id
- when running the cron, check if the server id matches

Cons:
- sounds like it will work, but it won't
- if a new server spawns, it will steal all the existing jobs
- but if it goes down (e.g. when instance ran, but deployment failed), the job won't go back to the old instance

## Using advisory lock

While it is possible, assigning the key is the actual problem. It is hard to design a key that works for scale, unless we have a centralized store to keep the key mapping.
