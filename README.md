# go-redis-queue

redis queue task for Go

use github.com/go-redis/redis

# use

```
	r := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379", Password: "", DB: 0})
	
	q := redisdb.NewQueue("redis_q", r)

	q.Push("job") 
	
	q.Schedule("job",time.Now().Add(time.Minute)) // a mintue exec job

    // a goroutine loop
	q.AysncTask(func(job string, err error) {
		// ..something
	})

```

