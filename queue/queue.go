package redisdb

import (
	"fmt"
	"github.com/go-redis/redis"
	"strconv"
	"time"
)

const ScheduleRate = time.Second * 3

var popJobsLuaScript *redis.Script

func init() {
	popJobsLuaScript = redis.NewScript(`
		local name = ARGV[1]
		local timestamp = ARGV[2]
        local limit = ARGV[3]
		local results = redis.call('zrangebyscore', name, '-inf', timestamp, 'LIMIT', 0, limit)
		if table.getn(results) > 0 then
			redis.call('zrem', name, unpack(results))
		end
        return results`)
}

type Queue struct {
	c    *redis.Client
	Name string
}

func NewQueue(queueName string, c *redis.Client) *Queue {
	return &Queue{
		c:    c,
		Name: queueName,
	}
}

func (q *Queue) Push(job string) (bool, error) {
	return q.Schedule(job, time.Now())
}

func (q *Queue) Schedule(job string, when time.Time) (bool, error) {
	score := when.UnixNano()
	added, err := q.c.ZAdd(q.Name, redis.Z{Score: float64(score), Member: job}).Result()
	return added > 0, err

}

func (q *Queue) Pending() (int64, error) {
	return q.c.ZCard(q.Name).Result()
}

func (q *Queue) Rem(key ...string) (int64, error) {
	return q.c.ZRem(q.Name, key).Result()
}

func (q *Queue) Clear() error {
	return q.c.Del(q.Name).Err()
}

func (q *Queue) Pop() (string, error) {
	jobs, err := q.PopJobs(1)
	if err != nil {
		return "", err
	}
	if len(jobs) == 0 {
		return "", nil
	}
	return jobs[0], nil
}

func (q *Queue) PopJobs(limit int) ([]string, error) {
	result, err := popJobsLuaScript.Run(q.c,
		[]string{"name", "timestamp", "limit"}, q.Name,
		fmt.Sprintf("%d", time.Now().UnixNano()), strconv.Itoa(limit)).Result()
	strs := make([]string, 0)
	for _, v := range result.([]interface{}) {
		strs = append(strs, v.(string))
	}
	return strs, err
}

func (q *Queue) AysncTask(f func(string, error)) {
	go func(fun func(string, error)) {
		for {
			job, err := q.Pop()
			if job != "" {
				fun(job, err)
			} else {
				time.Sleep(ScheduleRate)
			}
		}
	}(f)
}
