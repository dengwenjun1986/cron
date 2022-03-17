package common

import (
	"encoding/json"
	"strings"
)

// 任务
type Job struct {
	Name     string `json:"name"`     // 任务名
	Command  string `json:"command"`  // shell命令
	CronExpr string `json:"cronExpr"` // cron表达式
}

// HTTP接口应答
type Response struct {
	Errno int         `json:"errno"`
	Msg   string      `json:"msg"`
	Data  interface{} `json:"data"`
}

// 变化事件
type JobEvent struct {
	EventType int // SAVE DELETE
	job       *Job
}

// 应答方法
func BuildResp(errno int, msg string, data interface{}) (resp []byte, err error) {
	//1.定义一个response
	var (
		response Response
	)
	response.Errno = errno
	response.Msg = msg
	response.Data = data

	// 2.序列化json
	resp, err = json.Marshal(response)

	return
}

// 反序列化Job
func UnpackJob(value []byte) (ret *Job, err error) {
	var (
		job *Job
	)
	job = &Job{}
	if err = json.Unmarshal(value, job); err != nil {
		return
	}
	ret = job
	return
}

// 从etcd的key中提取任务名
func ExtratJobName(JobKey string) string {
	// /cron/jobs/job10 删掉 /cron/jobs
	return strings.TrimPrefix(JobKey, JOB_SAVE_DIR)
}

// 任务变化事件 1）更新任务  2）删除任务
func BuildEvent(eventType int, job *Job) (jobEvent *JobEvent) {

	return &JobEvent{
		EventType: eventType,
		job:       job,
	}
}
