package master

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/dengwenjun1986/cron/common"
	clientv3 "go.etcd.io/etcd/client/v3"
	"time"
	)

// 任务管理器
type JobMgr struct {
	client *clientv3.Client
	kv clientv3.KV
	lease clientv3.Lease
}

var (
	// 单例
	G_jobMgr *JobMgr
)

// 初始化管理器
func InitJobMgr()(err error){

	var (
		config clientv3.Config
		client *clientv3.Client
		kv clientv3.KV
		lease clientv3.Lease
	)

	// 初始化配置
	config = clientv3.Config{
		Endpoints:G_config.EtcdEndpoints, // 集群地址
		DialTimeout: time.Duration(G_config.EtcdDialtimeout) * time.Millisecond, // 链接超时
	}
	//建立链接
	if client,err = clientv3.New(config);err !=nil {
		return
	}

	// 得到kv和lease API子集
	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)

	// 赋值单例
	G_jobMgr = &JobMgr{
		client: client,
		kv: kv,
		lease: lease,
	}

	return
}

// 保存任务到etcd
func (jobMgr *JobMgr)SaveJob(job *common.Job)(oldjob *common.Job,err error) {
	// 把任务保存到/cron/jobs/任务名 -> json
	var (
		jobKey string
		jobValue []byte
		putResp *clientv3.PutResponse
		oldJobObj common.Job
	)

	// etcd保存key
	jobKey = "/cron/jobs/" + job.Name

	// 任务信息json
	if jobValue,err = json.Marshal(job);err != nil {
		return
	}
	fmt.Println(jobKey,string(jobValue))

	// 保存到etcd
	if putResp,err = jobMgr.kv.Put(context.TODO(),jobKey,string(jobValue),clientv3.WithPrevKV());err != nil {
		return
	}
	fmt.Println(putResp.Header.Revision)
	// 如果是更新，那么返回旧值
	if putResp.PrevKv != nil {
		// 对旧值做反序列化
		if err = json.Unmarshal(putResp.PrevKv.Value, &oldJobObj); err != nil {
			err = nil
			return
		}
		oldjob = &oldJobObj
	}
	return
}

// 删除任务
func(jobMgr *JobMgr)DeleteJob(name string)(oldJob *common.Job,err error)  {
	var (
		jobKey string
		delResp *clientv3.DeleteResponse
		oldJobObj common.Job
	)

	// etcd中保存任务的key
	jobKey = "/cron/jobs/" + name

	// 从etcd中删除他
	if delResp,err = jobMgr.kv.Delete(context.TODO(),jobKey,clientv3.WithPrevKV()); err != nil {

		return
	}

	// 返回被删除的任务信息
	if len(delResp.PrevKvs) != 0 {
		// 解析一下旧值，返回
		if err = json.Unmarshal(delResp.PrevKvs[0].Value,&oldJobObj);err != nil {
			err = nil
			return
		}
		oldJob = &oldJobObj

	}


	return
}