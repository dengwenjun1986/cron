package worker

import (
	"context"
	"github.com/dengwenjun1986/cron/common"
	"go.etcd.io/etcd/api/v3/mvccpb"
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
	getResp *clientv3.GetResponse
	kvpair *mvccpb.KeyValue
)

// 监听任务变化
func(jobMgr *JobMgr)watchJobs()(err error){
	// 1.get 一下/cron/jobs/目录下的所有任务，并且获知当前集群的revision
	if getResp,err = jobMgr.kv.Get(context.TODO(),common.JOB_SAVE_DIR,clientv3.WithPrevKV()); err != nil {
		return
	}
	for _,kvpair = range getResp.Kvs {
		// 反序列化json得到job

	}

	return
}



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







