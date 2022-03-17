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
	watcher clientv3.Watcher
}

var (
	// 单例
	G_jobMgr *JobMgr
)

// 监听任务变化
func(jobMgr *JobMgr)watchJobs()(err error){
	var (
		getResp *clientv3.GetResponse
		kvpair *mvccpb.KeyValue
		job *common.Job
		watchStartRevision int64
		watchChan clientv3.WatchChan
		watchResp clientv3.WatchResponse
		watchEvent *clientv3.Event
		jobName string
	)
	// 1.get 一下/cron/jobs/目录下的所有任务，并且获知当前集群的revision
	if getResp,err = jobMgr.kv.Get(context.TODO(),common.JOB_SAVE_DIR,clientv3.WithPrevKV()); err != nil {
		return
	}
	for _,kvpair = range getResp.Kvs {
		// 反序列化json得到job
		if job,err = common.UnpackJob(kvpair.Value);err != nil {
			// TODO: 是把这个job同步给scheduler（调度协程）
		}
	}

	// 2.从该revision向后监听事件变化
	go func() { //监听协程
		// 从GET时刻的后续版本监听变化
		watchStartRevision = getResp.Header.Revision + 1
		// 监听/cron/jobs/目录的后续变化
		watchChan = jobMgr.watcher.Watch(context.TODO(),common.JOB_SAVE_DIR,clientv3.WithRev(watchStartRevision))

		// 处理监听事件
		for watchResp = range watchChan {
			for watchEvent = range watchResp.Events {
				switch watchEvent.Type {
				case mvccpb.PUT: //任务保存事件
					if job,err = common.UnpackJob(watchEvent.Kv.Value);err != nil {
						continue
					}
					//构造一个Event事件




					// TODO:反序列化Job,推给scheduler

				case mvccpb.DELETE: //任务被删除了
					// Delete /cron/jobs/job10
					jobName = common.

					// TODO:推一个删除事件给scheduler
				}
			}
		}
	}()


	return
}



// 初始化管理器
func InitJobMgr()(err error){

	var (
		config clientv3.Config
		client *clientv3.Client
		kv clientv3.KV
		lease clientv3.Lease
		watcher clientv3.Watcher
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
	watcher = clientv3.NewWatcher(client)

	// 赋值单例
	G_jobMgr = &JobMgr{
		client: client,
		kv: kv,
		lease: lease,
		watcher: watcher,
	}

	return
}







