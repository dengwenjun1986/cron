package master

import (
	"encoding/json"
	"fmt"
	"github.com/dengwenjun1986/cron/common"
	"net"
	"net/http"
	"strconv"
	"time"
)

// 任务的http接口
type ApiServer struct {
	httpServer *http.Server
}

var (
	G_apiServer *ApiServer
)

// 保存任务接口
// POST job={"name": "job1","command": "echo hello","cronExpr": "* * * * *"}
func handleJobSave(resp http.ResponseWriter, req *http.Request) {
	var (
		err error
		postJob string
		job common.Job
		oldJob *common.Job
		bytes []byte
	)
	// 解析POST表单
	if err = req.ParseForm();err != nil{
		goto ERR
	}
	// 2.取表单中的Job字段
	postJob = req.PostForm.Get("job")
	// 3.反序列化job
	if err = json.Unmarshal([]byte(postJob),&job);err !=nil{
		goto ERR
	}

	// 4.保存到etcd
	if oldJob,err = G_jobMgr.SaveJob(&job);err != nil {
		goto ERR
	}

	// 5.返回正常应答({"errno":"0","msg":"","data":{...}})
	if bytes,err = common.BuildResp(0,"success",oldJob);err == nil {
		_, _ = resp.Write(bytes)
	}
	return

	ERR:
		//6.返回异常应答
		if bytes,err = common.BuildResp(-1,err.Error(),nil);err != nil {
			_, _ = resp.Write(bytes)
		}


// 任务保存到ETCD中
}

// 初始化服务
func InitApiServer() (err error) {
	var (
		mux        *http.ServeMux
		listener   net.Listener
		httpServer *http.Server
	)
	// 配置路由
	mux = http.NewServeMux()
	mux.HandleFunc("/job/save", handleJobSave)

	// 启动TCP监听
	if listener, err = net.Listen("tcp", ":" + strconv.Itoa(G_config.ApiPort)); err != nil {
		return
	}
	listener = listener

	// 创建一个HTTP服务
	httpServer = &http.Server{
		Handler:      mux,
		ReadTimeout:  time.Duration(G_config.ApiReadTimeout) * time.Millisecond,
		WriteTimeout: time.Duration(G_config.APiWriteTimeout) * time.Millisecond,
	}

	// 赋值单例
	G_apiServer = &ApiServer{httpServer: httpServer}
	fmt.Println(G_apiServer)
	// 启动了服务端
	go httpServer.Serve(listener)

	return
}
