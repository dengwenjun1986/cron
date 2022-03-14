 package main

 import (
	 "flag"
	 "fmt"
	 "github.com/dengwenjun1986/cron/master"
	 "runtime"
	 "time"
 )

func initEnv()  {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

var (
	configFile string // 配置文件路径
)
 // 解析命令行参数
 // master -config ./master.json
 // master -h
 func initArgs(){
	 flag.StringVar(&configFile,"config","./master.json","指定master.json文件")
	 flag.Parse()
 }


func main(){
	var (
		err error
	)

	// 初始化配置文件
	initArgs()

	// 初始化线程
	initEnv()


	// 加载配置
	if err = master.InitConfig(configFile);err != nil {
		goto ERR
	}

	// 任务管理器
	if err = master.InitJobMgr();err != nil {
		goto ERR
	}



	// 启动Api HTTP服务
	if err = master.InitApiServer();err != nil {
		goto ERR
	}

	// 正常退出
	for {
		time.Sleep(1 * time.Second)
	}
	//return
ERR:
	fmt.Println(err)
}