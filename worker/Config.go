package worker

import (
	"encoding/json"
	"io/ioutil"
)

// 程序配置
type Config struct {
	EtcdEndpoints []string `json:"etcdEndpoints"`
	EtcdDialtimeout int `json:"etcdDialTimeout"`
}

// 单例
var (
	G_config *Config
)

func InitConfig(filename string) (err error) {
	var (
		content []byte
		conf    Config
	)

	// 读配置文件
	if content, err = ioutil.ReadFile(filename); err != nil {
		return
	}
	// JSON反序列化
	if err = json.Unmarshal(content, &conf); err != nil {
		return
	}
	// 赋值单例
	G_config = &conf
	//fmt.Println(*G_config)
	return
}
