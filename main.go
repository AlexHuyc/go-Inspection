package main

import (
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"strconv"
	"time"
)

type body struct {
	Data struct {
		Result []struct {
			Metric struct {
				Name      string `json:"__name__"`
				ClusterId string `json:"clusterId"`
				Hostname  string `json:"hostname"`
				Instance  string `json:"instance"`
				Job       string `json:"job"`
				NodeId    string `json:"nodeId"`
				RackId    string `json:"rackId"`
			} `json:"metric"`
			Value []interface{} `json:"value"`
		} `json:"result"`
		ResultType string `json:"resultType"`
	} `json:"data"`
	Status string `json:"status"`
}

const (
	//KafkaInfomation="192.168.210.127:9192"
	//TopicInformation="testgo"
	//Separator string ="|+|"
	//node ="tdh1"
	//nodeurl ="http://"+node+":8690/api/v1/query?query="
	//

	KafkaInfomation         = "none-datacenter.kafka.chinner.com:9192"
	TopicInformation        = "tdh-hardwareinfo"
	Separator        string = "|+|"
	node                    = "node42"
	nodeurl                 = "http://" + node + ":8690/api/v1/query?query="

	cpuurl  = nodeurl + "(1-avg(irate(node_cpu_seconds_total{mode=\"idle\"}[1m]))by(hostname))*100"
	memurl  = nodeurl + "node_memory_MemTotal_bytes-node_memory_MemFree_bytes"
	loadurl = nodeurl + "node_load1"
)
const (
	cpuType = iota
	memoryType
	loadType
)

var (
	kafkamsg           string
	originalUnixtime   int64
	originalValue      string
	crawltype          string
	originalValueRatio string
	crawltypenum       int
)

func crawlpage(url string) string {
	//获取prometheus   每分钟cpu占比信息
	resp, err := http.Get(url)
	//判空报错
	if err != nil {
		// handle error
	}

	//关闭http session
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		// handle error
	}
	return string(body)
}

func castoiginalunixtime(val interface{}) int64 {
	switch va := val.(type) {
	case float64:
		return int64(va * math.Pow10(0))
	default:
		fmt.Print("ERROR xm.Data.Result[num].Value[0] : ", val)
		panic("unknow value type")
	}

}

func castoriginalvalue(val interface{}) string {
	switch va := val.(type) {
	case string:
		return va
	default:
		fmt.Print("ERROR xm.Data.Result[num].Value[1] : ", val)
		panic("unknow value type")
	}
}

func onlyanalysis(originalValue string) string {
	return castoriginalvalue(originalValue)
}

func memanalysis(originalValue string) string {
	floatov, _ := strconv.ParseFloat(originalValue, 64)
	return strconv.FormatFloat((floatov/1024)/131416292*100, 'f', -1, 64)
}

func originalvalueanalysis(typenum int, originalValue string) string {
	switch typenum {
	case 0, 2:
		return onlyanalysis(originalValue)
	case 1:
		return memanalysis(originalValue)
	}
	return "Data Analysis Error"
}

func analyusis(typeNum int, crawltype, url string) [150]string {
	var xm body
	err := json.Unmarshal([]byte(crawlpage(url)), &xm)
	if err != nil {
		panic(err)
	}
	array := [150]string{}
	//array := [3]string{}
	//遍历Result标签结构体数组
	for num := 0; num < len(xm.Data.Result); num++ {
		//获取主机名
		HostName := xm.Data.Result[num].Metric.Hostname
		//路由判断Result内Value数组 （由于go的[]interface{}接口结构特性，需要由使用者定于数据类型）
		originalUnixtime = castoiginalunixtime(xm.Data.Result[num].Value[0])
		//路由判断Result内Value数组 （由于go的[]interface{}接口结构特性，需要由使用者定于数据类型）
		originalValue = castoriginalvalue(xm.Data.Result[num].Value[1])
		//fmt.Printf("%T",originalValue)
		originalValueRatio = originalvalueanalysis(typeNum, originalValue)
		//组装kafka消息体
		array[num] =
			crawltype + strconv.FormatInt(originalUnixtime, 10) + HostName + Separator +
				HostName + Separator +
				crawltype + Separator +
				strconv.FormatInt(originalUnixtime, 10) + Separator +
				time.Unix(originalUnixtime, 0).Format("2006-01-02") + Separator +
				time.Unix(originalUnixtime, 0).Format("15:04:05") + Separator +
				originalValue + Separator + originalValueRatio
	}
	return array
}

func main() {
	tiker := time.NewTicker(time.Second * 15)
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	producer, err := sarama.NewSyncProducer([]string{KafkaInfomation}, config)
	if err != nil {
		panic(err)
	}

	//定时执行
	for i := 1; i > 0; i++ {
		i = 1
		//解析cpu
		for _, s := range analyusis(cpuType, "cpuinfo", cpuurl) {
			if s != "" {
				partition, offset, err := producer.SendMessage(&sarama.ProducerMessage{Topic: TopicInformation, Key: nil, Value: sarama.StringEncoder(s)})
				if err != nil {
					log.Fatal("消息发送失败: %q", err)
				}
				fmt.Println("kafka msg 发送:"+s, " paratition", partition, "  offset", offset)
			}
		}
		//解析内存
		for _, s := range analyusis(memoryType, "meminfo", memurl) {
			if s != "" {
				//fmt.Println(s)
				partition, offset, err := producer.SendMessage(&sarama.ProducerMessage{Topic: TopicInformation, Key: nil, Value: sarama.StringEncoder(s)})
				if err != nil {
					log.Fatal("消息发送失败: %q", err)
				}
				fmt.Println("kafka msg 发送:"+s, " paratition", partition, "  offset", offset)
			}
		}
		//解析负载
		for _, s := range analyusis(loadType, "loadinfo", loadurl) {
			if s != "" {
				partition, offset, err := producer.SendMessage(&sarama.ProducerMessage{Topic: TopicInformation, Key: nil, Value: sarama.StringEncoder(s)})
				if err != nil {
					log.Fatal("消息发送失败: %q", err)
				}
				fmt.Println("kafka msg 发送:"+s, " paratition", partition, "  offset", offset)
			}
		}
		<-tiker.C
	}
}
