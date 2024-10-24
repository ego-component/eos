package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/gotomicro/ego/core/econf"
	"github.com/gotomicro/ego/core/elog"

	"github.com/ego-component/eos"
)

func main() {
	confs := `
[s3]
debug = false
storageType = "s3"
s3HttpTransportMaxConnsPerHost = 100
s3HttpTransportIdleConnTimeout = "90s"
accessKeyID = "%s"
accessKeySecret = "%s"
endpoint = "%s"
bucket = "%s"
s3ForcePathStyle = false 
region = "%s"
ssl = false
    [s3.buckets.composePayload]
    bucket = "shimo-compose-payloads"
    [s3.buckets.fcTask]
    bucket = "shimo-fc-task"
`
	confs = fmt.Sprintf(confs, os.Getenv("AK_ID"), os.Getenv("AK_SECRET"), os.Getenv("ENDPOINT"), os.Getenv("BUCKET"), os.Getenv("REGION"))
	if err := econf.LoadFromReader(strings.NewReader(confs), toml.Unmarshal); err != nil {
		elog.Panic("LoadFromReader fail", elog.FieldErr(err))
	}
	cmp := eos.Load("s3").Build()
	res, err := cmp.Get(context.Background(), "32E5n4PzKxReUMLy/rules")
	if err != nil {
		elog.Error("Get fail", elog.FieldErr(err))
	}
	fmt.Printf("res--------------->"+"%+v\n", res)

	storage := cmp.Client("composePayload")
	res, err = storage.Get(context.Background(), "aaa")
	fmt.Printf("res, err--------------->"+"%+v, %+v\n", res, err)
}
