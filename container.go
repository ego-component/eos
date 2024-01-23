package eos

import (
	"strings"

	"github.com/gotomicro/ego/core/econf"
	"github.com/gotomicro/ego/core/elog"
)

type Container struct {
	config *config
	name   string
	logger *elog.Component
}

func DefaultContainer() *Container {
	return &Container{
		config: DefaultConfig(),
		logger: elog.EgoLogger.With(elog.FieldComponent(PackageName)),
	}
}

func Load(key string) *Container {
	c := DefaultContainer()
	if err := econf.UnmarshalKey(key, &c.config.BucketConfig); err != nil {
		c.logger.Panic("parse config error", elog.FieldErr(err), elog.FieldKey(key))
		return c
	}
	if err := econf.UnmarshalKey(key, &c.config); err != nil {
		c.logger.Panic("parse config error", elog.FieldErr(err), elog.FieldKey(key))
		return c
	}
	c.logger = c.logger.With(elog.FieldComponentName(key))
	c.name = key
	return c
}

func (c *Container) Build(options ...BuildOption) *Component {
	Register(DefaultGzipCompressor)
	for _, option := range options {
		option(c)
	}

	cmp := &Component{
		logger:  c.logger,
		config:  c.config,
		clients: make(map[string]Client),
	}

	// 初始化默认Storage实例
	if c.config.Bucket != "" {
		// 如果根配置下设置了 bucket，则用此来初始化默认Storage
		if c.config.BucketConfig.Prefix != "" {
			c.config.BucketConfig.Prefix = strings.Trim(c.config.BucketConfig.Prefix, "/") + "/"
		}
		defaultBucketCfg := c.config.BucketConfig
		s, err := newStorage(defaultBucketCfg.Bucket, &defaultBucketCfg, c.logger.With(elog.String("bucket", defaultBucketCfg.Bucket)))
		if err != nil {
			elog.Panic("newStorage fail", elog.String("key", defaultBucketCfg.Bucket), elog.FieldErr(err))
		}
		cmp.defaultClient = s
		cmp.clients[defaultClientKey] = s
	} else {
		// 否则打印日志
		elog.Info("default storage not set")
	}

	// 初始化其他Storage实例
	for bucketKey, bucketCfg := range c.config.Buckets {
		key := c.name + ".buckets." + bucketKey
		if bucketCfg.Bucket == "" {
			elog.Panic("Single bucket name can't be empty", elog.String("key", key), elog.String("invalidBucketName", key+".bucket"))
		}

		singleBucketCfg := c.config.BucketConfig
		if err := econf.UnmarshalKey(key, &singleBucketCfg); err != nil {
			elog.Panic("Single bucket unmarshalKey fail", elog.String("key", key), elog.FieldErr(err))
		}
		// 如果根配置下设置了 bucket，则用此来初始化默认Storage
		if singleBucketCfg.Prefix != "" {
			singleBucketCfg.Prefix = strings.Trim(singleBucketCfg.Prefix, "/") + "/"
		}
		s, err := newStorage(key, &singleBucketCfg, c.logger.With(elog.String("bucket", key)))
		if err != nil {
			elog.Panic("newStorage fail", elog.String("key", key), elog.FieldErr(err))
		}
		cmp.clients[bucketKey] = s
	}

	return cmp
}
