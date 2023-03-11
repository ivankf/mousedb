package storage

import (
	"errors"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
)

const (
	defaultExpirySecs    = 0       // 默认的过期时间
	defaultMaxFileSize   = 1 << 31 // // 最大文件大小 2G
	defaultTimeoutSecs   = 10      // 超时时间
	defaultValueMaxSize  = 1 << 20 // Value的最大大小
	defaultMergeSecs     = 60      // 合并策略
	defaultCheckSumCrc32 = false   //是否使用 CRC32 校验
)

type Config struct {
	ExpirySecs      int    `json:"expiry-secs,omitempty"`
	MaxFileSize     uint64 `json:"max-file-size,omitempty"`
	OpenTimeoutSecs int    `json:"open-timeout-secs,omitempty"`
	ReadWrite       bool   `json:"read-write,omitempty"`
	MergeSecs       int    `json:"merge-secs,omitempty"`
	CheckSumCrc32   bool   `json:"check-sum-crc-32,omitempty"`
	ValueMaxSize    uint64 `json:"value-max-size,omitempty"`
	Dir             string `json:"dir,omitempty"`
}

func NewConfig() *Config {
	c := &Config{}
	c.ExpirySecs = defaultExpirySecs
	c.MaxFileSize = defaultMaxFileSize
	c.OpenTimeoutSecs = defaultTimeoutSecs
	c.ReadWrite = true
	c.MergeSecs = defaultMergeSecs
	c.CheckSumCrc32 = defaultCheckSumCrc32
	c.ValueMaxSize = defaultValueMaxSize
	return c
}

func (c *Config) DefaultDir() error {
	var homeDir string
	// By default, store meta and data files in current users home directory
	u, err := user.Current()
	if err == nil {
		homeDir = u.HomeDir
	} else if os.Getenv("HOME") != "" {
		homeDir = os.Getenv("HOME")
	} else {
		return fmt.Errorf("failed to determine current user for storage")
	}
	c.Dir = filepath.Join(homeDir, ".mousedb/data")
	return err
}

func (c *Config) Validate() error {
	if c.ExpirySecs < 0 {
		errors.New("expiry_secs can't less than 0")
	}

	if c.MaxFileSize <= 0 {
		errors.New("max-file-size can't less than or equal 0")
	}

	if c.OpenTimeoutSecs < 0 {
		errors.New("timeout-secs can't less than 0")
	}
	return nil
}
