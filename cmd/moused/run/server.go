package run

import (
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"runtime/pprof"
	"time"

	"mousedb/service/storage"

	"go.uber.org/zap"
)

var startTime time.Time

func init() {
	startTime = time.Now()
}

// BuildInfo represents the build details for the server code.
type BuildInfo struct {
	Version string
	Commit  string
	Branch  string
	Time    string
}

type Server struct {
	BuildInfo BuildInfo

	err     chan error
	closing chan struct{}

	BindAddress string
	Listener    net.Listener

	Logger *zap.Logger

	Services []Service

	// Profiling
	CPUProfile            string
	CPUProfileWriteCloser io.WriteCloser
	MemProfile            string
	MemProfileWriteCloser io.WriteCloser

	config *Config
}

// Service represents a service attached to the server.
type Service interface {
	WithLogger(log *zap.Logger)
	Open() error
	Close() error
}

// Err returns an error channel that multiplexes all out of band errors received from all services.
func (s *Server) Err() <-chan error { return s.err }

func (s *Server) Close() error {
	close(s.closing)
	return nil
}

// NewServer returns a new instance of Server built from a config.
func NewServer(c *Config, buildInfo *BuildInfo) (*Server, error) {
	// TODO init server
	// First grab the base tls config we will use for all clients and servers

	// TODO Create store dir

	bind := c.BindAddress
	s := &Server{
		BuildInfo:   *buildInfo,
		err:         make(chan error),
		closing:     make(chan struct{}),
		BindAddress: bind,
		config:      c,
	}

	//TODO add listen

	return s, nil
}

// Open opens the meta and data store and all services.
func (s *Server) Open() error {
	// Start profiling if requested.
	if err := s.startProfile(); err != nil {
		return err
	}

	// Open shared TCP connection.
	ln, err := net.Listen("tcp", s.BindAddress)
	if err != nil {
		return fmt.Errorf("listen: %s", err)
	}
	s.Listener = ln

	//TODO 设置路由
	//TODO 装载服务
	s.appendStorage(&s.config.Storage)
	//TODO 启动服务
	for _, service := range s.Services {
		service.Open()
	}
	return nil
}

func (s *Server) appendStorage(c *storage.Config) {
	storage := storage.New(c)
	s.Services = append(s.Services, storage)
}

// prof stores the file locations of active profiles.
// StartProfile initializes the cpu and memory profile, if specified.
func (s *Server) startProfile() error {
	if s.CPUProfile != "" {
		f, err := os.Create(s.CPUProfile)
		if err != nil {
			return fmt.Errorf("cpuprofile: %v", err)
		}

		s.CPUProfileWriteCloser = f
		if err := pprof.StartCPUProfile(s.CPUProfileWriteCloser); err != nil {
			return err
		}

		s.Logger.Info("writing CPU profile", zap.String("location", s.CPUProfile))
	}

	if s.MemProfile != "" {
		f, err := os.Create(s.MemProfile)
		if err != nil {
			return fmt.Errorf("memprofile: %v", err)
		}

		s.MemProfileWriteCloser = f
		runtime.MemProfileRate = 4096

		s.Logger.Info("writing mem profile", zap.String("location", s.MemProfile))
	}

	return nil
}
