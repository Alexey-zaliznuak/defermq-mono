package loadtest

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Duration time.Duration

func (d *Duration) UnmarshalText(text []byte) error {
	value, err := time.ParseDuration(string(text))
	if err != nil {
		return err
	}
	*d = Duration(value)
	return nil
}

func (d Duration) MarshalText() ([]byte, error) {
	return []byte(time.Duration(d).String()), nil
}

func (d Duration) Value() time.Duration { return time.Duration(d) }

type Config struct {
	Name       string          `yaml:"name"`
	Seed       int64           `yaml:"seed"`
	Gateway    GatewayConfig   `yaml:"gateway"`
	Receiver   ReceiverConfig  `yaml:"receiver"`
	Load       LoadConfig      `yaml:"load"`
	Resources  ResourceConfig  `yaml:"resources"`
	Assertions AssertionConfig `yaml:"assertions"`
	Report     ReportConfig    `yaml:"report"`
	Groups     []MessageGroup  `yaml:"message_groups"`
}

type GatewayConfig struct {
	URL     string   `yaml:"url"`
	Timeout Duration `yaml:"timeout"`
}

type ReceiverConfig struct {
	ListenAddress string `yaml:"listen_address"`
	PublicURL     string `yaml:"public_url"`
	Path          string `yaml:"path"`
}

type LoadConfig struct {
	CreateConcurrency int      `yaml:"create_concurrency"`
	StatusConcurrency int      `yaml:"status_concurrency"`
	PollInterval      Duration `yaml:"poll_interval"`
	AwaitTimeout      Duration `yaml:"await_timeout"`
	Warmup            Duration `yaml:"warmup"`
	Cooldown          Duration `yaml:"cooldown"`
	EarlyTolerance    Duration `yaml:"early_tolerance"`
}

type ResourceConfig struct {
	Enabled        bool     `yaml:"enabled"`
	SampleInterval Duration `yaml:"sample_interval"`
	ComposeProject string   `yaml:"compose_project"`
	GoServices     []string `yaml:"go_services"`
	NonGoServices  []string `yaml:"non_go_services"`
	CommandTimeout Duration `yaml:"command_timeout"`
}

type ReportConfig struct {
	Directory      string `yaml:"directory"`
	JSONFile       string `yaml:"json_file"`
	MarkdownFile   string `yaml:"markdown_file"`
	IncludeSamples bool   `yaml:"include_resource_samples"`
}

type AssertionConfig struct {
	MaxCreateErrors        int      `yaml:"max_create_errors"`
	MaxActionErrors        int      `yaml:"max_action_errors"`
	MaxMissing             int      `yaml:"max_missing"`
	MaxDuplicates          int      `yaml:"max_duplicates"`
	MaxEarlyDeliveries     int      `yaml:"max_early_deliveries"`
	MinDeliverySuccessRate float64  `yaml:"min_delivery_success_rate"`
	MaxDeliveryLag         Duration `yaml:"max_delivery_lag"`
}

type MessageGroup struct {
	Name               string       `yaml:"name"`
	Count              int          `yaml:"count"`
	AdmissionOffset    Distribution `yaml:"admission_offset"`
	DeliveryDelay      Distribution `yaml:"delivery_delay"`
	PayloadBytes       int          `yaml:"payload_bytes"`
	MaxAttempts        int          `yaml:"max_attempts"`
	FailFirstAttempts  int          `yaml:"fail_first_attempts"`
	FailureStatus      int          `yaml:"failure_status"`
	CancelFraction     float64      `yaml:"cancel_fraction"`
	RescheduleFraction float64      `yaml:"reschedule_fraction"`
	RescheduleDelay    Distribution `yaml:"reschedule_delay"`
}

type Distribution struct {
	Kind   string   `yaml:"kind"`
	Mean   Duration `yaml:"mean"`
	StdDev Duration `yaml:"stddev"`
	Min    Duration `yaml:"min"`
	Max    Duration `yaml:"max"`
}

func LoadConfigFile(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read load test config: %w", err)
	}
	var config Config
	decoder := yaml.NewDecoder(strings.NewReader(string(data)))
	decoder.KnownFields(true)
	if err := decoder.Decode(&config); err != nil {
		return Config{}, fmt.Errorf("decode load test config: %w", err)
	}
	config.defaults()
	if err := config.Validate(); err != nil {
		return Config{}, err
	}
	return config, nil
}

func (c *Config) defaults() {
	if c.Name == "" {
		c.Name = "defermq-load-test"
	}
	if c.Seed == 0 {
		c.Seed = 1
	}
	if c.Gateway.Timeout == 0 {
		c.Gateway.Timeout = Duration(10 * time.Second)
	}
	if c.Receiver.ListenAddress == "" {
		c.Receiver.ListenAddress = "127.0.0.1:18080"
	}
	if c.Receiver.Path == "" {
		c.Receiver.Path = "/defermq"
	}
	if c.Load.CreateConcurrency == 0 {
		c.Load.CreateConcurrency = 32
	}
	if c.Load.StatusConcurrency == 0 {
		c.Load.StatusConcurrency = 32
	}
	if c.Load.PollInterval == 0 {
		c.Load.PollInterval = Duration(250 * time.Millisecond)
	}
	if c.Load.AwaitTimeout == 0 {
		c.Load.AwaitTimeout = Duration(10 * time.Minute)
	}
	if c.Load.EarlyTolerance == 0 {
		c.Load.EarlyTolerance = Duration(100 * time.Millisecond)
	}
	if c.Resources.SampleInterval == 0 {
		c.Resources.SampleInterval = Duration(time.Second)
	}
	if c.Resources.ComposeProject == "" {
		c.Resources.ComposeProject = "defermq"
	}
	if len(c.Resources.GoServices) == 0 {
		c.Resources.GoServices = []string{"gateway", "postgres-manager", "pusher"}
	}
	if len(c.Resources.NonGoServices) == 0 {
		c.Resources.NonGoServices = []string{"postgres", "nats", "kafka", "rabbitmq", "target-postgres"}
	}
	if c.Resources.CommandTimeout == 0 {
		c.Resources.CommandTimeout = Duration(10 * time.Second)
	}
	if c.Report.Directory == "" {
		c.Report.Directory = "test-results/load"
	}
	if c.Report.JSONFile == "" {
		c.Report.JSONFile = "report.json"
	}
	if c.Report.MarkdownFile == "" {
		c.Report.MarkdownFile = "report.md"
	}
}

func (c Config) Validate() error {
	var errs []error
	if c.Gateway.URL == "" {
		errs = append(errs, errors.New("gateway.url is required"))
	}
	if c.Receiver.PublicURL == "" {
		errs = append(errs, errors.New("receiver.public_url is required"))
	} else if parsed, err := url.Parse(c.Receiver.PublicURL); err != nil ||
		(parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		errs = append(errs, errors.New("receiver.public_url must be an absolute HTTP URL"))
	}
	if c.Receiver.ListenAddress == "" || !strings.HasPrefix(c.Receiver.Path, "/") {
		errs = append(errs, errors.New("receiver.listen_address and absolute receiver.path are required"))
	}
	if c.Gateway.Timeout.Value() <= 0 || c.Load.PollInterval.Value() <= 0 ||
		c.Load.AwaitTimeout.Value() <= 0 || c.Resources.SampleInterval.Value() <= 0 ||
		c.Resources.CommandTimeout.Value() <= 0 {
		errs = append(errs, errors.New("timeouts and sampling intervals must be positive"))
	}
	if c.Load.CreateConcurrency <= 0 || c.Load.StatusConcurrency <= 0 {
		errs = append(errs, errors.New("load concurrency must be positive"))
	}
	if len(c.Groups) == 0 {
		errs = append(errs, errors.New("at least one message group is required"))
	}
	if c.Assertions.MaxCreateErrors < 0 || c.Assertions.MaxActionErrors < 0 || c.Assertions.MaxMissing < 0 ||
		c.Assertions.MaxDuplicates < 0 || c.Assertions.MaxEarlyDeliveries < 0 ||
		c.Assertions.MinDeliverySuccessRate < 0 || c.Assertions.MinDeliverySuccessRate > 1 ||
		c.Assertions.MaxDeliveryLag.Value() < 0 {
		errs = append(errs, errors.New("assertion limits are invalid"))
	}
	names := make(map[string]struct{}, len(c.Groups))
	for index, group := range c.Groups {
		if err := group.validate(); err != nil {
			errs = append(errs, fmt.Errorf("message_groups[%d]: %w", index, err))
		}
		if _, exists := names[group.Name]; exists {
			errs = append(errs, fmt.Errorf("duplicate message group name %q", group.Name))
		}
		names[group.Name] = struct{}{}
	}
	return errors.Join(errs...)
}

func (g MessageGroup) validate() error {
	if g.Name == "" || g.Count <= 0 || g.PayloadBytes < 0 || g.MaxAttempts <= 0 {
		return errors.New("name, positive count/max_attempts and non-negative payload_bytes are required")
	}
	if g.FailFirstAttempts < 0 || g.FailFirstAttempts >= g.MaxAttempts {
		return errors.New("fail_first_attempts must be non-negative and lower than max_attempts")
	}
	if g.FailFirstAttempts > 0 && (g.FailureStatus < 400 || g.FailureStatus > 599) {
		return errors.New("failure_status must be an HTTP error status")
	}
	if g.CancelFraction < 0 || g.RescheduleFraction < 0 ||
		g.CancelFraction > 1 || g.RescheduleFraction > 1 ||
		g.CancelFraction+g.RescheduleFraction > 1 {
		return errors.New("cancel/reschedule fractions must fit into [0,1]")
	}
	for name, distribution := range map[string]Distribution{
		"admission_offset": g.AdmissionOffset,
		"delivery_delay":   g.DeliveryDelay,
		"reschedule_delay": g.RescheduleDelay,
	} {
		if err := distribution.validate(); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
	}
	return nil
}

func (d Distribution) validate() error {
	if d.Kind != "" && d.Kind != "normal" && d.Kind != "uniform" {
		return errors.New("kind must be normal or uniform")
	}
	if d.StdDev.Value() < 0 {
		return errors.New("stddev cannot be negative")
	}
	if d.Max != 0 && d.Min.Value() > d.Max.Value() {
		return errors.New("min cannot exceed max")
	}
	if d.Kind == "uniform" && d.Max == 0 {
		return errors.New("uniform distribution requires max")
	}
	return nil
}
