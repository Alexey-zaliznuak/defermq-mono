package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/defermq/defermq/internal/loadtest"
)

func main() {
	os.Exit(run())
}

func run() int {
	configPath := flag.String("config", "configs/loadtest.example.yml", "path to load test YAML")
	timeout := flag.Duration("timeout", 0, "optional overall test timeout")
	flag.Parse()

	config, err := loadtest.LoadConfigFile(*configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "load configuration:", err)
		return 2
	}

	root, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx := root
	cancel := func() {}
	if *timeout > 0 {
		ctx, cancel = context.WithTimeout(root, *timeout)
	}
	defer cancel()

	options := make([]loadtest.Option, 0, 1)
	if config.Resources.Enabled {
		options = append(options, loadtest.WithResourceSampler(
			loadtest.NewDockerResourceSampler(config.Resources, nil),
		))
	}
	runner, err := loadtest.NewRunner(config, options...)
	if err != nil {
		fmt.Fprintln(os.Stderr, "initialize load test:", err)
		return 2
	}

	fmt.Printf("load test %q started: groups=%d seed=%d\n", config.Name, len(config.Groups), config.Seed)
	result, runErr := runner.Run(ctx)
	report := loadtest.AggregateRun(config, result)
	if err := loadtest.WriteReports(report, config.Report); err != nil {
		fmt.Fprintln(os.Stderr, "write reports:", err)
		return 2
	}

	jsonPath := filepath.Join(config.Report.Directory, config.Report.JSONFile)
	markdownPath := filepath.Join(config.Report.Directory, config.Report.MarkdownFile)
	fmt.Printf(
		"finished in %.3fs: accepted=%d delivered=%d attempts=%d duplicates=%d early=%d missing=%d max_lag=%.3fms\n",
		report.DurationSeconds, report.Accepted, report.DeliveredUnique, report.DeliveryAttempts,
		report.Duplicates, report.EarlyDeliveries, report.Missing, report.DeliveryLagMS.Max,
	)
	fmt.Printf("reports: %s, %s\n", jsonPath, markdownPath)

	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		fmt.Fprintln(os.Stderr, "load test execution:", runErr)
		return 1
	}
	if assertionErr := loadtest.EvaluateAssertions(report, config.Assertions); assertionErr != nil {
		fmt.Fprintln(os.Stderr, "assertions failed:", assertionErr)
		return 1
	}
	if errors.Is(runErr, context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		fmt.Fprintln(os.Stderr, "load test interrupted:", ctx.Err())
		return 1
	}
	fmt.Println("all assertions passed")
	return 0
}
