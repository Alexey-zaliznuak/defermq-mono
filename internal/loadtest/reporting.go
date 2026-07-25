package loadtest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// WriteReports writes JSON and Markdown reports to paths configured relative
// to ReportConfig.Directory. Each file is replaced atomically.
func WriteReports(report Report, config ReportConfig) error {
	if err := os.MkdirAll(config.Directory, 0o755); err != nil {
		return fmt.Errorf("create report directory: %w", err)
	}
	if err := WriteJSONReport(filepath.Join(config.Directory, config.JSONFile), report); err != nil {
		return err
	}
	if err := WriteMarkdownReport(filepath.Join(config.Directory, config.MarkdownFile), report); err != nil {
		return err
	}
	return nil
}

// WriteJSONReport atomically writes an indented JSON report.
func WriteJSONReport(path string, report Report) error {
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("encode JSON report: %w", err)
	}
	data = append(data, '\n')
	if err := atomicWrite(path, data, 0o644); err != nil {
		return fmt.Errorf("write JSON report: %w", err)
	}
	return nil
}

// WriteMarkdownReport atomically writes a compact, human-readable report.
func WriteMarkdownReport(path string, report Report) error {
	var output bytes.Buffer
	if err := RenderMarkdownReport(&output, report); err != nil {
		return err
	}
	if err := atomicWrite(path, output.Bytes(), 0o644); err != nil {
		return fmt.Errorf("write Markdown report: %w", err)
	}
	return nil
}

// RenderMarkdownReport renders summary data only; individual messages,
// deliveries, statuses, containers, and resource samples are intentionally
// omitted to avoid high-cardinality output.
func RenderMarkdownReport(writer io.Writer, report Report) error {
	var output strings.Builder
	fmt.Fprintf(&output, "# Load test report: %s\n\n", markdownText(report.Name))
	fmt.Fprintf(&output, "- Schema version: %d\n", report.SchemaVersion)
	fmt.Fprintf(&output, "- Seed: %d\n", report.Seed)
	fmt.Fprintf(&output, "- Started: %s\n", report.StartedAt.Format("2006-01-02T15:04:05.999999999Z07:00"))
	fmt.Fprintf(&output, "- Finished: %s\n", report.FinishedAt.Format("2006-01-02T15:04:05.999999999Z07:00"))
	fmt.Fprintf(&output, "- Duration: %.3f s\n", report.DurationSeconds)
	fmt.Fprintf(&output, "- Admission duration: %.3f s\n", report.AdmissionSeconds)
	fmt.Fprintf(&output, "- Admission RPS: %.3f\n\n", report.AdmissionRPS)

	output.WriteString("## Results\n\n")
	output.WriteString("| Planned | Accepted | Create errors | Action errors | Expected deliveries | Delivered unique | Attempts | Duplicates | Early | Receiver early | Missing | Cancelled | Dead | Throughput/s |\n")
	output.WriteString("|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
	fmt.Fprintf(&output, "| %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %.3f |\n\n",
		report.Planned, report.Accepted, report.CreateErrors, report.ActionErrors, report.ExpectedDeliveries,
		report.DeliveredUnique, report.DeliveryAttempts, report.Duplicates,
		report.EarlyDeliveries, report.ReceiverEarly, report.Missing, report.Cancelled, report.Dead,
		report.DeliveryThroughput)

	output.WriteString("## Latencies\n\n")
	output.WriteString("Percentiles use the nearest-rank method.\n\n")
	output.WriteString("| Metric | Count | Min | Mean | P50 | P95 | P99 | Max |\n")
	output.WriteString("|---|---:|---:|---:|---:|---:|---:|---:|\n")
	writeDistributionRow(&output, "Create latency (ms)", report.CreateLatencyMS)
	writeDistributionRow(&output, "Delivery lag (ms)", report.DeliveryLagMS)
	writeDistributionRow(&output, "Receiver-observed lag (ms)", report.ReceiverLagMS)
	output.WriteString("\nDelivery lag and Early use database timestamps. Receiver-observed values use the load-generator clock and can include Docker VM clock skew.\n")

	output.WriteString("\n## Message groups\n\n")
	output.WriteString("| Group | Planned | Accepted | Create errors | Action errors | Expected | Unique | Attempts | Duplicates | Early | Receiver early | Missing | Cancelled | Dead | Other statuses | Create p95 ms | Lag p95 ms | Receiver lag p95 ms |\n")
	output.WriteString("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|---:|---:|\n")
	for _, group := range report.Groups {
		fmt.Fprintf(&output, "| %s | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %s | %s | %s | %s |\n",
			markdownCell(group.Name), group.Planned, group.Accepted, group.CreateErrors,
			group.ActionErrors, group.ExpectedDeliveries, group.DeliveredUnique, group.DeliveryAttempts,
			group.Duplicates, group.EarlyDeliveries, group.ReceiverEarly, group.Missing, group.Cancelled,
			group.Dead, markdownCell(formatStatuses(group.OtherFinalStatuses)),
			formatFloat(group.CreateLatencyMS.P95), formatFloat(group.DeliveryLagMS.P95),
			formatFloat(group.ReceiverLagMS.P95))
	}

	output.WriteString("\n## Resources\n\n")
	output.WriteString("I/O values are cumulative counters; the table shows their maximum observed values.\n\n")
	output.WriteString("| Scope | Samples | CPU avg | CPU p95 | CPU p99 | CPU max | Memory avg MB | Memory p95 MB | Memory p99 MB | Memory max MB | PIDs avg | PIDs p95 | PIDs p99 | PIDs max | Net RX max MB | Net TX max MB | Block read max MB | Block write max MB |\n")
	output.WriteString("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, name := range []string{"go", "non_go", "all"} {
		resource := report.Resources[name]
		fmt.Fprintf(&output, "| %s | %d | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n",
			name, resource.Samples,
			formatFloat(resource.CPUPercent.Mean), formatFloat(resource.CPUPercent.P95),
			formatFloat(resource.CPUPercent.P99), formatFloat(resource.CPUPercent.Max),
			formatFloat(resource.MemoryMB.Mean), formatFloat(resource.MemoryMB.P95),
			formatFloat(resource.MemoryMB.P99), formatFloat(resource.MemoryMB.Max),
			formatFloat(resource.PIDs.Mean), formatFloat(resource.PIDs.P95),
			formatFloat(resource.PIDs.P99), formatFloat(resource.PIDs.Max),
			formatFloat(resource.NetRXMB.Max), formatFloat(resource.NetTXMB.Max),
			formatFloat(resource.BlockReadMB.Max), formatFloat(resource.BlockWriteMB.Max))
	}

	if len(report.Warnings) > 0 {
		output.WriteString("\n## Warnings\n\n")
		const maxMarkdownWarnings = 100
		limit := min(len(report.Warnings), maxMarkdownWarnings)
		for _, warning := range report.Warnings[:limit] {
			fmt.Fprintf(&output, "- %s\n", markdownText(warning))
		}
		if omitted := len(report.Warnings) - limit; omitted > 0 {
			fmt.Fprintf(&output, "- … %d additional warnings omitted; see JSON report\n", omitted)
		}
	}

	if _, err := io.WriteString(writer, output.String()); err != nil {
		return fmt.Errorf("render Markdown report: %w", err)
	}
	return nil
}

func writeDistributionRow(output *strings.Builder, name string, summary DistributionSummary) {
	fmt.Fprintf(output, "| %s | %d | %s | %s | %s | %s | %s | %s |\n",
		name, summary.Count, formatFloat(summary.Min), formatFloat(summary.Mean),
		formatFloat(summary.P50), formatFloat(summary.P95), formatFloat(summary.P99),
		formatFloat(summary.Max))
}

func formatStatuses(statuses map[string]int) string {
	if len(statuses) == 0 {
		return "—"
	}
	names := make([]string, 0, len(statuses))
	for name := range statuses {
		names = append(names, name)
	}
	sort.Strings(names)
	values := make([]string, 0, len(names))
	for _, name := range names {
		values = append(values, name+"="+strconv.Itoa(statuses[name]))
	}
	return strings.Join(values, ", ")
}

func formatFloat(value float64) string {
	return strconv.FormatFloat(value, 'f', 3, 64)
}

func markdownCell(value string) string {
	return strings.ReplaceAll(markdownText(value), "|", "\\|")
}

func markdownText(value string) string {
	return strings.NewReplacer("\r", " ", "\n", " ").Replace(value)
}

func atomicWrite(path string, data []byte, mode os.FileMode) (returnErr error) {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return err
	}
	file, err := os.CreateTemp(directory, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tempPath := file.Name()
	defer func() {
		_ = file.Close()
		if returnErr != nil {
			_ = os.Remove(tempPath)
		}
	}()
	if err := file.Chmod(mode); err != nil {
		return err
	}
	if _, err := file.Write(data); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(tempPath, path); err != nil {
		return err
	}
	return nil
}
