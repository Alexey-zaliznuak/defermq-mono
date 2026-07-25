package loadtest

import (
	"math"
	"testing"
)

func TestParseBytesSIAndIEC(t *testing.T) {
	tests := map[string]float64{
		"0B":       0,
		"1.5 kB":   1500,
		"2MB":      2e6,
		"1.5GiB":   1.5 * 1024 * 1024 * 1024,
		"2.25 TiB": 2.25 * math.Pow(1024, 4),
	}
	for input, want := range tests {
		t.Run(input, func(t *testing.T) {
			got, err := parseBytes(input)
			if err != nil {
				t.Fatal(err)
			}
			if got != want {
				t.Fatalf("parseBytes(%q) = %v, want %v", input, got, want)
			}
		})
	}
}

func TestParseDockerStatsLine(t *testing.T) {
	line := `{"ID":"abc","Name":"defermq-gateway-1","CPUPerc":"12.50%","MemUsage":"1.5GiB / 2GiB","NetIO":"10.2kB / 3MB","BlockIO":"4KiB / 1.25MiB","PIDs":"17"}`
	row, point, err := parseDockerStatsLine(line)
	if err != nil {
		t.Fatal(err)
	}
	if row.ID != "abc" || point.CPUPercent != 12.5 || point.PIDs != 17 {
		t.Fatalf("unexpected parsed stats: row=%+v point=%+v", row, point)
	}
	if point.MemoryBytes != 1.5*1024*1024*1024 ||
		point.NetRXBytes != 10.2e3 || point.NetTXBytes != 3e6 ||
		point.BlockRead != 4*1024 || point.BlockWrite != 1.25*1024*1024 {
		t.Fatalf("unexpected byte values: %+v", point)
	}
}

func TestParseDockerStatsLineRejectsMalformedValues(t *testing.T) {
	line := `{"ID":"abc","CPUPerc":"bad%","MemUsage":"1MiB / 2MiB","NetIO":"0B / 0B","BlockIO":"0B / 0B","PIDs":"1"}`
	if _, _, err := parseDockerStatsLine(line); err == nil {
		t.Fatal("expected malformed CPU error")
	}
}

func TestClassifyServiceStrictlyFromConfig(t *testing.T) {
	config := ResourceConfig{
		GoServices:    []string{"gateway"},
		NonGoServices: []string{"postgres"},
	}
	if got := classifyService("gateway", config); got != resourceGroupGo {
		t.Fatalf("gateway group = %q", got)
	}
	if got := classifyService("postgres", config); got != resourceGroupNonGo {
		t.Fatalf("postgres group = %q", got)
	}
	if got := classifyService("unconfigured", config); got != "" {
		t.Fatalf("unconfigured service must not be classified, got %q", got)
	}
}
