package loadtest

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
)

type dockerStatsRow struct {
	ID       string `json:"ID"`
	Name     string `json:"Name"`
	CPUPerc  string `json:"CPUPerc"`
	MemUsage string `json:"MemUsage"`
	NetIO    string `json:"NetIO"`
	BlockIO  string `json:"BlockIO"`
	PIDs     string `json:"PIDs"`
}

func parseDockerStatsLine(line string) (dockerStatsRow, ResourcePoint, error) {
	var row dockerStatsRow
	if err := json.Unmarshal([]byte(line), &row); err != nil {
		return row, ResourcePoint{}, fmt.Errorf("decode docker stats: %w", err)
	}

	cpu, err := parsePercent(row.CPUPerc)
	if err != nil {
		return row, ResourcePoint{}, fmt.Errorf("parse CPU %q: %w", row.CPUPerc, err)
	}
	memory, err := parseUsage(row.MemUsage)
	if err != nil {
		return row, ResourcePoint{}, fmt.Errorf("parse memory %q: %w", row.MemUsage, err)
	}
	pids, err := parseNumber(row.PIDs)
	if err != nil {
		return row, ResourcePoint{}, fmt.Errorf("parse PIDs %q: %w", row.PIDs, err)
	}
	netRX, netTX, err := parseIOPair(row.NetIO)
	if err != nil {
		return row, ResourcePoint{}, fmt.Errorf("parse network IO %q: %w", row.NetIO, err)
	}
	blockRead, blockWrite, err := parseIOPair(row.BlockIO)
	if err != nil {
		return row, ResourcePoint{}, fmt.Errorf("parse block IO %q: %w", row.BlockIO, err)
	}

	return row, ResourcePoint{
		CPUPercent:  cpu,
		MemoryBytes: memory,
		PIDs:        pids,
		NetRXBytes:  netRX,
		NetTXBytes:  netTX,
		BlockRead:   blockRead,
		BlockWrite:  blockWrite,
	}, nil
}

func parsePercent(value string) (float64, error) {
	return parseNumber(strings.TrimSpace(strings.TrimSuffix(value, "%")))
}

func parseUsage(value string) (float64, error) {
	parts := strings.SplitN(value, "/", 2)
	return parseBytes(parts[0])
}

func parseIOPair(value string) (float64, float64, error) {
	parts := strings.Split(value, "/")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("expected two values separated by /")
	}
	first, err := parseBytes(parts[0])
	if err != nil {
		return 0, 0, err
	}
	second, err := parseBytes(parts[1])
	if err != nil {
		return 0, 0, err
	}
	return first, second, nil
}

func parseBytes(value string) (float64, error) {
	value = strings.TrimSpace(value)
	index := 0
	for index < len(value) && (value[index] == '.' || value[index] == '+' ||
		value[index] == '-' || value[index] >= '0' && value[index] <= '9') {
		index++
	}
	if index == 0 {
		return 0, fmt.Errorf("missing number")
	}
	number, err := strconv.ParseFloat(value[:index], 64)
	if err != nil {
		return 0, err
	}
	unit := strings.ToLower(strings.TrimSpace(value[index:]))
	multipliers := map[string]float64{
		"": 1, "b": 1,
		"k": 1e3, "kb": 1e3, "m": 1e6, "mb": 1e6, "g": 1e9, "gb": 1e9,
		"t": 1e12, "tb": 1e12, "p": 1e15, "pb": 1e15,
		"ki": math.Pow(1024, 1), "kib": math.Pow(1024, 1),
		"mi": math.Pow(1024, 2), "mib": math.Pow(1024, 2),
		"gi": math.Pow(1024, 3), "gib": math.Pow(1024, 3),
		"ti": math.Pow(1024, 4), "tib": math.Pow(1024, 4),
		"pi": math.Pow(1024, 5), "pib": math.Pow(1024, 5),
	}
	multiplier, ok := multipliers[unit]
	if !ok {
		return 0, fmt.Errorf("unknown unit %q", unit)
	}
	return number * multiplier, nil
}

func parseNumber(value string) (float64, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, fmt.Errorf("empty number")
	}
	return strconv.ParseFloat(value, 64)
}
