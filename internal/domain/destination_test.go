package domain

import "testing"

func TestDestinationValidate(t *testing.T) {
	enabled := map[DestinationType]bool{
		DestinationHTTP:     true,
		DestinationKafka:    true,
		DestinationRabbit:   true,
		DestinationPostgres: true,
	}
	tests := []struct {
		name    string
		value   Destination
		wantErr bool
	}{
		{
			name: "valid http defaults method",
			value: Destination{
				Type: DestinationHTTP,
				HTTP: &HTTPDestination{URL: "https://example.com/hook"},
			},
		},
		{
			name: "multiple sections",
			value: Destination{
				Type:  DestinationHTTP,
				HTTP:  &HTTPDestination{URL: "https://example.com"},
				Kafka: &KafkaDestination{Topic: "events"},
			},
			wantErr: true,
		},
		{
			name: "userinfo rejected",
			value: Destination{
				Type: DestinationHTTP,
				HTTP: &HTTPDestination{URL: "https://user:pass@example.com"},
			},
			wantErr: true,
		},
		{
			name: "system header rejected",
			value: Destination{
				Type: DestinationHTTP,
				HTTP: &HTTPDestination{
					URL:     "https://example.com",
					Headers: map[string]string{"Idempotency-Key": "override"},
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.value.Validate(enabled)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil && tt.value.Type == DestinationHTTP && tt.value.HTTP.Method != "POST" {
				t.Fatalf("default method = %q, want POST", tt.value.HTTP.Method)
			}
		})
	}
}
