package httpapi

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/defermq/defermq/internal/app/gateway"
	"github.com/defermq/defermq/internal/domain"
	"github.com/google/uuid"
)

type createRequest struct {
	DeliverAt   time.Time          `json:"deliver_at"`
	MaxAttempts int                `json:"max_attempts,omitempty"`
	Destination domain.Destination `json:"destination"`
	Payload     payloadRequest     `json:"payload"`
}

type payloadRequest struct {
	ContentType string            `json:"content_type"`
	Headers     map[string]string `json:"headers,omitempty"`
	Body        json.RawMessage   `json:"body,omitempty"`
	BodyBase64  *string           `json:"body_base64,omitempty"`
}

func (r createRequest) input(idempotencyKey string) (gateway.CreateInput, error) {
	body, err := r.Payload.decode()
	if err != nil {
		return gateway.CreateInput{}, err
	}
	payloadID, err := uuid.NewV7()
	if err != nil {
		return gateway.CreateInput{}, fmt.Errorf("generate payload ID: %w", err)
	}
	return gateway.CreateInput{
		Payload: domain.Payload{
			ID:          payloadID,
			Body:        body,
			Headers:     r.Payload.Headers,
			ContentType: r.Payload.ContentType,
			SizeBytes:   int64(len(body)),
		},
		Destination:    r.Destination,
		DeliverAt:      r.DeliverAt,
		MaxAttempts:    r.MaxAttempts,
		IdempotencyKey: idempotencyKey,
	}, nil
}

func (p payloadRequest) decode() ([]byte, error) {
	hasJSON := len(p.Body) != 0
	hasBase64 := p.BodyBase64 != nil
	if hasJSON == hasBase64 {
		return nil, fmt.Errorf("%w: payload must contain exactly one of body or body_base64", gateway.ErrValidation)
	}
	if hasJSON {
		if !json.Valid(p.Body) {
			return nil, fmt.Errorf("%w: payload body must be valid JSON", gateway.ErrValidation)
		}
		var compact bytes.Buffer
		if err := json.Compact(&compact, p.Body); err != nil {
			return nil, fmt.Errorf("%w: compact payload JSON: %v", gateway.ErrValidation, err)
		}
		return compact.Bytes(), nil
	}
	decoded, err := base64.StdEncoding.Strict().DecodeString(*p.BodyBase64)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid payload body_base64", gateway.ErrValidation)
	}
	return decoded, nil
}

type scheduleRequest struct {
	DeliverAt time.Time `json:"deliver_at"`
}

type messageResponse struct {
	ID               uuid.UUID              `json:"id"`
	Status           domain.DeliveryStatus  `json:"status"`
	DeliverAt        time.Time              `json:"deliver_at"`
	DestinationType  domain.DestinationType `json:"destination_type"`
	Destination      *domain.Destination    `json:"destination,omitempty"`
	ScheduleRevision int64                  `json:"schedule_revision"`
	Attempts         int                    `json:"attempts"`
	MaxAttempts      int                    `json:"max_attempts"`
	LastError        *string                `json:"last_error,omitempty"`
	CreatedAt        time.Time              `json:"created_at"`
	UpdatedAt        time.Time              `json:"updated_at"`
	DeliveredAt      *time.Time             `json:"delivered_at,omitempty"`
	CancelledAt      *time.Time             `json:"cancelled_at,omitempty"`
	Payload          *payloadMetadata       `json:"payload,omitempty"`
}

type payloadMetadata struct {
	ID          uuid.UUID `json:"id"`
	ContentType string    `json:"content_type"`
	SizeBytes   int64     `json:"size_bytes"`
	CreatedAt   time.Time `json:"created_at"`
}

func responseFromDelivery(delivery domain.Delivery, detailed bool) (messageResponse, error) {
	response := messageResponse{
		ID:               delivery.ID,
		Status:           delivery.Status,
		DeliverAt:        delivery.DeliverAt.UTC(),
		DestinationType:  delivery.DestinationType,
		ScheduleRevision: delivery.ScheduleRevision,
		Attempts:         delivery.Attempts,
		MaxAttempts:      delivery.MaxAttempts,
		LastError:        delivery.LastError,
		CreatedAt:        delivery.CreatedAt.UTC(),
		UpdatedAt:        delivery.UpdatedAt.UTC(),
		DeliveredAt:      utcPointer(delivery.DeliveredAt),
		CancelledAt:      utcPointer(delivery.CancelledAt),
	}
	if detailed {
		var destination domain.Destination
		if err := json.Unmarshal(delivery.Destination, &destination); err != nil {
			return messageResponse{}, errors.New("stored destination is invalid")
		}
		response.Destination = &destination
	}
	return response, nil
}

func responseFromMessage(message gateway.Message) (messageResponse, error) {
	response, err := responseFromDelivery(message.Delivery, true)
	if err != nil {
		return messageResponse{}, err
	}
	response.Payload = &payloadMetadata{
		ID:          message.Payload.ID,
		ContentType: message.Payload.ContentType,
		SizeBytes:   message.Payload.SizeBytes,
		CreatedAt:   message.Payload.CreatedAt.UTC(),
	}
	return response, nil
}

func utcPointer(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	utc := value.UTC()
	return &utc
}

type errorEnvelope struct {
	Error apiError `json:"error"`
}

type apiError struct {
	Code      string         `json:"code"`
	Message   string         `json:"message"`
	RequestID string         `json:"request_id"`
	Details   map[string]any `json:"details"`
}
