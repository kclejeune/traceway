package telemetry

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tracewayapp/traceway/backend/app/models"
)

func TestSpanRepository_InsertAndFindByTraceId(t *testing.T) {
	setupTestDB(t)
	ctx := context.Background()
	projectId := uuid.New()
	traceId := uuid.New()
	now := truncateMs(time.Now().UTC())

	s1 := makeSpan(projectId, traceId, "db.query", now, 100*time.Millisecond)
	s2 := makeSpan(projectId, traceId, "http.request", now.Add(10*time.Millisecond), 200*time.Millisecond)
	s3 := makeSpan(projectId, traceId, "cache.get", now.Add(20*time.Millisecond), 50*time.Millisecond)

	err := SpanRepository.InsertAsync(ctx, []models.Span{s1, s2, s3})
	if err != nil {
		t.Fatalf("InsertAsync failed: %v", err)
	}

	found, err := SpanRepository.FindByTraceId(ctx, projectId, traceId, nil)
	if err != nil {
		t.Fatalf("FindByTraceId failed: %v", err)
	}

	if len(found) != 3 {
		t.Fatalf("expected 3 spans, got %d", len(found))
	}

	// Ordered by start_time ASC
	if found[0].Name != "db.query" {
		t.Errorf("expected first span name 'db.query', got %q", found[0].Name)
	}
	if found[1].Name != "http.request" {
		t.Errorf("expected second span name 'http.request', got %q", found[1].Name)
	}
	if found[2].Name != "cache.get" {
		t.Errorf("expected third span name 'cache.get', got %q", found[2].Name)
	}

	if found[0].Duration != 100*time.Millisecond {
		t.Errorf("expected duration 100ms, got %v", found[0].Duration)
	}
}

func TestSpanRepository_FindByTraceIds(t *testing.T) {
	setupTestDB(t)
	ctx := context.Background()
	projectId := uuid.New()
	otherProjectId := uuid.New()
	traceA := uuid.New()
	traceB := uuid.New()
	now := truncateMs(time.Now().UTC())

	err := SpanRepository.InsertAsync(ctx, []models.Span{
		makeSpan(projectId, traceA, "a1", now, 100*time.Millisecond),
		makeSpan(projectId, traceA, "a2", now.Add(10*time.Millisecond), 100*time.Millisecond),
		makeSpan(projectId, traceB, "b1", now, 100*time.Millisecond),
		makeSpan(projectId, uuid.New(), "other-trace", now, 100*time.Millisecond),
		makeSpan(otherProjectId, traceA, "other-project", now, 100*time.Millisecond),
	})
	if err != nil {
		t.Fatalf("InsertAsync failed: %v", err)
	}

	found, err := SpanRepository.FindByTraceIds(ctx, []uuid.UUID{projectId}, []uuid.UUID{traceA, traceB}, now, now)
	if err != nil {
		t.Fatalf("FindByTraceIds failed: %v", err)
	}
	if len(found) != 3 {
		t.Fatalf("expected 3 spans, got %d", len(found))
	}

	countByTrace := map[uuid.UUID]int{}
	for _, s := range found {
		countByTrace[s.TraceId]++
	}
	if countByTrace[traceA] != 2 || countByTrace[traceB] != 1 {
		t.Errorf("expected 2 spans for traceA and 1 for traceB, got %v", countByTrace)
	}

	empty, err := SpanRepository.FindByTraceIds(ctx, []uuid.UUID{projectId}, nil, now, now)
	if err != nil {
		t.Fatalf("FindByTraceIds with no trace ids failed: %v", err)
	}
	if len(empty) != 0 {
		t.Fatalf("expected 0 spans for empty trace id list, got %d", len(empty))
	}
}

func TestSpanRepository_AttributesRoundTrip(t *testing.T) {
	setupTestDB(t)
	ctx := context.Background()
	projectId := uuid.New()
	traceId := uuid.New()
	now := truncateMs(time.Now().UTC())

	withAttrs := makeSpan(projectId, traceId, "db.query", now, 100*time.Millisecond)
	withAttrs.Attributes = map[string]string{
		"db.system":     "postgresql",
		"db.query.text": "SELECT * FROM users",
	}
	withoutAttrs := makeSpan(projectId, traceId, "cache.get", now.Add(10*time.Millisecond), 50*time.Millisecond)

	if err := SpanRepository.InsertAsync(ctx, []models.Span{withAttrs, withoutAttrs}); err != nil {
		t.Fatalf("InsertAsync failed: %v", err)
	}

	found, err := SpanRepository.FindByTraceId(ctx, projectId, traceId, nil)
	if err != nil {
		t.Fatalf("FindByTraceId failed: %v", err)
	}
	if len(found) != 2 {
		t.Fatalf("expected 2 spans, got %d", len(found))
	}

	if found[0].Attributes["db.system"] != "postgresql" {
		t.Errorf("expected db.system 'postgresql', got %q", found[0].Attributes["db.system"])
	}
	if found[0].Attributes["db.query.text"] != "SELECT * FROM users" {
		t.Errorf("expected db.query.text 'SELECT * FROM users', got %q", found[0].Attributes["db.query.text"])
	}
	if len(found[1].Attributes) != 0 {
		t.Errorf("expected no attributes, got %v", found[1].Attributes)
	}
}

func TestSpanRepository_FindByTraceId_Empty(t *testing.T) {
	setupTestDB(t)
	ctx := context.Background()

	found, err := SpanRepository.FindByTraceId(ctx, uuid.New(), uuid.New(), nil)
	if err != nil {
		t.Fatalf("FindByTraceId failed: %v", err)
	}

	if len(found) != 0 {
		t.Errorf("expected 0 spans for unknown trace, got %d", len(found))
	}
}

func TestSpanRepository_InsertEmpty(t *testing.T) {
	setupTestDB(t)
	ctx := context.Background()

	err := SpanRepository.InsertAsync(ctx, []models.Span{})
	if err != nil {
		t.Fatalf("InsertAsync with empty slice should not error: %v", err)
	}
}

func TestSpanRepository_ProjectIsolation(t *testing.T) {
	setupTestDB(t)
	ctx := context.Background()
	project1 := uuid.New()
	project2 := uuid.New()
	traceId := uuid.New()
	now := truncateMs(time.Now().UTC())

	s1 := makeSpan(project1, traceId, "span-p1", now, 100*time.Millisecond)
	s2 := makeSpan(project2, traceId, "span-p2", now, 200*time.Millisecond)

	if err := SpanRepository.InsertAsync(ctx, []models.Span{s1, s2}); err != nil {
		t.Fatalf("InsertAsync failed: %v", err)
	}

	found, err := SpanRepository.FindByTraceId(ctx, project1, traceId, nil)
	if err != nil {
		t.Fatalf("FindByTraceId failed: %v", err)
	}
	if len(found) != 1 {
		t.Fatalf("expected 1 span for project1, got %d", len(found))
	}
	if found[0].Name != "span-p1" {
		t.Errorf("expected span name 'span-p1', got %q", found[0].Name)
	}
}
