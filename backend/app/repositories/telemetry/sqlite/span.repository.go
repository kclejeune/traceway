//go:build !telemetry_ch && !telemetry_duckdb

package sqlite

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/tracewayapp/traceway/backend/app/repositories/telemetry/shared"
	"github.com/tracewayapp/traceway/backend/app/repositories/telemetry/sqlitetypes"

	"github.com/google/uuid"
	"github.com/tracewayapp/lit/v2"
	"github.com/tracewayapp/traceway/backend/app/db"
	"github.com/tracewayapp/traceway/backend/app/models"
)

type span struct {
	Id           uuid.UUID                 `lit:"id"`
	TraceId      uuid.UUID                 `lit:"trace_id"`
	ProjectId    uuid.UUID                 `lit:"project_id"`
	Name         string                    `lit:"name"`
	StartTime    sqlitetypes.SQLiteTime    `lit:"start_time"`
	Duration     int64                     `lit:"duration"`
	RecordedAt   sqlitetypes.SQLiteTime    `lit:"recorded_at"`
	ParentSpanId *uuid.UUID                `lit:"parent_span_id"`
	Attributes   sqlitetypes.SQLiteJSONMap `lit:"attributes"`
}

func init() {
	models.ExtensionModelRegistrations = append(models.ExtensionModelRegistrations, func(driver lit.Driver) {
		lit.RegisterModel[span](driver)
	})
}

func spanToRow(s models.Span) span {
	return span{
		Id:           s.Id,
		TraceId:      s.TraceId,
		ProjectId:    s.ProjectId,
		Name:         s.Name,
		StartTime:    sqlitetypes.NewSQLiteTime(s.StartTime),
		Duration:     int64(s.Duration),
		RecordedAt:   sqlitetypes.NewSQLiteTime(s.RecordedAt),
		ParentSpanId: s.ParentSpanId,
		Attributes:   sqlitetypes.NewSQLiteJSONMap(s.Attributes),
	}
}

func (r *span) toModel() models.Span {
	s := models.Span{
		Id:           r.Id,
		TraceId:      r.TraceId,
		ProjectId:    r.ProjectId,
		Name:         r.Name,
		StartTime:    r.StartTime.Time,
		Duration:     time.Duration(r.Duration),
		RecordedAt:   r.RecordedAt.Time,
		ParentSpanId: r.ParentSpanId,
	}
	if r.Attributes != nil {
		s.Attributes = map[string]string(r.Attributes)
	}
	return s
}

type spanRepository struct{}

func (r *spanRepository) InsertAsync(ctx context.Context, spans []models.Span) error {
	if len(spans) == 0 {
		return nil
	}

	tx, err := db.TelemetryDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, s := range spans {
		row := spanToRow(s)
		if err := lit.InsertExistingUuid(tx, &row); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (r *spanRepository) FindByTraceId(ctx context.Context, projectId, traceId uuid.UUID, recordedAt *time.Time) ([]models.Span, error) {
	query := `SELECT id, trace_id, project_id, name, start_time, duration, recorded_at, parent_span_id, attributes
		FROM spans
		WHERE project_id = :project_id AND trace_id = :trace_id`
	params := lit.P{"project_id": projectId, "trace_id": traceId}
	if recordedAt != nil {
		from, to := shared.TraceWindowBounds(*recordedAt)
		query += ` AND recorded_at >= :from AND recorded_at <= :to`
		params["from"] = sqlitetypes.NewSQLiteTime(from)
		params["to"] = sqlitetypes.NewSQLiteTime(to)
	}
	query += ` ORDER BY start_time ASC`

	rows, err := lit.SelectNamed[span](db.TelemetryDB, query, params)
	if err != nil {
		return nil, err
	}

	spans := make([]models.Span, 0, len(rows))
	for _, row := range rows {
		spans = append(spans, row.toModel())
	}
	return spans, nil
}

func (r *spanRepository) FindByTraceIds(ctx context.Context, projectIds, traceIds []uuid.UUID, recordedAtMin, recordedAtMax time.Time) ([]models.Span, error) {
	if len(projectIds) == 0 || len(traceIds) == 0 {
		return []models.Span{}, nil
	}

	params := lit.P{}
	projectPlaceholders := make([]string, len(projectIds))
	for i, pid := range projectIds {
		key := fmt.Sprintf("pid_%d", i)
		projectPlaceholders[i] = ":" + key
		params[key] = pid
	}
	tracePlaceholders := make([]string, len(traceIds))
	for i, tid := range traceIds {
		key := fmt.Sprintf("tid_%d", i)
		tracePlaceholders[i] = ":" + key
		params[key] = tid
	}
	from, _ := shared.TraceWindowBounds(recordedAtMin)
	_, to := shared.TraceWindowBounds(recordedAtMax)
	params["from"] = sqlitetypes.NewSQLiteTime(from)
	params["to"] = sqlitetypes.NewSQLiteTime(to)

	query := `SELECT id, trace_id, project_id, name, start_time, duration, recorded_at, parent_span_id, attributes
		FROM spans
		WHERE project_id IN (` + strings.Join(projectPlaceholders, ",") + `)
		AND trace_id IN (` + strings.Join(tracePlaceholders, ",") + `)
		AND recorded_at >= :from AND recorded_at <= :to
		ORDER BY start_time ASC`

	rows, err := lit.SelectNamed[span](db.TelemetryDB, query, params)
	if err != nil {
		return nil, err
	}

	spans := make([]models.Span, 0, len(rows))
	for _, row := range rows {
		spans = append(spans, row.toModel())
	}
	return spans, nil
}

var SpanRepository = &spanRepository{}
