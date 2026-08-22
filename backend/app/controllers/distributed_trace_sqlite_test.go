//go:build !transactional_pg && !telemetry_ch && !telemetry_duckdb

package controllers

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/tracewayapp/traceway/backend/app/db"
	"github.com/tracewayapp/traceway/backend/app/dbtest"
	"github.com/tracewayapp/traceway/backend/app/middleware"
	"github.com/tracewayapp/traceway/backend/app/models"
	"github.com/tracewayapp/traceway/backend/app/repositories/telemetry"
	"github.com/tracewayapp/traceway/backend/app/repositories/transactional"
)

func TestGetDistributedTraceHydratesSpans(t *testing.T) {
	dbtest.SetupSQLite(t)

	orgId := createTestOrg(t, "org-trace")
	user, err := db.ExecuteTransaction(func(tx *sql.Tx) (*models.User, error) {
		u, err := transactional.UserRepository.Create(tx, "trace@example.com", "trace", "x")
		if err != nil {
			return nil, err
		}
		if _, err := transactional.OrganizationRepository.AddUser(tx, orgId, u.Id, "admin"); err != nil {
			return nil, err
		}
		return u, nil
	})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	project, err := db.ExecuteTransaction(func(tx *sql.Tx) (*models.Project, error) {
		return transactional.ProjectRepository.CreateWithOrganization(tx, "proj-trace", "gin", orgId)
	})
	if err != nil {
		t.Fatalf("create project: %v", err)
	}

	ctx := context.Background()
	distributedTraceId := uuid.New()
	endpointId := uuid.New()
	recordedAt := time.Now().UTC().Truncate(time.Second)

	err = telemetry.EndpointRepository.InsertAsync(ctx, []models.Endpoint{{
		Id:                 endpointId,
		ProjectId:          project.Id,
		Endpoint:           "GET /narinfo",
		Duration:           2 * time.Second,
		RecordedAt:         recordedAt,
		StatusCode:         200,
		DistributedTraceId: &distributedTraceId,
	}})
	if err != nil {
		t.Fatalf("insert endpoint: %v", err)
	}
	err = telemetry.SpanRepository.InsertAsync(ctx, []models.Span{
		{
			Id:         uuid.New(),
			TraceId:    endpointId,
			ProjectId:  project.Id,
			Name:       "SELECT 1",
			StartTime:  recordedAt,
			Duration:   1900 * time.Millisecond,
			RecordedAt: recordedAt,
		},
		{
			Id:         uuid.New(),
			TraceId:    endpointId,
			ProjectId:  project.Id,
			Name:       "fetch",
			StartTime:  recordedAt.Add(1900 * time.Millisecond),
			Duration:   100 * time.Millisecond,
			RecordedAt: recordedAt,
		},
	})
	if err != nil {
		t.Fatalf("insert spans: %v", err)
	}

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/distributed-traces/"+distributedTraceId.String(), nil)
	c.Params = gin.Params{{Key: "distributedTraceId", Value: distributedTraceId.String()}}
	c.Set(middleware.UserIdContextKey, user.Id)

	DistributedTraceController.GetDistributedTrace(c)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var resp DistributedTraceResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if len(resp.Nodes) != 1 {
		t.Fatalf("nodes = %d, want 1", len(resp.Nodes))
	}
	node := resp.Nodes[0]
	if node.TraceType != "endpoint" {
		t.Fatalf("traceType = %q, want endpoint", node.TraceType)
	}
	if len(node.Spans) != 2 {
		t.Fatalf("spans = %d, want 2", len(node.Spans))
	}
	if node.Spans[0].Name != "SELECT 1" || node.Spans[1].Name != "fetch" {
		t.Fatalf("span order/names = %q, %q", node.Spans[0].Name, node.Spans[1].Name)
	}
}
