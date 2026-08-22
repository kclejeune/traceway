package controllers

import (
	"context"
	"database/sql"
	"net/http"
	"time"

	"github.com/tracewayapp/traceway/backend/app/db"
	"github.com/tracewayapp/traceway/backend/app/middleware"
	"github.com/tracewayapp/traceway/backend/app/models"
	"github.com/tracewayapp/traceway/backend/app/repositories/telemetry"
	"github.com/tracewayapp/traceway/backend/app/repositories/transactional"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	traceway "go.tracewayapp.com"
)

type distributedTraceController struct{}

type distributedTraceRequest struct {
	RecordedAt *time.Time `json:"recordedAt"`
}

type DistributedTraceNode struct {
	ProjectId   uuid.UUID              `json:"projectId"`
	ProjectName string                 `json:"projectName"`
	TraceType   string                 `json:"traceType"`
	Endpoint    *models.Endpoint       `json:"endpoint,omitempty"`
	Task        *models.Task           `json:"task,omitempty"`
	AiTrace     *models.AiTrace        `json:"aiTrace,omitempty"`
	Spans       []models.Span          `json:"spans"`
	Exception   *EndpointExceptionInfo `json:"exception,omitempty"`
}

type DistributedTraceResponse struct {
	DistributedTraceId string                 `json:"distributedTraceId"`
	Nodes              []DistributedTraceNode `json:"nodes"`
}

func (d distributedTraceController) GetDistributedTrace(c *gin.Context) {
	distributedTraceIdStr := c.Param("distributedTraceId")
	distributedTraceId, err := uuid.Parse(distributedTraceIdStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid distributedTraceId"})
		return
	}

	var request distributedTraceRequest
	_ = c.ShouldBindJSON(&request)

	userId := middleware.GetUserId(c)

	projects, err := db.ExecuteTransaction(func(tx *sql.Tx) ([]*models.Project, error) {
		return transactional.ProjectRepository.FindByUserId(tx, userId)
	})
	if err != nil {
		c.AbortWithError(http.StatusInternalServerError, traceway.NewStackTraceErrorf("failed to load user projects: %w", err))
		return
	}

	if len(projects) == 0 {
		c.JSON(http.StatusOK, DistributedTraceResponse{
			DistributedTraceId: distributedTraceIdStr,
			Nodes:              []DistributedTraceNode{},
		})
		return
	}

	projectIds := make([]uuid.UUID, len(projects))
	projectNameMap := make(map[uuid.UUID]string, len(projects))
	for i, p := range projects {
		projectIds[i] = p.Id
		projectNameMap[p.Id] = p.Name
	}

	ctx := context.Background()

	endpoints, err := telemetry.EndpointRepository.FindByDistributedTraceId(ctx, distributedTraceId, projectIds, request.RecordedAt)
	if err != nil {
		c.AbortWithError(http.StatusInternalServerError, traceway.NewStackTraceErrorf("failed to query endpoints: %w", err))
		return
	}

	tasks, err := telemetry.TaskRepository.FindByDistributedTraceId(ctx, distributedTraceId, projectIds, request.RecordedAt)
	if err != nil {
		c.AbortWithError(http.StatusInternalServerError, traceway.NewStackTraceErrorf("failed to query tasks: %w", err))
		return
	}

	aiTraces, err := telemetry.AiTraceRepository.FindByDistributedTraceId(ctx, distributedTraceId, projectIds, request.RecordedAt)
	if err != nil {
		c.AbortWithError(http.StatusInternalServerError, traceway.NewStackTraceErrorf("failed to query ai traces: %w", err))
		return
	}

	exceptions, err := telemetry.ExceptionStackTraceRepository.FindByDistributedTraceId(ctx, distributedTraceId, projectIds, request.RecordedAt)
	if err != nil {
		c.AbortWithError(http.StatusInternalServerError, traceway.NewStackTraceErrorf("failed to query exceptions: %w", err))
		return
	}

	exceptionByTraceId := make(map[uuid.UUID]*EndpointExceptionInfo)
	for _, exc := range exceptions {
		if exc.TraceId != nil {
			if _, exists := exceptionByTraceId[*exc.TraceId]; !exists {
				exceptionByTraceId[*exc.TraceId] = &EndpointExceptionInfo{
					ExceptionHash: exc.ExceptionHash,
					StackTrace:    exc.StackTrace,
					RecordedAt:    exc.RecordedAt.Format("2006-01-02T15:04:05Z07:00"),
				}
			}
		}
	}

	matchedIds := make(map[uuid.UUID]bool)
	for _, ep := range endpoints {
		matchedIds[ep.Id] = true
	}
	for _, t := range tasks {
		matchedIds[t.Id] = true
	}
	for _, a := range aiTraces {
		matchedIds[a.Id] = true
	}

	spanTraceIds := make([]uuid.UUID, 0, len(endpoints)+len(tasks)+len(aiTraces)+len(exceptions))
	spanTraceIdSet := make(map[uuid.UUID]struct{}, cap(spanTraceIds))
	var recordedAtMin, recordedAtMax time.Time
	addSpanTrace := func(traceId uuid.UUID, recordedAt time.Time) {
		if recordedAtMin.IsZero() || recordedAt.Before(recordedAtMin) {
			recordedAtMin = recordedAt
		}
		if recordedAt.After(recordedAtMax) {
			recordedAtMax = recordedAt
		}
		if _, exists := spanTraceIdSet[traceId]; exists {
			return
		}
		spanTraceIdSet[traceId] = struct{}{}
		spanTraceIds = append(spanTraceIds, traceId)
	}
	for _, ep := range endpoints {
		addSpanTrace(ep.Id, ep.RecordedAt)
	}
	for _, t := range tasks {
		addSpanTrace(t.Id, t.RecordedAt)
	}
	for _, a := range aiTraces {
		addSpanTrace(a.Id, a.RecordedAt)
	}
	for _, exc := range exceptions {
		if exc.TraceId != nil && !matchedIds[*exc.TraceId] {
			addSpanTrace(*exc.TraceId, exc.RecordedAt)
		}
	}

	spansByTraceId := make(map[uuid.UUID][]models.Span)
	if len(spanTraceIds) > 0 {
		span := traceway.StartSpan(c, "loading spans")
		allSpans, err := telemetry.SpanRepository.FindByTraceIds(c, projectIds, spanTraceIds, recordedAtMin, recordedAtMax)
		span.End()
		if err != nil {
			c.AbortWithError(http.StatusInternalServerError, traceway.NewStackTraceErrorf("failed to query spans: %w", err))
			return
		}
		for _, s := range allSpans {
			spansByTraceId[s.TraceId] = append(spansByTraceId[s.TraceId], s)
		}
	}
	nodeSpans := func(traceId uuid.UUID) []models.Span {
		if spans := spansByTraceId[traceId]; spans != nil {
			return spans
		}
		return []models.Span{}
	}

	var nodes []DistributedTraceNode

	for _, ep := range endpoints {
		node := DistributedTraceNode{
			ProjectId:   ep.ProjectId,
			ProjectName: projectNameMap[ep.ProjectId],
			TraceType:   "endpoint",
			Endpoint:    &ep,
			Spans:       nodeSpans(ep.Id),
			Exception:   exceptionByTraceId[ep.Id],
		}
		nodes = append(nodes, node)
	}

	for _, t := range tasks {
		node := DistributedTraceNode{
			ProjectId:   t.ProjectId,
			ProjectName: projectNameMap[t.ProjectId],
			TraceType:   "task",
			Task:        &t,
			Spans:       nodeSpans(t.Id),
			Exception:   exceptionByTraceId[t.Id],
		}
		nodes = append(nodes, node)
	}

	for _, a := range aiTraces {
		node := DistributedTraceNode{
			ProjectId:   a.ProjectId,
			ProjectName: projectNameMap[a.ProjectId],
			TraceType:   "ai_trace",
			AiTrace:     &a,
			Spans:       nodeSpans(a.Id),
			Exception:   exceptionByTraceId[a.Id],
		}
		nodes = append(nodes, node)
	}

	for _, exc := range exceptions {
		if exc.TraceId != nil && matchedIds[*exc.TraceId] {
			continue
		}
		spans := []models.Span{}
		if exc.TraceId != nil {
			spans = nodeSpans(*exc.TraceId)
		}
		nodes = append(nodes, DistributedTraceNode{
			ProjectId:   exc.ProjectId,
			ProjectName: projectNameMap[exc.ProjectId],
			TraceType:   "exception",
			Spans:       spans,
			Exception: &EndpointExceptionInfo{
				ExceptionHash: exc.ExceptionHash,
				StackTrace:    exc.StackTrace,
				RecordedAt:    exc.RecordedAt.Format("2006-01-02T15:04:05Z07:00"),
			},
		})
	}

	if nodes == nil {
		nodes = []DistributedTraceNode{}
	}

	c.JSON(http.StatusOK, DistributedTraceResponse{
		DistributedTraceId: distributedTraceIdStr,
		Nodes:              nodes,
	})
}

var DistributedTraceController = distributedTraceController{}
