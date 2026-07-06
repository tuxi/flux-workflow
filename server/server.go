package server

import (
	"context"
	"flux-workflow/adapter/postgres"
	"flux-workflow/cost"
	"flux-workflow/engine"
	"flux-workflow/eventbus"
	"flux-workflow/handler"
	"flux-workflow/internal/config"
	"flux-workflow/internal/consts"
	"flux-workflow/pkg/llm"
	"flux-workflow/pkg/lock"
	"flux-workflow/pkg/oss"
	"flux-workflow/registry"
	"flux-workflow/repository"
	"flux-workflow/repository/query"
	"flux-workflow/repository/query/taskapi"
	"flux-workflow/service"
	"flux-workflow/websocket"
	"flux-workflow/worker"
	"flux-workflow/workflow"
	"flux-workflow/workflow/nodes"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/tuxi/flux"
	"github.com/tuxi/flux/model"
	"github.com/tuxi/flux/tool"

	"github.com/tuxi/flux/tool/builtin"

	"github.com/gin-gonic/gin"
	websocket2 "github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

type Server struct {
	wsHub *websocket.WSHub

	WorkflowRepo        repository.WorkflowRepository
	WorkflowVersionRepo repository.WorkflowVersionRepository
	TaskRepo            repository.TaskRepository
	TaskEventRepo       repository.EventRepository
	NodeRuntimeRepo     repository.NodeRuntimeRepository
	TaskCostTraceRepo   repository.TaskCostTraceRepository
	AwaitBindingRepo    repository.AwaitBindingRepository
	CreativeDetailSvc   service.CreativeDetailService
	VideoTimelineSvc    service.VideoTimelineService

	WorkflowHandler          *handler.WorkflowHandler
	AwaitHandler             *handler.AwaitHandler
	AliyunEventBridgeHandler *handler.AliyunEventBridgeHandler
	AwaitReplayHandler       *handler.AwaitReplayHandler
	runInspectorHandler      *handler.RunInspectorHandler

	planWorkflowTool *flux.WorkflowTool // v3: Agent-driven DAG 工具
	aiEngine         *engine.Engine     // v1 engine: 可靠执行
	deepseekAPIKey   string             // DAGPlanner LLM 调用用
	deepseekBaseURL  string             // DAGPlanner LLM 调用用
	eventBridge      *AgentEventBridge  // v3: task_events → Agent events
}

func NewServer(db *gorm.DB, rdb *redis.Client, llmClient *llm.Client, ossClient oss.Client, cfg *config.Config) *Server {
	queue := query.NewRedisQueue(
		rdb,
		"video_task_queue",
		"video_task_processing",
		"video_task_dead",
		2*time.Second,
	)

	taskRepo := query.NewTaskRepository(db, queue)
	// 业务侧分页/详情查询（返回 dto）：在核心 taskRepo 之上包一层，
	// 仅 HTTP handler 使用，核心存储层不引入 dto 依赖。
	taskQueryRepo := taskapi.New(db, taskRepo)
	eventRepo := query.NewEventRepository(db)
	workflowVersionRepo := query.NewWorkflowVersionRepository(db)
	workflowRepo := query.NewWorkflowRepository(db)
	nodeRuntimeRepo := query.NewNodeRuntimeRepository(db)
	taskCostTraceRepo := query.NewTaskCostTraceRepository(db)
	awaitBindingRepo := query.NewAwaitBindingRepository(db)

	// ── Flux v3 Store 适配器：将 GORM repository 包装为 Store 接口 ──
	pgWorkflowStore := postgres.NewWorkflowStore(nodeRuntimeRepo, taskRepo)
	pgAwaitStore := postgres.NewAwaitStore(awaitBindingRepo)
	pgTraceStore := postgres.NewTraceStore(db)
	_ = pgWorkflowStore
	_ = pgAwaitStore
	_ = pgTraceStore

	wsHub := websocket.NewWSHub(websocket.NewRepositoryTaskAccessChecker(taskRepo))

	jobQueue := engine.NewRedisStreamJobQueue(
		rdb,
		"workflow_jobs",
		"workflow_group",
	)

	eventBus := eventbus.NewEventBus(
		eventRepo,
		wsHub,
	)
	eventBridge := NewAgentEventBridge(eventBus)

	//--------------------------------
	// Tool Registry
	//--------------------------------
	toolRegistry := tool.NewRegistry()
	toolRegistry.Register(builtin.NewMergeResultTool())

	nodeRegistry := nodes.InitNodeRegistry(toolRegistry)

	// 工作流注册
	wfRegistry := registry.NewWorkflowRegistry(
		workflowRepo,
		workflowVersionRepo,
	)

	// 注册图片生成视频工作流
	//wfRegistry.Register(images.TextToImageWorkflowDSL())

	// ── Flux v3: 将已有的预制工作流注册为工具，Agent 可选择复用 ──
	// 先注册元数据（供 DAGPlanner 发现），executor 在 eng 创建后注入
	textToImageTool := tool.NewWorkflowAsTool("text_to_image_workflow",
		"预制工作流：文本生成图片。全流程：参数校验→供应商路由→prompt增强→缓存查询→图片提交→等待→下载→后处理→上传。输入: user_prompt(必填), style, size, aspect_ratio, negative_prompt",
		tool.DataSchema{
			Fields: map[string]tool.FieldSchema{
				"user_prompt":     {Type: "string", Required: false, Desc: "图片描述提示词（必填，但由 workflow 内部校验）"},
				"style":           {Type: "string", Required: false, Desc: "风格"},
				"size":            {Type: "string", Required: false, Desc: "尺寸"},
				"aspect_ratio":    {Type: "string", Required: false, Desc: "宽高比"},
				"negative_prompt": {Type: "string", Required: false, Desc: "负面提示词"},
			},
		}, nil)
	toolRegistry.Register(textToImageTool)

	// ── Flux v3: 创建 WorkflowTool，让 Agent 可以自主规划 DAG ──
	// DAGPlanner 使用和 DreamAI 相同的 DeepSeek API
	dagLLM := &model.OpenAICompatibleProvider{
		BaseURL:    cfg.DeepSeek.BaseURL,
		APIKey:     cfg.DeepSeek.ApiKey,
		HTTPClient: &http.Client{Timeout: 5 * time.Minute},
	}
	planWorkflowTool := flux.NewWorkflowTool(flux.WorkflowToolConfig{
		Provider:   dagLLM,
		ModelName:  "deepseek-v4-pro",
		ToolReg:    toolRegistry,    // ← DreamAI 的全部 50+ 工具
		WFStore:    pgWorkflowStore, // ← PG adapter
		AwaitStore: pgAwaitStore,    // ← PG adapter
		TraceStore: pgTraceStore,    // ← PG adapter
	})
	toolRegistry.Register(planWorkflowTool)

	ctx := context.Background()

	// 同步工作流程模版到数据库
	err := wfRegistry.Sync(ctx)
	if err != nil {
		panic(err)
	}

	// 构建工作流
	builder := workflow.NewBuilder(nodeRegistry)

	// 创建异步worker
	asyncWorker := worker.NewAsyncWorker(
		jobQueue,
		taskRepo,
		nodeRuntimeRepo,
		toolRegistry,
		eventBus,
	)
	dLocker := lock.NewRedisLock(rdb)

	// 初始化engine
	eng := engine.NewEngine(
		taskRepo,
		nodeRuntimeRepo,
		awaitBindingRepo,
		workflowVersionRepo,
		workflowRepo,
		builder,
		eventBus,
		jobQueue,
		dLocker,
		eventRepo,
	)
	eng.SetCostRecorder(cost.NewTaskCostTraceRecorder(taskRepo, taskCostTraceRepo))
	eng.SetSubWorkflowBinding(cfg.AiEngine.SubWorkflowAwaitBinding)

	// v3: 注入预制工作流的 executor — 委托给 v1 engine 执行
	//textToImageTool.SetExecutor(func(ctx context.Context, input map[string]any, emitter tool.ToolEmitter) (*tool.Result, error) {
	//	def := images.TextToImageWorkflowDSL()
	//	taskID := time.Now().UnixNano()
	//	task := &domain.Task{
	//		ID:     taskID,
	//		RootID: taskID,
	//		UserID: 0,
	//		Status: domain.TaskPending,
	//	}
	//	if b, _ := json.Marshal(input); len(b) > 0 {
	//		task.InputJSON = b
	//	}
	//	// RunWithResult 传 def != nil 时不查 DB — 直接执行
	//	result := eng.RunWithResult(ctx, task, def)
	//	if result.Status == engine.RunFailed {
	//		return tool.Fail(fmt.Errorf("workflow failed: %w", result.Err)), nil
	//	}
	//	// 收集 output
	//	output := make(map[string]any)
	//	nodes, _ := nodeRuntimeRepo.FindByTaskID(ctx, taskID)
	//	for _, n := range nodes {
	//		for k, v := range n.Output {
	//			output[k] = v
	//		}
	//	}
	//	return tool.Success(output), nil
	//})

	awaitPollWorker := worker.NewAwaitPollWorker(
		awaitBindingRepo,
		toolRegistry,
		eng,
		eventBus,
		dLocker,
	)

	taskRetryService := service.NewTaskRetryService(
		workflowVersionRepo,
		taskRepo,
		nodeRuntimeRepo,
		awaitBindingRepo,
		builder,
	)
	//billingEntitlementSvc := internalservice.NewBillingEntitlementService(
	//	query.NewUserSubscriptionRepo(db),
	//	query.NewUserMembershipPeriodRepo(db),
	//	query.NewBillingProductRepo(db),
	//	query.NewUserDailyUsageStatRepo(db),
	//)
	//billingPricingRuleSvc := internalservice.NewBillingPricingRuleService(
	//	query.NewBillingPricingRuleRepo(db),
	//)
	//billingExpirationSvc := internalservice.NewBillingExpirationService(db)
	//billingTaskSvc := internalservice.NewBillingTaskService(db, billingEntitlementSvc, billingPricingRuleSvc, billingExpirationSvc)
	//taskBillingSettlementListener := fluxService.NewTaskBillingSettlementListener(billingTaskSvc, eventBus, taskRepo)

	w := worker.NewWorker(
		eng,
		taskRepo,
		nodeRuntimeRepo,
		workflowVersionRepo,
		workflowRepo,
		queue,
		jobQueue,
		eventBus,
		nodeRegistry,
		dLocker,
		builder,
		taskRetryService,
	)

	w.StartRecoveryScanner(ctx)
	go w.Loop(ctx)
	go w.Loop(ctx)
	go w.Loop(ctx)
	go worker.TaskQueueRecovery(queue, ctx)
	worker.StartAsyncWorkers(ctx, asyncWorker, 4)
	worker.StartAwaitPollWorkers(ctx, awaitPollWorker, 1)
	//taskBillingSettlementListener.Start(ctx, eventBus)

	taskService := service.NewTaskForkService(
		taskRepo,
		workflowVersionRepo,
		builder, eng,
	)
	creativeDetailSvc := service.NewCreativeDetailService(
		taskRepo,
		workflowVersionRepo,
		eng,
		toolRegistry,
	)
	//.WithAssetSigner(storageAssetService)

	nodeReplaySvc := service.NewNodeReplayService(
		taskRepo,
		nodeRuntimeRepo,
		workflowVersionRepo,
		eng,
		toolRegistry,
	)

	workflowHandler := handler.NewWorkflowHandler(
		workflowRepo,
		workflowVersionRepo,
		taskQueryRepo,
		taskCostTraceRepo,
		eventRepo,
		nodeRuntimeRepo,
		builder,
		taskService,
		taskRetryService,
		nil,
		eng,
	).WithNodeReplayService(nodeReplaySvc)
	//.WithAssetSigner(storageAssetService).WithCreativeDetailService(creativeDetailSvc).WithVideoTimelineService(videoTimelineSvc)
	awaitHandler := handler.NewAwaitHandler(eng, awaitBindingRepo)
	aliyunEventBridgeService := service.NewAliyunEventBridgeService(eng, awaitBindingRepo, toolRegistry, eventBus)
	aliyunEventBridgeHandler := handler.NewAliyunEventBridgeHandler(aliyunEventBridgeService)
	awaitReplayService := service.NewAwaitReplayService(eng, awaitBindingRepo, toolRegistry, eventBus)
	awaitReplayEnabled := strings.EqualFold(strings.TrimSpace(cfg.Server.Mode), "debug")
	awaitReplayHandler := handler.NewAwaitReplayHandler(awaitReplayService, awaitReplayEnabled)

	runInspectorHandler := handler.NewRunInspectorHandler(
		eng,
		taskQueryRepo,
		nodeRuntimeRepo,
		eventRepo,
		awaitBindingRepo,
		workflowRepo,
		workflowVersionRepo,
		builder,
		taskService,
	)

	return &Server{
		wsHub: wsHub,

		WorkflowRepo:             workflowRepo,
		WorkflowVersionRepo:      workflowVersionRepo,
		TaskRepo:                 taskRepo,
		TaskEventRepo:            eventRepo,
		NodeRuntimeRepo:          nodeRuntimeRepo,
		TaskCostTraceRepo:        taskCostTraceRepo,
		AwaitBindingRepo:         awaitBindingRepo,
		CreativeDetailSvc:        creativeDetailSvc,
		VideoTimelineSvc:         nil,
		WorkflowHandler:          workflowHandler,
		AwaitHandler:             awaitHandler,
		AliyunEventBridgeHandler: aliyunEventBridgeHandler,
		AwaitReplayHandler:       awaitReplayHandler,
		runInspectorHandler:      runInspectorHandler,

		planWorkflowTool: planWorkflowTool,
		aiEngine:         eng,
		deepseekAPIKey:   cfg.DeepSeek.ApiKey,
		deepseekBaseURL:  cfg.DeepSeek.BaseURL,
		eventBridge:      eventBridge,
	}
}

// assetProviderResolverAdapter adapts StorageAssetService to the builtin.AssetProviderResolver
// interface, keeping the ai-engine tool layer free of internal service imports.
type assetProviderResolverAdapter struct {
	svc service.StorageAssetService
}

func newAssetProviderResolverAdapter(svc service.StorageAssetService) *assetProviderResolverAdapter {
	return &assetProviderResolverAdapter{svc: svc}
}

func (a *assetProviderResolverAdapter) ResolveForProvider(ctx context.Context, userID int64, assetID int64, expireSeconds int64) (string, string, *time.Time, error) {
	result, err := a.svc.ResolveForProvider(ctx, userID, assetID, expireSeconds)
	if err != nil {
		return "", "", nil, err
	}
	return result.ProviderURL, result.OSSKey, result.ExpiresAt, nil
}

// RegisterRoutes 注册内部Api路由
func (s *Server) wsHandler(c *gin.Context) {
	if !websocket2.IsWebSocketUpgrade(c.Request) {
		log.Println("Not websocket upgrade request")
	}
	userID := c.GetInt64(consts.UserID)
	deviceType := c.GetString(consts.DeviceType)

	s.wsHub.ServeWS(c.Writer, c.Request, fmt.Sprintf("%v", userID), deviceType)
}

func (s *Server) RegisterRoutes(rg *gin.RouterGroup) {
	wh := s.WorkflowHandler
	ah := s.AwaitHandler
	arh := s.AwaitReplayHandler
	rih := s.runInspectorHandler

	// v3: Agent-driven DAG 端点
	//rg.POST("/plan-workflow", s.handlePlanWorkflow)
	//rg.GET("/plan-workflow/:taskID/events", s.handlePlanWorkflowEvents)

	// v2 Sidecar: App-facing Agent 入口，代理到 codeagentd（见 dreamai-agent-integration.md）
	//rg.GET("/agent/ws", s.handleAgentWS)

	workflow := rg.Group("/workflows")
	{
		workflow.POST("", wh.CreateWorkflow)
		workflow.GET("", wh.ListWorkflows)
		workflow.GET("/:id", wh.GetWorkflow)
		workflow.POST("/:id/run", wh.RunWorkflow)
	}

	task := rg.Group("/tasks")
	{
		task.GET("", wh.GetTasksByUser)
		task.GET("/:id", wh.GetTask)
		task.GET("/:id/creative-detail", wh.GetTaskCreativeDetail)
		task.GET("/:id/timeline", wh.GetTaskVideoTimeline)
		task.POST("", wh.CreateTaskFromWorkflow)
		task.POST("/resume", wh.ResumeTask)
		task.POST("/:id/fork", wh.ForkTask)
		task.GET("/:id/events", wh.GetEvents)
		task.GET("/:id/nodes/:node/children", wh.GetChildrenByNode)
		task.POST("/:id/nodes/:node/replay-input", wh.ReplayTaskNodeInput)
		task.POST("/:id/nodes/:node/replay", wh.ReplayTaskNode)
		task.GET("/:id/replay", wh.ReplayTask)
		task.POST("/:id/replay/stream", wh.ReplayTaskStream)
	}

	await := rg.Group("/await")
	{
		await.POST("/signals", ah.HandleSignal)
	}

	if arh != nil {
		internalAwait := rg.Group("/internal/await")
		{
			internalAwait.POST("/providers/:provider/replay", arh.HandleProviderReplay)
		}
	}

	run := rg.Group("/runs")
	{
		run.GET("", rih.ListRuns)
		run.GET("/:id/inspect", rih.GetRunInspector)
		run.GET("/:id/dag", rih.GetRunDAG)
		run.GET("/:id/timeline", rih.GetRunTimeline)
		run.GET("/:id/nodes/:node", rih.GetRunNodeDetail)
		run.GET("/:id/nodes/:node/diff", rih.GetRunNodeDiff)
		run.GET("/:id/nodes/:node/expansion", rih.GetRunNodeExpansion)
		run.POST("/:id/patch-preview", rih.PatchPreview)
		// 真正创建分叉运行
		run.POST("/:id/redo", rih.RedoRun)
	}

	rg.GET("/ws", s.wsHandler)

}

func (s *Server) RegisterWebhookRoutes(rg *gin.RouterGroup) {
	ah := s.AwaitHandler
	aeh := s.AliyunEventBridgeHandler

	await := rg.Group("/await")
	{
		await.POST("/aliyun/eventbridge", aeh.HandleAsyncTaskFinish)
		await.POST("/:provider", ah.HandleProviderWebhook)
	}
}

//// handlePlanWorkflow 是 v3 Agent-driven DAG 生成的 HTTP 端点。
//// POST /plan-workflow  body: {"goal": "..."}
////
//// 流程：DAGPlanner → 持久化 Workflow → 创建 Task → Enqueue → Worker 异步调度
//func (s *Server) handlePlanWorkflow(c *gin.Context) {
//	var req struct {
//		Goal string `json:"goal"`
//	}
//	if err := c.ShouldBindJSON(&req); err != nil || req.Goal == "" {
//		c.JSON(400, gin.H{"error": "missing 'goal' field"})
//		return
//	}
//
//	// 核心逻辑抽到 RunPlanWorkflow，供 HTTP 端点与 Agent 客户端工具共用。
//	taskID, wfName, nodeCount, err := s.RunPlanWorkflow(
//		c.Request.Context(), req.Goal,
//		c.GetHeader("X-Agent-Session"), c.GetHeader("X-Agent-Turn"),
//	)
//	if err != nil {
//		c.JSON(500, gin.H{"error": err.Error()})
//		return
//	}
//
//	c.JSON(200, gin.H{
//		"success":    true,
//		"task_id":    taskID,
//		"wf_name":    wfName,
//		"node_count": nodeCount,
//		"status":     "enqueued",
//	})
//}
//
//// handlePlanWorkflowEvents 提供 SSE 流，Agent 可以实时接收任务执行事件。
//// GET /plan-workflow/:taskID/events
//func (s *Server) handlePlanWorkflowEvents(c *gin.Context) {
//	taskIDStr := c.Param("taskID")
//	taskID, err := strconv.ParseInt(taskIDStr, 10, 64)
//	if err != nil {
//		c.JSON(400, gin.H{"error": "invalid task_id"})
//		return
//	}
//
//	sessionID := c.GetHeader("X-Agent-Session")
//	turnID := c.GetHeader("X-Agent-Turn")
//
//	// 使用 gin 的 Stream 实现 SSE
//	eventCh := make(chan BridgeEvent, 32)
//	emitter := &channelEmitter{ch: eventCh}
//	s.eventBridge.RegisterSession(taskID, sessionID, turnID, emitter)
//	defer s.eventBridge.UnregisterSession(taskID)
//
//	c.Stream(func(w io.Writer) bool {
//		select {
//		case ev, ok := <-eventCh:
//			if !ok {
//				return false
//			}
//			data, _ := json.Marshal(ev)
//			fmt.Fprintf(w, "data: %s\n\n", data)
//			return true
//		case <-c.Request.Context().Done():
//			return false
//		}
//	})
//}

// channelEmitter puts BridgeEvents on a channel for the SSE stream.
type channelEmitter struct {
	ch chan BridgeEvent
}

func (e *channelEmitter) EmitBridgeEvent(ev BridgeEvent) {
	select {
	case e.ch <- ev:
	default: // drop if channel full
	}
}
