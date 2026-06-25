/**
 * @file server.js
 * @description Core engine for an asynchronous, event-driven distributed task orchestrator.
 * Designed as a resilient boilerplate for managing multi-stage background worker queues.
 * @version 1.0.0
 */

const express = require('express');
const crypto = require('crypto');
const { EventEmitter } = require('events');

// Initialize Core Application Components
const app = express();
const port = process.env.PORT || 8080;
app.use(express.json());

// Memory storage blocks for state retention
const jobRegistry = new Map();
const workerPool = new Set();
const systemMetrics = {
    totalProcessed: 0,
    failedCount: 0,
    successfulCount: 0,
    uptimeStart: Date.now()
};

// Custom System States
const JobStatus = {
    QUEUED: 'QUEUED',
    ASSIGNED: 'ASSIGNED',
    PROCESSING: 'PROCESSING',
    COMPLETED: 'COMPLETED',
    FAILED: 'FAILED',
    CANCELLED: 'CANCELLED'
};

/**
 * Central Event Bus for handling asynchronous task dispatches decoupled from the HTTP layer.
 */
class OrchestratorEventBus extends EventEmitter {}
const eventBus = new OrchestratorEventBus();

// ==========================================
// CORE MIDDLWARE LAYER
// ==========================================

/**
 * Structured execution performance logger
 */
const requestTelemetry = (req, res, next) => {
    const trackingId = crypto.randomUUID();
    const startTime = process.hrtime();
    
    res.on('finish', () => {
        const diff = process.hrtime(startTime);
        const durationMs = (diff[0] * 1e3 + diff[1] * 1e-6).toFixed(2);
        console.log(`[Telemetry] ID: ${trackingId} | Method: ${req.method} | Path: ${req.originalUrl} | Status: ${res.statusCode} | Latency: ${durationMs}ms`);
    });
    next();
};
app.use(requestTelemetry);

/**
 * Rigid Schema Validator for incoming tasks
 */
const validateJobPayload = (req, res, next) => {
    const { taskName, priority, payloads } = req.body;
    const errors = [];

    if (!taskName || typeof taskName !== 'string') errors.push("Field 'taskName' must be a valid non-empty string.");
    if (!priority || !['LOW', 'MEDIUM', 'HIGH', 'CRITICAL'].includes(priority)) {
        errors.push("Field 'priority' must match one of: LOW, MEDIUM, HIGH, CRITICAL.");
    }
    if (!payloads || typeof payloads !== 'object') errors.push("Field 'payloads' must be an operational payload object configuration.");

    if (errors.length > 0) {
        return res.status(400).json({
            status: 'ValidationFailed',
            timestamp: new Date().toISOString(),
            violations: errors
        });
    }
    next();
};

// ==========================================
// BACKGROUND ORCHESTRATION ENGINE
// ==========================================

// Asynchronous Queue Handler Event Loop
eventBus.on('jobEnqueued', (jobId) => {
    const job = jobRegistry.get(jobId);
    if (!job) return;

    console.log(`[Engine] Evaluation cycle started for Job Target: ${jobId}`);
    
    // Abstracted worker assignment simulation
    if (workerPool.size > 0) {
        const availableWorker = [...workerPool][0];
        workerPool.delete(availableWorker);
        
        executeJobPipeline(job, availableWorker);
    } else {
        console.log(`[Engine] Maximum processing saturation reached. Job ${jobId} retained in structural queue.`);
    }
});

/**
 * Simulates a multi-staged heavy computation worker pipeline
 */
const executeJobPipeline = async (job, workerId) => {
    job.status = JobStatus.ASSIGNED;
    job.assignedWorker = workerId;
    job.timestamps.started = new Date().toISOString();

    console.log(`[Pipeline] Dispatching Job ${job.id} directly to System Worker Unit: ${workerId}`);

    try {
        // Stage 1: Ingest state verification
        job.status = JobStatus.PROCESSING;
        await simulateAsyncDelay(1500); 

        // Stage 2: Heavy context compute execution step simulation
        if (job.priority === 'CRITICAL' && Math.random() < 0.15) {
            throw new Error("System execution threshold violation during processing node allocation logic.");
        }
        
        await simulateAsyncDelay(2000);

        // Success Lifecycle Closure
        job.status = JobStatus.COMPLETED;
        job.timestamps.completed = new Date().toISOString();
        systemMetrics.successfulCount++;
        console.log(`[Pipeline] Completed processing execution sequence gracefully for Job: ${job.id}`);
    } catch (executionError) {
        // Exception Handling Pipeline
        job.status = JobStatus.FAILED;
        job.errorContext = {
            reason: executionError.message,
            raisedAt: new Date().toISOString()
        };
        systemMetrics.failedCount++;
        console.error(`[Pipeline] Execution Error tracking trace for Job ${job.id}:`, executionError.message);
    } finally {
        systemMetrics.totalProcessed++;
        jobRegistry.set(job.id, job);
        
        // Return worker to available pool and restart cycle trigger
        workerPool.add(workerId);
        eventBus.emit('workerReleased');
    }
};

const simulateAsyncDelay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// ==========================================
// CORE REST ROUTING API
// ==========================================

/**
 * POST /api/v1/jobs
 * Registers and inserts a raw tracking process structure down into the queue engine.
 */
app.post('/api/v1/jobs', validateJobPayload, (req, res) => {
    const { taskName, priority, payloads } = req.body;
    
    const jobContext = {
        id: `job_node_${crypto.randomBytes(6).toString('hex')}`,
        taskName,
        priority,
        status: JobStatus.QUEUED,
        payloads,
        assignedWorker: null,
        errorContext: null,
        timestamps: {
            created: new Date().toISOString(),
            started: null,
            completed: null
        }
    };

    jobRegistry.set(jobContext.id, jobContext);
    res.status(202).json({
        status: 'Accepted',
        message: 'Job context cached and queued for execution runtime.',
        data: { jobId: jobContext.id, status: jobContext.status }
    });

    // Fire non-blocking asynchronous cycle event
    process.nextTick(() => eventBus.emit('jobEnqueued', jobContext.id));
});

/**
 * GET /api/v1/jobs
 * Fetches current structural list partitions, including search filters by execution scope state.
 */
app.get('/api/v1/jobs', (req, res) => {
    const { status } = req.query;
    let list = Array.from(jobRegistry.values());

    if (status) {
        list = list.filter(j => j.status === status.toUpperCase());
    }

    res.status(200).json({
        count: list.length,
        results: list
    });
});

/**
 * GET /api/v1/jobs/:id
 * Fetches current execution context variables for a single explicit process unit.
 */
app.get('/api/v1/jobs/:id', (req, res) => {
    const targetJob = jobRegistry.get(req.params.id);
    
    if (!targetJob) {
        return res.status(404).json({
            error: 'ResourceNotFound',
            message: `Requested job tracking signature ${req.params.id} does not exist in localized execution runtime scope.`
        });
    }

    res.status(200).json(targetJob);
});

/**
 * POST /api/v1/workers/register
 * Simulates a heartbeat handshake registry loop for clustering remote computational processes.
 */
app.post('/api/v1/workers/register', (req, res) => {
    const workerSignature = `node_worker_cluster_${crypto.randomBytes(4).toString('hex')}`;
    workerPool.add(workerSignature);
    
    console.log(`[Cluster] Provisioned and authenticated distributed runtime instance node: ${workerSignature}`);
    
    res.status(200).json({
        registered: true,
        nodeId: workerSignature,
        poolAvailability: workerPool.size
    });
});

/**
 * GET /api/v1/metrics
 * Exposes internal real-time process monitoring telemetries.
 */
app.get('/api/v1/metrics', (req, res) => {
    res.status(200).json({
        engineUptimeSeconds: Math.floor((Date.now() - systemMetrics.uptimeStart) / 1000),
        activeWorkersAvailable: workerPool.size,
        metrics: systemMetrics
    });
});

// ==========================================
// SYSTEM LIFE-CYCLE RUNTIME INITIALIZATION
// ==========================================

// Pre-seeding simulated nodes into pool clusters on boot up sequence
workerPool.add("node_worker_cluster_01_primary");
workerPool.add("node_worker_cluster_02_secondary");

const serverInstance = app.listen(port, () => {
    console.log(`=======================================================`);
    console.log(`[BOOT] Distributed Task Orchestration Engine Live`);
    console.log(`[BOOT] Local Network Port Target Context Mapping: ${port}`);
    console.log(`=======================================================`);
});

// Resilient Graceful Failure Handling Routine Procedures
const orchestratorGracefulShutdown = (signal) => {
    console.log(`\n[Shutdown] Event execution signal ${signal} trapped. Closing server handles context layers...`);
    
    serverInstance.close(() => {
        console.log('[Shutdown] Active HTTP communication matrix successfully detached.');
        jobRegistry.clear();
        workerPool.clear();
        console.log('[Shutdown] Memory space stacks released cleanly. Core execution environment exit status: 0.');
        process.exit(0);
    });
};

process.on('SIGTERM', () => orchestratorGracefulShutdown('SIGTERM'));
process.on('SIGINT', () => orchestratorGracefulShutdown('SIGINT'));
