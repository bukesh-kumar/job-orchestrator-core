# job-orchestrator-core
A lightweight, event-driven distributed task orchestration engine built with Node.js. Features in-memory queue management, custom transaction telemetry tracking, structured validation middleware, and automated worker pool clustering.


# job-orchestrator-core

A highly resilient, single-instance asynchronous task orchestrator and job distribution engine built natively on Node.js and Express. This repository serves as a lightweight sandbox prototype for testing event-driven architectures, structured in-memory state machines, and microservice worker pooling configurations without heavy external infrastructure dependencies.

The engine decouples the HTTP ingestion layer from the execution loop using a centralized Node.js `EventEmitter` bus, ensuring non-blocking performance under heavy task saturation.

---

## ✨ Key Features

* **Event-Driven Architecture:** Decouples incoming REST API requests from actual task processing streams via a custom event loop.
* **In-Memory Queue State Machine:** Tracks execution lifecycles in real-time through explicit state changes (`QUEUED`, `ASSIGNED`, `PROCESSING`, `COMPLETED`, `FAILED`).
* **Automated Worker Pooling:** Simulates distributed runtime node clustering with automated load distribution and worker availability state tracking.
* **Built-in Transaction Telemetry:** Custom structural middleware logs execution duration parameters down to millisecond precision (`hrtime`).
* **Rigid Payload Validation:** Schema-enforced entry boundaries protect the processing layer from malformed execution contexts.

---

## ⚙️ Technical Stack

* **Runtime Environment:** Node.js (v18.x or higher recommended)
* **Framework:** Express.js (Minimalist routing layer)
* **Core Modules:** `crypto` (UUID generation), `events` (Decoupled event bus)

---

## 🚀 Getting Started

### 1. Prerequisites
Ensure you have Node.js and npm installed on your machine.
```bash
node --version
npm --version
