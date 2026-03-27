# SOW & PO Procurement Agent v3 — Andor Tech

## Folder Structure
```
sow_agent/
│
├── main.py                        ← Entry point — run this
├── requirements.txt
├── .env
│
├── config/
│   ├── __init__.py                ← loads settings + logging
│   ├── settings.yaml              ← all app configuration
│   └── logging_config.yaml        ← rotating file + console logs
│
├── backend/                       ← all server logic, split cleanly
│   ├── routes/
│   │   ├── pages.py               ← HTML page routes (/, /run, /results, /history)
│   │   ├── api.py                 ← JSON API (/api/job, /api/health, /api/gpu-check)
│   │   └── downloads.py           ← file downloads (CSV, PDF, Word)
│   ├── middleware/
│   │   ├── cors.py                ← CORS config
│   │   └── security.py            ← security headers
│   ├── services/
│   │   ├── session_service.py     ← per-user UUID session management
│   │   ├── cache_service.py       ← SHA-256 file hash cache (30 min TTL)
│   │   └── render_service.py      ← DataFrame → HTML table helpers
│   └── jobs/
│       ├── job_registry.py        ← in-memory job status store
│       └── worker.py              ← ProcessPoolExecutor async job runner
│
├── src/
│   ├── agent/
│   │   ├── state.py               ← AgentState TypedDict
│   │   ├── nodes.py               ← 7 LangGraph nodes
│   │   └── graph.py               ← graph wiring + run_agent()
│   ├── extractors/
│   │   ├── extract_metadata.py
│   │   ├── extract_sow_schedules.py
│   │   ├── extract_po.py
│   │   └── validate_procurement.py
│   ├── prompts/                   ← all LLM prompts in one place
│   │   ├── sow_prompt.py          ← Schedule 1 & 9 extraction prompt
│   │   ├── po_prompt.py           ← Purchase Order extraction prompt
│   │   ├── metadata_prompt.py     ← Company metadata extraction prompt
│   │   └── audit_prompt.py        ← Audit report generation prompt
│   └── handlers/
│       └── error_handler.py       ← centralised error handling
│
├── utils/
│   ├── db.py                      ← SQLite layer
│   ├── gpu_client.py              ← Colab GPU HTTP client
│   ├── logger.py                  ← logger factory
│   └── torch_fix.py               ← Windows torch DLL fix
│
├── data/
│   ├── logs/                      ← agent.log (auto-created)
│   └── outputs/                   ← exported files
│
└── templates/
    ├── index.html
    ├── history.html
    └── waiting.html
```

## Setup
```bash
pip install -r requirements.txt
# Edit .env → COLAB_GPU_URL=https://your-ngrok.ngrok-free.app
python main.py
```

## Cloud Deploy
```bash
PORT=8000 MAX_WORKERS=20 python main.py
# or
gunicorn main:app -w 1 -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:8000
```

## API Endpoints
| Endpoint | Method | Purpose |
|---|---|---|
| `/` | GET | Home page |
| `/run` | POST | Submit agent job |
| `/results` | GET | View results |
| `/history` | GET | Run history |
| `/api/job/{id}` | GET | Poll job status |
| `/api/health` | GET | Health check |
| `/api/gpu-check` | GET | GPU status |
| `/download/csv` | GET | Download CSV |
| `/download/pdf` | GET | Download PDF |
| `/download/word` | GET | Download Word |
