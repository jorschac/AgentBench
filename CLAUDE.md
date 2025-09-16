# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

AgentBench is a comprehensive benchmark for evaluating Large Language Models (LLMs) as autonomous agents across 8 diverse environments:
- Operating System (OS) - Ubuntu Docker environments with bash commands
- Database (DB) - Real SQL databases and queries  
- Knowledge Graph (KG) - SPARQL queries on large knowledge graphs
- Digital Card Game (DCG) - Turn-based strategy card game (Aquawar)
- Lateral Thinking Puzzles (LTP) - Deductive reasoning puzzles
- House-Holding (HH) - ALFWorld embodied tasks
- Web Shopping (WS) - WebShop e-commerce simulations
- Web Browsing (WB) - Mind2Web website interaction tasks

## Architecture

The framework uses a distributed client-server architecture with three main components:

### 1. Task Server
- **Task Controller** (port 5000): Coordinates all task workers, single global instance
- **Task Workers** (ports 5001+): Individual environments for each task type
- Communication via HTTP API (`/api/start_sample`, `/api/interact`)

### 2. Agent Server  
- Hosts LLM models (via FastChat for local models, direct API for cloud models)
- Provides inference interface for agents

### 3. Client
- **Assigner**: Coordinates tasks and models using maximum flow algorithms
- **Agent Client**: Unified interface to various LLM agents
- **Task Client**: Interfaces with Task Controller

## Essential Commands

### Environment Setup
```bash
# Create conda environment
conda create -n agent-bench python=3.9
conda activate agent-bench
pip install -r requirements.txt

# Verify Docker is running
docker ps

# Build required Docker images for basic tasks
docker pull mysql
docker pull ubuntu
docker build -f data/os_interaction/res/dockerfiles/default data/os_interaction/res/dockerfiles --tag local-os/default
docker build -f data/os_interaction/res/dockerfiles/packages data/os_interaction/res/dockerfiles --tag local-os/packages  
docker build -f data/os_interaction/res/dockerfiles/ubuntu data/os_interaction/res/dockerfiles --tag local-os/ubuntu
```

### Development Commands
```bash
# Test agent configuration
python -m src.client.agent_test
python -m src.client.agent_test --config configs/agents/api_agents.yaml --agent gpt-3.5-turbo-0125

# Start task servers (requires ports 5000-5015 available)
python -m src.start_task -a

# Run assignments/evaluations
python -m src.assigner

# Analysis of results
python -m src.analysis
```

### Configuration Files
- **Agent configs**: `configs/agents/` - Model configurations (OpenAI, FastChat, etc.)
- **Task configs**: `configs/tasks/` - Individual task environment settings  
- **Assignment configs**: `configs/assignments/` - Test case assignments and evaluation setups
- **Start task config**: `configs/start_task.yaml` - Task server startup configuration

## Key Implementation Details

### Task Implementation
- Each task inherits from base `Task` class in `src/server/tasks/`
- Tasks are configured via YAML files and support Docker containerization
- Multiple concurrent instances supported via separate Task Workers

### Agent Integration
- Agent clients implement unified `AgentClient.inference(history)` interface
- Support for both API-based (OpenAI, Anthropic) and local models (FastChat)
- Configuration through YAML files with model-specific parameters

### Evaluation Flow
1. Assigner reads configuration and creates bipartite graph (Agent ↔ Task)
2. Maximum flow algorithm allocates test cases to available workers
3. Task Client forwards agent outputs to Task Controller
4. Task Workers execute environments and return feedback
5. Results stored in `outputs/` directory with timestamps

### Resource Requirements
Task startup times and memory usage vary significantly:
- webshop: ~3min, ~15G RAM
- mind2web: ~5min, ~1G RAM  
- db: ~20s, <500M RAM
- alfworld: ~10s, <500M RAM
- card_game, ltp, os, kg: ~5s, <500M RAM

### Docker Images for Full Evaluation
Additional task environments require pulling Docker images:
```bash
docker pull longinyu/agentbench-ltp
docker pull longinyu/agentbench-webshop  
docker pull longinyu/agentbench-mind2web
docker pull longinyu/agentbench-card_game
docker pull longinyu/agentbench-alfworld
```

### Port Configuration
- Default controller port: 5000
- Task worker ports: 5001-5015 (configurable)
- Ensure ports are available before starting services
- On macOS, port 5000 may need to be freed from system services