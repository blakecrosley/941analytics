# 941analytics

## Project Context
941analytics - FastAPI web application with HTMX frontend

## Stack
- **Type**: FASTAPI
- **Primary Language**: Python 3.11+

---

## Quality Standards

This project follows the **Jiro Craftsmanship** philosophy.

> See `~/.claude/skills/jiro/` for the full Shokunin craftsmanship system.

### The Pride Check (Required Before Completion)
1. Would a 10x engineer respect this approach?
2. Does this code explain itself?
3. Have I handled the edge cases?
4. Is this the right simplicity level?
5. Did I leave it better?

### Quality Loop
1. **Implement** - Get it working
2. **Review** - Read it fresh
3. **Evaluate** - Pride Check
4. **Refine** - Fix issues
5. **Zoom Out** - Check integration
6. **Repeat** - Until satisfied
7. **Report** - Summarize with evidence

---

## Command Center Integration

This project is tracked by the **Jiro Command Center**.

- **PRDs**: `.claude/prds/*.json` - Feature specifications
- **Progress**: Synced automatically to dashboard
- **Stories**: Tracked with acceptance criteria

### PRD Format
```json
{
  "project": {"name": "...", "description": "..."},
  "stories": [
    {
      "id": "story-1",
      "title": "Feature name",
      "description": "What it does",
      "acceptance_criteria": ["Step 1", "Step 2"],
      "priority": 1,
      "passes": false,
      "attempts": 0
    }
  ]
}
```

---

## Development Workflow

1. Check Command Center for active stories
2. Pick a story to work on
3. Follow the Quality Loop
4. Mark story as complete when Pride Check passes
5. Sync to Command Center

---

## Local Rules

Project-specific rules are in `.claude/rules/`.

See `.claude/rules/fastapi.md` for API design and HTMX patterns.

---

## Key Files
- `pyproject.toml`
