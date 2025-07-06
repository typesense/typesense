---
name: Bug report
about: Create a report to help us improve
title: Brief summary of the issue
labels: pre-triage
assignees: ''
---

## Bug Description
A clear and concise description of the problem.

## Reproduction Steps
Please include a minimal, reproducible example **using curl**. [Use this script as a guide.](https://gist.github.com/jasonbosco/7c3432713216c378472f13e72246f46b)

```bash
<code here>
```
## Expected vs Actual
**Expected behavior**  
_What did you expect to happen?_

**Actual behavior**  
_What actually happened? Include exact error messages or incorrect outputs._

## Environment
- **Typesense version:** e.g. `v29.0.rc`  
- **Operating system:** e.g. `Ubuntu 22.04 x86_64`, `macOS 13.3`  
- **Client library & version:** (if using one) e.g. `typesense-js v1.3.0`

## Schema / Configuration

Include any relevant schema definitions, JSON payloads, or config files:

```json
{
  "name": "my_collection",
  "fields": [
    { "name": "title", "type": "string" },
    { "name": "published_year", "type": "int32" }
  ]
}
```

## Additional Context
Anything else that might help, including links to related issues or docs.
