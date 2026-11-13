package queue

import "time"

// JobValues serializes a job into the field map used for every stream entry
// that carries a job: the API-facing jobs stream and the per-worker streams.
func JobValues(job Job) map[string]any {
	return map[string]any{
		"job_id":     job.ID,
		"trace_id":   job.TraceID,
		"model":      job.Model,
		"prompt":     job.Prompt,
		"created_at": job.CreatedAt.UTC().Format(time.RFC3339Nano),
	}
}

// JobFromValues parses a stream entry back into a Job. It reports false when
// the entry lacks a job_id or prompt. A missing or malformed created_at falls
// back to the current time.
func JobFromValues(values map[string]any) (Job, bool) {
	jobID := stringValue(values["job_id"])
	prompt := stringValue(values["prompt"])
	if jobID == "" || prompt == "" {
		return Job{}, false
	}
	createdAt, err := time.Parse(time.RFC3339Nano, stringValue(values["created_at"]))
	if err != nil {
		createdAt = time.Now().UTC()
	}
	return Job{
		ID:        jobID,
		TraceID:   stringValue(values["trace_id"]),
		Model:     stringValue(values["model"]),
		Prompt:    prompt,
		CreatedAt: createdAt,
	}, true
}
