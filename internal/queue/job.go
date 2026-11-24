package queue

import (
	"strconv"
	"time"
)

// JobValues serializes a job into the field map used for every stream entry
// that carries a job: the API-facing jobs stream and the per-worker streams.
func JobValues(job Job) map[string]any {
	values := map[string]any{
		"job_id":      job.ID,
		"trace_id":    job.TraceID,
		"traceparent": job.Traceparent,
		"tracestate":  job.Tracestate,
		"model":       job.Model,
		"prompt":      job.Prompt,
		"created_at":  job.CreatedAt.UTC().Format(time.RFC3339Nano),
		"max_tokens":  strconv.FormatInt(job.MaxTokens, 10),
		"ignore_eos":  strconv.FormatBool(job.IgnoreEOS),
	}
	if job.Temperature != nil {
		values["temperature"] = strconv.FormatFloat(*job.Temperature, 'f', -1, 64)
	}
	return values
}

// JobFromValues parses a stream entry back into a Job. It reports false when
// the entry lacks a job_id or prompt. A missing or malformed created_at falls
// back to the current time. Generation-parameter fields (max_tokens,
// ignore_eos, temperature) are optional: absent or malformed values leave the
// zero value in place, so entries written before those fields existed still
// parse.
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
	job := Job{
		ID:          jobID,
		TraceID:     stringValue(values["trace_id"]),
		Traceparent: stringValue(values["traceparent"]),
		Tracestate:  stringValue(values["tracestate"]),
		Model:       stringValue(values["model"]),
		Prompt:      prompt,
		CreatedAt:   createdAt,
	}
	if v := stringValue(values["max_tokens"]); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			job.MaxTokens = n
		}
	}
	if v := stringValue(values["ignore_eos"]); v != "" {
		job.IgnoreEOS, _ = strconv.ParseBool(v)
	}
	if v := stringValue(values["temperature"]); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			job.Temperature = &f
		}
	}
	return job, true
}
