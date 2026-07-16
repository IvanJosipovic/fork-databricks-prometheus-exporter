package collector

import (
	"fmt"
	"time"
)

// SQL queries for Databricks System Tables
// See: https://docs.databricks.com/aws/en/admin/system-tables

// durationToSQLInterval converts a Go duration to a Databricks SQL INTERVAL string.
// Uses correct singular/plural grammar (1 HOUR vs 2 HOURS, 1 DAY vs 2 DAYS).
// Note: Databricks SQL accepts both singular and plural forms (e.g., "1 HOUR" and "1 HOURS"
// are both valid), but we use grammatically correct forms for clarity.
// Examples: 24h -> "1 DAY", 48h -> "2 DAYS", 1h -> "1 HOUR", 2h -> "2 HOURS", 30m -> "30 MINUTES"
func durationToSQLInterval(d time.Duration) string {
	hours := int(d.Hours())
	if hours >= 24 && hours%24 == 0 {
		days := hours / 24
		if days == 1 {
			return "1 DAY"
		}
		return fmt.Sprintf("%d DAYS", days)
	}
	if hours == 1 {
		return "1 HOUR"
	}
	if hours > 0 {
		return fmt.Sprintf("%d HOURS", hours)
	}
	minutes := int(d.Minutes())
	if minutes == 1 {
		return "1 MINUTE"
	}
	return fmt.Sprintf("%d MINUTES", minutes)
}

// ===== Billing & Cost Queries =====

// BuildBillingDBUsQuery returns the query for DBU consumption with configurable lookback.
func BuildBillingDBUsQuery(lookback time.Duration) string {
	interval := durationToSQLInterval(lookback)
	return fmt.Sprintf(`
		SELECT 
			workspace_id,
			sku_name,
			COALESCE(tag_key, '') as tag_key,
			COALESCE(tag_value, '') as tag_value,
			SUM(usage_quantity) as dbus_total
		FROM system.billing.usage
		LATERAL VIEW OUTER EXPLODE(custom_tags) tags AS tag_key, tag_value
		WHERE usage_date >= current_date() - INTERVAL %s
			AND workspace_id IS NOT NULL
			AND sku_name IS NOT NULL
		GROUP BY workspace_id, sku_name, tag_key, tag_value
		ORDER BY workspace_id, sku_name, tag_key, tag_value
	`, interval)
}

// BuildBillingCostEstimateQuery returns the query for cost estimates with configurable lookback.
// Uses current prices only (price_end_time IS NULL) to avoid expensive temporal JOINs.
func BuildBillingCostEstimateQuery(lookback time.Duration) string {
	interval := durationToSQLInterval(lookback)
	return fmt.Sprintf(`
		WITH current_prices AS (
			SELECT DISTINCT sku_name, cloud, pricing.default as unit_price
			FROM system.billing.list_prices
			WHERE price_end_time IS NULL
		)
		SELECT 
			u.workspace_id,
			u.sku_name,
			COALESCE(tag_key, '') as tag_key,
			COALESCE(tag_value, '') as tag_value,
			SUM(u.usage_quantity * COALESCE(p.unit_price, 0)) as cost_estimate_usd
		FROM system.billing.usage u
		LEFT JOIN current_prices p ON u.sku_name = p.sku_name AND u.cloud = p.cloud
		LATERAL VIEW OUTER EXPLODE(u.custom_tags) tags AS tag_key, tag_value
		WHERE u.usage_date >= current_date() - INTERVAL %s
			AND u.workspace_id IS NOT NULL
			AND u.sku_name IS NOT NULL
		GROUP BY u.workspace_id, u.sku_name, tag_key, tag_value
		ORDER BY u.workspace_id, u.sku_name, tag_key, tag_value
	`, interval)
}

// BuildPriceChangeEventsQuery returns the query for price change events with configurable lookback.
func BuildPriceChangeEventsQuery(lookback time.Duration) string {
	interval := durationToSQLInterval(lookback)
	return fmt.Sprintf(`
		SELECT 
			sku_name,
			COUNT(*) as price_change_count
		FROM system.billing.list_prices
		WHERE price_start_time >= current_timestamp() - INTERVAL %s
			AND sku_name IS NOT NULL
		GROUP BY sku_name
		HAVING COUNT(*) > 1
		ORDER BY price_change_count DESC
	`, interval)
}

// ===== Jobs Query Builders =====

const latestJobsWithTagsCTE = `
		WITH latest_jobs AS (
			SELECT workspace_id, job_id, name, tags
			FROM system.lakeflow.jobs
			WHERE delete_time IS NULL
			QUALIFY ROW_NUMBER() OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC) = 1
		),
		jobs_with_tags AS (
			SELECT
				workspace_id,
				job_id,
				name,
				COALESCE(tag_key, '') as tag_key,
				COALESCE(tag_value, '') as tag_value
			FROM latest_jobs
			LATERAL VIEW OUTER EXPLODE(tags) tag_map AS tag_key, tag_value
		)`

// BuildJobRunsQuery returns the query for job run counts with configurable lookback.
func BuildJobRunsQuery(lookback time.Duration) string {
	interval := durationToSQLInterval(lookback)
	return fmt.Sprintf(`
		%s
		SELECT 
			t.workspace_id,
			t.job_id,
			COALESCE(j.name, CONCAT('job-', t.job_id)) as job_name,
			COALESCE(j.tag_key, '') as tag_key,
			COALESCE(j.tag_value, '') as tag_value,
			COUNT(*) as run_count
		FROM system.lakeflow.job_run_timeline t
		LEFT JOIN jobs_with_tags j ON t.workspace_id = j.workspace_id AND t.job_id = j.job_id
		WHERE t.period_start_time >= current_timestamp() - INTERVAL %s
		GROUP BY t.workspace_id, t.job_id, j.name, j.tag_key, j.tag_value
	`, latestJobsWithTagsCTE, interval)
}

// BuildJobRunStatusQuery returns the query for job status counts with configurable lookback.
func BuildJobRunStatusQuery(lookback time.Duration) string {
	interval := durationToSQLInterval(lookback)
	return fmt.Sprintf(`
		%s
		SELECT 
			t.workspace_id,
			t.job_id,
			COALESCE(j.name, CONCAT('job-', t.job_id)) as job_name,
			COALESCE(j.tag_key, '') as tag_key,
			COALESCE(j.tag_value, '') as tag_value,
			t.result_state as status,
			COUNT(*) as run_count
		FROM system.lakeflow.job_run_timeline t
		LEFT JOIN jobs_with_tags j ON t.workspace_id = j.workspace_id AND t.job_id = j.job_id
		WHERE t.period_start_time >= current_timestamp() - INTERVAL %s
			AND t.result_state IS NOT NULL
		GROUP BY t.workspace_id, t.job_id, j.name, j.tag_key, j.tag_value, t.result_state
	`, latestJobsWithTagsCTE, interval)
}

// BuildJobRunDurationQuery returns the query for job duration quantiles with configurable lookback.
func BuildJobRunDurationQuery(lookback time.Duration) string {
	interval := durationToSQLInterval(lookback)
	return fmt.Sprintf(`
		%s
		SELECT 
			workspace_id,
			job_id,
			job_name,
			tag_key,
			tag_value,
			percentile_approx(duration_seconds, 0.5) as p50,
			percentile_approx(duration_seconds, 0.95) as p95,
			percentile_approx(duration_seconds, 0.99) as p99
		FROM (
			SELECT 
				t.workspace_id,
				t.job_id,
				COALESCE(j.name, CONCAT('job-', t.job_id)) as job_name,
				COALESCE(j.tag_key, '') as tag_key,
				COALESCE(j.tag_value, '') as tag_value,
				unix_timestamp(t.period_end_time) - unix_timestamp(t.period_start_time) as duration_seconds
			FROM system.lakeflow.job_run_timeline t
			LEFT JOIN jobs_with_tags j ON t.workspace_id = j.workspace_id AND t.job_id = j.job_id
			WHERE t.period_start_time >= current_timestamp() - INTERVAL %s
				AND t.period_end_time IS NOT NULL
				AND t.period_end_time > t.period_start_time
		)
		GROUP BY workspace_id, job_id, job_name, tag_key, tag_value
	`, latestJobsWithTagsCTE, interval)
}

// BuildTaskRetriesQuery returns the query for task retries with configurable lookback.
func BuildTaskRetriesQuery(lookback time.Duration) string {
	interval := durationToSQLInterval(lookback)
	return fmt.Sprintf(`
		%s
		SELECT 
			t.workspace_id,
			t.job_id,
			COALESCE(j.name, CONCAT('job-', t.job_id)) as job_name,
			COALESCE(j.tag_key, '') as tag_key,
			COALESCE(j.tag_value, '') as tag_value,
			t.task_key,
			COUNT(*) - COUNT(DISTINCT CONCAT(t.job_run_id, '-', t.task_key)) as retry_count
		FROM system.lakeflow.job_task_run_timeline t
		LEFT JOIN jobs_with_tags j ON t.workspace_id = j.workspace_id AND t.job_id = j.job_id
		WHERE t.period_start_time >= current_timestamp() - INTERVAL %s
			AND t.job_run_id IS NOT NULL
		GROUP BY t.workspace_id, t.job_id, j.name, j.tag_key, j.tag_value, t.task_key
		HAVING COUNT(*) > COUNT(DISTINCT CONCAT(t.job_run_id, '-', t.task_key))
	`, latestJobsWithTagsCTE, interval)
}

// BuildJobSLAMissQuery returns the query for SLA misses with configurable lookback and threshold.
func BuildJobSLAMissQuery(lookback time.Duration, slaThresholdSeconds int) string {
	interval := durationToSQLInterval(lookback)
	return fmt.Sprintf(`
		%s
		SELECT 
			workspace_id,
			job_id,
			job_name,
			tag_key,
			tag_value,
			COUNT(*) as sla_miss_count
		FROM (
			SELECT 
				t.workspace_id,
				t.job_id,
				COALESCE(j.name, CONCAT('job-', t.job_id)) as job_name,
				COALESCE(j.tag_key, '') as tag_key,
				COALESCE(j.tag_value, '') as tag_value,
				unix_timestamp(t.period_end_time) - unix_timestamp(t.period_start_time) as duration_seconds
			FROM system.lakeflow.job_run_timeline t
			LEFT JOIN jobs_with_tags j ON t.workspace_id = j.workspace_id AND t.job_id = j.job_id
			WHERE t.period_start_time >= current_timestamp() - INTERVAL %s
				AND t.period_end_time IS NOT NULL
				AND t.period_end_time > t.period_start_time
		)
		WHERE duration_seconds > %d
		GROUP BY workspace_id, job_id, job_name, tag_key, tag_value
	`, latestJobsWithTagsCTE, interval, slaThresholdSeconds)
}

// ===== Pipelines Query Builders =====

const latestPipelinesWithTagsCTE = `
		WITH latest_pipelines AS (
			SELECT workspace_id, pipeline_id, name, tags
			FROM system.lakeflow.pipelines
			WHERE delete_time IS NULL
			QUALIFY ROW_NUMBER() OVER (PARTITION BY workspace_id, pipeline_id ORDER BY change_time DESC) = 1
		),
		pipelines_with_tags AS (
			SELECT
				workspace_id,
				pipeline_id,
				name,
				COALESCE(tag_key, '') as tag_key,
				COALESCE(tag_value, '') as tag_value
			FROM latest_pipelines
			LATERAL VIEW OUTER EXPLODE(tags) tag_map AS tag_key, tag_value
		)`

// BuildPipelineRunsQuery returns the query for pipeline run counts with configurable lookback.
func BuildPipelineRunsQuery(lookback time.Duration) string {
	interval := durationToSQLInterval(lookback)
	return fmt.Sprintf(`
		%s
		SELECT 
			t.workspace_id,
			t.pipeline_id,
			COALESCE(p.name, CONCAT('pipeline-', t.pipeline_id)) as pipeline_name,
			COALESCE(p.tag_key, '') as tag_key,
			COALESCE(p.tag_value, '') as tag_value,
			COUNT(*) as run_count
		FROM system.lakeflow.pipeline_update_timeline t
		LEFT JOIN pipelines_with_tags p ON t.workspace_id = p.workspace_id AND t.pipeline_id = p.pipeline_id
		WHERE t.period_start_time >= current_timestamp() - INTERVAL %s
		GROUP BY t.workspace_id, t.pipeline_id, p.name, p.tag_key, p.tag_value
	`, latestPipelinesWithTagsCTE, interval)
}

// BuildPipelineRunStatusQuery returns the query for pipeline status counts with configurable lookback.
func BuildPipelineRunStatusQuery(lookback time.Duration) string {
	interval := durationToSQLInterval(lookback)
	return fmt.Sprintf(`
		%s
		SELECT 
			t.workspace_id,
			t.pipeline_id,
			COALESCE(p.name, CONCAT('pipeline-', t.pipeline_id)) as pipeline_name,
			COALESCE(p.tag_key, '') as tag_key,
			COALESCE(p.tag_value, '') as tag_value,
			t.result_state as status,
			COUNT(*) as run_count
		FROM system.lakeflow.pipeline_update_timeline t
		LEFT JOIN pipelines_with_tags p ON t.workspace_id = p.workspace_id AND t.pipeline_id = p.pipeline_id
		WHERE t.period_start_time >= current_timestamp() - INTERVAL %s
			AND t.result_state IS NOT NULL
		GROUP BY t.workspace_id, t.pipeline_id, p.name, p.tag_key, p.tag_value, t.result_state
	`, latestPipelinesWithTagsCTE, interval)
}

// BuildPipelineRunDurationQuery returns the query for pipeline duration quantiles with configurable lookback.
func BuildPipelineRunDurationQuery(lookback time.Duration) string {
	interval := durationToSQLInterval(lookback)
	return fmt.Sprintf(`
		%s
		SELECT 
			workspace_id,
			pipeline_id,
			pipeline_name,
			tag_key,
			tag_value,
			percentile_approx(duration_seconds, 0.5) as p50,
			percentile_approx(duration_seconds, 0.95) as p95,
			percentile_approx(duration_seconds, 0.99) as p99
		FROM (
			SELECT 
				t.workspace_id,
				t.pipeline_id,
				COALESCE(p.name, CONCAT('pipeline-', t.pipeline_id)) as pipeline_name,
				COALESCE(p.tag_key, '') as tag_key,
				COALESCE(p.tag_value, '') as tag_value,
				unix_timestamp(t.period_end_time) - unix_timestamp(t.period_start_time) as duration_seconds
			FROM system.lakeflow.pipeline_update_timeline t
			LEFT JOIN pipelines_with_tags p ON t.workspace_id = p.workspace_id AND t.pipeline_id = p.pipeline_id
			WHERE t.period_start_time >= current_timestamp() - INTERVAL %s
				AND t.period_end_time IS NOT NULL
				AND t.period_end_time > t.period_start_time
		)
		GROUP BY workspace_id, pipeline_id, pipeline_name, tag_key, tag_value
	`, latestPipelinesWithTagsCTE, interval)
}

// BuildPipelineRetryEventsQuery returns the query for pipeline retry events with configurable lookback.
func BuildPipelineRetryEventsQuery(lookback time.Duration) string {
	interval := durationToSQLInterval(lookback)
	return fmt.Sprintf(`
		%s
		SELECT 
			t.workspace_id,
			t.pipeline_id,
			COALESCE(p.name, CONCAT('pipeline-', t.pipeline_id)) as pipeline_name,
			COALESCE(p.tag_key, '') as tag_key,
			COALESCE(p.tag_value, '') as tag_value,
			COUNT(*) - COUNT(DISTINCT t.update_id) as retry_count
		FROM system.lakeflow.pipeline_update_timeline t
		LEFT JOIN pipelines_with_tags p ON t.workspace_id = p.workspace_id AND t.pipeline_id = p.pipeline_id
		WHERE t.period_start_time >= current_timestamp() - INTERVAL %s
		GROUP BY t.workspace_id, t.pipeline_id, p.name, p.tag_key, p.tag_value
		HAVING COUNT(*) > COUNT(DISTINCT t.update_id)
	`, latestPipelinesWithTagsCTE, interval)
}

// BuildPipelineFreshnessLagQuery returns the query for pipeline freshness lag with configurable lookback.
func BuildPipelineFreshnessLagQuery(lookback time.Duration) string {
	interval := durationToSQLInterval(lookback)
	return fmt.Sprintf(`
		%s
		SELECT 
			t.workspace_id,
			t.pipeline_id,
			COALESCE(p.name, CONCAT('pipeline-', t.pipeline_id)) as pipeline_name,
			COALESCE(p.tag_key, '') as tag_key,
			COALESCE(p.tag_value, '') as tag_value,
			AVG(unix_timestamp(current_timestamp()) - unix_timestamp(t.period_end_time)) as freshness_lag_seconds
		FROM system.lakeflow.pipeline_update_timeline t
		LEFT JOIN pipelines_with_tags p ON t.workspace_id = p.workspace_id AND t.pipeline_id = p.pipeline_id
		WHERE t.period_start_time >= current_timestamp() - INTERVAL %s
			AND t.period_end_time IS NOT NULL
			AND t.result_state = 'COMPLETED'
		GROUP BY t.workspace_id, t.pipeline_id, p.name, p.tag_key, p.tag_value
	`, latestPipelinesWithTagsCTE, interval)
}

// ===== SQL Warehouse Query Builders =====

const latestWarehousesWithTagsCTE = `
		WITH latest_warehouses AS (
			SELECT workspace_id, warehouse_id, tags
			FROM system.compute.warehouses
			QUALIFY ROW_NUMBER() OVER (PARTITION BY workspace_id, warehouse_id ORDER BY change_time DESC) = 1
				AND delete_time IS NULL
		),
		warehouses_with_tags AS (
			SELECT
				workspace_id,
				warehouse_id,
				COALESCE(tag_key, '') as tag_key,
				COALESCE(tag_value, '') as tag_value
			FROM latest_warehouses
			LATERAL VIEW OUTER EXPLODE(tags) tag_map AS tag_key, tag_value
		)`

// BuildQueriesQuery returns the query for SQL query counts with configurable lookback.
func BuildQueriesQuery(lookback time.Duration) string {
	interval := durationToSQLInterval(lookback)
	return fmt.Sprintf(`
		%s
		SELECT 
			q.workspace_id,
			COALESCE(q.compute.warehouse_id, 'unknown') as warehouse_id,
			COALESCE(w.tag_key, '') as tag_key,
			COALESCE(w.tag_value, '') as tag_value,
			COUNT(*) as query_count
		FROM system.query.history q
		LEFT JOIN warehouses_with_tags w
			ON q.workspace_id = w.workspace_id
			AND q.compute.warehouse_id = w.warehouse_id
		WHERE q.start_time >= current_timestamp() - INTERVAL %s
		GROUP BY q.workspace_id, q.compute.warehouse_id, w.tag_key, w.tag_value
	`, latestWarehousesWithTagsCTE, interval)
}

// BuildQueryErrorsQuery returns the query for SQL query errors with configurable lookback.
func BuildQueryErrorsQuery(lookback time.Duration) string {
	interval := durationToSQLInterval(lookback)
	return fmt.Sprintf(`
		%s
		SELECT 
			q.workspace_id,
			COALESCE(q.compute.warehouse_id, 'unknown') as warehouse_id,
			COALESCE(w.tag_key, '') as tag_key,
			COALESCE(w.tag_value, '') as tag_value,
			COUNT(*) as error_count
		FROM system.query.history q
		LEFT JOIN warehouses_with_tags w
			ON q.workspace_id = w.workspace_id
			AND q.compute.warehouse_id = w.warehouse_id
		WHERE q.start_time >= current_timestamp() - INTERVAL %s
			AND q.error_message IS NOT NULL
		GROUP BY q.workspace_id, q.compute.warehouse_id, w.tag_key, w.tag_value
	`, latestWarehousesWithTagsCTE, interval)
}

// BuildQueryDurationQuery returns the query for SQL query duration quantiles with configurable lookback.
func BuildQueryDurationQuery(lookback time.Duration) string {
	interval := durationToSQLInterval(lookback)
	return fmt.Sprintf(`
		%s
		SELECT 
			workspace_id,
			warehouse_id,
			tag_key,
			tag_value,
			percentile_approx(duration_seconds, 0.5) as p50,
			percentile_approx(duration_seconds, 0.95) as p95,
			percentile_approx(duration_seconds, 0.99) as p99
		FROM (
			SELECT 
				q.workspace_id,
				COALESCE(q.compute.warehouse_id, 'unknown') as warehouse_id,
				COALESCE(w.tag_key, '') as tag_key,
				COALESCE(w.tag_value, '') as tag_value,
				q.total_duration_ms / 1000.0 as duration_seconds
			FROM system.query.history q
			LEFT JOIN warehouses_with_tags w
				ON q.workspace_id = w.workspace_id
				AND q.compute.warehouse_id = w.warehouse_id
			WHERE q.start_time >= current_timestamp() - INTERVAL %s
				AND q.total_duration_ms IS NOT NULL
				AND q.total_duration_ms > 0
		)
		GROUP BY workspace_id, warehouse_id, tag_key, tag_value
	`, latestWarehousesWithTagsCTE, interval)
}

// BuildQueriesRunningQuery returns the query for concurrent queries estimate with configurable lookback.
func BuildQueriesRunningQuery(lookback time.Duration) string {
	interval := durationToSQLInterval(lookback)
	return fmt.Sprintf(`
		%s
		SELECT 
			workspace_id,
			warehouse_id,
			tag_key,
			tag_value,
			MAX(concurrent_count) as max_concurrent
		FROM (
			SELECT 
				q1.workspace_id,
				COALESCE(q1.compute.warehouse_id, 'unknown') as warehouse_id,
				COALESCE(w.tag_key, '') as tag_key,
				COALESCE(w.tag_value, '') as tag_value,
				q1.start_time as time_point,
				COUNT(*) as concurrent_count
			FROM system.query.history q1
			JOIN system.query.history q2
				ON q1.workspace_id = q2.workspace_id
				AND COALESCE(q1.compute.warehouse_id, 'unknown') = COALESCE(q2.compute.warehouse_id, 'unknown')
				AND q2.start_time <= q1.start_time
				AND (q2.end_time >= q1.start_time OR q2.end_time IS NULL)
			LEFT JOIN warehouses_with_tags w
				ON q1.workspace_id = w.workspace_id
				AND q1.compute.warehouse_id = w.warehouse_id
			WHERE q1.start_time >= current_timestamp() - INTERVAL %s
			GROUP BY q1.workspace_id, q1.compute.warehouse_id, w.tag_key, w.tag_value, q1.start_time
		)
		GROUP BY workspace_id, warehouse_id, tag_key, tag_value
	`, latestWarehousesWithTagsCTE, interval)
}
