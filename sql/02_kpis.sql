SELECT
  to_date(int_last_update_ts) AS kpi_date,
  state,
  plan_id,
  COUNT(*) AS total_members,
  SUM(CASE WHEN has_mismatch THEN 1 ELSE 0 END) AS mismatched_members,
  (SUM(CASE WHEN has_mismatch THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) AS mismatch_rate
FROM workspace.elig_pbm_gold.elig_pbm_recon
GROUP BY 1,2,3
ORDER BY kpi_date DESC, mismatch_rate DESC;

SELECT
  mismatch_type,
  COUNT(DISTINCT member_id) AS members_impacted
FROM workspace.elig_pbm_gold.elig_pbm_recon
WHERE has_mismatch = true
GROUP BY 1
ORDER BY members_impacted DESC;

SELECT
  kpi_date,
  reject_reason,
  batch_id,
  file_version,
  SUM(reject_count) AS rejects,
  SUM(rejects_with_mismatch_flag) AS rejects_linked_to_mismatch,
  (SUM(rejects_with_mismatch_flag) * 1.0 / NULLIF(SUM(reject_count),0)) AS pct_linked
FROM workspace.elig_pbm_gold.member_impact_daily
GROUP BY 1,2,3,4
ORDER BY kpi_date DESC, rejects DESC;

SELECT
  reject_reason,
  AVG(days_to_resolution) AS avg_days_to_resolution,
  COUNT(*) AS rejects
FROM workspace.elig_pbm_gold.rx_claim_rejects
WHERE days_to_resolution IS NOT NULL
GROUP BY 1
ORDER BY rejects DESC;
