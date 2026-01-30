WITH c AS (
  SELECT member_id, COUNT(*) AS recs
  FROM workspace.elig_pbm_bronze.pbm_eligibility_loaded_bronze
  WHERE member_id IS NOT NULL
  GROUP BY 1
)
SELECT COUNT(*) AS members_with_duplicate_pbm_records
FROM c
WHERE recs > 1;

SELECT
  state,
  plan_id,
  COUNT(*) AS late_load_members
FROM workspace.elig_pbm_gold.elig_pbm_recon
WHERE late_pbm_load_flag = true
GROUP BY 1,2
ORDER BY late_load_members DESC;
