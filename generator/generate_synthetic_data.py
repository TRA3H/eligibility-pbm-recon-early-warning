import os
import random
from datetime import datetime, timedelta, date
import numpy as np
import pandas as pd
from faker import Faker

fake = Faker()

SEED = 42
random.seed(SEED)
np.random.seed(SEED)

OUT_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
os.makedirs(OUT_DIR, exist_ok=True)

STATES = ["CA", "TX", "FL", "WA", "OH"]
PRODUCT_LINES = ["Medicaid", "Marketplace", "Medicare"]
PLANS = [
    ("CA_MCD_A", "PBM_CA_01"),
    ("CA_MCD_B", "PBM_CA_02"),
    ("TX_MCD_A", "PBM_TX_01"),
    ("FL_MCD_A", "PBM_FL_01"),
    ("WA_MCD_A", "PBM_WA_01"),
    ("OH_MCD_A", "PBM_OH_01"),
]

REJECTS = [
    ("70", "Product/Service Not Covered"),
    ("75", "Prior Authorization Required"),
    ("26", "Coverage Terminated"),
    ("R3", "Member Not Found in PBM"),
]

def daterange(start: date, end: date):
    for n in range(int((end - start).days) + 1):
        yield start + timedelta(n)

def pick_plan(state):
    candidates = [p for p in PLANS if p[0].startswith(state)]
    return random.choice(candidates) if candidates else random.choice(PLANS)

def generate_file_loads(num_batches=18):
    base = datetime.now().replace(hour=2, minute=0, second=0, microsecond=0) - timedelta(days=28)
    rows = []
    for i in range(num_batches):
        load_dt = base + timedelta(days=i * 1.5)
        batch_id = f"BATCH_{load_dt.strftime('%Y%m%d')}_{i:02d}"
        file_version = f"v{1 + (i // 6)}.{i % 6}"
        rows.append({
            "batch_id": batch_id,
            "file_version": file_version,
            "pbm_load_datetime": load_dt,
            "records_in_file": int(random.randint(1500, 4000)),
            "source_system": "PBM_VENDOR_X"
        })
    return pd.DataFrame(rows)

def generate_eligibility_current(num_members=6000):
    today = date.today()
    start_window = today - timedelta(days=90)
    rows = []
    for i in range(num_members):
        member_id = f"M{100000 + i}"
        state = random.choice(STATES)
        plan_id, pbm_plan_id = pick_plan(state)
        product_line = random.choice(PRODUCT_LINES)

        eff = start_window + timedelta(days=random.randint(0, 60))
        if random.random() < 0.72:
            end = date(9999, 12, 31)
            status = "ACTIVE"
        else:
            end = eff + timedelta(days=random.randint(10, 70))
            status = "TERMINATED"

        last_update = datetime.now() - timedelta(days=random.randint(0, 25), hours=random.randint(0, 23))

        rows.append({
            "member_id": member_id,
            "state": state,
            "plan_id": plan_id,
            "product_line": product_line,
            "elig_effective_date": eff,
            "elig_end_date": end,
            "elig_status": status,
            "internal_last_update_ts": last_update,
            "dob_fake": fake.date_of_birth(minimum_age=18, maximum_age=85),
        })
    return pd.DataFrame(rows)

def generate_pbm_loaded(elig_df: pd.DataFrame, loads_df: pd.DataFrame):
    rows = []
    for _, r in elig_df.iterrows():
        chosen_load = loads_df.sample(1, random_state=random.randint(0, 999999)).iloc[0]
        pbm_load_dt = pd.to_datetime(chosen_load["pbm_load_datetime"])

        rows.append({
            "member_id": r["member_id"],
            "state": r["state"],
            "pbm_plan_id": dict(PLANS).get(r["plan_id"], "PBM_UNKNOWN"),
            "plan_id_ref": r["plan_id"],
            "product_line": r["product_line"],
            "pbm_elig_effective_date": r["elig_effective_date"],
            "pbm_elig_end_date": r["elig_end_date"],
            "pbm_elig_status": "ACTIVE" if r["elig_status"] == "ACTIVE" else "TERMINATED",
            "batch_id": chosen_load["batch_id"],
            "file_version": chosen_load["file_version"],
            "pbm_load_datetime": pbm_load_dt,
            "pbm_record_created_ts": pbm_load_dt + timedelta(minutes=random.randint(1, 240)),
        })

    pbm = pd.DataFrame(rows)

    # Inject mismatch scenarios (at least 5)
    n = len(pbm)
    idx = np.random.choice(pbm.index, size=int(n * 0.10), replace=False)
    scenario_splits = np.array_split(idx, 5)

    # 1) ACTIVE internally but PBM ended early
    for i in scenario_splits[0]:
        pbm.loc[i, "pbm_elig_end_date"] = (pd.to_datetime(pbm.loc[i, "pbm_elig_effective_date"]) + timedelta(days=20)).date()
        pbm.loc[i, "pbm_elig_status"] = "TERMINATED"

    # 2) Plan mapping mismatch
    for i in scenario_splits[1]:
        pbm.loc[i, "pbm_plan_id"] = "PBM_WRONG_PLAN"

    # 3) Term date off by 1 day
    for i in scenario_splits[2]:
        if str(pbm.loc[i, "pbm_elig_end_date"]) != "9999-12-31":
            pbm.loc[i, "pbm_elig_end_date"] = (pd.to_datetime(pbm.loc[i, "pbm_elig_end_date"]) + timedelta(days=1)).date()

    # 4) PBM load delayed
    for i in scenario_splits[3]:
        pbm.loc[i, "pbm_load_datetime"] = pd.to_datetime(pbm.loc[i, "pbm_load_datetime"]) + timedelta(days=6)
        pbm.loc[i, "pbm_record_created_ts"] = pd.to_datetime(pbm.loc[i, "pbm_record_created_ts"]) + timedelta(days=6)

    # 5) Duplicate PBM record overrides good record (latest batch bad)
    dup_members = pbm.loc[scenario_splits[4], "member_id"].tolist()
    if dup_members:
        latest_load = loads_df.sort_values("pbm_load_datetime").tail(1).iloc[0]
        for m in dup_members[: min(300, len(dup_members))]:
            base_row = pbm[pbm["member_id"] == m].iloc[0].to_dict()
            base_row["batch_id"] = latest_load["batch_id"]
            base_row["file_version"] = latest_load["file_version"]
            base_row["pbm_load_datetime"] = pd.to_datetime(latest_load["pbm_load_datetime"])
            base_row["pbm_record_created_ts"] = base_row["pbm_load_datetime"] + timedelta(minutes=10)
            base_row["pbm_elig_status"] = "TERMINATED"
            base_row["pbm_elig_end_date"] = (pd.to_datetime(base_row["pbm_elig_effective_date"]) + timedelta(days=5)).date()
            pbm = pd.concat([pbm, pd.DataFrame([base_row])], ignore_index=True)

    # data quality issues
    for _ in range(10):
        i = random.randint(0, len(pbm) - 1)
        pbm.loc[i, "member_id"] = None
    for _ in range(10):
        i = random.randint(0, len(pbm) - 1)
        pbm.loc[i, "pbm_elig_end_date"] = date(1900, 1, 1)

    return pbm

def generate_rx_claims(elig_df: pd.DataFrame, pbm_df: pd.DataFrame):
    today = date.today()
    claim_start = today - timedelta(days=28)
    rows = []

    pbm_latest = pbm_df.sort_values("pbm_record_created_ts").groupby("member_id", dropna=False).tail(1)
    pbm_latest = pbm_latest[pbm_latest["member_id"].notna()]
    pbm_bad = set(pbm_latest[
        (pbm_latest["pbm_plan_id"].isin(["PBM_WRONG_PLAN"])) |
        (pbm_latest["pbm_elig_status"] == "TERMINATED")
    ]["member_id"].tolist())

    members = elig_df["member_id"].tolist()

    for day in daterange(claim_start, today):
        daily_claims = random.randint(350, 650)
        for _ in range(daily_claims):
            m = random.choice(members)
            e = elig_df[elig_df["member_id"] == m].iloc[0]

            claim_id = f"C{fake.unique.random_int(min=1000000, max=9999999)}"
            fill_dt = day
            created_ts = datetime.combine(fill_dt, datetime.min.time()) + timedelta(
                hours=random.randint(8, 20),
                minutes=random.randint(0, 59)
            )

            is_reject = random.random() < 0.12
            if m in pbm_bad and random.random() < 0.55:
                is_reject = True

            if is_reject:
                if m in pbm_bad and random.random() < 0.65:
                    reject_code, reject_reason = ("26", "Coverage Terminated")
                elif m in pbm_bad and random.random() < 0.50:
                    reject_code, reject_reason = ("R3", "Member Not Found in PBM")
                else:
                    reject_code, reject_reason = random.choice(REJECTS[:-2])
                paid_flag = "N"
            else:
                reject_code, reject_reason = (None, None)
                paid_flag = "Y"

            if paid_flag == "N":
                resolved_days = int(np.clip(np.random.normal(loc=3.5, scale=2.2), 0, 10))
                resolved_ts = created_ts + timedelta(days=resolved_days, hours=random.randint(1, 12))
                resolution_status = "RESOLVED" if random.random() < 0.85 else "OPEN"
                if resolution_status == "OPEN":
                    resolved_ts = None
            else:
                resolved_ts = None
                resolution_status = None

            rows.append({
                "claim_id": claim_id,
                "member_id": m,
                "state": e["state"],
                "plan_id": e["plan_id"],
                "product_line": e["product_line"],
                "fill_date": fill_dt,
                "claim_created_ts": created_ts,
                "paid_flag": paid_flag,
                "reject_code": reject_code,
                "reject_reason": reject_reason,
                "ndc_fake": f"{random.randint(10000,99999)}-{random.randint(1000,9999)}-{random.randint(10,99)}",
                "pharmacy_id_fake": f"PH{random.randint(1000,9999)}",
                "resolution_status": resolution_status,
                "resolved_ts": resolved_ts,
            })

    df = pd.DataFrame(rows)

    for _ in range(8):
        i = random.randint(0, len(df) - 1)
        df.loc[i, "member_id"] = None

    return df

def main():
    loads = generate_file_loads(num_batches=18)
    elig = generate_eligibility_current(num_members=6000)
    pbm = generate_pbm_loaded(elig, loads)
    rx = generate_rx_claims(elig, pbm)

    loads.to_csv(os.path.join(OUT_DIR, "eligibility_file_loads.csv"), index=False)
    elig.to_csv(os.path.join(OUT_DIR, "eligibility_current.csv"), index=False)
    pbm.to_csv(os.path.join(OUT_DIR, "pbm_eligibility_loaded.csv"), index=False)
    rx.to_csv(os.path.join(OUT_DIR, "rx_claims.csv"), index=False)

    print("Wrote CSVs to:", OUT_DIR)
    print("Row counts:",
          {"eligibility_current": len(elig), "pbm_loaded": len(pbm), "rx_claims": len(rx), "file_loads": len(loads)})

if __name__ == "__main__":
    main()
