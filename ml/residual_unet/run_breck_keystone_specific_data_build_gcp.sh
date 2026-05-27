#!/usr/bin/env bash
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

BUCKET="${MWN_GCP_BUCKET:-mwn-ml-general-9p6-spring-nova-475120-r0}"
BASE="runtime/ml/residual_unet/domain_specific/breck_keystone_specific_v1"
LOG_DIR="$BASE/logs"
WRAPPER_LOG="$LOG_DIR/data_build_package_sync_and_stop.log"
STATUS_FILE="$LOG_DIR/data_build_package_status.txt"
RESTORE_RUNTIME_TEMP="${RESTORE_RUNTIME_TEMP:-1}"
PYTHON_BIN=".venv/bin/python"
if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="python3"
fi

mkdir -p "$LOG_DIR"

run_wave() {
  local wave_name="$1"
  shift
  local scripts=("$@")
  local pids=()

  echo "wave_started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ) ${wave_name}"
  for script in "${scripts[@]}"; do
    local label
    label="$(basename "$(dirname "$script")")"
    local log_path="$LOG_DIR/${label}.log"
    echo "starting ${label}; log=${log_path}"
    "$script" >"$log_path" 2>&1 &
    pids+=("$!")
  done

  local failed=0
  for pid in "${pids[@]}"; do
    if ! wait "$pid"; then
      failed=1
    fi
  done
  echo "wave_finished_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ) ${wave_name} status=${failed}"
  return "$failed"
}

restore_inputs() {
  echo "restore_static_data_started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  gcloud storage rsync -r "gs://${BUCKET}/static_data" static_data
  local static_status=$?
  echo "restore_static_data_status=${static_status}"
  [[ "$static_status" -eq 0 ]] || return "$static_status"

  if [[ "$RESTORE_RUNTIME_TEMP" != "1" ]]; then
    echo "restore_runtime_temp=skipped"
    return 0
  fi

  mkdir -p runtime/temp
  for domain in \
    breck_tenmile_9p6 \
    breck_tenmile_9p6_mass \
    keystone_9p6 \
    keystone_9p6_mass
  do
    echo "restore_runtime_temp_domain=${domain}"
    gcloud storage cp -r "gs://${BUCKET}/runtime_temp/${domain}_*" runtime/temp/ || true
  done
}

stage_run_scripts() {
  local midpoint_dirs
  midpoint_dirs="7.5,22.5,37.5,52.5,67.5,82.5,97.5,112.5,127.5,142.5,157.5,172.5,187.5,202.5,217.5,232.5,247.5,262.5,277.5,292.5,307.5,322.5,337.5,352.5"
  local speeds_mph
  speeds_mph="5,10,15,20,25,30,40,50,60,70,80"

  "$PYTHON_BIN" -m ml.residual_unet.hrrr_pair_runs \
    --start 202505010000 \
    --end 202605010000 \
    --chunk-hours 24 \
    --threads 6 \
    --momentum-domain breck_tenmile_9p6 \
    --mass-domain breck_tenmile_9p6_mass \
    --label breck_tenmile_9p6_hrrr_20250501_20260501_v1 \
    --write-run-script

  "$PYTHON_BIN" -m ml.residual_unet.hrrr_pair_runs \
    --start 202505010000 \
    --end 202605010000 \
    --chunk-hours 24 \
    --threads 6 \
    --momentum-domain keystone_9p6 \
    --mass-domain keystone_9p6_mass \
    --label keystone_9p6_hrrr_20250501_20260501_v1 \
    --write-run-script

  "$PYTHON_BIN" -m ml.residual_unet.controlled_pairs \
    --profile training \
    --speeds-mph "$speeds_mph" \
    --directions "$midpoint_dirs" \
    --raw-root runtime/ml/residual_unet/raw/controlled_9p6_7p5_midpoints/breck_tenmile_9p6 \
    --domain-label breck_tenmile_9p6 \
    --terrain-file static_data/breck_tenmile_9p6.lcp \
    --num-threads 6 \
    --write-configs \
    --write-run-script

  "$PYTHON_BIN" -m ml.residual_unet.controlled_pairs \
    --profile training \
    --speeds-mph "$speeds_mph" \
    --directions "$midpoint_dirs" \
    --raw-root runtime/ml/residual_unet/raw/controlled_9p6_7p5_midpoints/keystone_9p6 \
    --domain-label keystone_9p6 \
    --terrain-file static_data/keystone_9p6.lcp \
    --num-threads 6 \
    --write-configs \
    --write-run-script
}

package_domain() {
  local key="$1"
  local dataset="$2"
  local processed_dir="ml/residual_unet/data/processed/${dataset}"
  local notebook="ml/residual_unet/notebooks/06_train_site_specific_9p6_colab.ipynb"

  echo "package_domain_started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ) ${key}"
  "$PYTHON_BIN" -m ml.residual_unet.build_domain_specific_lcp_canopy \
    --domain "$key" \
    --force
  local build_status=$?
  echo "package_domain_build_status=${build_status} ${key}"
  [[ "$build_status" -eq 0 ]] || return "$build_status"

  "$PYTHON_BIN" -m ml.residual_unet.prepare_colab_upload \
    --processed-dir "$processed_dir" \
    --skip-build \
    --force \
    --gcs-bucket "$BUCKET" \
    --notebook "$notebook"
  local upload_status=$?
  echo "package_domain_upload_status=${upload_status} ${key}"
  return "$upload_status"
}

main() {
  local restore_status=0
  local hrrr_status=0
  local stage_status=0
  local controlled_status=0
  local package_breck_status=0
  local package_keystone_status=0
  local sync_ml_status=0
  local sync_temp_status=0

  echo "started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "bucket=gs://${BUCKET}"
  echo "restore_runtime_temp=${RESTORE_RUNTIME_TEMP}"
  echo "python=${PYTHON_BIN}"
  df -h .

  restore_inputs
  restore_status=$?
  echo "restore_status=${restore_status}"

  if [[ "$restore_status" -eq 0 ]]; then
    stage_run_scripts
    stage_status=$?
  else
    stage_status=99
  fi
  echo "stage_status=${stage_status}"

  if [[ "$restore_status" -eq 0 && "$stage_status" -eq 0 ]]; then
    run_wave "breck and keystone full-year HRRR" \
      "runtime/ml/residual_unet/hrrr_pairs/breck_tenmile_9p6_hrrr_20250501_20260501_v1/run_hrrr_pairs.sh" \
      "runtime/ml/residual_unet/hrrr_pairs/keystone_9p6_hrrr_20250501_20260501_v1/run_hrrr_pairs.sh"
    hrrr_status=$?
  else
    hrrr_status=99
  fi
  echo "hrrr_status=${hrrr_status}"

  if [[ "$stage_status" -eq 0 ]]; then
    run_wave "breck and keystone 7.5-degree midpoint controlled" \
      "runtime/ml/residual_unet/raw/controlled_9p6_7p5_midpoints/breck_tenmile_9p6/run_controlled_matrix.sh" \
      "runtime/ml/residual_unet/raw/controlled_9p6_7p5_midpoints/keystone_9p6/run_controlled_matrix.sh"
    controlled_status=$?
  else
    controlled_status=99
  fi
  echo "controlled_status=${controlled_status}"

  if [[ "$hrrr_status" -eq 0 && "$controlled_status" -eq 0 ]]; then
    package_domain breck breck_tenmile_9p6_specific_lcp_canopy_v1
    package_breck_status=$?
    package_domain keystone keystone_9p6_specific_lcp_canopy_v1
    package_keystone_status=$?
  else
    package_breck_status=99
    package_keystone_status=99
    echo "package_status=skipped_after_data_failure"
  fi

  echo "syncing runtime/ml/residual_unet"
  gcloud storage rsync -r runtime/ml/residual_unet "gs://${BUCKET}/runtime_ml"
  sync_ml_status=$?
  echo "sync_ml_status=${sync_ml_status}"

  echo "syncing runtime/temp"
  gcloud storage rsync -r runtime/temp "gs://${BUCKET}/runtime_temp"
  sync_temp_status=$?
  echo "sync_temp_status=${sync_temp_status}"

  {
    echo "restore_status=${restore_status}"
    echo "stage_status=${stage_status}"
    echo "hrrr_status=${hrrr_status}"
    echo "controlled_status=${controlled_status}"
    echo "package_breck_status=${package_breck_status}"
    echo "package_keystone_status=${package_keystone_status}"
    echo "sync_ml_status=${sync_ml_status}"
    echo "sync_temp_status=${sync_temp_status}"
    echo "breck_dataset_zip=gs://${BUCKET}/drive_upload/breck_tenmile_9p6_specific_lcp_canopy_v1_dataset.zip"
    echo "keystone_dataset_zip=gs://${BUCKET}/drive_upload/keystone_9p6_specific_lcp_canopy_v1_dataset.zip"
    echo "finished_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  } >"$STATUS_FILE"

  df -h .
  echo "finished_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"

  if [[ "$restore_status" -ne 0 || "$stage_status" -ne 0 || "$hrrr_status" -ne 0 || "$controlled_status" -ne 0 || "$package_breck_status" -ne 0 || "$package_keystone_status" -ne 0 || "$sync_ml_status" -ne 0 || "$sync_temp_status" -ne 0 ]]; then
    return 1
  fi
  return 0
}

main 2>&1 | tee "$WRAPPER_LOG"
status=${PIPESTATUS[0]}

gcloud storage cp "$WRAPPER_LOG" "gs://${BUCKET}/${WRAPPER_LOG}" || true
gcloud storage cp "$STATUS_FILE" "gs://${BUCKET}/${STATUS_FILE}" || true
echo "shutdown_requested_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee -a "$WRAPPER_LOG"
gcloud storage cp "$WRAPPER_LOG" "gs://${BUCKET}/${WRAPPER_LOG}" || true
sudo shutdown -h now
exit "$status"
