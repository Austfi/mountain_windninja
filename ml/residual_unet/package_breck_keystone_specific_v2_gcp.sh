#!/usr/bin/env bash
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

BUCKET="${MWN_GCP_BUCKET:-mwn-ml-general-9p6-spring-nova-475120-r0}"
BASE="runtime/ml/residual_unet/domain_specific/breck_keystone_specific_v2_package"
LOG_DIR="$BASE/logs"
WRAPPER_LOG="$LOG_DIR/package_sync.log"
STATUS_FILE="$LOG_DIR/package_status.txt"
RESTORE_RUNTIME_TEMP="${RESTORE_RUNTIME_TEMP:-1}"
RESTORE_RUNTIME_ML="${RESTORE_RUNTIME_ML:-1}"
SHUTDOWN_ON_COMPLETE="${SHUTDOWN_ON_COMPLETE:-0}"
PYTHON_BIN=".venv/bin/python"
if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="python3"
fi

mkdir -p "$LOG_DIR"

restore_inputs() {
  echo "restore_static_data_started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  gcloud storage rsync -r "gs://${BUCKET}/static_data" static_data
  local static_status=$?
  echo "restore_static_data_status=${static_status}"
  [[ "$static_status" -eq 0 ]] || return "$static_status"

  if [[ "$RESTORE_RUNTIME_ML" = "1" ]]; then
    echo "restore_runtime_ml_started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    mkdir -p runtime/ml/residual_unet
    gcloud storage rsync -r "gs://${BUCKET}/runtime_ml" runtime/ml/residual_unet
    local runtime_ml_status=$?
    echo "restore_runtime_ml_status=${runtime_ml_status}"
    [[ "$runtime_ml_status" -eq 0 ]] || return "$runtime_ml_status"
  else
    echo "restore_runtime_ml=skipped"
  fi

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
  local package_breck_status=0
  local package_keystone_status=0
  local sync_ml_status=0
  local sync_temp_status=0

  echo "started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "bucket=gs://${BUCKET}"
  echo "restore_runtime_temp=${RESTORE_RUNTIME_TEMP}"
  echo "restore_runtime_ml=${RESTORE_RUNTIME_ML}"
  echo "shutdown_on_complete=${SHUTDOWN_ON_COMPLETE}"
  echo "python=${PYTHON_BIN}"
  df -h .

  restore_inputs
  restore_status=$?
  echo "restore_status=${restore_status}"

  if [[ "$restore_status" -eq 0 ]]; then
    package_domain breck breck_tenmile_9p6_specific_lcp_canopy_v2
    package_breck_status=$?
    package_domain keystone keystone_9p6_specific_lcp_canopy_v2
    package_keystone_status=$?
  else
    package_breck_status=99
    package_keystone_status=99
  fi

  echo "syncing runtime/ml/residual_unet"
  gcloud storage rsync -r runtime/ml/residual_unet "gs://${BUCKET}/runtime_ml"
  sync_ml_status=$?
  echo "sync_ml_status=${sync_ml_status}"

  if [[ -d runtime/temp ]]; then
    echo "syncing runtime/temp"
    gcloud storage rsync -r runtime/temp "gs://${BUCKET}/runtime_temp"
    sync_temp_status=$?
  else
    sync_temp_status=0
  fi
  echo "sync_temp_status=${sync_temp_status}"

  {
    echo "restore_status=${restore_status}"
    echo "package_breck_status=${package_breck_status}"
    echo "package_keystone_status=${package_keystone_status}"
    echo "sync_ml_status=${sync_ml_status}"
    echo "sync_temp_status=${sync_temp_status}"
    echo "breck_dataset_zip=gs://${BUCKET}/drive_upload/breck_tenmile_9p6_specific_lcp_canopy_v2_dataset.zip"
    echo "keystone_dataset_zip=gs://${BUCKET}/drive_upload/keystone_9p6_specific_lcp_canopy_v2_dataset.zip"
    echo "notebook=gs://${BUCKET}/drive_upload/06_train_site_specific_9p6_colab.ipynb"
    echo "finished_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  } >"$STATUS_FILE"

  df -h .
  echo "finished_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"

  if [[ "$restore_status" -ne 0 || "$package_breck_status" -ne 0 || "$package_keystone_status" -ne 0 || "$sync_ml_status" -ne 0 || "$sync_temp_status" -ne 0 ]]; then
    return 1
  fi
  return 0
}

main 2>&1 | tee "$WRAPPER_LOG"
status=${PIPESTATUS[0]}

gcloud storage cp "$WRAPPER_LOG" "gs://${BUCKET}/${WRAPPER_LOG}" || true
gcloud storage cp "$STATUS_FILE" "gs://${BUCKET}/${STATUS_FILE}" || true

if [[ "$SHUTDOWN_ON_COMPLETE" = "1" ]]; then
  echo "shutdown_requested_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee -a "$WRAPPER_LOG"
  gcloud storage cp "$WRAPPER_LOG" "gs://${BUCKET}/${WRAPPER_LOG}" || true
  sudo shutdown -h now
fi

exit "$status"
