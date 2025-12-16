#!/usr/bin/env bash
set -Eeuo pipefail
umask 002

# -------------------------------
# Cron-safe environment
# -------------------------------
export HOME="/home/austin_finnell"
export USER="austin_finnell"
export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

BASE_DIR="/home/austin_finnell/keystone_automation"
LOG_DIR="$BASE_DIR/logs"
mkdir -p "$LOG_DIR"

LOG_FILE="$LOG_DIR/scheduled_18h_$(date -u +%Y%m%dT%H%M%SZ).log"

# Start logging immediately
exec > >(tee -a "$LOG_FILE") 2>&1

echo "================================================"
echo "START scheduled WindNinja run"
echo "UTC: $(date -u)"
echo "USER: $(whoami)"
echo "PWD: $(pwd)"
echo "================================================"

cd "$BASE_DIR"

# -------------------------------
# Clean state (matches manual run)
# -------------------------------
rm -rf "$BASE_DIR/temp/"*
rm -rf "$BASE_DIR/static_data/NINJAFOAM_"*

# -------------------------------
# OpenFOAM (load into current shell)
# -------------------------------
echo "Sourcing OpenFOAM environment..."

export ZSH_NAME=""
export BASH_NAME="bash"

if [ -f /opt/openfoam9/etc/bashrc ]; then
    set +e
    set +u
    source /opt/openfoam9/etc/bashrc
    set -u
    set -e
else
    echo "ERROR: OpenFOAM bashrc not found"
    exit 1
fi

echo "OpenFOAM environment loaded successfully"

# Sanity check: these should exist after sourcing
echo "WM_PROJECT_DIR=${WM_PROJECT_DIR:-<unset>}"
echo "FOAM_ETC=${FOAM_ETC:-<unset>}"

# -------------------------------
# OpenFOAM runtime paths (FULL)
# -------------------------------
export FOAM_PLATFORM="linux64GccDPInt32Opt"
export FOAM_BASE="/opt/openfoam9/platforms/$FOAM_PLATFORM"

export FOAM_BIN="$FOAM_BASE/bin"

export FOAM_LIB_CORE="$FOAM_BASE/lib"
export FOAM_LIB_MPI="$FOAM_BASE/lib/openmpi-system"
export FOAM_LIB_DUMMY="$FOAM_BASE/lib/dummy"

export PATH="$FOAM_BIN:$PATH"
export LD_LIBRARY_PATH="$FOAM_LIB_CORE:$FOAM_LIB_MPI:$FOAM_LIB_DUMMY:${LD_LIBRARY_PATH:-}"

echo "FOAM_BIN=$FOAM_BIN"
echo "FOAM_LIB_CORE=$FOAM_LIB_CORE"
echo "FOAM_LIB_MPI=$FOAM_LIB_MPI"
echo "FOAM_LIB_DUMMY=$FOAM_LIB_DUMMY"
echo "LD_LIBRARY_PATH=$LD_LIBRARY_PATH"


# -------------------------------
# Run forecast
# -------------------------------
/usr/bin/python3 "$BASE_DIR/scripts/daily_run.py" \
    --mode forecast \
    --model HRRR \
    --hours 18

touch "$LOG_DIR/last_success"
echo "END $(date -u)"
