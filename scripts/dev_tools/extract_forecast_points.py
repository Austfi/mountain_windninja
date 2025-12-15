import os
import subprocess
import glob

# Config
BASE_DIR = "/home/austin_finnell/keystone_automation/temp/multi_model"
STATION_LON = -105.91444
STATION_LAT = 39.56216

def get_val(path):
    cmd = ["gdallocationinfo", "-valonly", "-wgs84", path, str(STATION_LON), str(STATION_LAT)]
    try:
        res = subprocess.check_output(cmd).decode().strip()
        if not res: return "NaN"
        return float(res)
    except:
        return "NaN"

def extract_points():
    print("Time|Raw_HRRR|WN_HRRR|WN_NAM|WN_NBM")
    print("---|---|---|---|---")
    
    # Sort steps
    steps = sorted(glob.glob(os.path.join(BASE_DIR, "step_*")))
    
    for step in steps:
        time_str = os.path.basename(step).replace("step_", "")
        
        # Raw HRRR
        raw_hrrr = "NaN"
        hrrr_file = glob.glob(os.path.join(step, "hrrr_*_spd.asc"))
        if hrrr_file: raw_hrrr = get_val(hrrr_file[0])
        
        # WN HRRR
        wn_hrrr = "NaN"
        wn_hrrr_f = os.path.join(step, f"keystone_{time_str}_HRRR_vel.asc")
        if os.path.exists(wn_hrrr_f): wn_hrrr = get_val(wn_hrrr_f)
        
        # WN NAM
        wn_nam = "NaN"
        wn_nam_f = os.path.join(step, f"keystone_{time_str}_NAM_vel.asc")
        if os.path.exists(wn_nam_f): wn_nam = get_val(wn_nam_f)
        
        # WN NBM
        wn_nbm = "NaN"
        wn_nbm_f = os.path.join(step, f"keystone_{time_str}_NBM_vel.asc")
        if os.path.exists(wn_nbm_f): wn_nbm = get_val(wn_nbm_f)
        
        print(f"{time_str}|{raw_hrrr}|{wn_hrrr}|{wn_nam}|{wn_nbm}")

if __name__ == "__main__":
    extract_points()
