#!/usr/bin/env python3
import os
import glob
import re
import datetime
from xml.etree.ElementTree import Element, SubElement, tostring
import xml.dom.minidom
import zipfile

def parse_datetime_from_filename(filename):
    """
    Return timezone-aware UTC datetime or None.
    Supports:
      YYYYMMDD_HHMM
      MM-DD-YYYY_HHMM
    """
    basename = os.path.basename(filename)
    
    # Try MM-DD-YYYY_HHMM (e.g., keystone_square_30m_12-15-2025_1700_80m.kmz)
    match = re.search(r'(\d{2})-(\d{2})-(\d{4})_(\d{4})', basename)
    if match:
        month, day, year, time_str = match.groups()
        dt_str = f"{year}{month}{day}{time_str}"
        try:
            dt = datetime.datetime.strptime(dt_str, "%Y%m%d%H%M")
            return dt.replace(tzinfo=datetime.timezone.utc)
        except ValueError:
            pass

    # Try YYYYMMDD_HHMM
    match = re.search(r'(\d{8})_(\d{4})', basename)
    if match:
        date_str, time_str = match.groups()
        try:
            dt = datetime.datetime.strptime(f"{date_str}{time_str}", "%Y%m%d%H%M")
            return dt.replace(tzinfo=datetime.timezone.utc)
        except ValueError:
            pass

    return None

def extract_legends(first_kmz, output_dir):
    """
    Extracts legend image files (bmp/png) from the first KMZ into the output_dir.
    Only extracts files with 'legend' in the name (case-insensitive).
    Returns list of filenames.
    """
    legends = []
    try:
        with zipfile.ZipFile(first_kmz, 'r') as z:
            for f in z.namelist():
                lower_f = f.lower()
                if 'legend' in lower_f and (lower_f.endswith('.bmp') or lower_f.endswith('.png')):
                    # Extract to run_dir
                    source = z.open(f)
                    target_path = os.path.join(output_dir, os.path.basename(f))
                    with open(target_path, 'wb') as t:
                        t.write(source.read())
                    legends.append(os.path.basename(f))
                    print(f"Extracted legend: {os.path.basename(f)}")
    except Exception as e:
        print(f"Warning: Failed to extract legend from {first_kmz}: {e}")
    return legends

def add_global_overlays(doc_element, legend_files, gcs_base_url):
    """
    Adds ScreenOverlay elements to the KML Document for each legend file.
    Points to GCS URL for the image.
    """
    for lfile in legend_files:
        overlay = SubElement(doc_element, 'ScreenOverlay')
        name = SubElement(overlay, 'name')
        name.text = "Wind Scale Legend"
        
        icon = SubElement(overlay, 'Icon')
        href = SubElement(icon, 'href')
        # Legend images are uploaded to the same run folder
        href.text = f"{gcs_base_url}/{lfile}"
        
        # Position: Bottom Left
        overlayXY = SubElement(overlay, 'overlayXY', x="0", y="0", xunits="fraction", yunits="fraction")
        screenXY = SubElement(overlay, 'screenXY', x="0", y="0", xunits="fraction", yunits="fraction")
        rotationXY = SubElement(overlay, 'rotationXY', x="0", y="0", xunits="fraction", yunits="fraction")
        size = SubElement(overlay, 'size', x="0", y="0", xunits="fraction", yunits="fraction")

def create_timeseries_kml(run_dir, gcs_base_url, run_label):
    """
    Generates a master KML file containing NetworkLinks to hourly KMZs on GCS.
    """
    kmz_files = glob.glob(os.path.join(run_dir, "*.kmz"))
    # Filter out any non-hourly KMZs if they exist (though output dir should be clean)
    kmz_files = [f for f in kmz_files if "playable" not in f and "latest" not in f]
    
    if not kmz_files:
        print("No KMZ files found.")
        return None

    # Sort and validate
    entries = []
    for kmz in kmz_files:
        dt = parse_datetime_from_filename(os.path.basename(kmz))
        if dt:
            entries.append((dt, kmz))
        else:
            print(f"Warning: Could not parse timestamp from {kmz}")

    entries.sort(key=lambda x: x[0])
    
    if not entries:
        print("No valid timestamped KMZs found.")
        return None

    print(f"Found {len(entries)} valid hourly KMZs.")

    # Extract legends from first KMZ
    legend_files = extract_legends(entries[0][1], run_dir)

    # Build KML
    kml = Element('kml', xmlns="http://www.opengis.net/kml/2.2")
    document = SubElement(kml, 'Document')
    name_tag = SubElement(document, 'name')
    name_tag.text = f"Keystone {run_label}"
    
    open_tag = SubElement(document, 'open')
    open_tag.text = "1"

    # Add legends
    add_global_overlays(document, legend_files, gcs_base_url)

    # Add NetworkLinks
    for dt, kmz_path in entries:
        basename = os.path.basename(kmz_path)
        dt_end = dt + datetime.timedelta(hours=1)
        
        # Time format: YYYY-MM-DDTHH:MM:SSZ
        start_fmt = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_fmt = dt_end.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        network_link = SubElement(document, 'NetworkLink')
        nl_name = SubElement(network_link, 'name')
        nl_name.text = dt.strftime("%H:%M UTC")
        
        # Visibility control
        visibility = SubElement(network_link, 'visibility')
        visibility.text = "0" # Hidden by default, let time slider control check?
        # Actually Google Earth Time Slider works on visible items with TimeSpans.
        # But if we want them ALL to be "checked" in the sidebar but filtered by time, that's best.
        # However, usually we leave them visible=1 and let TimeSpan hide them.
        # User requested: <refreshVisibility>0</refreshVisibility> <flyToView>0</flyToView>
        
        # Let's set visibility to 1 so they are active, but filtered by TimeSpan.
        # Wait, if all are checked, GE loads them all?
        # NetworkLinks with TimeSpans are only loaded when the time slider covers them.
        visibility.text = "1"

        open_nl = SubElement(network_link, 'open')
        open_nl.text = "0"
        
        # TimeSpan
        time_span = SubElement(network_link, 'TimeSpan')
        begin = SubElement(time_span, 'begin')
        begin.text = start_fmt
        end = SubElement(time_span, 'end')
        end.text = end_fmt
        
        # Link
        link = SubElement(network_link, 'Link')
        href = SubElement(link, 'href')
        href.text = f"{gcs_base_url}/{basename}"
        
        view_refresh_mode = SubElement(link, 'viewRefreshMode')
        view_refresh_mode.text = "never"
        
        # User constraints
        fly_to_view = SubElement(network_link, 'flyToView')
        fly_to_view.text = "0"
        
        refresh_visibility = SubElement(network_link, 'refreshVisibility')
        refresh_visibility.text = "0"

    # Write KML
    output_filename = os.path.join(run_dir, f"{run_label}.kml")
    xml_str = xml.dom.minidom.parseString(tostring(kml)).toprettyxml(indent="  ")
    
    with open(output_filename, 'w') as f:
        f.write(xml_str)
        
    print(f"Created Time-Series KML: {output_filename}")
    return output_filename

def create_playable_kmz(run_dir, output_name):
    """
    Bundles all hourly KMZ files from run_dir into a single playable KMZ.
    Creates a master KML with TimeSpan for each hour and embeds all data.
    Returns the path to the created KMZ file.
    """
    kmz_files = glob.glob(os.path.join(run_dir, "*.kmz"))
    kmz_files = [f for f in kmz_files if "playable" not in f and "latest" not in f]
    
    if not kmz_files:
        print("No KMZ files found for bundling.")
        return None

    # Parse and sort by timestamp
    entries = []
    for kmz in kmz_files:
        dt = parse_datetime_from_filename(os.path.basename(kmz))
        if dt:
            entries.append((dt, kmz))
    
    entries.sort(key=lambda x: x[0])
    
    if not entries:
        print("No valid timestamped KMZs found.")
        return None
    
    print(f"Bundling {len(entries)} hourly KMZs into playable KMZ...")

    # Extract legend from first KMZ
    legend_files = extract_legends(entries[0][1], run_dir)

    # Build master KML
    kml = Element('kml', xmlns="http://www.opengis.net/kml/2.2")
    document = SubElement(kml, 'Document')
    name_tag = SubElement(document, 'name')
    name_tag.text = f"Keystone WindNinja Forecast"
    
    open_tag = SubElement(document, 'open')
    open_tag.text = "1"
    
    # Add legend overlay
    for lfile in legend_files:
        overlay = SubElement(document, 'ScreenOverlay')
        ol_name = SubElement(overlay, 'name')
        ol_name.text = "Wind Speed Legend"
        icon = SubElement(overlay, 'Icon')
        href = SubElement(icon, 'href')
        href.text = lfile  # Local path inside KMZ
        SubElement(overlay, 'overlayXY', x="0", y="0", xunits="fraction", yunits="fraction")
        SubElement(overlay, 'screenXY', x="0.02", y="0.05", xunits="fraction", yunits="fraction")
        SubElement(overlay, 'size', x="0", y="0.25", xunits="fraction", yunits="fraction")

    # Create output KMZ
    output_kmz_path = os.path.join(run_dir, f"{output_name}.kmz")
    
    with zipfile.ZipFile(output_kmz_path, 'w', zipfile.ZIP_DEFLATED) as zout:
        # Add each hourly KMZ's contents
        for dt, kmz_path in entries:
            dt_end = dt + datetime.timedelta(hours=1)
            basename = os.path.basename(kmz_path)
            folder_name = dt.strftime("%H%M")
            
            # Create folder for this hour
            folder = SubElement(document, 'Folder')
            f_name = SubElement(folder, 'name')
            f_name.text = dt.strftime("%H:%M UTC")
            
            # TimeSpan
            time_span = SubElement(folder, 'TimeSpan')
            begin = SubElement(time_span, 'begin')
            begin.text = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            end = SubElement(time_span, 'end')
            end.text = dt_end.strftime("%Y-%m-%dT%H:%M:%SZ")
            
            # Extract and add contents from hourly KMZ
            with zipfile.ZipFile(kmz_path, 'r') as zin:
                for item in zin.namelist():
                    data = zin.read(item)
                    # Prefix with folder name to avoid collisions
                    arc_name = f"{folder_name}/{item}"
                    zout.writestr(arc_name, data)
                    
                    # Add NetworkLink to this content
                    if item.endswith('.kml'):
                        nl = SubElement(folder, 'NetworkLink')
                        nl_name = SubElement(nl, 'name')
                        nl_name.text = "Data"
                        link = SubElement(nl, 'Link')
                        href = SubElement(link, 'href')
                        href.text = arc_name
        
        # Add legend files
        for lfile in legend_files:
            lpath = os.path.join(run_dir, lfile)
            if os.path.exists(lpath):
                zout.write(lpath, lfile)
        
        # Add master KML
        master_kml = xml.dom.minidom.parseString(tostring(kml)).toprettyxml(indent="  ")
        zout.writestr("doc.kml", master_kml)
    
    print(f"Created playable KMZ: {output_kmz_path}")
    return output_kmz_path

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 3:
        create_timeseries_kml(sys.argv[1], sys.argv[2], sys.argv[3])
    else:
        print("Usage: python3 create_time_series.py <run_dir> <gcs_base_url> <run_label>")
