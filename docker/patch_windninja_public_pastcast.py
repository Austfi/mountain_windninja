#!/usr/bin/env python3
"""Patch upstream WindNinja to allow unsigned public GCS pastcast reads."""

from pathlib import Path
import sys


START_MARKER = """    if(CPLGetConfigOption("GS_SECRET_ACCESS_KEY", NULL) == NULL || CPLGetConfigOption("GS_ACCESS_KEY_ID", NULL) == NULL)"""

END_MARKER = """    CPLDebug( "GCP", "Starting download..." );"""


NEW = """    const bool bNoSignRequest = CSLTestBoolean(
        CPLGetConfigOption("GS_NO_SIGN_REQUEST", "NO")
    );
    if(!bNoSignRequest &&
       (CPLGetConfigOption("GS_SECRET_ACCESS_KEY", NULL) == NULL ||
        CPLGetConfigOption("GS_ACCESS_KEY_ID", NULL) == NULL))
    {
        if(CPLGetConfigOption("GS_OAUTH2_PRIVATE_KEY_FILE", NULL) == NULL ||
           CPLGetConfigOption("GS_OAUTH2_CLIENT_EMAIL", NULL) == NULL)
        {
            throw std::runtime_error(
                "Missing required GCS credentials. Set GS_NO_SIGN_REQUEST=YES for public data,\\n"
                "or provide one of the following credential pairs:\\n"
                "GS_SECRET_ACCESS_KEY and GS_ACCESS_KEY_ID \\n"
                "                OR \\n"
                "GS_OAUTH2_PRIVATE_KEY_FILE and GS_OAUTH2_CLIENT_EMAIL"
            );
        }
    }
"""


def main() -> int:
    if len(sys.argv) != 2:
        raise SystemExit("usage: patch_windninja_public_pastcast.py <gcp_wx_init.cpp>")

    target = Path(sys.argv[1])
    text = target.read_text(encoding="utf-8")
    start = text.find(START_MARKER)
    end = text.find(END_MARKER)
    if start == -1 or end == -1 or end <= start:
        raise SystemExit(f"expected upstream credential block not found in {target}")
    target.write_text(text[:start] + NEW + "\n" + text[end:], encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
