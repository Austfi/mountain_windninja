#!/usr/bin/env python3
"""Patch upstream WindNinja generic NetCDF warping to avoid GDAL segfaults."""

from pathlib import Path
import sys


OLD = """        int nBandCount = srcDS->GetRasterCount();

        psWarpOptions->nBandCount = nBandCount;

        psWarpOptions->padfDstNoDataReal =
            (double*) CPLMalloc( sizeof( double ) * nBandCount );
        psWarpOptions->padfDstNoDataImag =
            (double*) CPLMalloc( sizeof( double ) * nBandCount );

        for( int b = 0;b < srcDS->GetRasterCount();b++ ) {
            psWarpOptions->padfDstNoDataReal[b] = dfNoData;
            psWarpOptions->padfDstNoDataImag[b] = dfNoData;
        }

        if( pbSuccess == false )
            dfNoData = -9999.0;
"""


NEW = """        int nBandCount = srcDS->GetRasterCount();

        if( pbSuccess == false )
            dfNoData = -9999.0;

        psWarpOptions->nBandCount = nBandCount;
        psWarpOptions->panSrcBands =
            (int*) CPLMalloc( sizeof( int ) * nBandCount );
        psWarpOptions->panDstBands =
            (int*) CPLMalloc( sizeof( int ) * nBandCount );
        psWarpOptions->padfDstNoDataReal =
            (double*) CPLMalloc( sizeof( double ) * nBandCount );
        psWarpOptions->padfDstNoDataImag =
            (double*) CPLMalloc( sizeof( double ) * nBandCount );

        for( int b = 0;b < nBandCount;b++ ) {
            psWarpOptions->panSrcBands[b] = b + 1;
            psWarpOptions->panDstBands[b] = b + 1;
            psWarpOptions->padfDstNoDataReal[b] = dfNoData;
            psWarpOptions->padfDstNoDataImag[b] = dfNoData;
        }
"""


def main() -> int:
    if len(sys.argv) != 2:
        raise SystemExit("usage: patch_windninja_generic_warp.py <genericSurfInitialization.cpp>")

    target = Path(sys.argv[1])
    text = target.read_text(encoding="utf-8")
    if OLD not in text:
        raise SystemExit(f"expected generic warp block not found in {target}")
    target.write_text(text.replace(OLD, NEW, 1), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
