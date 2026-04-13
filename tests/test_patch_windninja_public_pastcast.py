from pathlib import Path
import subprocess


UPSTREAM_SAMPLE = """std::string GCPWxModel::fetchForecast(std::string demFile, int nhours)
{
    if(CPLGetConfigOption("GS_SECRET_ACCESS_KEY", NULL) == NULL || CPLGetConfigOption("GS_ACCESS_KEY_ID", NULL) == NULL)
    {
        if(CPLGetConfigOption("GS_OAUTH2_PRIVATE_KEY_FILE", NULL) == NULL || CPLGetConfigOption("GS_OAUTH2_CLIENT_EMAIL", NULL) == NULL)
        {
          throw std::runtime_error(
              "Missing required GCS credentials. One of the following pairs of environment variables must be set:\\n"
              "GS_SECRET_ACCESS_KEY and GS_ACCESS_KEY_ID \\n"
              "                OR \\n"
              "GS_OAUTH2_PRIVATE_KEY_FILE and GS_OAUTH2_CLIENT_EMAIL"
              );
        }
    }

    CPLDebug( "GCP", "Starting download..." );
    if (pfnProgress)
    {
        pfnProgress(0.0, "Starting download...", NULL);
    }
}
"""


def test_patch_script_handles_upstream_3122_credential_block(tmp_path):
    target = tmp_path / "gcp_wx_init.cpp"
    target.write_text(UPSTREAM_SAMPLE, encoding="utf-8")

    subprocess.run(
        [
            "python3",
            "docker/patch_windninja_public_pastcast.py",
            str(target),
        ],
        check=True,
        cwd=Path(__file__).resolve().parents[1],
    )

    patched = target.read_text(encoding="utf-8")
    assert 'GS_NO_SIGN_REQUEST' in patched
    assert 'Set GS_NO_SIGN_REQUEST=YES for public data' in patched
    assert 'CPLDebug( "GCP", "Starting download..." );' in patched
    assert 'Missing required GCS credentials. One of the following pairs' not in patched
