# ML Experiments

This top-level package contains research code that is adjacent to the operational
WindNinja wrapper. It should not be imported by the normal `mwn.sh` forecast,
terrain, or validation commands.

Current experiment:

- [`residual_unet`](residual_unet/) - learns a residual correction from
  WindNinja mass-solver output toward WindNinja momentum-solver output. The
  first model is Berthoud-only; the current data-build branch expands training
  data to Berthoud, Breck/Tenmile, Keystone, and Loveland/A-Basin 9.6 km boxes.

Generated datasets, checkpoints, Colab return artifacts, and ZIP uploads are
git-ignored. Keep committed files limited to source code, configs, notebooks,
tests, and small documentation.
