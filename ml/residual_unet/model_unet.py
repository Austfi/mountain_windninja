"""Small U-Net for WindNinja residual vector regression."""
from __future__ import annotations


def _group_count(channels: int) -> int:
    for candidate in (8, 4, 2):
        if channels % candidate == 0:
            return candidate
    return 1


def _torch():
    try:
        import torch
        import torch.nn as nn
    except ImportError as exc:
        raise RuntimeError(
            "PyTorch is required for training. Install ml/residual_unet/requirements.txt."
        ) from exc
    return torch, nn


class ConvBlock:
    """Factory wrapper that avoids importing torch at module import time."""

    def __new__(cls, in_channels: int, out_channels: int):
        _torch_module, nn = _torch()
        return nn.Sequential(
            nn.Conv2d(in_channels, out_channels, kernel_size=3, padding=1),
            nn.GroupNorm(_group_count(out_channels), out_channels),
            nn.ReLU(inplace=True),
            nn.Conv2d(out_channels, out_channels, kernel_size=3, padding=1),
            nn.GroupNorm(_group_count(out_channels), out_channels),
            nn.ReLU(inplace=True),
        )


class ResidualConvBlock:
    """Small residual block for architecture ablations."""

    def __new__(cls, in_channels: int, out_channels: int):
        _torch_module, nn = _torch()

        class Block(nn.Module):
            def __init__(self) -> None:
                super().__init__()
                self.main = nn.Sequential(
                    nn.Conv2d(in_channels, out_channels, kernel_size=3, padding=1),
                    nn.GroupNorm(_group_count(out_channels), out_channels),
                    nn.ReLU(inplace=True),
                    nn.Conv2d(out_channels, out_channels, kernel_size=3, padding=1),
                    nn.GroupNorm(_group_count(out_channels), out_channels),
                )
                self.skip = (
                    nn.Identity()
                    if in_channels == out_channels
                    else nn.Conv2d(in_channels, out_channels, kernel_size=1)
                )
                self.out = nn.ReLU(inplace=True)

            def forward(self, x):
                return self.out(self.main(x) + self.skip(x))

        return Block()


def _block(in_channels: int, out_channels: int, block_type: str):
    if block_type == "conv":
        return ConvBlock(in_channels, out_channels)
    if block_type == "residual":
        return ResidualConvBlock(in_channels, out_channels)
    raise ValueError(
        f"Unsupported U-Net block_type {block_type!r}; expected 'conv' or 'residual'."
    )


def build_unet(
    in_channels: int = 5,
    out_channels: int = 2,
    base_channels: int = 32,
    block_type: str = "conv",
):
    torch, nn = _torch()
    block_type = block_type.strip().lower()

    class ResidualUNet(nn.Module):
        def __init__(self) -> None:
            super().__init__()
            c = base_channels
            self.enc1 = _block(in_channels, c, block_type)
            self.enc2 = _block(c, c * 2, block_type)
            self.enc3 = _block(c * 2, c * 4, block_type)
            self.enc4 = _block(c * 4, c * 8, block_type)
            self.pool = nn.MaxPool2d(2)
            self.bottleneck = _block(c * 8, c * 16, block_type)
            self.up4 = nn.ConvTranspose2d(c * 16, c * 8, kernel_size=2, stride=2)
            self.dec4 = _block(c * 16, c * 8, block_type)
            self.up3 = nn.ConvTranspose2d(c * 8, c * 4, kernel_size=2, stride=2)
            self.dec3 = _block(c * 8, c * 4, block_type)
            self.up2 = nn.ConvTranspose2d(c * 4, c * 2, kernel_size=2, stride=2)
            self.dec2 = _block(c * 4, c * 2, block_type)
            self.up1 = nn.ConvTranspose2d(c * 2, c, kernel_size=2, stride=2)
            self.dec1 = _block(c * 2, c, block_type)
            self.out = nn.Conv2d(c, out_channels, kernel_size=1)

        def forward(self, x):
            e1 = self.enc1(x)
            e2 = self.enc2(self.pool(e1))
            e3 = self.enc3(self.pool(e2))
            e4 = self.enc4(self.pool(e3))
            bottleneck = self.bottleneck(self.pool(e4))
            d4 = self.dec4(torch.cat([self.up4(bottleneck), e4], dim=1))
            d3 = self.dec3(torch.cat([self.up3(d4), e3], dim=1))
            d2 = self.dec2(torch.cat([self.up2(d3), e2], dim=1))
            d1 = self.dec1(torch.cat([self.up1(d2), e1], dim=1))
            return self.out(d1)

    return ResidualUNet()
