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


def build_unet(in_channels: int = 5, out_channels: int = 2, base_channels: int = 32):
    torch, nn = _torch()

    class ResidualUNet(nn.Module):
        def __init__(self) -> None:
            super().__init__()
            c = base_channels
            self.enc1 = ConvBlock(in_channels, c)
            self.enc2 = ConvBlock(c, c * 2)
            self.enc3 = ConvBlock(c * 2, c * 4)
            self.enc4 = ConvBlock(c * 4, c * 8)
            self.pool = nn.MaxPool2d(2)
            self.bottleneck = ConvBlock(c * 8, c * 16)
            self.up4 = nn.ConvTranspose2d(c * 16, c * 8, kernel_size=2, stride=2)
            self.dec4 = ConvBlock(c * 16, c * 8)
            self.up3 = nn.ConvTranspose2d(c * 8, c * 4, kernel_size=2, stride=2)
            self.dec3 = ConvBlock(c * 8, c * 4)
            self.up2 = nn.ConvTranspose2d(c * 4, c * 2, kernel_size=2, stride=2)
            self.dec2 = ConvBlock(c * 4, c * 2)
            self.up1 = nn.ConvTranspose2d(c * 2, c, kernel_size=2, stride=2)
            self.dec1 = ConvBlock(c * 2, c)
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

