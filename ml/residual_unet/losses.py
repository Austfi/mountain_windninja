"""Training losses and tensor metrics."""
from __future__ import annotations


def masked_mean(values, mask):
    mask = mask.to(dtype=values.dtype)
    while mask.ndim < values.ndim:
        mask = mask.unsqueeze(1)
    total = (values * mask).sum()
    count = mask.expand_as(values).sum().clamp_min(1.0)
    return total / count


def spatial_gradient_loss(pred_uv, mom_uv, valid_mask):
    """Compare local wind-field gradients on adjacent valid pixels."""

    mask_x = valid_mask[..., 1:] * valid_mask[..., :-1]
    pred_dx = pred_uv[..., 1:] - pred_uv[..., :-1]
    mom_dx = mom_uv[..., 1:] - mom_uv[..., :-1]

    mask_y = valid_mask[..., 1:, :] * valid_mask[..., :-1, :]
    pred_dy = pred_uv[..., 1:, :] - pred_uv[..., :-1, :]
    mom_dy = mom_uv[..., 1:, :] - mom_uv[..., :-1, :]

    return 0.5 * (
        masked_mean((pred_dx - mom_dx) ** 2, mask_x)
        + masked_mean((pred_dy - mom_dy) ** 2, mask_y)
    )


def corrected_loss(
    pred_delta,
    mass_uv,
    mom_uv,
    valid_mask,
    *,
    speed_weight: float = 0.1,
    gradient_weight: float = 0.0,
):
    import torch

    pred_uv = mass_uv + pred_delta
    vec_loss = masked_mean((pred_uv - mom_uv) ** 2, valid_mask)
    pred_speed = torch.sqrt((pred_uv ** 2).sum(dim=1) + 1e-6)
    mom_speed = torch.sqrt((mom_uv ** 2).sum(dim=1) + 1e-6)
    speed_loss = masked_mean(torch.abs(pred_speed - mom_speed), valid_mask)
    grad_loss = spatial_gradient_loss(pred_uv, mom_uv, valid_mask)
    return vec_loss + speed_weight * speed_loss + gradient_weight * grad_loss, {
        "vec_loss": float(vec_loss.detach().cpu()),
        "speed_loss": float(speed_loss.detach().cpu()),
        "gradient_loss": float(grad_loss.detach().cpu()),
    }


def vector_rmse_torch(pred_uv, target_uv, valid_mask) -> float:
    import torch

    err = ((pred_uv - target_uv) ** 2).sum(dim=1)
    rmse = torch.sqrt(masked_mean(err, valid_mask))
    return float(rmse.detach().cpu())


def speed_mae_torch(pred_uv, target_uv, valid_mask) -> float:
    import torch

    pred_speed = torch.sqrt((pred_uv ** 2).sum(dim=1) + 1e-6)
    target_speed = torch.sqrt((target_uv ** 2).sum(dim=1) + 1e-6)
    return float(masked_mean(torch.abs(pred_speed - target_speed), valid_mask).detach().cpu())
