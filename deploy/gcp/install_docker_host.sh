#!/usr/bin/env bash
set -euo pipefail

if ! command -v apt-get >/dev/null 2>&1; then
  echo "This installer currently expects Ubuntu or another apt-based Linux host."
  exit 1
fi

sudo apt-get update
sudo apt-get install -y curl docker.io git nano

if apt-cache show docker-compose-v2 >/dev/null 2>&1; then
  sudo apt-get install -y docker-compose-v2
elif apt-cache show docker-compose-plugin >/dev/null 2>&1; then
  sudo apt-get install -y docker-compose-plugin
elif apt-cache show docker-compose >/dev/null 2>&1; then
  sudo apt-get install -y docker-compose
fi

sudo systemctl enable docker
sudo systemctl start docker

if getent group docker >/dev/null 2>&1; then
  sudo usermod -aG docker "$USER"
fi

echo "Docker host dependencies installed."
echo "Run 'newgrp docker' or log out and back in before running docker without sudo."
