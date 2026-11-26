#!/bin/sh
# ==============================================================================
# TapisaurusDB Pre-Built Binary Installer for Linux & macOS
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/tapiruslab/TapisaurusDB/main/scripts/install.sh | sh
#
# Optional environment variables:
#   TAPISAURUS_VERSION=v0.1.0           Pin a specific release tag (default: latest)
#   TAPISAURUS_INSTALL_DIR=/custom/bin  Override install directory
#   TAPISAURUS_SKIP_VERIFY=1            Skip SHA-256 verification (not recommended)
#
# Copyright (c) 2026 Ahmad Faiz <faiz@tapirusdb.com>
# ==============================================================================

set -eu

REPO="tapiruslab/TapisaurusDB"
VERSION="${TAPISAURUS_VERSION:-latest}"

info()  { printf '[tapisaurus] %s\n' "$1"; }
err()   { printf '[tapisaurus] ERROR: %s\n' "$1" >&2; exit 1; }

need_cmd() {
    command -v "$1" >/dev/null 2>&1 || err "Required command '$1' not found in PATH."
}

need_cmd uname
need_cmd tar
need_cmd mktemp

if command -v curl >/dev/null 2>&1; then
    DOWNLOADER="curl"
elif command -v wget >/dev/null 2>&1; then
    DOWNLOADER="wget"
else
    err "Neither 'curl' nor 'wget' was found. Please install one and retry."
fi

download() {
    url="$1"
    dest="$2"
    if [ "$DOWNLOADER" = "curl" ]; then
        curl -fsSL --retry 3 --retry-delay 1 "$url" -o "$dest"
    else
        wget -qO "$dest" "$url"
    fi
}

# 1. Detect OS & Architecture
OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"

ASSET=""
case "$OS" in
    linux)
        case "$ARCH" in
            x86_64|amd64)      ASSET="tapisaurus-linux-x86_64.tar.gz" ;;
            aarch64|arm64)     ASSET="tapisaurus-linux-arm64-robot-chip.tar.gz" ;;
            armv7l|armv7)      ASSET="tapisaurus-linux-armv7-embedded-chip.tar.gz" ;;
            *) err "Unsupported Linux architecture: $ARCH" ;;
        esac
        ;;
    darwin)
        case "$ARCH" in
            arm64|aarch64)     ASSET="tapisaurus-macos-arm64.tar.gz" ;;
            x86_64|amd64)      ASSET="tapisaurus-macos-x86_64.tar.gz" ;;
            *) err "Unsupported macOS architecture: $ARCH" ;;
        esac
        ;;
    *)
        err "Unsupported OS: $OS. On Windows, run: irm https://raw.githubusercontent.com/tapiruslab/TapisaurusDB/main/scripts/install.ps1 | iex"
        ;;
esac

if [ "$VERSION" = "latest" ]; then
    BASE_URL="https://github.com/${REPO}/releases/latest/download"
else
    BASE_URL="https://github.com/${REPO}/releases/download/${VERSION}"
fi

ARCHIVE_URL="${BASE_URL}/${ASSET}"
SHA_URL="${ARCHIVE_URL}.sha256"

# 2. Target install directory
if [ -n "${TAPISAURUS_INSTALL_DIR:-}" ]; then
    INSTALL_DIR="$TAPISAURUS_INSTALL_DIR"
elif [ "$(id -u)" -eq 0 ]; then
    INSTALL_DIR="/usr/local/bin"
else
    INSTALL_DIR="${HOME}/.local/bin"
fi
mkdir -p "$INSTALL_DIR"

TMP_DIR="$(mktemp -d)"
cleanup() { rm -rf "$TMP_DIR"; }
trap cleanup EXIT INT TERM

info "Detected platform: ${OS}/${ARCH}"
info "Downloading ${ASSET} (${VERSION})..."
download "$ARCHIVE_URL" "${TMP_DIR}/${ASSET}"

# 3. Verify SHA-256 checksum
if [ "${TAPISAURUS_SKIP_VERIFY:-0}" != "1" ]; then
    info "Verifying SHA-256 checksum..."
    download "$SHA_URL" "${TMP_DIR}/${ASSET}.sha256"
    EXPECTED="$(awk '{print $1}' "${TMP_DIR}/${ASSET}.sha256" | tr '[:upper:]' '[:lower:]')"
    if command -v sha256sum >/dev/null 2>&1; then
        ACTUAL="$(sha256sum "${TMP_DIR}/${ASSET}" | awk '{print $1}' | tr '[:upper:]' '[:lower:]')"
    elif command -v shasum >/dev/null 2>&1; then
        ACTUAL="$(shasum -a 256 "${TMP_DIR}/${ASSET}" | awk '{print $1}' | tr '[:upper:]' '[:lower:]')"
    else
        err "Neither 'sha256sum' nor 'shasum' found. Set TAPISAURUS_SKIP_VERIFY=1 to bypass verification."
    fi
    if [ -z "$EXPECTED" ] || [ "$EXPECTED" != "$ACTUAL" ]; then
        err "SHA-256 mismatch! Expected ${EXPECTED}, got ${ACTUAL}"
    fi
    info "SHA-256 verified (${ACTUAL})."
fi

# 4. Extract and install binary
tar -xzf "${TMP_DIR}/${ASSET}" -C "$TMP_DIR"
[ -f "${TMP_DIR}/tapisaurus" ] || err "Archive did not contain expected 'tapisaurus' binary."
cp "${TMP_DIR}/tapisaurus" "${INSTALL_DIR}/tapisaurus"
chmod +x "${INSTALL_DIR}/tapisaurus"

# 5. PATH hint
case ":${PATH}:" in
    *":${INSTALL_DIR}:"*) ;;
    *)
        info "Add ${INSTALL_DIR} to your PATH to run 'tapisaurus' from anywhere:"
        printf '  export PATH="%s:$PATH"\n' "$INSTALL_DIR"
        ;;
esac

INSTALLED_VER="$("${INSTALL_DIR}/tapisaurus" --version 2>/dev/null || echo "${VERSION}")"
printf '\n'
info "Installed successfully: ${INSTALLED_VER} -> ${INSTALL_DIR}/tapisaurus"
printf '\nQuick start:\n'
printf '  tapisaurus serve     # Launch all 5 wire gateways (PG 5432, MySQL 3306, Mongo 27017, gRPC 50051, REST 27018)\n'
printf '  tapisaurus shell     # Interactive SQL / Mongo / openCypher / TapisaurusQL REPL\n'
printf '  tapisaurus doctor    # Probe active gateways and storage health\n\n'
