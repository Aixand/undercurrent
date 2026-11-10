#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

readonly PRODUCT_ROOT="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly REPOSITORY_ROOT="$(CDPATH= cd -- "${PRODUCT_ROOT}" && pwd)"

# Preserve the existing source-install route and its process/receipt checks.
product_route=false
case "${1:-}" in
    doctor|develop|check|test|build|package|verify|inspect|commands|plan|upgrade|rollback|prune|export-release|release-check) product_route=true ;;
    install|reinstall|uninstall)
        for argument in "$@"; do
            case "$argument" in --artifact|--artifact=*|--prefix|--prefix=*|--product|--product=*|--scope|--scope=*|--owner|--owner=*|--register-path|--command-policy|--command-policy=*) product_route=true ;; esac
        done
        ;;
esac
if [[ "$product_route" == true ]]; then
    exec "${AIXAND_DEV_PYTHON:-${UNDERCURRENT_DEV_PYTHON:-python3}}" -B "${PRODUCT_ROOT}/scripts/dev/cli.py" --project "${PRODUCT_ROOT}" "$@"
fi
readonly -a ARTIFACTS=(uc uc-daemon uc-session-host uc-mcp)

usage() {
    cat <<'EOF'
Usage:
  ./dev.sh package --output dist/release
  ./dev.sh install --artifact dist/release --prefix /absolute/product-store --accept-license SHA256
  ./dev.sh uninstall --product undercurrent --prefix /absolute/product-store --stopped
  ./dev.sh install --prod
  ./dev.sh reinstall [--prod]
  ./dev.sh uninstall

Commands:
  doctor            Inspect the revision-2 product configuration and tools.
  develop           Prepare the pinned development tools.
  check             Check formatting, docs, repository files, metadata, and notices.
  test              Run the configured Rust, Python, and tooling tests.
  build --release   Build the configured native components.
  package           Assemble a portable release (--output selects a new directory).
  verify            Verify --artifact PATH; --install adds disposable runtime checks.
  plan / inspect    Inspect an artifact installation plan or owned store.
  commands          Inspect command selection and explicit Windows PATH registration.
  upgrade           Replace an owned product with an explicit --artifact PATH.
  export-release    Export a verified artifact and its --report for release handoff.
  install --prod     Build the release binaries and install a new copy.
  reinstall          Stop the daemon, build release binaries, and replace an installed copy.
  uninstall          Remove the installed binaries while preserving product state.

Environment:
  UNDERCURRENT_DEV_INSTALL_DIR  Binary directory (default: $CARGO_HOME/bin or ~/.cargo/bin)
  UNDERCURRENT_CACHE_ROOT           Build-cache root (default: ~/data/cache)
  CARGO_TARGET_DIR              Exact Cargo target directory override

Supported hosts: Linux x86_64 (GNU libc), macOS arm64 and x86_64.
macOS requires Xcode Command Line Tools and the repository Rust toolchain.

Use --artifact, --product, or --prefix for revision-2 install/reinstall/uninstall.
Bare reinstall/uninstall and install --prod retain the source-development route.
Use 'python3 scripts/dev/cli.py help' for the complete product command reference.
EOF
}

fail() {
    printf 'undercurrent dev installer: %s\n' "$1" >&2
    exit 1
}

require_commands() {
    local command
    for command in "$@"; do
        command -v "${command}" >/dev/null 2>&1 ||
            fail "required command is unavailable: ${command}"
    done
}

require_supported_host() {
    case "$(uname -s):$(uname -m)" in
        Linux:x86_64)
            require_commands getconf readlink
            [[ "$(getconf GNU_LIBC_VERSION 2>/dev/null)" == glibc\ * ]] ||
                fail 'the supported Linux development install host requires GNU libc'
            HOST_PLATFORM=Linux
            ;;
        Darwin:arm64|Darwin:x86_64)
            require_commands lsof
            HOST_PLATFORM=Darwin
            ;;
        *)
            fail 'supported development install hosts are Linux x86_64 (GNU libc) and macOS arm64 or x86_64'
            ;;
    esac
    readonly HOST_PLATFORM
}

resolve_install_directory() {
    local cargo_root
    cargo_root="${CARGO_HOME:-${HOME:?HOME is required}/.cargo}"
    INSTALL_DIRECTORY="${UNDERCURRENT_DEV_INSTALL_DIR:-${cargo_root}/bin}"
    [[ "${INSTALL_DIRECTORY}" == /* ]] ||
        fail 'UNDERCURRENT_DEV_INSTALL_DIR and CARGO_HOME must resolve to an absolute path'
    [[ "${INSTALL_DIRECTORY}" != / ]] || fail 'refusing to use the filesystem root as the install directory'
    [[ "${INSTALL_DIRECTORY}" != *$'\n'* ]] || fail 'the install directory must not contain a newline'
    readonly INSTALL_DIRECTORY
}

resolve_target_directory() {
    local cache_root
    cache_root="${UNDERCURRENT_CACHE_ROOT:-${HOME:?HOME is required}/data/cache}"
    [[ "${cache_root}" == /* ]] || fail 'UNDERCURRENT_CACHE_ROOT must be an absolute path'
    [[ "${cache_root}" != / && "${cache_root}" != *$'\n'* ]] ||
        fail 'UNDERCURRENT_CACHE_ROOT must be a bounded absolute path without newlines'

    TARGET_DIRECTORY="${CARGO_TARGET_DIR:-${cache_root}/undercurrent/dev-installer-target}"
    [[ "${TARGET_DIRECTORY}" == /* ]] || fail 'CARGO_TARGET_DIR must be an absolute path'
    [[ "${TARGET_DIRECTORY}" != / && "${TARGET_DIRECTORY}" != *$'\n'* ]] ||
        fail 'CARGO_TARGET_DIR must be a bounded absolute path without newlines'
    readonly TARGET_DIRECTORY
}

prepare_install_directory() {
    if [[ -L "${INSTALL_DIRECTORY}" ]]; then
        fail "refusing to use a symbolic-link install directory: ${INSTALL_DIRECTORY}"
    fi
    if [[ -e "${INSTALL_DIRECTORY}" && ! -d "${INSTALL_DIRECTORY}" ]]; then
        fail "install path is not a directory: ${INSTALL_DIRECTORY}"
    fi
    mkdir -p -- "${INSTALL_DIRECTORY}"
    [[ ! -L "${INSTALL_DIRECTORY}" && -d "${INSTALL_DIRECTORY}" ]] ||
        fail "install directory became unsafe: ${INSTALL_DIRECTORY}"
}

installed_process_pid() {
    local executable="$1"
    local expected
    local process_executable
    local actual

    [[ -f "${executable}" && ! -L "${executable}" ]] || return 1
    if [[ "${HOST_PLATFORM}" == Darwin ]]; then
        # Match the executable's file identity, not a truncated process name or argv.
        # lsof returns 1 with no output when no process has the file open.
        local listing
        local status=0
        local attempt
        # Process-table snapshots can fail transiently. Every retry must still
        # prove absence or identify an exact executable; errors never mean stopped.
        for attempt in 1 2 3; do
            status=0
            listing="$(lsof -F p -a -d txt -- "${executable}" 2>&1)" || status=$?
            if [[ -z "${listing}" && "${status}" == 1 ]]; then
                return 1
            fi
            if [[ "${status}" == 0 && "${listing}" =~ ^p[0-9]+ ]]; then
                actual="${listing%%$'\n'*}"
                printf '%s\n' "${actual#p}"
                return 0
            fi
            [[ "${attempt}" == 3 ]] || sleep 0.1
        done
        printf 'could not inspect running executables (lsof exit %s): %s\n' "${status}" "${listing}" >&2
        return 2
    fi
    expected="$(readlink -f -- "${executable}")"
    for process_executable in /proc/[0-9]*/exe; do
        actual="$(readlink -f -- "${process_executable}" 2>/dev/null)" || continue
        if [[ "${actual}" == "${expected}" ]]; then
            actual="${process_executable#/proc/}"
            printf '%s\n' "${actual%%/*}"
            return 0
        fi
    done
    return 1
}

require_installed_artifacts_stopped() {
    local artifact
    local destination
    local pid
    local status

    for artifact in "${ARTIFACTS[@]}"; do
        destination="${INSTALL_DIRECTORY}/${artifact}"
        if pid="$(installed_process_pid "${destination}")"; then
            fail "${artifact} is still running as PID ${pid}; stop the daemon and sessions before replacing or uninstalling"
        else
            status=$?
            [[ "${status}" == 1 ]] || fail "could not verify that ${artifact} is stopped"
        fi
    done
}

preflight_destinations() {
    local replace="$1"
    local artifact
    local destination

    for artifact in "${ARTIFACTS[@]}"; do
        destination="${INSTALL_DIRECTORY}/${artifact}"
        if [[ -e "${destination}" || -L "${destination}" ]]; then
            if [[ "${replace}" != true ]]; then
                fail "${destination} already exists; use './dev.sh reinstall' to replace it"
            fi
            [[ -f "${destination}" && ! -L "${destination}" ]] ||
                fail "refusing to replace a non-regular or symbolic-link path: ${destination}"
        fi
    done
}

build_production_artifacts() {
    mkdir -p -- "${TARGET_DIRECTORY}"
    CARGO_TARGET_DIR="${TARGET_DIRECTORY}" cargo build \
        --manifest-path "${REPOSITORY_ROOT}/Cargo.toml" \
        --locked --release \
        --package undercurrent-cli \
        --package undercurrent-daemon \
        --package undercurrent-mcp \
        --bins
}

verify_artifact_set() {
    local directory="$1"
    local artifact
    local path
    local version

    for artifact in "${ARTIFACTS[@]}"; do
        path="${directory}/${artifact}"
        [[ -f "${path}" && ! -L "${path}" && -x "${path}" ]] ||
            fail "required release artifact is missing, unsafe, or not executable: ${path}"
        version="$("${path}" --version)" || fail "release artifact version probe failed: ${path}"
        [[ "${version}" == "${artifact} "* ]] ||
            fail "release artifact returned an unexpected version identity: ${version}"
    done
}

install_artifacts() {
    local replace="$1"
    local release_directory="${TARGET_DIRECTORY}/release"
    local staging_directory
    local artifact

    verify_artifact_set "${release_directory}"
    preflight_destinations "${replace}"
    require_installed_artifacts_stopped

    staging_directory="$(mktemp -d "${INSTALL_DIRECTORY}/.undercurrent-dev-install.XXXXXX")"
    chmod 700 "${staging_directory}"
    (
        cleanup_staging() {
            local staged_artifact
            for staged_artifact in "${ARTIFACTS[@]}"; do
                rm -f -- "${staging_directory}/${staged_artifact}"
            done
            rmdir -- "${staging_directory}" 2>/dev/null || true
        }
        trap cleanup_staging EXIT

        for artifact in "${ARTIFACTS[@]}"; do
            install -m 0755 -- "${release_directory}/${artifact}" "${staging_directory}/${artifact}"
        done
        verify_artifact_set "${staging_directory}"
        for artifact in "${ARTIFACTS[@]}"; do
            mv -f -- "${staging_directory}/${artifact}" "${INSTALL_DIRECTORY}/${artifact}"
        done
        verify_artifact_set "${INSTALL_DIRECTORY}"
    )
}

install_product() {
    local replace="$1"

    prepare_install_directory
    preflight_destinations "${replace}"
    if [[ "${replace}" == true && -x "${INSTALL_DIRECTORY}/uc" ]]; then
        "${INSTALL_DIRECTORY}/uc" daemon stop || fail 'could not stop the daemon; reinstall aborted'
    fi
    require_installed_artifacts_stopped
    resolve_target_directory
    build_production_artifacts
    install_artifacts "${replace}"

    if [[ "${replace}" == true ]]; then
        printf 'Reinstalled Undercurrent release binaries in %s\n' "${INSTALL_DIRECTORY}"
    else
        printf 'Installed Undercurrent release binaries in %s\n' "${INSTALL_DIRECTORY}"
    fi
    printf '%s\n' 'The daemon remains stopped; start it explicitly with: uc daemon start'
}

uninstall_product() {
    local artifact
    local destination
    local removed=0

    if [[ ! -e "${INSTALL_DIRECTORY}" && ! -L "${INSTALL_DIRECTORY}" ]]; then
        printf 'Undercurrent development binaries are not installed in %s\n' "${INSTALL_DIRECTORY}"
        return 0
    fi
    [[ -d "${INSTALL_DIRECTORY}" && ! -L "${INSTALL_DIRECTORY}" ]] ||
        fail "refusing to uninstall through an unsafe install directory: ${INSTALL_DIRECTORY}"

    preflight_destinations true
    require_installed_artifacts_stopped
    for artifact in "${ARTIFACTS[@]}"; do
        destination="${INSTALL_DIRECTORY}/${artifact}"
        if [[ -e "${destination}" ]]; then
            rm -f -- "${destination}"
            removed=1
        fi
    done

    if ((removed == 1)); then
        printf 'Uninstalled Undercurrent development binaries from %s\n' "${INSTALL_DIRECTORY}"
    else
        printf 'Undercurrent development binaries are not installed in %s\n' "${INSTALL_DIRECTORY}"
    fi
    printf '%s\n' 'Undercurrent configuration, database, logs, and session state were preserved.'
}

if (($# == 0)); then
    usage >&2
    exit 2
fi

case "$1" in
    install)
        (($# == 2)) && [[ "$2" == --prod ]] || {
            usage >&2
            exit 2
        }
        require_commands cargo chmod install mkdir mktemp mv rm rmdir uname
        require_supported_host
        resolve_install_directory
        install_product false
        ;;
    reinstall)
        if (($# == 2)) && [[ "$2" == --prod ]]; then
            :
        elif (($# != 1)); then
            usage >&2
            exit 2
        fi
        require_commands cargo chmod install mkdir mktemp mv rm rmdir uname
        require_supported_host
        resolve_install_directory
        install_product true
        ;;
    uninstall)
        (($# == 1)) || {
            usage >&2
            exit 2
        }
        require_commands rm uname
        require_supported_host
        resolve_install_directory
        uninstall_product
        ;;
    --help|-h|help)
        (($# == 1)) || {
            usage >&2
            exit 2
        }
        usage
        ;;
    *)
        usage >&2
        exit 2
        ;;
esac
