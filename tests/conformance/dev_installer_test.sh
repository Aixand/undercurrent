#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

readonly ROOT="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
readonly CACHE_BASE="${UNDERCURRENT_CACHE_ROOT:-${HOME:?HOME is required}/data/cache}/undercurrent"
mkdir -p -- "${CACHE_BASE}"
readonly WORK_ROOT="$(mktemp -d "${CACHE_BASE}/dev-installer-test.XXXXXX")"
readonly FAKE_BIN="${WORK_ROOT}/fake-bin"
readonly INSTALL_BIN="${WORK_ROOT}/install/bin"
readonly CARGO_LOG="${WORK_ROOT}/cargo.log"
readonly TEST_CACHE="${WORK_ROOT}/cache"
readonly STATE_FILE="${WORK_ROOT}/state/undercurrent.db"
running_pid=""

cleanup() {
    if [[ -n "${running_pid}" ]]; then
        kill "${running_pid}" 2>/dev/null || true
        wait "${running_pid}" 2>/dev/null || true
    fi
    rm -rf -- "${WORK_ROOT}"
}
trap cleanup EXIT

mkdir -p -- "${FAKE_BIN}" "${INSTALL_BIN}" "$(dirname -- "${STATE_FILE}")"
printf '%s\n' 'preserve-me' >"${STATE_FILE}"

cat >"${FAKE_BIN}/cargo" <<'FAKE_CARGO'
#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' "$*" >>"${FAKE_CARGO_LOG:?}"
mkdir -p -- "${CARGO_TARGET_DIR:?}/release"
for artifact in uc uc-daemon uc-session-host uc-mcp; do
    {
        printf '%s\n' '#!/usr/bin/env bash'
        printf 'if [[ "${1:-}" == --version ]]; then printf '\''%%s\\n'\'' '\''%s 0.1.0-beta'\''; exit 0; fi\n' "${artifact}"
        if [[ "${artifact}" == uc ]]; then
            cat <<'FAKE_STOP'
if [[ "$*" == 'daemon stop' ]]; then
    printf '%s\n' stop >>"${FAKE_CARGO_LOG:?}"
    [[ "${FAKE_STOP_FAIL:-false}" != true ]] || exit 1
    if [[ -n "${FAKE_DAEMON_PID:-}" ]]; then
        kill "${FAKE_DAEMON_PID}"
        for ((attempt = 0; attempt < 100; attempt++)); do
            kill -0 "${FAKE_DAEMON_PID}" 2>/dev/null || exit 0
            sleep 0.01
        done
        exit 1
    fi
    exit 0
fi
FAKE_STOP
        fi
        printf 'printf '\''%%s\\n'\'' '\''%s %s'\''\n' "${artifact}" "${FAKE_BUILD_MARKER:?}"
        printf '# build marker: %s\n' "${FAKE_BUILD_MARKER}"
    } >"${CARGO_TARGET_DIR}/release/${artifact}"
    chmod 755 "${CARGO_TARGET_DIR}/release/${artifact}"
done
FAKE_CARGO
chmod 755 "${FAKE_BIN}/cargo"

run_dev() {
    env \
        PATH="${FAKE_BIN}:${PATH}" \
        UNDERCURRENT_DEV_INSTALL_DIR="${INSTALL_BIN}" \
        UNDERCURRENT_CACHE_ROOT="${TEST_CACHE}" \
        CARGO_TARGET_DIR="${WORK_ROOT}/target" \
        FAKE_CARGO_LOG="${CARGO_LOG}" \
        FAKE_BUILD_MARKER="${FAKE_BUILD_MARKER:-first}" \
        FAKE_STOP_FAIL="${FAKE_STOP_FAIL:-false}" \
        FAKE_DAEMON_PID="${running_pid}" \
        FAKE_LSOF_MODE="${FAKE_LSOF_MODE:-}" \
        FAKE_LSOF_STATE="${WORK_ROOT}/lsof-attempts" \
        bash "${ROOT}/dev.sh" "$@"
}

assert_installed_marker() {
    local expected="$1"
    local artifact
    for artifact in uc uc-daemon uc-session-host uc-mcp; do
        [[ -x "${INSTALL_BIN}/${artifact}" && ! -L "${INSTALL_BIN}/${artifact}" ]]
        grep -Fq "# build marker: ${expected}" "${INSTALL_BIN}/${artifact}"
        [[ "$("${INSTALL_BIN}/${artifact}" --version)" == "${artifact} 0.1.0-beta" ]]
    done
}

cat >"${WORK_ROOT}/sleeper.c" <<'C'
#include <unistd.h>
int main(void) { sleep(30); return 0; }
C
cc "${WORK_ROOT}/sleeper.c" -o "${WORK_ROOT}/sleeper"

bash -n "${ROOT}/dev.sh"
run_dev --help | grep -Fq './dev.sh install --prod'

if run_dev install >"${WORK_ROOT}/invalid.out" 2>"${WORK_ROOT}/invalid.err"; then
    printf '%s\n' 'install without --prod unexpectedly succeeded' >&2
    exit 1
fi

run_dev install --prod >"${WORK_ROOT}/install.out"
assert_installed_marker first
grep -Eq 'build --manifest-path .*/Cargo.toml --locked --release --package undercurrent-cli --package undercurrent-daemon --package undercurrent-mcp --bins' "${CARGO_LOG}"

if run_dev install --prod >"${WORK_ROOT}/duplicate.out" 2>"${WORK_ROOT}/duplicate.err"; then
    printf '%s\n' 'duplicate install unexpectedly succeeded' >&2
    exit 1
fi
grep -Fq "already exists; use './dev.sh reinstall'" "${WORK_ROOT}/duplicate.err"

if FAKE_STOP_FAIL=true run_dev reinstall >"${WORK_ROOT}/stop-failure.out" 2>"${WORK_ROOT}/stop-failure.err"; then
    printf '%s\n' 'reinstall unexpectedly ignored a failed daemon stop' >&2
    exit 1
fi
grep -Fq 'could not stop the daemon; reinstall aborted' "${WORK_ROOT}/stop-failure.err"
assert_installed_marker first
[[ "$(grep -c '^build ' "${CARGO_LOG}")" == 1 ]]

install -m 0755 -- "${WORK_ROOT}/sleeper" "${INSTALL_BIN}/uc-daemon"
"${INSTALL_BIN}/uc-daemon" 30 &
running_pid="$!"
sleep 0.1
FAKE_BUILD_MARKER=second run_dev reinstall >"${WORK_ROOT}/reinstall.out"
wait "${running_pid}" || true
running_pid=""
assert_installed_marker second
[[ "$(tail -n 2 "${CARGO_LOG}" | head -n 1)" == stop ]]

FAKE_BUILD_MARKER=second run_dev reinstall --prod >"${WORK_ROOT}/stopped-reinstall.out"
assert_installed_marker second
[[ "$(tail -n 2 "${CARGO_LOG}" | head -n 1)" == stop ]]

run_dev uninstall >"${WORK_ROOT}/uninstall.out"
for artifact in uc uc-daemon uc-session-host uc-mcp; do
    [[ ! -e "${INSTALL_BIN}/${artifact}" && ! -L "${INSTALL_BIN}/${artifact}" ]]
done
[[ "$(cat "${STATE_FILE}")" == preserve-me ]]
grep -Fq 'configuration, database, logs, and session state were preserved' "${WORK_ROOT}/uninstall.out"

run_dev uninstall >"${WORK_ROOT}/second-uninstall.out"
grep -Fq 'are not installed' "${WORK_ROOT}/second-uninstall.out"

ln -s -- "${WORK_ROOT}/foreign-uc" "${INSTALL_BIN}/uc"
if FAKE_BUILD_MARKER=third run_dev reinstall >"${WORK_ROOT}/symlink.out" 2>"${WORK_ROOT}/symlink.err"; then
    printf '%s\n' 'reinstall unexpectedly replaced a symbolic-link destination' >&2
    exit 1
fi
[[ -L "${INSTALL_BIN}/uc" ]]
rm -f -- "${INSTALL_BIN}/uc"

install -m 0755 -- "${WORK_ROOT}/sleeper" "${INSTALL_BIN}/uc-daemon"
"${INSTALL_BIN}/uc-daemon" 30 &
running_pid="$!"
sleep 0.1
if run_dev uninstall >"${WORK_ROOT}/running.out" 2>"${WORK_ROOT}/running.err"; then
    printf '%s\n' 'uninstall unexpectedly removed a running installed executable' >&2
    exit 1
fi
grep -Fq "uc-daemon is still running as PID ${running_pid}" "${WORK_ROOT}/running.err"
kill "${running_pid}"
wait "${running_pid}" || true
running_pid=""
run_dev uninstall >"${WORK_ROOT}/stopped-uninstall.out"
[[ ! -e "${INSTALL_BIN}/uc-daemon" ]]

# Exercise host admission independently of the machine running this suite.
cat >"${FAKE_BIN}/uname" <<'FAKE_UNAME'
#!/usr/bin/env bash
case "$1" in
    -s) printf '%s\n' "${FAKE_OS}" ;;
    -m) printf '%s\n' "${FAKE_ARCH}" ;;
esac
FAKE_UNAME
cat >"${FAKE_BIN}/getconf" <<'FAKE_GETCONF'
#!/usr/bin/env bash
printf '%s\n' "${FAKE_LIBC:-glibc 2.39}"
FAKE_GETCONF
# No installed binaries are running during the platform matrix.
cat >"${FAKE_BIN}/lsof" <<'FAKE_LSOF'
#!/usr/bin/env bash
if [[ "${FAKE_LSOF_FAIL:-false}" == true ]]; then
    printf '%s\n' 'process inspection failed' >&2
    exit 1
fi
if [[ -n "${FAKE_LSOF_MODE:-}" ]]; then
    count=0
    [[ ! -f "${FAKE_LSOF_STATE}" ]] || read -r count <"${FAKE_LSOF_STATE}"
    count=$((count + 1))
    printf '%s\n' "${count}" >"${FAKE_LSOF_STATE}"
    if [[ "${FAKE_LSOF_MODE}" == transient-then-running ]]; then
        [[ "${count}" != 1 ]] || exit 0
        printf '%s\n' p424242
        exit 0
    fi
    [[ "${count}" -gt 2 ]] || exit 0
fi
exit 1
FAKE_LSOF
chmod 755 "${FAKE_BIN}/uname" "${FAKE_BIN}/getconf" "${FAKE_BIN}/lsof"
for host in Linux:x86_64 Darwin:arm64 Darwin:x86_64; do
    export FAKE_OS="${host%%:*}" FAKE_ARCH="${host#*:}"
    run_dev install --prod >"${WORK_ROOT}/matrix-install.out"
    assert_installed_marker first
    if [[ "${FAKE_OS}" == Darwin ]]; then
        if FAKE_LSOF_FAIL=true run_dev uninstall >"${WORK_ROOT}/inspection.out" 2>"${WORK_ROOT}/inspection.err"; then
            printf '%s\n' 'uninstall ignored failed process inspection' >&2
            exit 1
        fi
        assert_installed_marker first
        grep -Fq 'could not verify that uc is stopped' "${WORK_ROOT}/inspection.err"
        rm -f -- "${WORK_ROOT}/lsof-attempts"
        if FAKE_LSOF_MODE=transient-then-running run_dev uninstall >"${WORK_ROOT}/retry-running.out" 2>"${WORK_ROOT}/retry-running.err"; then
            printf '%s\n' 'uninstall ignored a running executable after inspection retry' >&2
            exit 1
        fi
        grep -Fq 'uc is still running as PID 424242' "${WORK_ROOT}/retry-running.err"
        [[ "$(cat "${WORK_ROOT}/lsof-attempts")" == 2 ]]
        assert_installed_marker first
        rm -f -- "${WORK_ROOT}/lsof-attempts"
        FAKE_LSOF_MODE=transient-empty run_dev uninstall >"${WORK_ROOT}/retry-stopped.out"
        [[ "$(cat "${WORK_ROOT}/lsof-attempts")" -ge 3 ]]
    fi
    run_dev uninstall >"${WORK_ROOT}/matrix-uninstall.out"
done
for host in Linux:aarch64 Darwin:ppc64 FreeBSD:x86_64; do
    export FAKE_OS="${host%%:*}" FAKE_ARCH="${host#*:}"
    if run_dev install --prod >"${WORK_ROOT}/unsupported.out" 2>"${WORK_ROOT}/unsupported.err"; then
        printf 'unsupported host accepted: %s\n' "${host}" >&2
        exit 1
    fi
    grep -Fq 'supported development install hosts' "${WORK_ROOT}/unsupported.err"
done
export FAKE_OS=Linux FAKE_ARCH=x86_64
if FAKE_LIBC=musl run_dev install --prod >"${WORK_ROOT}/libc.out" 2>"${WORK_ROOT}/libc.err"; then
    printf '%s\n' 'non-GNU Linux host accepted' >&2
    exit 1
fi
grep -Fq 'requires GNU libc' "${WORK_ROOT}/libc.err"

printf '%s\n' 'Undercurrent developer installer tests passed'
