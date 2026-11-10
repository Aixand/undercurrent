// SPDX-License-Identifier: Apache-2.0
// Compile the production adapter against deterministic Darwin API responses.
#define proc_pidinfo fixture_pidinfo
#define proc_pidfdinfo fixture_pidfdinfo
#include "../src/listeners.c"
#include <assert.h>

static int descriptor_count;
static int mode;
static int snapshots;
static int socket_reads;
static int max_buffer;

int fixture_pidinfo(int pid, int flavor, uint64_t arg, void *buffer, int size) {
    (void)pid; (void)flavor; (void)arg;
    snapshots++;
    if (size > max_buffer) max_buffer = size;
    if (mode == 1) return -1; // Permission failure.
    if (mode == 2) return 1; // Malformed byte count.
    if (mode == 3) return size + 1; // Invalid kernel response.
    int count = descriptor_count;
    int capacity = size / (int)sizeof(struct proc_fdinfo);
    if (count > capacity) count = capacity;
    struct proc_fdinfo *fds = buffer;
    for (int i = 0; i < count; i++) {
        fds[i].proc_fd = i;
        fds[i].proc_fdtype = i == descriptor_count - 1 ? PROX_FDTYPE_SOCKET : PROX_FDTYPE_VNODE;
    }
    if (mode == 6 && socket_reads) fds[0].proc_fd++; // Snapshot changed.
    return count * (int)sizeof(*fds);
}

int fixture_pidfdinfo(int pid, int fd, int flavor, void *buffer, int size) {
    (void)pid; (void)fd; (void)flavor;
    socket_reads++;
    assert(size == sizeof(struct socket_fdinfo));
    if (mode == 4) return -1; // Socket closed or access revoked.
    struct socket_fdinfo *info = buffer;
    memset(info, 0, sizeof(*info));
    info->psi.soi_kind = SOCKINFO_TCP;
    info->psi.soi_type = SOCK_STREAM;
    info->psi.soi_protocol = IPPROTO_TCP;
    info->psi.soi_family = AF_INET;
    info->psi.soi_proto.pri_tcp.tcpsi_state = TSI_S_LISTEN;
    info->psi.soi_so = mode == 5 ? socket_reads : 1; // Reused descriptor.
    if (mode == 7 && socket_reads == 2) info->psi.soi_proto.pri_tcp.tcpsi_state = 0;
    return size;
}

static void check(int count, int failure, int expected) {
    descriptor_count = count;
    mode = failure;
    snapshots = socket_reads = max_buffer = 0;
    assert(uc_macos_tcp_listener(123) == expected);
    assert(max_buffer <= (UC_FD_LIMIT + 1) * (int)sizeof(struct proc_fdinfo));
    assert(snapshots <= 6); // Five growth steps plus final validation.
}

int main(void) {
    check(256, 0, 1);
    check(257, 0, 1);
    check(700, 0, 1);
    check(4096, 0, 1);
    check(4097, 0, 2);
    assert(socket_reads == 0); // Truncated evidence must never certify a socket.
    for (int failure = 1; failure <= 7; failure++) check(700, failure, -1);
    return 0;
}
