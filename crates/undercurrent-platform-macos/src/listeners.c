// SPDX-License-Identifier: Apache-2.0
// Keep SDK-owned socket layouts in C. No pointers or kernel addresses escape.
#include <libproc.h>
#include <sys/proc_info.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <string.h>
#include <stdlib.h>

// Geometric growth bounds allocation and retries; one extra slot detects overflow.
#define UC_FD_INITIAL 256
#define UC_FD_LIMIT 4096

// -1 unavailable, 0 absent, 1 present, 2 truncated.
static int inspect_snapshot(int pid, struct proc_fdinfo *before,
                            struct proc_fdinfo *after, int bytes, int capacity) {
    int count = bytes / (int)sizeof(struct proc_fdinfo);
    int present = 0;
    for (int i = 0; i < count; i++) {
        if (before[i].proc_fd < 0) return -1;
        if (before[i].proc_fdtype != PROX_FDTYPE_SOCKET) continue;
        struct socket_fdinfo info = {0};
        int n = proc_pidfdinfo(pid, before[i].proc_fd, PROC_PIDFDSOCKETINFO,
                              &info, sizeof(info));
        if (n != (int)sizeof(info)) return -1;
        if (info.psi.soi_kind == SOCKINFO_TCP &&
            info.psi.soi_type == SOCK_STREAM && info.psi.soi_protocol == IPPROTO_TCP &&
            (info.psi.soi_family == AF_INET || info.psi.soi_family == AF_INET6) &&
            info.psi.soi_proto.pri_tcp.tcpsi_state == TSI_S_LISTEN) present = 1;
        // Re-read socket identity/state; descriptor reuse must not certify a listener.
        struct socket_fdinfo verify = {0};
        n = proc_pidfdinfo(pid, before[i].proc_fd, PROC_PIDFDSOCKETINFO,
                          &verify, sizeof(verify));
        if (n != (int)sizeof(verify) || verify.psi.soi_so != info.psi.soi_so ||
            verify.psi.soi_kind != info.psi.soi_kind ||
            (info.psi.soi_kind == SOCKINFO_TCP &&
             verify.psi.soi_proto.pri_tcp.tcpsi_state != info.psi.soi_proto.pri_tcp.tcpsi_state)) return -1;
    }
    int final_bytes = proc_pidinfo(pid, PROC_PIDLISTFDS, 0, after, capacity * (int)sizeof(*after));
    if (final_bytes != bytes || memcmp(before, after, (size_t)bytes) != 0) return -1;
    return present;
}

int uc_macos_tcp_listener(int pid) {
    for (int limit = UC_FD_INITIAL; limit <= UC_FD_LIMIT; limit *= 2) {
        int capacity = limit + 1;
        int size = capacity * (int)sizeof(struct proc_fdinfo);
        struct proc_fdinfo *before = calloc((size_t)capacity, sizeof(*before));
        struct proc_fdinfo *after = calloc((size_t)capacity, sizeof(*after));
        if (!before || !after) {
            free(before);
            free(after);
            return -1;
        }
        int bytes = proc_pidinfo(pid, PROC_PIDLISTFDS, 0, before, size);
        int result = -1;
        if (bytes > 0 && bytes <= size && bytes % (int)sizeof(*before) == 0) {
            if (bytes / (int)sizeof(*before) > limit) {
                result = 2;
            } else {
                result = inspect_snapshot(pid, before, after, bytes, capacity);
            }
        }
        free(before);
        free(after);
        if (result != 2 || limit == UC_FD_LIMIT) return result;
    }
    return 2;
}
