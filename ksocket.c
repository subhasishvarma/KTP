/*
=====================================================
Mini Project 1 Submission
Group Details :
Member 1 Name : M Subhasish Varma
Member 1 Roll number : 23CS30034
Member 2 Name : K Abhiram
Member 2 Roll number : 23CS10036
=====================================================
*/

#include "ksocket.h"
#include <sys/shm.h>
#include <arpa/inet.h>

KTPSocket *SM = NULL;
int my_errno = 0;

static int attach_shm(void)
{
    if (SM != NULL) return 0;

    key_t key   = ftok(FTOK_PATH, FTOK_ID);
    int   shmid = shmget(key, sizeof(KTPSocket) * MAX_SOCKETS, 0666);
    if (shmid < 0) {
        perror("[ksocket] shmget failed — is initksocket running?");
        return -1;
    }

    SM = (KTPSocket *)shmat(shmid, NULL, 0);
    if (SM == (void *)-1) {
        perror("[ksocket] shmat failed");
        SM = NULL;
        return -1;
    }
    return 0;
}

int k_socket(int domain, int type, int protocol)
{
    if (type != SOCK_KTP) {
        fprintf(stderr, "[k_socket] ERROR: type must be SOCK_KTP.\n");
        return -1;
    }
    if (attach_shm() < 0) return -1;

    for (int i = 0; i < MAX_SOCKETS; i++) {
        pthread_mutex_lock(&SM[i].lock);

        if (!SM[i].is_free) {
            pthread_mutex_unlock(&SM[i].lock);
            continue;
        }

        // Claim the slot
        SM[i].is_free    = false;
        SM[i].owner_pid  = getpid();
        SM[i].udp_fd     = -1;
        SM[i].local_port = 0;
        SM[i].peer_port  = 0;
        memset(SM[i].peer_ip, 0, INET_ADDRSTRLEN);

        SM[i].sbuf_write      = 0;
        SM[i].sbuf_count      = 0;
        SM[i].swnd.base_seq   = 1;
        SM[i].swnd.next_seq   = 1;
        SM[i].swnd.win_size   = MSG_BUF_SLOTS;
        SM[i].last_sent_ts    = 0;
        SM[i].close_ts        = 0;
        SM[i].total_transmissions = 0;

        SM[i].rbuf_in         = 0;
        SM[i].rbuf_out        = 0;
        SM[i].rbuf_ready      = 0;
        SM[i].rbuf_total      = 0;
        SM[i].nospace         = false;
        SM[i].rwnd.expected_seq = 1;
        for (int j = 0; j < MSG_BUF_SLOTS; j++) {
            SM[i].rbuf_valid[j]   = false;
            SM[i].sbuf_msg_len[j] = 0;
            SM[i].rbuf_msg_len[j] = 0;
        }

        pthread_mutex_unlock(&SM[i].lock);

        printf("[k_socket] PID %d: allocated KTP socket id=%d.\n",
               (int)getpid(), i);
        return i;
    }

    my_errno = ENOSPACE;
    fprintf(stderr, "[k_socket] ERROR: no free KTP socket slots (max=%d).\n",MAX_SOCKETS);
    return -1;
}

int k_bind(int kfd, const struct sockaddr *src, socklen_t src_len,const struct sockaddr *dst, socklen_t dst_len)
{
    if (attach_shm() < 0 || kfd < 0 || kfd >= MAX_SOCKETS) return -1;

    const struct sockaddr_in *s = (const struct sockaddr_in *)src;
    const struct sockaddr_in *d = (const struct sockaddr_in *)dst;

    pthread_mutex_lock(&SM[kfd].lock);
    SM[kfd].local_port = ntohs(s->sin_port);
    SM[kfd].peer_port  = ntohs(d->sin_port);
    inet_ntop(AF_INET, &d->sin_addr, SM[kfd].peer_ip, INET_ADDRSTRLEN);
    pthread_mutex_unlock(&SM[kfd].lock);

    printf("[k_bind] KTP %d: requesting bind — local_port=%d, peer=%s:%d\n",kfd, ntohs(s->sin_port), SM[kfd].peer_ip, SM[kfd].peer_port);
    printf("[k_bind] KTP %d: waiting for Thread R to bind UDP socket...\n", kfd);

    /* Poll until Thread R either creates the UDP socket (udp_fd >= 0)
     * or signals failure (local_port zeroed, is_free set back to true). */
    while (1) {
        pthread_mutex_lock(&SM[kfd].lock);

        if (SM[kfd].udp_fd >= 0) {
            /* Success: Thread R bound the UDP socket */
            pthread_mutex_unlock(&SM[kfd].lock);
            break;
        }

        if (SM[kfd].local_port == 0) {
            // Thread R signalled bind failure (port in use or similar)
            pthread_mutex_unlock(&SM[kfd].lock);
            fprintf(stderr,"[k_bind] ERROR: Thread R rejected UDP bind on KTP %d ""(port already in use?).\n", kfd);
            return -1;
        }

        pthread_mutex_unlock(&SM[kfd].lock);
        usleep(10000);
    }

    printf("[k_bind] KTP %d: UDP socket ready (fd=%d). Bind complete.\n",kfd, SM[kfd].udp_fd);
    return 0;
}

int k_sendto(int kfd, const void *msg, size_t len, int flags,const struct sockaddr *dst, socklen_t dst_len)
{
    if (attach_shm() < 0 || kfd < 0 || kfd >= MAX_SOCKETS) return -1;

    /* Verify that destination matches the bound peer */
    const struct sockaddr_in *d = (const struct sockaddr_in *)dst;
    char req_ip[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &d->sin_addr, req_ip, INET_ADDRSTRLEN);
    int req_port = ntohs(d->sin_port);

    pthread_mutex_lock(&SM[kfd].lock);

    if (strcmp(req_ip, SM[kfd].peer_ip) != 0 || req_port != SM[kfd].peer_port) {
        my_errno = ENOTBOUND;
        pthread_mutex_unlock(&SM[kfd].lock);
        fprintf(stderr,
                "[k_sendto] ERROR: KTP %d destination mismatch "
                "(requested %s:%d, bound to %s:%d).\n",
                kfd, req_ip, req_port, SM[kfd].peer_ip, SM[kfd].peer_port);
        return -1;
    }

    if (SM[kfd].sbuf_count >= MSG_BUF_SLOTS) {
        my_errno = ENOSPACE;
        pthread_mutex_unlock(&SM[kfd].lock);
        return -1;
    }

    // Copy into the next free send-buffer slot */
    int    slot     = SM[kfd].sbuf_write;
    size_t copy_len = (len > MAX_MSG_SIZE) ? MAX_MSG_SIZE : len;

    memset(SM[kfd].sbuf[slot], 0, MAX_MSG_SIZE);
    memcpy(SM[kfd].sbuf[slot], msg, copy_len);

    SM[kfd].sbuf_msg_len[slot] = copy_len; /* Track actual content length */
    SM[kfd].sbuf_write = (slot + 1) % MSG_BUF_SLOTS;
    SM[kfd].sbuf_count++;

    pthread_mutex_unlock(&SM[kfd].lock);
    return (int)copy_len;
}

int k_recvfrom(int kfd, void *buf, size_t len, int flags,struct sockaddr *src, socklen_t *src_len)
{
    if (attach_shm() < 0 || kfd < 0 || kfd >= MAX_SOCKETS) return -1;

    pthread_mutex_lock(&SM[kfd].lock);

    if (SM[kfd].rbuf_ready <= 0) {
        my_errno = ENOMESSAGE;
        pthread_mutex_unlock(&SM[kfd].lock);
        return -1;
    }

    int    slot     = SM[kfd].rbuf_out;
    size_t actual_len = SM[kfd].rbuf_msg_len[slot];
    size_t copy_len = (len > actual_len) ? actual_len : len;
    memcpy(buf, SM[kfd].rbuf[slot], copy_len);

    SM[kfd].rbuf_valid[slot] = false;
    memset(SM[kfd].rbuf[slot], 0, MAX_MSG_SIZE);
    SM[kfd].rbuf_msg_len[slot] = 0;
    SM[kfd].rbuf_out   = (slot + 1) % MSG_BUF_SLOTS;
    SM[kfd].rbuf_ready--;
    SM[kfd].rbuf_total--;

    if (src != NULL && src_len != NULL) {
        struct sockaddr_in *sa = (struct sockaddr_in *)src;
        sa->sin_family = AF_INET;
        sa->sin_port   = htons((uint16_t)SM[kfd].peer_port);
        inet_pton(AF_INET, SM[kfd].peer_ip, &sa->sin_addr);
        *src_len = sizeof(struct sockaddr_in);
    }

    pthread_mutex_unlock(&SM[kfd].lock);
    return (int)copy_len;
}

int k_close(int kfd)
{
    if (attach_shm() < 0 || kfd < 0 || kfd >= MAX_SOCKETS) return -1;

    printf("[k_close] KTP %d: waiting for send buffer to drain...\n", kfd);

    time_t last_print = 0;
    while (1) {
        pthread_mutex_lock(&SM[kfd].lock);
        int remaining = SM[kfd].sbuf_count;
        pthread_mutex_unlock(&SM[kfd].lock);

        if (remaining == 0) break;

        time_t now = time(NULL);
        if (now - last_print >= T) {
            printf("[k_close] KTP %d: %d message(s) still in flight "
                   "(waiting for ACK)...\n", kfd, remaining);
            last_print = now;
        }
        usleep(50000);
    }

    // Enter TIME_WAIT: setting close_ts to now.
    pthread_mutex_lock(&SM[kfd].lock);
    SM[kfd].owner_pid = 0;
    SM[kfd].close_ts  = time(NULL);
    pthread_mutex_unlock(&SM[kfd].lock);

    printf("[k_close] KTP %d: send buffer drained. "
           "Entering TIME_WAIT (%d s) for final ACK safety...\n", kfd, 2 * T);
    return 0;
}

int dropMessage(float p)
{
    return ((float)rand() / RAND_MAX) < p ? 1 : 0;
}