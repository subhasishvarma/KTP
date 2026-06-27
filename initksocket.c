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
#include <signal.h>
#include <errno.h>

KTPSocket *SM      = NULL;
int        g_shmid = -1;

static void handle_signal(int sig)
{
    printf("\n[initksocket] Caught signal %d — cleaning up shared memory...\n", sig);
    if (SM != NULL)   shmdt(SM);
    if (g_shmid >= 0) shmctl(g_shmid, IPC_RMID, NULL);
    printf("[initksocket] Shared memory removed. Exiting.\n");
    exit(0);
}

int dropMessage(float p)
{
    return ((float)rand() / RAND_MAX) < p ? 1 : 0;
}

void *thread_R(void *arg)
{
    fd_set         rfds;
    struct timeval tv;

    while (1)
    {
        /* Step 1: close UDP sockets whose TIME_WAIT linger (2*T) has expired */
        time_t now_ts = time(NULL);
        for (int i = 0; i < MAX_SOCKETS; i++)
        {
            pthread_mutex_lock(&SM[i].lock);
            if (!SM[i].is_free && SM[i].close_ts > 0 && SM[i].udp_fd >= 0)
            {
                if (now_ts - SM[i].close_ts >= 2 * T)
                {
                    printf("[Thread R] KTP %d: TIME_WAIT expired. Closing UDP fd=%d and freeing slot.\n",
                           i, SM[i].udp_fd);
                    close(SM[i].udp_fd);
                    SM[i].udp_fd     = -1;
                    SM[i].local_port =  0;
                    SM[i].close_ts   =  0;
                    SM[i].peer_port  =  0;
                    memset(SM[i].peer_ip, 0, INET_ADDRSTRLEN);
                    SM[i].is_free    = true;
                }
            }
            pthread_mutex_unlock(&SM[i].lock);
        }

        /* Step 2: create and bind UDP sockets for pending k_bind() requests */
        for (int i = 0; i < MAX_SOCKETS; i++)
        {
            if (!SM[i].is_free && SM[i].udp_fd == -1
                && SM[i].local_port != 0 && SM[i].close_ts == 0)
            {
                pthread_mutex_lock(&SM[i].lock);
                int ufd = socket(AF_INET, SOCK_DGRAM, 0);
                if (ufd >= 0)
                {
                    struct sockaddr_in la;
                    memset(&la, 0, sizeof(la));
                    la.sin_family      = AF_INET;
                    la.sin_addr.s_addr = INADDR_ANY;
                    la.sin_port        = htons(SM[i].local_port);

                    if (bind(ufd, (struct sockaddr *)&la, sizeof(la)) == 0)
                    {
                        SM[i].udp_fd = ufd;
                        printf("[Thread R] KTP %d: UDP fd=%d bound on port %d.\n",
                               i, ufd, SM[i].local_port);
                    }
                    else
                    {
                        perror("[Thread R] UDP bind failed");
                        close(ufd);
                        SM[i].is_free    = true; /* signal k_bind() that bind failed */
                        SM[i].local_port = 0;
                    }
                }
                else
                {
                    perror("[Thread R] socket() failed");
                }
                pthread_mutex_unlock(&SM[i].lock);
            }
        }

        /* Step 3: build select() read-set — include active and TIME_WAIT sockets */
        FD_ZERO(&rfds);
        int max_fd = 0;
        tv.tv_sec  = T;
        tv.tv_usec = 0;

        for (int i = 0; i < MAX_SOCKETS; i++)
        {
            int fd = SM[i].udp_fd;
            if (fd >= 0 && (!SM[i].is_free || SM[i].close_ts > 0))
            {
                FD_SET(fd, &rfds);
                if (fd > max_fd) max_fd = fd;
            }
        }

        int ready = select(max_fd + 1, &rfds, NULL, NULL, &tv);

        if (ready < 0) {
            perror("[Thread R] select() error");
            break;
        }

        /* Timeout: send beacon ACK if receiver buffer has freed up space */
        if (ready == 0)
        {
            for (int i = 0; i < MAX_SOCKETS; i++)
            {
                if (SM[i].is_free || SM[i].udp_fd < 0) continue;
                pthread_mutex_lock(&SM[i].lock);

                if (SM[i].nospace)
                {
                    int free_slots = MSG_BUF_SLOTS - SM[i].rbuf_total;
                    if (free_slots > 0)
                    {
                        struct sockaddr_in pa;
                        memset(&pa, 0, sizeof(pa));
                        pa.sin_family = AF_INET;
                        pa.sin_port   = htons((uint16_t)SM[i].peer_port);
                        inet_pton(AF_INET, SM[i].peer_ip, &pa.sin_addr);

                        KTPHeader beacon = {
                            .pkt_type = 'A',
                            .seq      = (uint8_t)(SM[i].rwnd.expected_seq - 1),
                            .rwnd_sz  = free_slots
                        };
                        sendto(SM[i].udp_fd, &beacon, sizeof(beacon), 0,
                               (struct sockaddr *)&pa, sizeof(pa));
                        printf("[Thread R] KTP %d: nospace beacon ACK seq=%d rwnd=%d sent.\n",
                               i, beacon.seq, free_slots);
                        /* nospace NOT cleared here — if this ACK is lost sender would deadlock */
                    }
                }
                pthread_mutex_unlock(&SM[i].lock);
            }
            continue;
        }

        /* Process each socket that has a pending datagram */
        for (int i = 0; i < MAX_SOCKETS; i++)
        {
            int fd = SM[i].udp_fd;
            if (fd < 0 || (SM[i].is_free && SM[i].close_ts == 0)) continue;
            if (!FD_ISSET(fd, &rfds)) continue;

            char              pktbuf[MAX_PKT_SIZE];
            struct sockaddr_in sender;
            socklen_t          sender_len = sizeof(sender);

            int n = recvfrom(fd, pktbuf, MAX_PKT_SIZE, 0,
                             (struct sockaddr *)&sender, &sender_len);
            if (n <= 0) continue;

            /* Simulate network packet loss */
            if (dropMessage(DROP_PROB)) {
                printf("[Thread R] KTP %d: *** PACKET DROPPED (simulated loss) ***\n", i);
                continue;
            }

            KTPHeader *hdr     = (KTPHeader *)pktbuf;
            char      *payload = pktbuf + sizeof(KTPHeader);
            int        plen    = n - (int)sizeof(KTPHeader);

            pthread_mutex_lock(&SM[i].lock);

            if (hdr->pkt_type == 'D')
            {
                printf("[Thread R] KTP %d: received DATA seq=%d (expected=%d)\n",
                       i, hdr->seq, SM[i].rwnd.expected_seq);

                uint8_t expected   = SM[i].rwnd.expected_seq;
                uint8_t offset     = hdr->seq - expected; /* 8-bit wrap handles sequence rollover */
                int     free_slots = MSG_BUF_SLOTS - SM[i].rbuf_total;

                if (offset < (uint8_t)free_slots)
                {
                    int slot = (SM[i].rbuf_in + (int)offset) % MSG_BUF_SLOTS;

                    if (!SM[i].rbuf_valid[slot])
                    {
                        memset(SM[i].rbuf[slot], 0, MAX_MSG_SIZE);
                        memcpy(SM[i].rbuf[slot], payload,
                               plen > MAX_MSG_SIZE ? MAX_MSG_SIZE : plen);
                        SM[i].rbuf_msg_len[slot] = hdr->msg_len;
                        SM[i].rbuf_valid[slot]   = true;
                        SM[i].rbuf_total++;

                        if (offset == 0)
                        {
                            /* In-order: slide window forward over any contiguous buffered packets */
                            while (SM[i].rbuf_valid[SM[i].rbuf_in] &&
                                   SM[i].rbuf_ready < MSG_BUF_SLOTS)
                            {
                                SM[i].rwnd.expected_seq++;
                                SM[i].rbuf_in = (SM[i].rbuf_in + 1) % MSG_BUF_SLOTS;
                                SM[i].rbuf_ready++;
                            }

                            free_slots = MSG_BUF_SLOTS - SM[i].rbuf_total;
                            SM[i].nospace = (free_slots == 0);

                            KTPHeader ack = {
                                .pkt_type = 'A',
                                .seq      = (uint8_t)(SM[i].rwnd.expected_seq - 1),
                                .rwnd_sz  = free_slots
                            };
                            printf("[Thread R] KTP %d: sending ACK seq=%d rwnd=%d\n",
                                   i, ack.seq, free_slots);
                            sendto(fd, &ack, sizeof(ack), 0,
                                   (struct sockaddr *)&sender, sender_len);
                        }
                        else
                        {
                            /* Out-of-order: buffer silently, no ACK sent */
                            printf("[Thread R] KTP %d: buffered OUT-OF-ORDER seq=%d (expected=%d) — no ACK sent.\n",
                                   i, hdr->seq, expected);
                        }
                    }
                    else
                    {
                        /* Duplicate within window: re-ACK to keep sender updated */
                        printf("[Thread R] KTP %d: duplicate seq=%d within window — sending ACK.\n",
                               i, hdr->seq);
                        free_slots = MSG_BUF_SLOTS - SM[i].rbuf_total;
                        KTPHeader ack = {
                            .pkt_type = 'A',
                            .seq      = (uint8_t)(expected - 1),
                            .rwnd_sz  = free_slots
                        };
                        sendto(fd, &ack, sizeof(ack), 0,
                               (struct sockaddr *)&sender, sender_len);
                    }
                }
                else if (offset > 128)
                {
                    /* offset > 128 in 8-bit arithmetic means seq is behind expected (old retransmit) */
                    free_slots = MSG_BUF_SLOTS - SM[i].rbuf_total;
                    KTPHeader ack = {
                        .pkt_type = 'A',
                        .seq      = (uint8_t)(expected - 1),
                        .rwnd_sz  = free_slots
                    };
                    printf("[Thread R] KTP %d: old duplicate seq=%d (expected=%d) — re-ACKing.\n",
                           i, hdr->seq, expected);
                    sendto(fd, &ack, sizeof(ack), 0,
                           (struct sockaddr *)&sender, sender_len);
                }
                else
                {
                    printf("[Thread R] KTP %d: seq=%d outside window (free_slots=%d) — dropped.\n",
                           i, hdr->seq, free_slots);
                }
            }
            else if (hdr->pkt_type == 'A')
            {
                printf("[Thread R] KTP %d: received ACK seq=%d rwnd=%d\n",
                       i, hdr->seq, hdr->rwnd_sz);

                uint8_t base   = SM[i].swnd.base_seq;
                uint8_t offset = hdr->seq - base; /* distance from oldest unACKed seq */

                if ((int)offset < SM[i].sbuf_count)
                {
                    /* New ACK: slide sender window forward */
                    int newly_acked = (int)offset + 1;
                    SM[i].sbuf_count -= newly_acked;
                    if (SM[i].sbuf_count < 0) SM[i].sbuf_count = 0;
                    SM[i].swnd.base_seq = hdr->seq + 1;
                    SM[i].swnd.win_size = hdr->rwnd_sz;
                    printf("[Thread R] KTP %d: %d msg(s) newly ACKed. sbuf_count=%d swnd.win_size=%d\n",
                           i, newly_acked, SM[i].sbuf_count, SM[i].swnd.win_size);
                }
                else
                {
                    /* Duplicate ACK: only update window size (e.g. beacon ACK from receiver) */
                    SM[i].swnd.win_size = hdr->rwnd_sz;
                    printf("[Thread R] KTP %d: duplicate ACK — updated swnd.win_size to %d.\n",
                           i, hdr->rwnd_sz);
                }
            }

            pthread_mutex_unlock(&SM[i].lock);
        }
    }
    return NULL;
}

void *thread_S(void *arg)
{
    while (1)
    {
        usleep((T * 1000000) / 2); /* sleep T/2 seconds between scans */
        time_t now = time(NULL);

        for (int i = 0; i < MAX_SOCKETS; i++)
        {
            if (SM[i].is_free || SM[i].udp_fd < 0 || SM[i].close_ts > 0) continue;

            pthread_mutex_lock(&SM[i].lock);

            uint8_t in_flight = SM[i].swnd.next_seq - SM[i].swnd.base_seq;

            if (in_flight > 0 && (now - SM[i].last_sent_ts) >= T)
            {
                printf("[Thread S] KTP %d: TIMEOUT — rewinding to seq=%d for retransmission.\n",
                       i, SM[i].swnd.base_seq);
                SM[i].swnd.next_seq = SM[i].swnd.base_seq; /* go-back-N: retransmit entire window */
            }

            /* oldest unACKed slot in the circular send buffer */
            int     oldest_slot = (SM[i].sbuf_write - SM[i].sbuf_count + MSG_BUF_SLOTS) % MSG_BUF_SLOTS;
            uint8_t offset      = SM[i].swnd.next_seq - SM[i].swnd.base_seq;

            while ((int)offset < SM[i].sbuf_count
                   && (int)offset < SM[i].swnd.win_size
                   && SM[i].swnd.win_size > 0)
            {
                int slot = (oldest_slot + (int)offset) % MSG_BUF_SLOTS;

                char      pktbuf[MAX_PKT_SIZE];
                KTPHeader hdr = {
                    .pkt_type = 'D',
                    .seq      = SM[i].swnd.next_seq,
                    .rwnd_sz  = 0,
                    .msg_len  = (uint16_t)SM[i].sbuf_msg_len[slot]
                };
                memcpy(pktbuf, &hdr, sizeof(hdr));
                memcpy(pktbuf + sizeof(hdr), SM[i].sbuf[slot], MAX_MSG_SIZE);

                struct sockaddr_in dst;
                memset(&dst, 0, sizeof(dst));
                dst.sin_family = AF_INET;
                dst.sin_port   = htons((uint16_t)SM[i].peer_port);
                inet_pton(AF_INET, SM[i].peer_ip, &dst.sin_addr);

                sendto(SM[i].udp_fd, pktbuf, MAX_PKT_SIZE, 0,
                       (struct sockaddr *)&dst, sizeof(dst));

                SM[i].total_transmissions++;
                printf("[Thread S] KTP %d: sent DATA seq=%d (buf_slot=%d) [total_tx=%d]\n",
                       i, hdr.seq, slot, SM[i].total_transmissions);

                if (offset == 0) SM[i].last_sent_ts = now; /* reset timer only for oldest unACKed msg */

                SM[i].swnd.next_seq++;
                offset = SM[i].swnd.next_seq - SM[i].swnd.base_seq;
            }

            pthread_mutex_unlock(&SM[i].lock);
        }
    }
    return NULL;
}

void *thread_GC(void *arg)
{
    while (1)
    {
        sleep(10);
        printf("[GC] Running periodic garbage-collection pass...\n");

        for (int i = 0; i < MAX_SOCKETS; i++)
        {
            if (SM[i].is_free || SM[i].owner_pid <= 0) continue;

            if (kill(SM[i].owner_pid, 0) == -1) /* returns -1 if process no longer exists */
            {
                pthread_mutex_lock(&SM[i].lock);
                printf("[GC] KTP %d: owner PID %d is gone — reclaiming slot.\n",
                       i, SM[i].owner_pid);
                if (SM[i].udp_fd >= 0) {
                    close(SM[i].udp_fd);
                    SM[i].udp_fd = -1;
                }
                SM[i].is_free    = true;
                SM[i].owner_pid  = 0;
                SM[i].local_port = 0;
                pthread_mutex_unlock(&SM[i].lock);
            }
        }
    }
    return NULL;
}

int main(void)
{
    printf("[initksocket] Starting KTP socket daemon...\n");

    srand((unsigned int)(time(NULL) ^ (unsigned int)getpid())); /* seed RNG so each run gets different drops */
    printf("[initksocket] Random seed initialised (time ^ pid).\n");

    signal(SIGINT,  handle_signal);
    signal(SIGTERM, handle_signal);

    key_t key = ftok(FTOK_PATH, FTOK_ID);
    if (key < 0) { perror("[initksocket] ftok"); return 1; }

    size_t shm_size = sizeof(KTPSocket) * MAX_SOCKETS;
    printf("[initksocket] Shared memory size requested: %zu bytes.\n", shm_size);

    g_shmid = shmget(key, shm_size, IPC_CREAT | IPC_EXCL | 0666);
    if (g_shmid < 0)
    {
        if (errno == EEXIST)
        {
            /* Stale segment from a previous run — delete and recreate */
            printf("[initksocket] Stale shared memory segment found. Removing and recreating...\n");
            int old_id = shmget(key, 0, 0666); /* size=0 just fetches the existing id */
            if (old_id >= 0) shmctl(old_id, IPC_RMID, NULL);
            g_shmid = shmget(key, shm_size, IPC_CREAT | IPC_EXCL | 0666);
        }
        if (g_shmid < 0) { perror("[initksocket] shmget"); return 1; }
    }

    SM = (KTPSocket *)shmat(g_shmid, NULL, 0);
    if (SM == (void *)-1) { perror("[initksocket] shmat"); return 1; }

    pthread_mutexattr_t mattr;
    pthread_mutexattr_init(&mattr);
    pthread_mutexattr_setpshared(&mattr, PTHREAD_PROCESS_SHARED); /* mutex must be visible across processes */

    for (int i = 0; i < MAX_SOCKETS; i++)
    {
        SM[i].is_free             = true;
        SM[i].udp_fd              = -1;
        SM[i].local_port          = 0;
        SM[i].close_ts            = 0;
        SM[i].total_transmissions = 0;
        for (int j = 0; j < MSG_BUF_SLOTS; j++) {
            SM[i].sbuf_msg_len[j] = 0;
            SM[i].rbuf_msg_len[j] = 0;
            SM[i].rbuf_valid[j]   = false;
        }
        pthread_mutex_init(&SM[i].lock, &mattr);
    }
    pthread_mutexattr_destroy(&mattr);

    printf("[initksocket] Shared memory initialised (%d socket slots, each slot = %zu bytes).\n",
           MAX_SOCKETS, sizeof(KTPSocket));

    pthread_t tR, tS, tGC;
    pthread_create(&tR,  NULL, thread_R,  NULL);
    pthread_create(&tS,  NULL, thread_S,  NULL);
    pthread_create(&tGC, NULL, thread_GC, NULL);

    printf("[initksocket] Threads R, S, and GC launched. Ready.\n");

    pthread_join(tR,  NULL);
    pthread_join(tS,  NULL);
    pthread_join(tGC, NULL);

    shmdt(SM);
    shmctl(g_shmid, IPC_RMID, NULL);
    return 0;
}