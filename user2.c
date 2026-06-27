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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <arpa/inet.h>
#include "ksocket.h"

#define CHUNK_SIZE  512

extern int my_errno;

int main(int argc, char *argv[])
{
    if (argc != 6) {
        printf("Usage: %s <src_ip> <src_port> <dst_ip> <dst_port> <output_file>\n", argv[0]);
        return 1;
    }

    const char *src_ip    = argv[1];
    int         src_port  = atoi(argv[2]);
    const char *dst_ip    = argv[3];
    int         dst_port  = atoi(argv[4]);
    const char *save_file = argv[5];

    if (src_port <= 0 || src_port > 65535) {
        fprintf(stderr, "[user2] ERROR: invalid src_port '%s'.\n", argv[2]);
        return 1;
    }
    if (dst_port <= 0 || dst_port > 65535) {
        fprintf(stderr, "[user2] ERROR: invalid dst_port '%s'.\n", argv[4]);
        return 1;
    }

    printf("=== User 2 (Receiver) starting ===\n");
    printf("[user2] src=%s:%d  dst=%s:%d  file=%s\n",
           src_ip, src_port, dst_ip, dst_port, save_file);

    int kfd = k_socket(AF_INET, SOCK_KTP, 0);
    if (kfd < 0) {
        fprintf(stderr, "[user2] FATAL: k_socket() failed.\n");
        return 1;
    }
    printf("[user2] KTP socket id = %d\n", kfd);

    struct sockaddr_in src, dst;
    memset(&src, 0, sizeof(src));
    memset(&dst, 0, sizeof(dst));

    src.sin_family = AF_INET;
    src.sin_port   = htons((uint16_t)src_port);
    if (inet_pton(AF_INET, src_ip, &src.sin_addr) != 1) {
        fprintf(stderr, "[user2] FATAL: invalid src_ip '%s'.\n", src_ip);
        return 1;
    }

    dst.sin_family = AF_INET;
    dst.sin_port   = htons((uint16_t)dst_port);
    if (inet_pton(AF_INET, dst_ip, &dst.sin_addr) != 1) {
        fprintf(stderr, "[user2] FATAL: invalid dst_ip '%s'.\n", dst_ip);
        return 1;
    }

    printf("[user2] Binding KTP socket (local=%s:%d, peer=%s:%d)...\n",
           src_ip, src_port, dst_ip, dst_port);
    if (k_bind(kfd,
               (struct sockaddr *)&src, sizeof(src),
               (struct sockaddr *)&dst, sizeof(dst)) < 0)
    {
        fprintf(stderr, "[user2] FATAL: k_bind() failed.\n");
        return 1;
    }
    printf("[user2] Bind successful.\n");

    int fd = open(save_file, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (fd < 0) {
        fprintf(stderr, "[user2] FATAL: cannot open '%s': ", save_file);
        perror("");
        return 1;
    }

    char               msgbuf[CHUNK_SIZE + 1];
    struct sockaddr_in sender_addr;
    socklen_t          sender_len    = sizeof(sender_addr);
    int                total_received = 0;
    int                chunk_num      = 1;
    int                waiting_printed = 0;

    printf("[user2] Waiting for file transfer...\n");

    while (1)
    {
        int n = k_recvfrom(kfd, msgbuf, CHUNK_SIZE, 0,
                           (struct sockaddr *)&sender_addr, &sender_len);

        if (n > 0)
        {
            msgbuf[n] = '\0';

            if (strcmp(msgbuf, "<EOF>") == 0) {
                printf("[user2] EOF marker received. Transfer complete.\n");
                break;
            }

            printf("[user2] Chunk %d received (%d bytes).\n", chunk_num, n);
            write(fd, msgbuf, n);
            total_received += n;
            chunk_num++;
            waiting_printed = 0;
        }
        else if (n < 0)
        {
            if (my_errno == ENOMESSAGE) {
                if (!waiting_printed) {
                    printf("[user2] No message yet — polling...\n");
                    waiting_printed = 1;
                }
                usleep(10000); // 10 ms
            } else {
                fprintf(stderr,
                        "[user2] Unexpected error from k_recvfrom() "
                        "(my_errno=%d).\n", my_errno);
                break;
            }
        }
    }

    printf("[user2] Total bytes written to '%s': %d\n",
           save_file, total_received);

    close(fd);
    k_close(kfd);

    printf("=== User 2 (Receiver) done ===\n");
    return 0;
}