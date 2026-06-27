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
#include <sys/shm.h>
#include "ksocket.h"

#define CHUNK_SIZE  512

extern int my_errno;
extern KTPSocket *SM;

int main(int argc, char *argv[])
{
    if (argc != 6) {
        printf("Usage: %s <src_ip> <src_port> <dst_ip> <dst_port> <input_file>\n",argv[0]);
        return 1;
    }

    const char *src_ip    = argv[1];
    int         src_port  = atoi(argv[2]);
    const char *dst_ip    = argv[3];
    int         dst_port  = atoi(argv[4]);
    const char *send_file = argv[5];

    if (src_port <= 0 || src_port > 65535) {
        fprintf(stderr, "[user1] ERROR: invalid src_port '%s'.\n", argv[2]);
        return 1;
    }
    if (dst_port <= 0 || dst_port > 65535) {
        fprintf(stderr, "[user1] ERROR: invalid dst_port '%s'.\n", argv[4]);
        return 1;
    }

    printf("=== User 1 (Sender) starting ===\n");
    printf("[user1] src=%s:%d  dst=%s:%d  file=%s\n",
           src_ip, src_port, dst_ip, dst_port, send_file);

    int kfd = k_socket(AF_INET, SOCK_KTP, 0);
    if (kfd < 0) {
        fprintf(stderr, "[user1] FATAL: k_socket() failed.\n");
        return 1;
    }
    printf("[user1] KTP socket id = %d\n", kfd);

    struct sockaddr_in src, dst;
    memset(&src, 0, sizeof(src));
    memset(&dst, 0, sizeof(dst));

    src.sin_family = AF_INET;
    src.sin_port   = htons((uint16_t)src_port);
    if (inet_pton(AF_INET, src_ip, &src.sin_addr) != 1) {
        fprintf(stderr, "[user1] FATAL: invalid src_ip '%s'.\n", src_ip);
        return 1;
    }

    dst.sin_family = AF_INET;
    dst.sin_port   = htons((uint16_t)dst_port);
    if (inet_pton(AF_INET, dst_ip, &dst.sin_addr) != 1) {
        fprintf(stderr, "[user1] FATAL: invalid dst_ip '%s'.\n", dst_ip);
        return 1;
    }

    printf("[user1] Binding KTP socket (local=%s:%d, peer=%s:%d)...\n",
           src_ip, src_port, dst_ip, dst_port);
    if (k_bind(kfd,
               (struct sockaddr *)&src, sizeof(src),
               (struct sockaddr *)&dst, sizeof(dst)) < 0)
    {
        fprintf(stderr, "[user1] FATAL: k_bind() failed.\n");
        return 1;
    }
    printf("[user1] Bind successful.\n");

    int fd = open(send_file, O_RDONLY);
    if (fd < 0) {
        fprintf(stderr, "[user1] FATAL: cannot open '%s': ", send_file);
        perror("");
        return 1;
    }

    // Sending the file in CHUNK_SIZE blocks
    char    chunk[CHUNK_SIZE];
    ssize_t bytes_read;
    int     total_buffered   = 0;
    int     chunk_num        = 1;
    int     full_msg_printed = 0;

    printf("[user1] Starting file transfer from '%s'...\n", send_file);

    while ((bytes_read = read(fd, chunk, CHUNK_SIZE)) > 0)
    {
        int result = -1;
        while (result < 0)
        {
            result = k_sendto(kfd, chunk, (size_t)bytes_read, 0,
                              (struct sockaddr *)&dst, sizeof(dst));
            if (result < 0) {
                if (my_errno == ENOSPACE) {
                    if (!full_msg_printed) {
                        printf("[user1] Send buffer full — waiting for ACKs "
                               "(chunk %d)...\n", chunk_num);
                        full_msg_printed = 1;
                    }
                    usleep(10000); // 10 ms
                } else if (my_errno == ENOTBOUND) {
                    fprintf(stderr,
                            "[user1] FATAL: ENOTBOUND on chunk %d.\n",
                            chunk_num);
                    close(fd);
                    return 1;
                }
            }
        }

        printf("[user1] Chunk %d buffered (%d bytes).\n", chunk_num, result);
        full_msg_printed = 0;
        total_buffered  += result;
        chunk_num++;
    }

    close(fd);
    printf("[user1] All %d chunks buffered (%d bytes total).\n",
           chunk_num - 1, total_buffered);

    //Sending EOF marker
    const char eof_marker[] = "<EOF>";
    int result = -1;
    while (result < 0) {
        result = k_sendto(kfd, eof_marker, strlen(eof_marker), 0,
                          (struct sockaddr *)&dst, sizeof(dst));
        if (result < 0) usleep(10000);
    }
    printf("[user1] EOF marker sent.\n");

    printf("[user1] Closing socket (waiting for final ACKs)...\n");

    k_close(kfd);

    int    total_tx   = SM[kfd].total_transmissions;
    int    total_msgs = (chunk_num - 1) + 1;   // data chunks + EOF
    double avg_tx     = (total_msgs > 0) ? (double)total_tx / total_msgs : 0.0;

    printf("\n");
    printf("  TRANSMISSION STATISTICS (p=%.2f)\n", DROP_PROB);
    printf("  Messages generated (data + EOF) : %d\n",  total_msgs);
    printf("  Total transmissions (incl. retx): %d\n",  total_tx);
    printf("  Avg transmissions per message   : %.4f\n", avg_tx);

    printf("User 1 (Sender) done. Total bytes buffered: %d.\n",total_buffered);
    return 0;
}