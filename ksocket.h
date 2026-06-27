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

#ifndef KSOCKET_H
#define KSOCKET_H

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <pthread.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <netinet/in.h>

 // Tunable parameters 
#define T           5       
#define DROP_PROB   0.05f 

#define MAX_SOCKETS    100  
#define MSG_BUF_SLOTS   10  
#define MAX_MSG_SIZE   512  

// socket type identifier
#define SOCK_KTP   0x1234


// error codes
#define ENOTBOUND   1000   //Destination IP/port doesn't match k_bind()
#define ENOSPACE    1001   // Send buffer or socket table is full        
#define ENOMESSAGE  1002   // No message available in receive buffer


// Shared-memory key material
#define FTOK_PATH   "ksocket.h"
#define FTOK_ID     'K'

#define MAX_PKT_SIZE  (sizeof(KTPHeader) + MAX_MSG_SIZE)


// Global error variable
extern int my_errno;

typedef struct {
    char    pkt_type;
    uint8_t seq;
    int     rwnd_sz;
    uint16_t msg_len;
} KTPHeader;

typedef struct {
    uint8_t base_seq;
    uint8_t next_seq;
    int     win_size;
} SenderWindow;

typedef struct {
    uint8_t expected_seq;
} ReceiverWindow;

typedef struct {
    bool     is_free;
    pid_t    owner_pid;
    int      udp_fd;
    uint16_t local_port;
    char     peer_ip[INET_ADDRSTRLEN];
    int      peer_port;
    time_t   close_ts;

    // Send side
    char         sbuf[MSG_BUF_SLOTS][MAX_MSG_SIZE];
    size_t       sbuf_msg_len[MSG_BUF_SLOTS];
    int          sbuf_write;
    int          sbuf_count;
    int total_transmissions;
    SenderWindow swnd;
    time_t       last_sent_ts;

    // Receive side
    char           rbuf[MSG_BUF_SLOTS][MAX_MSG_SIZE];
    size_t         rbuf_msg_len[MSG_BUF_SLOTS];
    bool           rbuf_valid[MSG_BUF_SLOTS];
    int            rbuf_in;
    int            rbuf_out;
    int            rbuf_ready;
    int            rbuf_total;
    bool           nospace;
    ReceiverWindow rwnd;

    pthread_mutex_t lock;
} KTPSocket;


// LIBRARY API's

int k_socket  (int domain, int type, int protocol);
int k_bind    (int kfd,const struct sockaddr *src, socklen_t src_len,const struct sockaddr *dst, socklen_t dst_len);
int k_sendto  (int kfd, const void *msg, size_t len, int flags,const struct sockaddr *dst, socklen_t dst_len);
int k_recvfrom(int kfd, void *buf, size_t len, int flags,struct sockaddr *src, socklen_t *src_len);
int k_close   (int kfd);
int dropMessage(float p);

#endif