#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <cstring>
#include <iostream>
#include <csignal>

using namespace std;

const int MAX_LENGTH = 1024;

const char *NEW_CONNECT_MSG = "+OK New Connection!\r\n";
const char *BYE_MSG = "+OK Bye!";
const char *DISCONN_MSG = "+OK Connected Closed!";

int socket_fd;
struct sockaddr_in server_addr;

void signal_handler(int signal);

int main(int argc, char *argv[]) {

    signal(SIGINT, signal_handler);

    if (argc != 2) {
        fprintf(stderr, "*** Author: Zhengjia Mao (zmao)\n");
        exit(1);
    }

    char *ip = strtok(argv[1], ":");
    char *port = strtok(NULL, ":");

    if (port == NULL) {
        cerr << "Please enter valid IP and port" << endl;
        exit(EXIT_FAILURE);
    }

    socket_fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (socket_fd < 0) {
        cerr << "Error creating socket." << std::endl;
        exit(EXIT_FAILURE);
    }

    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(atoi(port));
    inet_pton(AF_INET, ip, &server_addr.sin_addr);

    cout << NEW_CONNECT_MSG;
    cout << "ip: " << ip << ", port: " << port << endl;

    // // Set stdin to non-blocking
    // int flags = fcntl(STDIN_FILENO, F_GETFL, 0);
    // fcntl(STDIN_FILENO, F_SETFL, flags | O_NONBLOCK);

    sockaddr_in src_addr;
    socklen_t src_len = sizeof(src_addr);

    // send messages to servers
    string message;
    while (true) {
        fd_set read_fds;
        FD_ZERO(&read_fds);
        FD_SET(socket_fd, &read_fds);
        FD_SET(STDIN_FILENO, &read_fds);

        select(socket_fd + 1, &read_fds, nullptr, nullptr, nullptr);

        // Check data from the server
        if (FD_ISSET(socket_fd, &read_fds)) {
            char buffer[MAX_LENGTH];
            ssize_t bytes_received = recvfrom(socket_fd, buffer, MAX_LENGTH - 1, 0, (struct sockaddr *)&src_addr, &src_len);
            if (bytes_received > 0) {
                buffer[bytes_received] = '\0';
                cout << buffer << endl;
            }
        }

        // Check data from the user
        if (FD_ISSET(STDIN_FILENO, &read_fds)) {
            string message;
            if (getline(cin, message)) {
                sendto(socket_fd, message.c_str(), message.size(), 0, (struct sockaddr *)&server_addr, sizeof(server_addr));
            }
            if (message.find("/quit") == 0) {
                cout << BYE_MSG << endl;
                break;
            }
        }
    }

    close(socket_fd);

    return 0;
}

void signal_handler(int signal) {
    string message = "/quit";
    sendto(socket_fd, message.c_str(), message.size(), 0, (struct sockaddr *)&server_addr, sizeof(server_addr));
    close(socket_fd);
    exit(0);
}