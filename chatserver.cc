#include <algorithm>
#include <arpa/inet.h>
#include <chrono>
#include <csignal>
#include <cstring>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <unordered_map>
#include <vector>

using namespace std;

struct Client {
    int cid;
    string nick_name;
    sockaddr_in address;
    int room;
};

struct Message {
    int msg_id;
    int sender_id;
    bool deliverable;
    vector<int> clock;
    string content;
};

struct Comparator {
    bool operator()(const Message &m1, const Message &m2) const {
        if (m1.msg_id < m2.msg_id) {
            return true;
        } else if (m1.msg_id == m2.msg_id && m1.sender_id < m2.sender_id) {
            return true;
        } else {
            return false;
        }
    }
};

struct Comparator2 {
    bool operator()(const Message &m1, const Message &m2) const {
        if (m1.msg_id > m2.msg_id) {
            return true;
        } else if (m1.msg_id == m2.msg_id && m1.sender_id < m2.sender_id) {
            return true;
        } else {
            return false;
        }
    }
};

const char *JOIN_OK_MSG = "+OK You are now in chat room #";
const char *LEFT_OK_MSG = "+OK You have left chat room #";
const char *NICK_OK_MSG = "+OK Nick name set to ";
const char *BYE_MSG = "+OK Bye!";
const char *JOIN_WARN_MSG = "-ERR You need to join a room.";
const char *JOIN_ERR_MSG = "-ERR You are already in room #";
const char *ARG_ERR_MSG = "-ERR An argument is needed.";
const char *UNKNOWN_ERR_MSG = "-ERR Unknown command.";
const char *ROOM_ERR_MSG = "-ERR There are only chat rooms.";
const char *PREFIX = "03:48:22.004328 S02 ";

const int MAX_LENGTH = 1024;
const int MAX_CLIENTS = 250;
const int NUM_OF_ROOMS = 10;

void initialize();
string timestamp_prefix();
bool compare_addr(sockaddr_in add1, sockaddr_in add2);
void signal_handler(int signal);
string clock_to_string(int group_id);
void basic_deliver(int fd, int room, string content);
void basic_multicast(int fd, string content);
void FIFO_deliver(int socket_fd, int sender_id, char *buffer);
void FIFO_multicast(int socket_fd, int cur_client_idx, string str_content);
void TOTAL_deliver(int socket_fd, int sender_id, char *buffer);
void TOTAL_multicast(int socket_fd, int cur_client_idx, string str_content);
void CAUSAL_deliver(int socket_fd, int sender_id, char *buffer);
void CAUSAL_multicast(int socket_fd, int cur_client_idx, string str_content);

// FIFO variables
vector<int> S;         // sequence numbers
vector<vector<int>> R; // latest delivered sequence numbers
vector<vector<unordered_map<int, string>>> FIFO_HOLDBACK;

// TOTAL variables
const int NEW_MSG = 1;
const int PROPOSAL = 2;
const int AGREEMENT = 3;
vector<vector<Message>> TOTAL_HOLDBACK;
unordered_map<string, vector<Message>> PROPOSALS;
vector<int> P;
vector<int> A;

// CAUSAL variables
vector<vector<Message>> CAUSAL_HOLDBACK;
vector<vector<int>> CLOCKS;

// shared variables
vector<Client> CLIENTS;
vector<sockaddr_in> SERVERS;

bool FLAG_DEBUG = false;
int self_id = 0;
int ORDER = 0; // default as unordered
int socket_fd;
int next_cid = 1;

/* =============================================== main =============================================== */
int main(int argc, char *argv[]) {

    signal(SIGINT, signal_handler);

    if (argc < 2) {
        fprintf(stderr, "*** Author: Zhengjia Mao (zmao)\n");
        exit(1);
    }

    int c;
    while ((c = getopt(argc, argv, "vo:")) != -1) {
        switch (c) {
        case 'v':
            FLAG_DEBUG = true;
            break;
        case 'o':
            if (strcasecmp(optarg, "unordered") == 0) {
                ORDER = 0;
            } else if (strcasecmp(optarg, "fifo") == 0) {
                ORDER = 1;
            } else if (strcasecmp(optarg, "total") == 0) {
                ORDER = 2;
            } else if (strcasecmp(optarg, "causal") == 0) {
                ORDER = 3;
            } else {
                cerr << "Invalid ordering" << endl;
                exit(EXIT_FAILURE);
            }
            break;
        default:
            cerr << "default" << endl;
            abort();
        }
    }

    // locate the config file
    string file_name = argv[optind];
    ifstream config_file(file_name);
    self_id = atoi(argv[optind + 1]) - 1;

    // locate the bind address and save all forwarding addresses
    int i = 0;
    string line;
    while (getline(config_file, line)) {
        string forward = line.substr(0, line.find(","));
        string ip = forward.substr(0, forward.find(":"));
        string port = forward.substr(forward.find(":") + 1);

        // save the forward addresses
        struct sockaddr_in forward_addr;
        memset(&forward_addr, 0, sizeof(forward_addr));
        forward_addr.sin_family = AF_INET;
        forward_addr.sin_port = htons(atoi(port.c_str()));
        inet_pton(AF_INET, ip.c_str(), &forward_addr.sin_addr);
        SERVERS.push_back(forward_addr);

        // bind to the bind address
        if (i == self_id) {
            string bind;
            if (line.find(",") != string::npos) {
                bind = line.substr(line.find(",") + 1);
            } else {
                bind = forward;
            }
            string ip = bind.substr(0, bind.find(":"));
            string port = bind.substr(bind.find(":") + 1);

            struct sockaddr_in bind_addr;
            memset(&bind_addr, 0, sizeof(bind_addr));
            bind_addr.sin_family = AF_INET;
            bind_addr.sin_port = htons(atoi(port.c_str()));
            inet_pton(AF_INET, ip.c_str(), &bind_addr.sin_addr);

            socket_fd = socket(AF_INET, SOCK_DGRAM, 0);
            if (socket_fd < 0) {
                cerr << "Error creating socket." << std::endl;
                exit(EXIT_FAILURE);
            }
            if (::bind(socket_fd, (struct sockaddr *)&bind_addr, sizeof(bind_addr)) < 0) {
                cerr << "bind server fails" << endl;
                exit(EXIT_FAILURE);
            }
        }
        i++;
    }

    // initialize all queues and variables
    initialize();

    while (1) {
        // receiving messages
        char buffer[MAX_LENGTH];
        struct sockaddr_in src_addr;
        socklen_t src_len = sizeof(src_addr);
        ssize_t bytes_received = recvfrom(socket_fd, buffer, MAX_LENGTH, 0, (struct sockaddr *)&src_addr, &src_len);
        buffer[bytes_received] = '\0';
        // cout << "Message received: " << buffer << endl;

        // identify the source of the message received
        string source = "UNKNOWN";
        int sender_id = 0;
        for (int i = 0; i < SERVERS.size(); i++) {
            if (compare_addr(SERVERS[i], src_addr)) {
                source = "SERVER";
                sender_id = i;
                break;
            }
        }
        if (source != "SERVER") {
            for (int i = 0; i < CLIENTS.size(); i++) {
                if (compare_addr(CLIENTS[i].address, src_addr)) {
                    source = "CLIENT";
                    sender_id = i;
                    break;
                }
            }
        }

        // if unknown: create a new client
        if (source == "UNKNOWN") {
            Client new_client;
            new_client.cid = next_cid;
            new_client.room = 0;
            new_client.address = src_addr;
            CLIENTS.push_back(new_client);
            next_cid++;
            int cur_client_idx = CLIENTS.size() - 1;

            string action = string(buffer);
            string cmd = string(buffer);
            transform(action.begin(), action.end(), action.begin(), ::tolower);
            string message;

            if (FLAG_DEBUG) {
                string prefix = timestamp_prefix();
                cout << prefix << " New Client " << CLIENTS[cur_client_idx].cid << " posts: '" << buffer << "'" << endl;
                // cout << "Current CLIENTS vector size: " << CLIENTS.size() << endl;
            }

            if (action.find("/join") == 0) {
                if (cmd.length() <= 6) {
                    message = ARG_ERR_MSG;
                } else {
                    int room = stoi(cmd.substr(cmd.find(" ") + 1));
                    if (room > NUM_OF_ROOMS) {
                        message = ROOM_ERR_MSG;
                    } else {
                        message = JOIN_OK_MSG + to_string(room);
                        CLIENTS[cur_client_idx].room = room;
                    }
                }
            } else if (action.find("/nick") == 0) {
                if (cmd.length() <= 6) {
                    message = ARG_ERR_MSG;
                } else {
                    CLIENTS[cur_client_idx].nick_name = cmd.substr(cmd.find(" ") + 1);
                    message = NICK_OK_MSG + CLIENTS[cur_client_idx].nick_name;
                }
            } else {
                message = JOIN_WARN_MSG;
            }
            sendto(socket_fd, message.c_str(), message.size(), 0, (struct sockaddr *)&CLIENTS[cur_client_idx].address, sizeof(CLIENTS[cur_client_idx].address));
        }

        // if from existing client: multicast to other servers
        else if (source == "CLIENT") {
            int cur_client_idx = sender_id;

            if (FLAG_DEBUG) {
                string prefix = timestamp_prefix();
                cout << prefix << " Existing Client " << CLIENTS[cur_client_idx].cid << " posts: '" << buffer << "' to chat room #" << CLIENTS[cur_client_idx].room << endl;
            }

            if (buffer[0] == '/') { // if client sends a command
                string action = string(buffer);
                string cmd = string(buffer);
                transform(action.begin(), action.end(), action.begin(), ::tolower);
                string message;

                if (action.find("/join") == 0) {
                    if (cmd.length() <= 6) {
                        message = ARG_ERR_MSG;
                    } else if (CLIENTS[cur_client_idx].room != 0) {
                        message = JOIN_ERR_MSG + to_string(CLIENTS[cur_client_idx].room);
                    } else {
                        int room = stoi(cmd.substr(cmd.find(" ") + 1));
                        if (room > NUM_OF_ROOMS) {
                            message = ROOM_ERR_MSG;
                        } else {
                            CLIENTS[cur_client_idx].room = room;
                            message = JOIN_OK_MSG + to_string(CLIENTS[cur_client_idx].room);
                        }
                    }
                } else if (action == "/part") {
                    if (CLIENTS[cur_client_idx].room != 0) {
                        message = LEFT_OK_MSG + to_string(CLIENTS[cur_client_idx].room);
                        CLIENTS[cur_client_idx].room = 0;
                    } else {
                        message = JOIN_WARN_MSG;
                    }
                } else if (action.find("/nick") == 0) {
                    if (cmd.length() <= 6) {
                        message = ARG_ERR_MSG;
                    } else {
                        CLIENTS[cur_client_idx].nick_name = cmd.substr(cmd.find(" ") + 1);
                        message = NICK_OK_MSG + CLIENTS[cur_client_idx].nick_name;
                    }
                } else if (action.find("/quit") == 0) {
                    message = BYE_MSG;
                    CLIENTS.erase(CLIENTS.begin() + cur_client_idx);
                    // if (FLAG_DEBUG) {
                    //     cout << "Current CLIENTS size: " << CLIENTS.size() << endl;
                    // }
                } else {
                    message = UNKNOWN_ERR_MSG;
                }
                sendto(socket_fd, message.c_str(), message.size(), 0, (struct sockaddr *)&CLIENTS[cur_client_idx].address, sizeof(CLIENTS[cur_client_idx].address));

            } else { // if client sends a message
                if (CLIENTS[cur_client_idx].room == 0) {
                    string message = JOIN_WARN_MSG;
                    sendto(socket_fd, message.c_str(), message.size(), 0, (struct sockaddr *)&CLIENTS[cur_client_idx].address, sizeof(CLIENTS[cur_client_idx].address));
                } else {
                    string name;
                    if (CLIENTS[cur_client_idx].nick_name.empty()) {
                        name = string(inet_ntoa(CLIENTS[cur_client_idx].address.sin_addr)) + ":" + to_string(CLIENTS[cur_client_idx].address.sin_port);
                    } else {
                        name = CLIENTS[cur_client_idx].nick_name;
                    }
                    string str_content = "<" + name + "> " + string(buffer);

                    if (ORDER == 0) { // Unordered
                        string message = to_string(CLIENTS[cur_client_idx].room) + "+" + str_content;
                        basic_multicast(socket_fd, message);

                    } else if (ORDER == 1) { // FIFO
                        FIFO_multicast(socket_fd, cur_client_idx, str_content);

                    } else if (ORDER == 2) { // TOTAL
                        TOTAL_multicast(socket_fd, cur_client_idx, str_content);

                    } else if (ORDER == 3) { // CAUSAL
                        CAUSAL_multicast(socket_fd, cur_client_idx, str_content);
                    }
                }
            }
        }

        // if from server: deliver the message
        else if (source == "SERVER") {
            if (ORDER == 0) {
                int room = atoi(strtok(buffer, "+"));
                char *content = strtok(NULL, "+");
                string str_content = string(content);
                basic_deliver(socket_fd, room, str_content);

            } else if (ORDER == 1) {
                FIFO_deliver(socket_fd, sender_id, buffer);

            } else if (ORDER == 2) {
                TOTAL_deliver(socket_fd, sender_id, buffer);

            } else if (ORDER == 3) {
                CAUSAL_deliver(socket_fd, sender_id, buffer);
            }
        }
    }

    return 0;
}
/* =============================================== main =============================================== */

bool compare_addr(sockaddr_in add1, sockaddr_in add2) { return add1.sin_port == add2.sin_port && strcmp(inet_ntoa(add1.sin_addr), inet_ntoa(add2.sin_addr)) == 0; }

void initialize() {
    for (int i = 0; i < NUM_OF_ROOMS; i++) {
        vector<int> inner_R;
        vector<unordered_map<int, string>> inner_FH;
        vector<int> inner_CLOCK;

        for (int j = 0; j < SERVERS.size(); j++) {
            unordered_map<int, string> inner_inner_FH;
            inner_FH.push_back(inner_inner_FH);
            inner_R.push_back(0);
            inner_CLOCK.push_back(0);
        }

        // FIFO
        S.push_back(0);
        R.push_back(inner_R);
        FIFO_HOLDBACK.push_back(inner_FH);

        // TOTAL
        P.push_back(0);
        A.push_back(0);
        vector<Message> vm;
        TOTAL_HOLDBACK.push_back(vm);

        // CAUSAL
        vector<Message> s;
        CAUSAL_HOLDBACK.push_back(s);
        CLOCKS.push_back(inner_CLOCK);
    }
}

void basic_multicast(int fd, string content) {
    for (int i = 0; i < SERVERS.size(); i++) {
        sendto(fd, content.c_str(), content.size(), 0, (struct sockaddr *)&SERVERS[i], sizeof(SERVERS[i]));

        if (FLAG_DEBUG) {
            string prefix = timestamp_prefix();
            cout << prefix << " Server " << i + 1 << " sends: '" << content << "'" << endl;
        }
    }
}

void basic_deliver(int fd, int room, string content) {
    for (int i = 0; i < CLIENTS.size(); i++) {
        if (CLIENTS[i].room == room) {
            sendto(fd, content.c_str(), content.size(), 0, (struct sockaddr *)&CLIENTS[i].address, sizeof(CLIENTS[i].address));

            if (FLAG_DEBUG) {
                string prefix = timestamp_prefix();
                cout << prefix << " Delivered '" << content << "' to Client " << CLIENTS[i].cid << " at room #" << CLIENTS[i].room << endl;
            }
        }
    }
}

// FIFO ordering, msg_id + room + content
void FIFO_multicast(int socket_fd, int cur_client_idx, string str_content) {
    int group_id = CLIENTS[cur_client_idx].room - 1;
    S[group_id]++;
    string message = to_string(S[group_id]) + "+" + to_string(CLIENTS[cur_client_idx].room) + "+" + str_content;
    basic_multicast(socket_fd, message);
}

void FIFO_deliver(int socket_fd, int sender_id, char *buffer) {
    int msg_id = atoi(strtok(buffer, "+"));
    int room = atoi(strtok(NULL, "+"));
    char *content = strtok(NULL, "+");
    string str_content = string(content);
    int group_id = room - 1;

    FIFO_HOLDBACK[group_id][sender_id][msg_id] = str_content;

    int next_id = R[group_id][sender_id] + 1;
    while (FIFO_HOLDBACK[group_id][sender_id].find(next_id) != FIFO_HOLDBACK[group_id][sender_id].end()) {
        string msg = FIFO_HOLDBACK[group_id][sender_id][next_id];
        basic_deliver(socket_fd, room, msg);
        FIFO_HOLDBACK[group_id][sender_id].erase(next_id);
        R[group_id][sender_id]++;
        next_id = R[group_id][sender_id] + 1;
    }
}

// TOTAL ordering, state + proposer + msg_id + room + content
void TOTAL_multicast(int socket_fd, int cur_client_idx, string str_content) {
    string message = to_string(NEW_MSG) + "+" + to_string(self_id) + "+0+" + to_string(CLIENTS[cur_client_idx].room) + "+" + str_content;
    basic_multicast(socket_fd, message);
}

void TOTAL_deliver(int socket_fd, int sender_id, char *buffer) {
    int state = atoi(strtok(buffer, "+"));
    int proposer = atoi(strtok(NULL, "+"));
    int msg_id = atoi(strtok(NULL, "+"));
    int room = atoi(strtok(NULL, "+"));
    char *content = strtok(NULL, "+");
    string str_content = string(content);
    int group_id = room - 1;
    string message;

    if (state == NEW_MSG) { // first step, receive new message
        P[group_id] = max(P[group_id], A[group_id]) + 1;
        Message m = {P[group_id], 0, false, {}, str_content};
        TOTAL_HOLDBACK[group_id].push_back(m);
        sort(TOTAL_HOLDBACK[group_id].begin(), TOTAL_HOLDBACK[group_id].end(), Comparator());
        message = to_string(PROPOSAL) + "+" + to_string(self_id) + "+" + to_string(P[group_id]) + "+" + to_string(room) + "+" + str_content;
        sendto(socket_fd, message.c_str(), message.size(), 0, (struct sockaddr *)&SERVERS[sender_id], sizeof(SERVERS[sender_id]));

    } else if (state == PROPOSAL) { // receive proposal response
        // keep tracking the proposals for each message sent out
        if (PROPOSALS.find(str_content) == PROPOSALS.end()) { // not found, create a new one
            vector<Message> mv;
            PROPOSALS[str_content] = mv;
        }
        Message m = {msg_id, proposer, false, {}, str_content};
        PROPOSALS[str_content].push_back(m);

        if (PROPOSALS[str_content].size() == SERVERS.size()) { // got all proposals
            sort(PROPOSALS[str_content].begin(), PROPOSALS[str_content].end(), Comparator2());
            int T_max = PROPOSALS[str_content].begin()->msg_id;
            int ID_max = PROPOSALS[str_content].begin()->sender_id;
            message = to_string(AGREEMENT) + "+" + to_string(ID_max) + "+" + to_string(T_max) + "+" + to_string(room) + "+" + content;
            basic_multicast(socket_fd, message);
            PROPOSALS.erase(str_content);
        }

    } else { // receive final agreement and deliver
        Message m = {msg_id, proposer, true, {}, str_content};
        for (int i = 0; i < TOTAL_HOLDBACK[group_id].size(); i++) {
            if (TOTAL_HOLDBACK[group_id][i].content == str_content) {
                TOTAL_HOLDBACK[group_id][i] = m;
            }
        }
        sort(TOTAL_HOLDBACK[group_id].begin(), TOTAL_HOLDBACK[group_id].end(), Comparator());
        A[group_id] = max(A[group_id], msg_id);
        // pop and deliver all deliverable messages
        while (TOTAL_HOLDBACK[group_id].begin()->deliverable) {
            if (TOTAL_HOLDBACK[group_id].size() > 0){
                string msg_to_deliver = TOTAL_HOLDBACK[group_id].begin()->content;
                basic_deliver(socket_fd, room, msg_to_deliver);
                TOTAL_HOLDBACK[group_id].erase(TOTAL_HOLDBACK[group_id].begin());
            }else{
                break;
            }
            sort(TOTAL_HOLDBACK[group_id].begin(), TOTAL_HOLDBACK[group_id].end(), Comparator());
        }
    }
}


// CAUSAL ordering, clock + msg_id + room + content
void CAUSAL_multicast(int socket_fd, int cur_client_idx, string str_content) {
    int group_id = CLIENTS[cur_client_idx].room - 1;
    CLOCKS[group_id][self_id]++;
    string str_clocks = clock_to_string(group_id);
    string message = str_clocks + "+" + to_string(self_id) + "+" + to_string(CLIENTS[cur_client_idx].room) + "+" + str_content;
    basic_multicast(socket_fd, message);
}

void CAUSAL_deliver(int socket_fd, int sender_id, char *buffer) {
    char *clock = strtok(buffer, "+");
    int msg_id = atoi(strtok(NULL, "+"));
    int room = atoi(strtok(NULL, "+"));
    char *content = strtok(NULL, "+");
    string str_content = string(content);
    int group_id = room - 1;
    if (sender_id == self_id) {
        basic_deliver(socket_fd, room, str_content);
    } else {
        Message m1 = {msg_id, sender_id, false, {}, ""};
        char *c_unit = strtok(clock, ",");
        m1.clock.push_back(atoi(c_unit));
        while ((c_unit = strtok(NULL, ",")) != NULL) {
            m1.clock.push_back(atoi(c_unit));
        }
        CAUSAL_HOLDBACK[group_id].push_back(m1);
    }

    while (true) {
        bool progress = false;
        for (int i = 0; i < CAUSAL_HOLDBACK[group_id].size(); i++) {
            Message m2 = CAUSAL_HOLDBACK[group_id][i];
            bool seen_all = true;
            for (int i = 0; i < CLOCKS[group_id].size(); i++) {
                if (m2.clock[i] > CLOCKS[group_id][i] && i != sender_id) {
                    seen_all = false;
                }
            }
            if (m2.clock[sender_id] == CLOCKS[group_id][sender_id] + 1 && seen_all) {
                basic_deliver(socket_fd, room, str_content);
                CLOCKS[group_id][sender_id]++;
                CAUSAL_HOLDBACK[group_id].erase(CAUSAL_HOLDBACK[group_id].begin() + i);
                progress = true;
            }
        }
        if (!progress) {
            break;
        }
    }
}

void signal_handler(int signal) {
    close(socket_fd);
    exit(0);
}

string clock_to_string(int group_id) {
    string str_clocks;
    for (int i = 0; i < CLOCKS[group_id].size(); i++) {
        if (i != 0) {
            str_clocks += ",";
        }
        str_clocks += to_string(CLOCKS[group_id][i]);
    }
    return str_clocks;
}

// prefix for format
string timestamp_prefix() {
    stringstream ss;
    struct timeval tv;
    gettimeofday(&tv, NULL);
    chrono::system_clock::time_point tp = chrono::system_clock::from_time_t(tv.tv_sec);
    time_t tt = chrono::system_clock::to_time_t(tp);
    tm tm = *localtime(&tt);

    ss << put_time(&tm, "%T") << "." << setfill('0') << setw(6) << tv.tv_usec;
    string printout = ss.str();

    if (self_id < 9) {
        printout += " S0" + to_string(self_id + 1);
    } else {
        printout += " S" + to_string(self_id + 1);
    }
    return printout;
}