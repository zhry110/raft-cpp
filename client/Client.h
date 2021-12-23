//
// Created by zhrys on 2021/12/18.
//

#ifndef RAFT_CLIENT_CLIENT_H_
#define RAFT_CLIENT_CLIENT_H_
#include <string>
#include <thread>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include "../message/Message.h"
class Client {
 public:
  static void connect_server(const std::string &host, int port) {
    struct sockaddr_in server{};
    int sock;
    sock = socket(AF_INET, SOCK_STREAM, 0);
    assert(sock != -1);
    server.sin_family = AF_INET;
    server.sin_port = htons(port);
    server.sin_addr.s_addr = inet_addr(host.c_str());
    int ret = connect(sock, (struct sockaddr *) &server, sizeof(server));
    assert(ret != -1);
    VoteMessageHeader msg(10);
    RequestVoteMessageHeader msg2(10, 10);
    while (true) {
      MessageLen len = sizeof(msg);
      write(sock, &len, sizeof(len));
      write(sock, &msg, sizeof(msg));

      write(sock, &len, sizeof(len));
      write(sock, &msg, sizeof(msg));
      //len = sizeof(msg2);
      //write(sock, &len, sizeof(len));
      //write(sock, &msg2, sizeof(msg2));
      std::this_thread::sleep_for(std::chrono::seconds(2));
    }
    std::this_thread::sleep_for(std::chrono::hours(10));
    close(sock);
  }
};

int main() {
  Client::connect_server("127.0.0.1", 6666);
}

#endif //RAFT_CLIENT_CLIENT_H_
