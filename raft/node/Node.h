//
// Created by zhrys on 2021/12/16.
//

#ifndef RAFT_NODE_NODE_H_
#define RAFT_NODE_NODE_H_
#include <string>
#include <thread>
#include <mutex>
#include <utility>
#include "spdlog/spdlog.h"
using std::string;
struct Node {
  string host;
  uint16_t port;
  Node(string ip, uint16_t port) : host(std::move(ip)), port(port) {}
  virtual ~Node() = default;
  [[nodiscard]] std::string name() const {
    return host + ":" + std::to_string(port);
  }
};

#endif //RAFT_NODE_NODE_H_
