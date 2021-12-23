#include <iostream>
#include "node/Node.h"
#include "Raft.h"
#include "server/Server.h"
int main() {
  Node node1("127.0.0.1", 3001);
  Node node2("127.0.0.1", 3002);
  Node node3("127.0.0.1", 3003);

  Server server1(node1, {node2, node3});
  server1.start();
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  Server server2(node2, {node1, node3});
  server2.start();
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  Server server3(node3, {node1, node2});
  server3.start();
  while (true) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
}
