//
// Created by zhrys on 2021/12/18.
//

#ifndef RAFT_NET_SERVER_H_
#define RAFT_NET_SERVER_H_
#include <unordered_map>
#include <thread>
#include <utility>
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>
#include <sys/event.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <fcntl.h>
#include "Raft.h"
#include "node/Node.h"
#include "spdlog/spdlog.h"
#include "message/Message.h"
#include "message/Message.h"
using std::string;
using std::unordered_map;
constexpr auto VOTE = MessageHeader::Type::VOTE;
constexpr auto APPEND_ENTRIES = MessageHeader::Type::APPEND_ENTRIES;
constexpr auto REQUEST_VOTE = MessageHeader::Type::REQUEST_VOTE;
constexpr auto AUTH = MessageHeader::Type::AUTH;
constexpr auto UNKNOWN = MessageHeader::Type::UNKNOWN;

enum class MessageState {
  READ_LEN,
  READ_HEADER,
  READ_CONTENT
};
struct Connection {
  constexpr static size_t BUFFER_SIZE = 2 * 1024 * 1024;
  int fd{};
  MessageState state{MessageState::READ_LEN};
  size_t left{0};
  size_t header_len{};
  size_t content_len{};
  MessageHeader::Type type{MessageHeader::UNKNOWN};
  Node *node{};
  bool closed{false};
  std::string close_purpose{};
  bool io_running{false};
  char buffer[BUFFER_SIZE]{};
  void close(const string &purpose) {
    if (closed) {
      return;
    }
    closed = true;
    close_purpose = purpose;
  }
};
class Server {
 private:
  constexpr static int EVENT_NUM = 32;
  Raft<Server> raft;
  std::unordered_set<Connection *> connections{};
  unordered_map<Node *, Connection *> nodes{};
  unordered_map<string, Node *> node_names{};
  std::mutex conn_mutex{};
  int poll_fd;
  int server_fd;
  unordered_map<MessageHeader::Type, std::function<void(Connection *conn, Message *)>> handlers{};
  volatile bool stop{false};
  Node host;
  std::unique_ptr<std::thread> net_worker{};
  std::unique_ptr<std::thread> keepalive{};
  std::unique_ptr<std::thread> raft_worker{};

  void auth_node(Connection *conn, Message *msg) {
    std::lock_guard<std::mutex> lock(conn_mutex);
    auto header = (AuthMessageHeader *) msg->header;
    if (node_names.find(header->name) == node_names.end()) {
      spdlog::error("not known node {}", header->name);
      return;
    }
    auto local_node = node_names.find(header->name)->second;
    conn->node = local_node;
    spdlog::info("connection from {} to {}", local_node->name(), host.name());
    add_connection(conn);
  }
  static void set_non_block(int fd) {
    int old_ctl = fcntl(fd, F_GETFL, 0);
    assert(old_ctl != -1);
    int ret = fcntl(fd, F_SETFL, old_ctl | O_NONBLOCK);
    assert(ret != -1);
  }
  static void reuse_address(int fd) {
    int on = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)) == -1) {
      spdlog::error("can not reuse socket error:{}", strerror(errno));
      std::terminate();
    }
  }
  void monitor_connection(int fd, int read_write, void *data) const {
    struct kevent changes[1];
    EV_SET(&changes[0], fd, read_write, EV_ADD | EV_ONESHOT, 0, 0, data);
    int ret = kevent(poll_fd, changes, 1, nullptr, 0, nullptr);
    assert(ret != -1);
  }
  void remove_monitor_connection(int fd, int read_write) const {
    struct kevent changes[1];
    EV_SET(&changes[0], fd, read_write, EV_DELETE | EV_ONESHOT, 0, 0, nullptr);
    kevent(poll_fd, changes, 1, nullptr, 0, nullptr);
  }
  void accept_connections() {
    struct sockaddr_in node_addr{};
    socklen_t node_addr_len = sizeof(node_addr);
    while (true) {
      int node_fd = accept(server_fd, reinterpret_cast<sockaddr *>(&node_addr), &node_addr_len);
      if (node_fd < 0) {
        if (errno == EAGAIN) break;
        else if (errno == EINTR) continue;
      } else {
        auto connection = new Connection{node_fd, MessageState::READ_LEN};
        std::lock_guard<std::mutex> conn_lock(conn_mutex);
        add_connection(connection);
      }
    }
  }
  void connect_node(Node *node) {
    struct sockaddr_in server{};
    int sock;
    sock = socket(AF_INET, SOCK_STREAM, 0);
    assert(sock != -1);
    Server::set_non_block(sock);
    server.sin_family = AF_INET;
    server.sin_port = htons(node->port);
    server.sin_addr.s_addr = inet_addr(host.host.c_str());
    int ret = ::connect(sock, (struct sockaddr *) &server, sizeof(server));
    if (ret < 0 && errno != EINPROGRESS) {
      spdlog::error("connect to {}:{} error, error:{}", host.host, host.port, strerror(errno));
      return;
    }
    auto conn = new Connection{sock};
    conn->node = node;
    monitor_connection(conn->fd, EVFILT_WRITE, conn);
  }
  void add_connection(Connection *conn) {
    if (conn->node != nullptr) {
      auto node_iter = nodes.find(conn->node);
      assert(node_iter != nodes.end());
      if (node_iter->second && node_iter->second != conn) {
        remove_connection(node_iter->second, "new connection established");
      }
    }
    connections.insert(conn);
    if (conn->node != nullptr) {
      nodes[conn->node] = conn;
    }
    monitor_connection(conn->fd, EVFILT_READ, conn);
  }
  void remove_connection(Connection *connection, const string &purpose) {
    auto iterator = connections.find(connection);
    if (iterator == connections.end()) return;
    connection->close(purpose);
    if (connection->io_running) {
      spdlog::warn("connection:{} will remove by it's io thread seen because of {}",
                   connection->fd, purpose);
      return;
    }
    remove_monitor_connection(connection->fd, EVFILT_READ);
    close(connection->fd);
    connections.erase(connection);
    if (connection->node != nullptr) {
      // todo avoid remove another
      nodes.erase(connection->node);
      spdlog::info("connection {}:{} removed, because of {}", connection->node->name(), purpose);
    } else {
      spdlog::info("connection fd={} removed, because of {}", connection->fd, purpose);
    }
    delete (connection);
  }
  void handle_msg(Connection *conn) {
    assert(connections.find(conn) != connections.end());
    while (true) {
      size_t used = 0;
      ssize_t rn = read(conn->fd, conn->buffer + conn->left, Connection::BUFFER_SIZE - conn->left);
      size_t total_len = rn + conn->left;
      if (rn <= 0) {
        std::lock_guard conn_lock(conn_mutex);
        if (rn == 0) conn->close("remote close");
        else if (errno == EINTR) continue;
        else if (errno != EAGAIN) conn->close(strerror(errno));
        break;
      }
      while (true) {
        if (conn->state == MessageState::READ_LEN) {
          if (sizeof(MessageLen) <= (total_len - used)) {
            conn->header_len = *(MessageLen *) (conn->buffer + used);
            conn->state = MessageState::READ_HEADER;
            used += sizeof(MessageLen);
          } else {
            break;
          }
        }
        if (conn->state == MessageState::READ_HEADER) {
          if (conn->header_len <= (total_len - used)) {
            auto header = (MessageHeader *) (conn->buffer + used);
            // spdlog::info("received a message type={}, length={}", (int) header->type_, conn->header_len);
            used += conn->header_len;
            conn->header_len = 0;
            Message msg{header, nullptr, 0};
            handlers[header->type_](conn, &msg);
            if (header->content_len == 0) {
              conn->state = MessageState::READ_LEN;
            } else {
              conn->state = MessageState::READ_CONTENT;
            }
          } else break;
        }
        if (conn->state == MessageState::READ_CONTENT) {
        }
      }
      conn->left = total_len - used;
      assert(used > 0);
      memcpy(conn->buffer, conn->buffer + used, conn->left);
    }
    std::unique_lock<std::mutex> lock(conn_mutex);
    conn->io_running = false;
    if (conn->closed) {
      remove_connection(conn, conn->close_purpose);
    } else {
      lock.unlock();
      monitor_connection(conn->fd, EVFILT_READ, conn);
    }
  }
 public:
  Server(const Node &host, const std::vector<Node> &nodes) : host(host), raft(this, &this->host, nodes.size() + 1) {
    poll_fd = kqueue();
    assert(poll_fd != -1);
    std::for_each(nodes.begin(), nodes.end(), [&](auto node) -> void {
      auto n = new Node(node);
      this->nodes.insert({n, nullptr});
      this->node_names.insert({n->name(), n});
    });
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
      spdlog::error("create socket error: {}(errno: {})", strerror(errno), errno);
      std::terminate();
    }
    Server::set_non_block(server_fd);
    Server::reuse_address(server_fd);
    handlers = {
        {VOTE, [&](Connection *conn, Message *msg) -> void { raft.vote_from(conn->node, msg); }},
        {REQUEST_VOTE, [&](Connection *conn, Message *msg) -> void { raft.vote_for(conn->node, msg); }},
        {AUTH, [&](Connection *conn, Message *msg) -> void { auth_node(conn, msg); }},
        {APPEND_ENTRIES, [&](Connection *conn, Message *msg) -> void { raft.append_entries(conn->node, msg); }}
    };
  }
  void start() {
    struct sockaddr_in servaddr{};
    memset(&servaddr, 0, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = inet_addr(host.host.c_str());
    servaddr.sin_port = htons(host.port);
    if (bind(server_fd, (struct sockaddr *) &servaddr, sizeof(servaddr)) == -1) {
      spdlog::error("bind socket error: {}(errno: {})", strerror(errno), errno);
      std::terminate();
    }
    if (listen(server_fd, 1024) == -1) {
      spdlog::error("listen socket error: {}(errno: {})", strerror(errno), errno);
      std::terminate();
    }
    spdlog::info("Server {} started", host.name());
    assert(net_worker.get() == nullptr);
    net_worker = std::make_unique<std::thread>([&]() -> void {
      struct kevent events[EVENT_NUM];
      monitor_connection(server_fd, EVFILT_READ, nullptr);
      while (!stop) {
        int event_num = kevent(poll_fd, nullptr, 0, events, EVENT_NUM, nullptr);
        if (event_num < 0) std::terminate();
        for (int i = 0; i < event_num; ++i) {
          if (events[i].ident == server_fd) {
            accept_connections();
            monitor_connection(server_fd, EVFILT_READ, nullptr);
          } else if (events[i].filter == EVFILT_READ) {
            auto connection = (Connection *) events[i].udata;
            std::unique_lock<std::mutex> lock(conn_mutex);
            if (connections.find(connection) == connections.end() || connection->fd < events[i].ident) {
              spdlog::warn("a closed fd={} readable, skipped");
              continue;
            }
            connection->io_running = true;
            lock.unlock();
            handle_msg(connection);
          } else if (events[i].filter == EVFILT_WRITE) {
            auto connection = (Connection *) events[i].udata;
            AuthMessageHeader message(host.name());
            send_message(connection, message);
            std::lock_guard<std::mutex> lock(conn_mutex);
            add_connection(connection);
          } else {
            assert(false);
          }
        }
      }
    });
    keepalive = std::make_unique<std::thread>([&]() -> void {
      while (!stop) {
        std::this_thread::sleep_for(std::chrono::seconds(2));
        for (auto node: nodes) {
          if (node.second == nullptr && node.first->name() > host.name()) {
            connect_node(node.first);
          }
        }
      }
    });
    raft_worker = std::make_unique<std::thread>([&]() -> void {
      std::this_thread::sleep_for(std::chrono::seconds(3));
      raft.start();
    });
  }
  template<class MsgType>
  void send_message(Node *node, MsgType msg) {
    if (nodes.find(node) == nodes.end() || nodes[node] == nullptr) {
      return;
    }
    MessageLen len = sizeof(msg);
    write(nodes[node]->fd, &len, sizeof(len));
    write(nodes[node]->fd, &msg, sizeof(msg));
  }
  template<class MsgType>
  void send_message(Connection *conn, MsgType msg) {
    MessageLen len = sizeof(msg);
    write(conn->fd, &len, sizeof(len));
    write(conn->fd, &msg, sizeof(msg));
  }
  template<class MsgType>
  void broadcast_message(MsgType msg) {
    MessageLen len = sizeof(msg);
    std::for_each(nodes.begin(), nodes.end(), [&](auto node) -> void {
      //spdlog::info("msg from {} to {}", host.name(), node.first->name());
      send_message(node.first, msg);
    });
  }
};

#endif //RAFT_NET_SERVER_H_
