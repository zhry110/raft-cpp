//
// Created by zhrys on 2021/12/16.
//

#ifndef RAFT_RAFT_RAFT_H_
#define RAFT_RAFT_RAFT_H_
#include <thread>
#include <atomic>
#include <unordered_set>
#include <condition_variable>
#include <random>
#include <thread>
#include <unistd.h>
#include <sys/event.h>
#include <spdlog/spdlog.h>
#include "node/Node.h"
#include "message/Message.h"
using term = size_t;
template<class MessageSender>
class Raft {
 public:
  enum Role {
    FOLLOWER,
    CANDIDATE,
    LEADER
  };
 private:
  Role role_{FOLLOWER};
  std::mutex role_mutex_{};
  std::condition_variable role_condition;

  std::atomic<term> current_term{0};
  Node *voted_for_{nullptr};
  Node *self{};
  size_t node_count;
  std::unordered_set<Node *> current_term_votes{};

  std::mutex term_lock;
  MessageSender *channel{};

#ifdef TEST_MODE
 public:
  size_t follower_call_count{0};
  size_t candidate_call_count{0};
  size_t election_count{0};
#endif

  volatile bool stop_ = false;
  std::chrono::milliseconds leader_random_wait() {
    static std::default_random_engine e;
    static std::uniform_int_distribution<unsigned> distribution_{150, 300};
    return std::chrono::milliseconds(distribution_(e));
  }
  term next_term() {
    newer_term(current_term + 1);
    // vote for self when next term started in candidate
    current_term_votes.insert(self);
    voted_for_ = self;
    role_ = CANDIDATE;
    return current_term;
  }
  void newer_term(term newer_term) {
    current_term = newer_term;
    current_term_votes.clear();
    voted_for_ = nullptr;
    role_ = FOLLOWER;
    role_condition.notify_one();
  }
  void vote_from(Node *node, term t) {
    std::lock_guard<std::mutex> lock(term_lock);
    spdlog::info("vote from {} to {} for term {}", node->name(), self->name(), t);
    assert(t <= current_term);
    if (t < current_term) {
      return;
    } else if (t == current_term) {
      current_term_votes.insert(node);
    }
  }
  bool already_voted(Node *node) {
    return voted_for_ != nullptr && voted_for_ != node;
  }

  void follower() {
#ifdef TEST_MODE
    follower_call_count++;
#endif
    spdlog::info("follower {}", self->name());
    while (!stop_) {
      std::unique_lock<std::mutex> unique_lock(role_mutex_);
      auto wait = leader_random_wait();
      auto status = role_condition.wait_for(unique_lock, wait);
      if (status == std::cv_status::timeout) {
        role_ = CANDIDATE;
        return;
      }
      assert(role_ == FOLLOWER);
    }
  }
  void candidate() {
    spdlog::info("candidate {}", self->name());
    while (!stop_) {
      if (role_ != CANDIDATE) return;
      std::unique_lock<std::mutex> lock(term_lock);
      term new_term = next_term();
      channel->broadcast_message(RequestVoteMessageHeader{new_term, 1});
      role_condition.wait_for(lock, std::chrono::milliseconds(100));
      if (current_term_votes.size() > node_count / 2) {
        role_ = LEADER;
        return;
      } else {
        spdlog::warn("term[{}]'s votes[{}] too less, request vote again", current_term, current_term_votes.size());
      }
    }
  }
  void leader() {
    spdlog::info("leader {}", self->name());
    while (!stop_ && role_ == LEADER) {
      channel->broadcast_message(AppendEntriesMessageHeader{current_term});
      std::this_thread::sleep_for(std::chrono::milliseconds(30));
    }
  }
 public:
  Raft(MessageSender *channel, Node *self, size_t node_count) :
      channel(channel), self(self), node_count(node_count) {
#ifdef TEST_MODE
    spdlog::info("TEST MODE");
#endif
  }
  void start() {
    while (!stop_) {
      switch (role_) {
        case FOLLOWER:follower();
          break;
        case CANDIDATE:candidate();
          break;
        case LEADER:leader();
          break;
      }
    }
  }
  void stop() {
    std::unique_lock<std::mutex> unique_lock(role_mutex_);
    role_condition.notify_all();
    stop_ = true;
  }
  void vote_for(Node *node, Message *msg) {
    std::unique_lock<std::mutex> lock(term_lock);
    auto header = (RequestVoteMessageHeader *) msg->header;
    if (header->t < current_term || already_voted(node)) {
      return;
    } else if (header->t > current_term) {
      newer_term(header->t);
      role_condition.notify_one();
    }
    voted_for_ = node;
    lock.unlock();
    spdlog::info("{} vote for {} term {}", self->name(), node->name(), header->t);
    if (node != self) {
      channel->send_message(node, VoteMessageHeader(header->t)); // vote
    } else {
      vote_from(node, header->t);
    }
  }
  void vote_from(Node *node, Message *msg) {
    vote_from(node, msg->header->t);
  }
  void append_entries(Node *node, Message *msg) {
    std::unique_lock<std::mutex> lock(term_lock);
    auto header = (AppendEntriesMessageHeader *) msg->header;
    if (header->t < current_term) return;
    std::unique_lock<std::mutex> unique_lock(role_mutex_);
    role_ = FOLLOWER;
    if (header->t > current_term) {
      newer_term(header->t);
    }
    role_condition.notify_one();
  }
  Role role() { return role_; }
};

#endif //RAFT_RAFT_RAFT_H_