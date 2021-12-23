//
// Created by zhrys on 2021/12/21.
//
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <Raft.h>
#include "node/Node.h"
#include <thread>
#include <memory>
class MockChannel {
 public:
  template<class MsgType>
  void send_message(Node *node, MsgType msg) {
  }
  template<class MsgType>
  void broadcast_message(MsgType msg) {
    MessageLen len = sizeof(msg);
  }
};
class MockRaft {

};
class RaftBasicTest : public testing::Test {
 protected:
  Node *self;
  MockChannel *channel_{};
  Raft<MockChannel> raft_;
  std::unique_ptr<std::thread> worker;
 public:
  RaftBasicTest() : self(new Node("127.0.0.1", 3001)),
                    channel_(new MockChannel),
                    raft_(channel_, self, 3) {
    worker = std::make_unique<std::thread>([&]() -> void { raft_.start(); });
  }
  ~RaftBasicTest() override {
    raft_.stop();
    worker->join();
  };
 protected:
  void SetUp() override {
    Test::SetUp();
  }
  void TearDown() override {
    Test::TearDown();
  }
};

TEST_F(RaftBasicTest, should_always_be_follower_when_revcieve_append_entries) { // NOLINT(cert-err58-cpp)
  Node node("127.0.0.1", 3002);
  AppendEntriesMessageHeader header{100};
  Message message{&header, nullptr, 0};
  for (int i = 0; i < 50; i++) {
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    raft_.append_entries(&node, &message);
    EXPECT_EQ(raft_.role(), Raft<MockChannel>::Role::FOLLOWER);
  }
  EXPECT_EQ(1, raft_.follower_call_count);
}

TEST_F(RaftBasicTest, should_be_candidate_when_others_down) { // NOLINT(cert-err58-cpp)
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  EXPECT_EQ(raft_.role(), Raft<MockChannel>::Role::CANDIDATE);
  EXPECT_EQ(1, raft_.follower_call_count);
}
