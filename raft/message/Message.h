//
// Created by zhrys on 2021/12/17.
//

#ifndef RAFT_MESSAGE_MESSAGE_H_
#define RAFT_MESSAGE_MESSAGE_H_
#include <iostream>
#include "spdlog/spdlog.h"
using term = size_t;
using MessageLen = uint16_t;

struct MessageHeader {
  enum Type : uint8_t {
    UNKNOWN,
    REQUEST_VOTE,
    VOTE,
    APPEND_ENTRIES,
    AUTH
  };
  Type type_;
  uint32_t content_len{0};
  term t;
  explicit MessageHeader(Type type, term t) : type_(type), t(t) {}
};
struct Message {
  MessageHeader *header;
  char *content;
  size_t content_len;
};
struct AuthMessageHeader : public MessageHeader {
  char name[256]{};
  unsigned char name_length{};
  explicit AuthMessageHeader(const std::string &name) : MessageHeader(AUTH, 0) {
    if (name.length() > 255) {
      spdlog::error("host name must in 255 bytes");
      std::terminate();
    }
    memcpy(this->name, name.c_str(), name.length());
    name_length = (unsigned char) name.length();
  }
};

struct RequestVoteMessageHeader : public MessageHeader {
  size_t log_index;
  term log_term;
  RequestVoteMessageHeader(term t, size_t log_index) : MessageHeader(REQUEST_VOTE, t),
                                                       log_index(log_index) {};
};

struct VoteMessageHeader : public MessageHeader {
  explicit VoteMessageHeader(term t) : MessageHeader(VOTE, t) {};
};

struct AppendEntriesMessageHeader : public MessageHeader {
  explicit AppendEntriesMessageHeader(term t) : MessageHeader(APPEND_ENTRIES, t) {};
};
#endif //RAFT_MESSAGE_MESSAGE_H_
