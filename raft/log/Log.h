//
// Created by zhrys on 2021/12/23.
//

#ifndef RAFT_RAFT_LOG_LOG_H_
#define RAFT_RAFT_LOG_LOG_H_
#include <vector>
using term = size_t;
using LogIndex = size_t;
struct LogEntry {
  term log_term;
  LogIndex log_index;
  char data[0];
  size_t len;
};
class Log {
 private:
  std::vector<LogEntry *> entries;
 public:
  const LogEntry *last_log_entry() {
    if (entries.empty()) {
      return nullptr;
    }
    return *(entries.end() - 1);
  }
};

#endif //RAFT_RAFT_LOG_LOG_H_
