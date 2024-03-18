#include "primer/trie.h"
#include <stack>
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // throw NotImplementedException("Trie::Get is not implemented.");

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  if (root_ == nullptr) {
    return nullptr;
  }

  std::shared_ptr<const TrieNode> cur_node = root_;
  for (auto &ch : key) {
    auto pair = cur_node->children_.find(ch);
    if (pair == cur_node->children_.end()) {
      return nullptr;
    }
    cur_node = pair->second;
  }

  auto node_with_value = dynamic_cast<const TrieNodeWithValue<T> *>(cur_node.get());
  return node_with_value == nullptr ? nullptr : node_with_value->value_.get();
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.

  std::shared_ptr<const TrieNode> cur_node = root_;
  std::stack<std::shared_ptr<const TrieNode>> node_stack;
  std::shared_ptr<T> &&shared_value = std::make_shared<T>(std::move(value));

  size_t idx = 0;
  size_t r_idx = key.size();
  for (; cur_node && idx < r_idx; ++idx) {
    node_stack.push(cur_node);
    auto pair = cur_node->children_.find(key[idx]);
    cur_node = pair == cur_node->children_.end() ? nullptr : pair->second;
  }

  // When cur_node is nullptr, create a node with value as last node
  std::shared_ptr<const TrieNodeWithValue<T>> leaf_node =
      cur_node ? std::make_shared<const TrieNodeWithValue<T>>(cur_node->children_, std::move(shared_value))
               : std::make_shared<const TrieNodeWithValue<T>>(std::move(shared_value));

  std::shared_ptr<const TrieNode> child_node = leaf_node;

  // Create a new node from down to up.
  while (idx < r_idx) {
    std::map<char, std::shared_ptr<const TrieNode>> children{{key[--r_idx], child_node}};
    child_node = std::make_shared<const TrieNode>(children);
  }

  // for existed nodes, clone and reuse them.
  // When key is "", node_stack is only contain root
  while (!node_stack.empty()) {
    auto &&parent_node = std::shared_ptr<TrieNode>(node_stack.top()->Clone());
    parent_node->children_[key[--idx]] = child_node;
    child_node = parent_node;
    node_stack.pop();
  }

  return Trie(child_node);
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
  std::shared_ptr<const TrieNode> cur_node = root_;
  std::stack<std::shared_ptr<const TrieNode>> node_stack;

  size_t idx = 0;
  for (; cur_node && idx < key.size(); ++idx) {
    node_stack.push(cur_node);
    auto pair = cur_node->children_.find(key[idx]);
    cur_node = pair == cur_node->children_.end() ? nullptr : pair->second;
  }

  std::shared_ptr<const TrieNode> child_node = std::shared_ptr<const TrieNode>(cur_node->Clone());

  if (idx == 0) {
    child_node = std::make_shared<const TrieNode>(cur_node->children_);
  }

  if (!node_stack.empty()) {
    auto &&parent_node = std::shared_ptr<TrieNode>(node_stack.top()->Clone());
    if (child_node->children_.empty()) {
      parent_node->children_.erase(key[--idx]);
    } else {
      child_node = std::make_shared<const TrieNode>(child_node->children_);
      parent_node->children_[key[--idx]] = child_node;
    }
    child_node = parent_node;
    node_stack.pop();
  }

  while (!node_stack.empty()) {
    auto &&parent_node = std::shared_ptr<TrieNode>(node_stack.top()->Clone());
    if (!child_node->is_value_node_ && child_node->children_.empty()) {
      parent_node->children_.erase(key[--idx]);
    } else {
      parent_node->children_[key[--idx]] = child_node;
    }
    child_node = parent_node;
    node_stack.pop();
  }

  return Trie(child_node->children_.empty() ? nullptr : child_node);
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
