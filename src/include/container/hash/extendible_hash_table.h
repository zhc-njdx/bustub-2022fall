//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.h
//
// Identification: src/include/container/hash/extendible_hash_table.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * extendible_hash_table.h
 *
 * Implementation of in-memory hash table using extendible hashing
 */

#pragma once

#include <list>
#include <memory>
#include <mutex>  // NOLINT
#include <utility>
#include <vector>

#include "container/hash/hash_table.h"

namespace bustub {

/**
 * ExtendibleHashTable implements a hash table using the extendible hashing algorithm.
 * @tparam K key type
 * @tparam V value type
 */
template <typename K, typename V>
class ExtendibleHashTable : public HashTable<K, V> {
 public:
  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief Create a new ExtendibleHashTable.
   * @param bucket_size: fixed size for each bucket
   */
  explicit ExtendibleHashTable(size_t bucket_size);

  /**
   * @brief Get the global depth of the directory.
   * @return The global depth of the directory.
   */
  auto GetGlobalDepth() const -> int;

  /**
   * @brief Get the local depth of the bucket that the given directory index points to.
   * @param dir_index The index in the directory.
   * @return The local depth of the bucket.
   */
  auto GetLocalDepth(int dir_index) const -> int;

  /**
   * @brief Get the number of buckets in the directory.
   * @return The number of buckets in the directory.
   */
  auto GetNumBuckets() const -> int;

  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief Find the value associated with the given key.
   *
   * Use IndexOf(key) to find the directory index the key hashes to.
   *
   * @param key The key to be searched.
   * @param[out] value The value associated with the key.
   * @return True if the key is found, false otherwise.
   */
  auto Find(const K &key, V &value) -> bool override;

  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief Insert the given key-value pair into the hash table.
   * If a key already exists, the value should be updated.
   * If the bucket is full and can't be inserted, do the following steps before retrying:
   *    1. If the local depth of the bucket is equal to the global depth,
   *        increment the global depth and double the size of the directory.
   *    2. Increment the local depth of the bucket.
   *    3. Split the bucket and redistribute directory pointers & the kv pairs in the bucket.
   *
   * @param key The key to be inserted.
   * @param value The value to be inserted.
   */
  void Insert(const K &key, const V &value) override;

  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief Given the key, remove the corresponding key-value pair in the hash table.
   * Shrink & Combination is not required for this project
   * @param key The key to be deleted.
   * @return True if the key exists, false otherwise.
   */
  auto Remove(const K &key) -> bool override;

  /**
   * Bucket class for each hash table bucket that the directory points to.
   */
  class Bucket {
   public:
    explicit Bucket(size_t size, int depth = 0);

    /** @brief Check if a bucket is full. */
    inline auto IsFull() const -> bool { return list_.size() == size_; }

    /** @brief Get the local depth of the bucket. */
    inline auto GetDepth() const -> int { return depth_; }

    /** @brief Increment the local depth of a bucket. */
    inline void IncrementDepth() { depth_++; }

    inline auto GetItems() -> std::list<std::pair<K, V>> & { return list_; }

    /**
     *
     * TODO(P1): Add implementation
     *
     * @brief Find the value associated with the given key in the bucket.
     * @param key The key to be searched.
     * @param[out] value The value associated with the key.
     * @return True if the key is found, false otherwise.
     */
    auto Find(const K &key, V &value) -> bool;

    /**
     *
     * TODO(P1): Add implementation
     *
     * @brief Given the key, remove the corresponding key-value pair in the bucket.
     * @param key The key to be deleted.
     * @return True if the key exists, false otherwise.
     */
    auto Remove(const K &key) -> bool;

    /**
     *
     * TODO(P1): Add implementation
     *
     * @brief Insert the given key-value pair into the bucket.
     *      1. If a key already exists, the value should be updated.
     *      2. If the bucket is full, do nothing and return false.
     * @param key The key to be inserted.
     * @param value The value to be inserted.
     * @return True if the key-value pair is inserted, false otherwise.
     */
    auto Insert(const K &key, const V &value) -> bool;

   private:
    // TODO(student): You may add additional private members and helper functions
    size_t size_;
    int depth_;
    std::list<std::pair<K, V>> list_;
  };

 private:
  // TODO(student): You may add additional private members and helper functions and remove the ones
  // you don't need.

  int global_depth_;    // The global depth of the directory
  size_t bucket_size_;  // The size of a bucket
  int num_buckets_;     // The number of buckets in the hash table
  mutable std::mutex latch_;
  std::vector<std::shared_ptr<Bucket>> dir_;  // The directory of the hash table

  // The following functions are completely optional, you can delete them if you have your own ideas.

  /**
   * @brief Redistribute the kv pairs in a full bucket.
   * @param bucket The bucket to be redistributed.
   */
  auto RedistributeBucket(size_t index) -> void {
    // in this condition, a bucket is referenced by serveral dir_ entry (at least 2)
    // split the bucket means using the local_depth can not distinguish the element, the local_depth
    // need to increase, so it can be referenced by less dir_ entry, so we need to create the new bucket
    // for the other dir_ entry

    int local_depth = GetLocalDepthInternal(index);   // the current local_depth
    int local_depth_before = local_depth - 1;         // local_depth before increasing
    int origin_mask = (1 << local_depth_before) - 1;  // the mask of the local_depth_before
    int mask = (1 << local_depth) - 1;                // the mask of the local_depth_current

    size_t origin_suffix = index & origin_mask;                  // origin suffix: x..x
    size_t suffix1 = origin_suffix;                              // 0x..x
    size_t suffix2 = (1 << local_depth_before) + origin_suffix;  // 1x..x

    std::shared_ptr<Bucket> bucket = dir_[index];  // referenced by dir_[suffix=0x..x]
    std::shared_ptr<Bucket> new_bucket =
        std::make_shared<Bucket>(bucket_size_, local_depth);  // referenced by dir_[suffix=1x..x]

    // make the dir_ entry point to the right bucket
    for (size_t i = 0; i < dir_.size(); i++) {
      if ((mask & i) == suffix1) {
        dir_[i] = bucket;
      } else if ((mask & i) == suffix2) {
        dir_[i] = new_bucket;
      }
    }

    IncrementNumBuckets();  // the number of bucket increase

    auto list = bucket->GetItems();
    for (auto it = list.begin(); it != list.end(); it++) {
      if (IndexOfDepth(it->first, local_depth) == suffix2) {
        bucket->Remove(it->first);
        new_bucket->Insert(it->first, it->second);
      }
    }
  }

  /**
   * @brief based on `depth` to calc the index of the `key`
   */
  auto IndexOfDepth(const K &key, int depth) -> size_t {
    int mask = (1 << depth) - 1;
    return std::hash<K>()(key) & mask;
  }

  /**
   * @brief Increment the global depth and double the size of the directory
   */
  auto IncrementGlobalDepth() -> void { global_depth_++; }

  auto IncrementNumBuckets() -> void { num_buckets_++; }

  /**
   * @brief double the capacity of the dir_
   */
  auto DoubleCapacity() -> void {
    size_t new_size = dir_.size() * 2;
    dir_.resize(new_size);  // double the size of the directory (not the number of buckets)
    // make the new dir_[i] point to the right bucket
    for (size_t i = 0; i < new_size / 2; i++) {
      dir_[i + (1 << (global_depth_ - 1))] = dir_[i];  // 0xx -> xx and 1xx -> xx
    }
  }

  /*****************************************************************
   * Must acquire latch_ first before calling the below functions. *
   *****************************************************************/

  /**
   * @brief For the given key, return the entry index in the directory where the key hashes to.
   * @param key The key to be hashed.
   * @return The entry index in the directory.
   */
  auto IndexOf(const K &key) -> size_t;

  auto GetGlobalDepthInternal() const -> int;
  auto GetLocalDepthInternal(int dir_index) const -> int;
  auto GetNumBucketsInternal() const -> int;
};

}  // namespace bustub
