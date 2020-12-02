#include "JoinQuery.hpp"
#include <assert.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <atomic>
#include <fstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

using namespace std;

//---------------------------------------------------------------------------
JoinQuery::JoinQuery(string lineitem, string orders, string customer)
{
   // getCustomerMap(&(customer[0]), this->customer_map);
   getCustomerMktSegments(&(customer[0]), this->customer_mktSegments);
   getOrderMap(&(orders[0]), this->orders_map);
   getLineMap(&(lineitem[0]), this->lineitem_map);
}

// Source code below is taken from the sixth lecture in Foundations in data
// engineering at TUM
constexpr uint64_t buildPattern(char c)
{
   // Convert 00000000000000CC -> CCCCCCCCCCCCCCCC
   uint64_t v = c;
   return (v << 56 | v << 48 | v << 40 | v << 32 | v << 24 | v << 16 | v << 8) |
          v;
}

// Source code below is taken from the sixth lecture in Foundations in data
// engineering at TUM
template <char separator>
static const char *findPattern(const char *iter, const char *end)
// Returns the position after the pattern within [iter, end[, or end if not
// found
{
   // Loop over the content in blocks of 8 characters
   auto end8 = end - 8;
   constexpr uint64_t pattern = buildPattern(separator);
   for (; iter < end8; iter += 8) {
      // Check the next 8 characters for the pattern
      uint64_t block = *reinterpret_cast<const uint64_t *>(iter);
      constexpr uint64_t high = 0x8080808080808080ull;
      constexpr uint64_t low = ~high;
      uint64_t lowChars = (~block) & high;
      uint64_t foundPattern = ~((((block & low) ^ pattern) + low) & high);
      uint64_t matches = foundPattern & lowChars;
      if (matches) return iter + (__builtin_ctzll(matches) >> 3) + 1;
   }

   // Check the last few characters explicitly
   while ((iter < end) && ((*iter) != separator)) ++iter;
   if (iter < end) ++iter;
   return iter;
}

// Source code below is taken from the sixth lecture in Foundations in data
// engineering at TUM
template <char separator>
static const char *findNthPattern(const char *iter, const char *end, unsigned n)
// Returns the position after the pattern within [iter, end[, or end if not
// found
{
   // Loop over the content in blocks of 8 characters
   auto end8 = end - 8;
   constexpr uint64_t pattern = buildPattern(separator);
   for (; iter < end8; iter += 8) {
      // Check the next 8 characters for the pattern
      uint64_t block = *reinterpret_cast<const uint64_t *>(iter);
      constexpr uint64_t high = 0x8080808080808080ull;
      constexpr uint64_t low = ~high;
      uint64_t lowChars = (~block) & high;
      uint64_t foundPattern = ~((((block & low) ^ pattern) + low) & high);
      uint64_t matches = foundPattern & lowChars;
      if (matches) {
         unsigned hits = __builtin_popcountll(
             matches);  // gives number of bits that are set
         if (hits >= n) {
            for (; n > 1; n--)
               matches &= matches - 1;  // clears the last bit that is one
            return iter + (__builtin_ctzll(matches) >> 3) + 1;
         }
         n -= hits;
      }
   }

   // Check the last few characters explicitly
   for (; iter < end; ++iter)
      if ((*iter) == separator)
         if ((--n) == 0) return iter + 1;

   return end;
}

// wrapped code from lecture into its own function
// maybe use atoi instead?
void parseInt(const char *first, const char *end, int &v)
{
   v = 0;
   while (first < end) {
      char f = *(first++);
      if ((f >= '0') && (f <= '9')) {
         v = 10 * v + f - '0';
      } else {
         break;
      }
   }
}

// content partly taken from the lecture
void JoinQuery::getCustomerMap(const char *file,
                               unordered_multimap<string, unsigned> &map)
{
   int handle = open(file, O_RDONLY);
   lseek(handle, 0, SEEK_END);
   auto length = lseek(handle, 0, SEEK_CUR);
   void *data =
       mmap(nullptr, length, PROT_READ, MAP_SHARED | MAP_POPULATE, handle, 0);
   madvise(data, length, MADV_SEQUENTIAL);
   madvise(data, length, MADV_WILLNEED);
   auto begin = static_cast<const char *>(data), end = begin + length;

   for (auto iter = begin; iter < end;) {
      auto last = findPattern<'|'>(iter, end);
      int cust_id = 0;
      parseInt(iter, last - 1, cust_id);

      last = findNthPattern<'|'>(iter, end, 6);
      iter = last;
      last = findPattern<'|'>(iter, end);
      int size = last - iter - 1;
      string mkt(iter, size);
      map.insert({mkt, cust_id});
      iter = findPattern<'\n'>(iter, end);
   }

   munmap(data, length);
   close(handle);
}

void JoinQuery::getCustomerMktSegments(const char *file, vector<string> &ids)
{
   int handle = open(file, O_RDONLY);
   lseek(handle, 0, SEEK_END);
   auto length = lseek(handle, 0, SEEK_CUR);
   void *data =
       mmap(nullptr, length, PROT_READ, MAP_SHARED | MAP_POPULATE, handle, 0);
   madvise(data, length, MADV_SEQUENTIAL);
   madvise(data, length, MADV_WILLNEED);
   auto begin = static_cast<const char *>(data), end = begin + length;

   // save into ordered vector -> indices of matching condition
   for (auto iter = begin; iter < end;) {
      auto last = findNthPattern<'|'>(iter, end, 6);
      iter = last;
      last = findPattern<'|'>(iter, end);
      int size = last - iter - 1;
      string mkt(iter, size);
      ids.push_back(mkt);
      iter = findPattern<'\n'>(iter, end);
   }

   munmap(data, length);
   close(handle);
}

// content partly taken from the lecture
void JoinQuery::getOrderMap(const char *file,
                            unordered_multimap<unsigned, unsigned> &map)
{
   int handle = open(file, O_RDONLY);
   lseek(handle, 0, SEEK_END);
   auto length = lseek(handle, 0, SEEK_CUR);
   void *data =
       mmap(nullptr, length, PROT_READ, MAP_SHARED | MAP_POPULATE, handle, 0);
   madvise(data, length, MADV_SEQUENTIAL);
   madvise(data, length, MADV_WILLNEED);
   auto begin = static_cast<const char *>(data), end = begin + length;

   for (auto iter = begin; iter < end;) {
      auto last = findPattern<'|'>(iter, end);
      int order_id = 0;
      parseInt(iter, last - 1, order_id);
      iter = last;
      last = findPattern<'|'>(iter, end);
      int v = 0;
      parseInt(iter, last - 1, v);
      map.insert({v, order_id});
      iter = findPattern<'\n'>(iter, end);
   }

   munmap(data, length);
   close(handle);
}

// content partly taken from the lecture
void JoinQuery::getLineMap(const char *file,
                           unordered_multimap<unsigned, unsigned> &map)
{
   int handle = open(file, O_RDONLY);
   lseek(handle, 0, SEEK_END);
   auto length = lseek(handle, 0, SEEK_CUR);
   void *data =
       mmap(nullptr, length, PROT_READ, MAP_SHARED | MAP_POPULATE, handle, 0);
   madvise(data, length, MADV_SEQUENTIAL);
   madvise(data, length, MADV_WILLNEED);
   auto begin = static_cast<const char *>(data), end = begin + length;

   for (auto iter = begin; iter < end;) {
      auto last = findPattern<'|'>(iter, end);
      int order_id = 0;
      parseInt(iter, last - 1, order_id);

      iter = findNthPattern<'|'>(iter, end, 4);
      last = findPattern<'|'>(iter, end);
      int v = 0;
      parseInt(iter, last - 1, v);
      map.insert({order_id, v});
      iter = findPattern<'\n'>(iter, end);
   }
   munmap(data, length);
   close(handle);
}

//---------------------------------------------------------------------------
// slightly slower variant but not prone to missing customer keys
size_t JoinQuery::avg2(std::string segmentParam)
{
   unsigned long long int sum = 0;
   unsigned long long int count = 0;

   auto iterators = customer_map.equal_range(segmentParam);
   for (auto iterator = iterators.first; iterator != iterators.second;
        ++iterator) {
      auto iters = orders_map.equal_range(iterator->second);
      for (auto iter = iters.first; iter != iters.second; ++iter) {
         auto its = lineitem_map.equal_range(iter->second);
         for (auto it = its.first; it != its.second; ++it) {
            sum += it->second;
            count += 1;
         }
      }
   }

   size_t avg = sum * 100 / count;
   return avg;
}

// assumes cust_key is sorted and has no missing values
size_t JoinQuery::avg(std::string segmentParam)
{
   vector<thread> threads;
   atomic<unsigned> sum;
   atomic<unsigned> count;
   sum = 0;
   count = 0;

   for (unsigned index = 0, threadCount = thread::hardware_concurrency();
        index != threadCount; ++index) {
      threads.push_back(
          thread([index, threadCount, this, segmentParam, &sum, &count]() {
             // Executed on a background thread
             for (unsigned i = 0; i < customer_mktSegments.size(); i++) {
                if (customer_mktSegments[i] == segmentParam) {
                   auto iters = orders_map.equal_range(i + 1);
                   for (auto iter = iters.first; iter != iters.second; ++iter) {
                      auto its = lineitem_map.equal_range(iter->second);
                      for (auto it = its.first; it != its.second; ++it) {
                         sum += it->second;
                         count += 1;
                      }
                   }
                }
             }
          }));
   }

   for (auto &t : threads) t.join();

   size_t avg = sum.load() * 100 / count.load();
   return avg;
}

//---------------------------------------------------------------------------
size_t JoinQuery::lineCount(std::string rel)
{
   std::ifstream relation(rel);
   assert(relation);  // make sure the provided string references a file
   size_t n = 0;
   for (std::string line; std::getline(relation, line);) n++;
   return n;
}
//---------------------------------------------------------------------------
