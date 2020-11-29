#include "JoinQuery.hpp"
#include <assert.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <boost/algorithm/string.hpp>
#include <fstream>
#include <iostream>
#include <numeric>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>

using namespace std;

//---------------------------------------------------------------------------
JoinQuery::JoinQuery(string lineitem, string orders, string customer)
{
   this->lineitem = lineitem;
   this->orders = orders;
   this->customer = customer;
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

void JoinQuery::getCustomerIds(const char *file, string segmentParam,
                               unordered_set<int> &ids)
{
   int handle = open(file, O_RDONLY);
   lseek(handle, 0, SEEK_END);
   auto length = lseek(handle, 0, SEEK_CUR);
   void *data = mmap(nullptr, length, PROT_READ, MAP_SHARED, handle, 0);
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
      if (mkt == segmentParam) ids.insert(cust_id);
      iter = findPattern<'\n'>(iter, end);
   }

   munmap(data, length);
   close(handle);
}

void JoinQuery::getOrderMap(const char *file, unordered_map<int, int> &map)
{
   int handle = open(file, O_RDONLY);
   lseek(handle, 0, SEEK_END);
   auto length = lseek(handle, 0, SEEK_CUR);
   void *data = mmap(nullptr, length, PROT_READ, MAP_SHARED, handle, 0);
   auto begin = static_cast<const char *>(data), end = begin + length;

   for (auto iter = begin; iter < end;) {
      auto last = findPattern<'|'>(iter, end);
      int order_id = 0;
      parseInt(iter, last - 1, order_id);
      iter = last;
      last = findPattern<'|'>(iter, end);
      int v = 0;
      parseInt(iter, last - 1, v);
      map[order_id] = v;
      iter = findPattern<'\n'>(iter, end);
   }

   munmap(data, length);
   close(handle);
}

void JoinQuery::getLineMap(const char *file, unordered_multimap<int, int> &map)
{
   int handle = open(file, O_RDONLY);
   lseek(handle, 0, SEEK_END);
   auto length = lseek(handle, 0, SEEK_CUR);
   void *data = mmap(nullptr, length, PROT_READ, MAP_SHARED, handle, 0);
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
size_t JoinQuery::avg(std::string segmentParam)
{
   unordered_set<int> customer_ids;
   const char *c_file =
       "/home/user/Documents/Github/Uni/Master/TUM_FDE/projects/p01/"
       "fde20-bonusproject-1/test/data/tpch/sf0_001/customer.tbl";
   getCustomerIds(c_file, segmentParam, customer_ids);

   unordered_map<int, int> orders_map;
   const char *o_file =
       "/home/user/Documents/Github/Uni/Master/TUM_FDE/projects/p01/"
       "fde20-bonusproject-1/test/data/tpch/sf0_001/orders.tbl";
   getOrderMap(o_file, orders_map);

   unordered_multimap<int, int> lineitem_map;
   const char *l_file =
       "/home/user/Documents/Github/Uni/Master/TUM_FDE/projects/p01/"
       "fde20-bonusproject-1/test/data/tpch/sf0_001/lineitem.tbl";
   getLineMap(l_file, lineitem_map);

   unordered_set<int> matches;
   for (auto p : orders_map) {
      for (auto q : customer_ids) {
         if (p.second == q) {
            matches.insert(p.first);
            break;
         }
      }
   }

   unsigned sum = 0;
   unsigned count = 0;
   for (auto p : lineitem_map) {
      for (auto q : matches) {
         if (p.first == q) {
            sum += p.second;
            count += 1;
            break;
         }
      }
   }
   size_t avg = sum * 100 / count;
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
