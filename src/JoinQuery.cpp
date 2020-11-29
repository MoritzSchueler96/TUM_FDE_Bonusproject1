#include "JoinQuery.hpp"
#include <assert.h>
#include <sys/mman.h>
#include <boost/algorithm/string.hpp>
#include <fstream>
#include <iostream>
#include <numeric>
#include <thread>
#include <unordered_map>
#include <unordered_set>

using namespace std;

//---------------------------------------------------------------------------
JoinQuery::JoinQuery(std::string lineitem, std::string orders,
                     std::string customer)
{
   this->lineitem = lineitem;
   this->orders = orders;
   this->customer = customer;
}
//---------------------------------------------------------------------------
size_t JoinQuery::avg(std::string segmentParam)
{
   /*
   // get needed columns as hashmaps
   customer_map = read(this->customer);
   orders_map = read(this->orders);
   lineitem_map = read(this->lineitem);

   // delete non matching entries
   deleteNonMatching(customer_map, segmentParam);
   deleteNonMatching(orders_map, customer_map);
   deleteNonMatching(lineitem_map, orders_map);

   // calc average and return
   return averageOfMapField(lineitem_map, 'quantity'); // sum / linecount
   */

   unordered_set<int> customer_ids;
   getCustomerIds(this->customer, segmentParam, customer_ids);
   unordered_map<int, int> orders_map;
   getOrderMap(this->orders, orders_map);
   unordered_multimap<int, int> lineitem_map;
   getLineMap(this->lineitem, lineitem_map);

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

void JoinQuery::getCustomerIds(string file, string segmentParam,
                               unordered_set<int> &ids)
{
   ifstream in(file);
   string line;
   bool init;
   unsigned cust_id = 0;
   while (getline(in, line)) {
      const char *last = nullptr;
      const char *iter = nullptr;
      unsigned col = 0;
      init = 0;
      for (char &c : line) {
         if (!init) {
            last = (&c);
            iter = (&c);
            init = 1;
         }
         if (c == '|') {
            ++col;
            if (col == 1) {
               while (iter < &c) {
                  char f = *(iter++);
                  if ((f >= '0') && (f <= '9')) {
                     cust_id = 10 * cust_id + f - '0';
                  }
               }
               // from_chars(last, &c, cust_id);
               // cout << "cust_id:" << cust_id << endl;
            }
            if (col == 6) {
               last = (&c) + 1;
               iter = (&c);
            } else if (col == 7) {
               int size = (&c) - last;
               string mkt(last, size);
               if (mkt == segmentParam) ids.insert(cust_id);
               // cout << "mkt:" << mkt << endl;
               // cout << "seg:" << segmentParam << endl;
               break;
            }
         }
      }
   }
}

void JoinQuery::getOrderMap(string file, unordered_map<int, int> &map)
{
   ifstream in(file);
   string line;
   bool init;
   int order_id;
   while (getline(in, line)) {
      const char *last = nullptr;
      const char *iter = nullptr;
      unsigned col = 0;
      init = 0;
      for (char &c : line) {
         if (!init) {
            last = (&c);
            iter = (&c);
            init = 1;
         }
         if (c == '|') {
            ++col;
            if (col == 1) {
               while (iter < &c) {
                  char f = *(iter++);
                  if ((f >= '0') && (f <= '9')) {
                     order_id = 10 * order_id + f - '0';
                  }
               }
               // from_chars(last, &c, order_id);
               last = (&c) + 1;
               iter = (&c);
               // cout << "order_id:" << order_id << endl;
            } else if (col == 2) {
               unsigned v;
               while (iter < &c) {
                  char f = *(iter++);
                  if ((f >= '0') && (f <= '9')) { v = 10 * v + f - '0'; }
               }
               // from_chars(last, &c, v);
               map[order_id] = v;
               // cout << "v:" << v << endl;
               break;
            }
         }
      }
   }
}

void JoinQuery::getLineMap(string file, unordered_multimap<int, int> &map)
{
   ifstream in(file);
   string line;
   bool init;
   int order_id;
   while (getline(in, line)) {
      const char *last = nullptr;
      const char *iter = nullptr;
      unsigned col = 0;
      init = 0;
      for (char &c : line) {
         if (!init) {
            last = (&c);
            iter = (&c);
            init = 1;
         }
         if (c == '|') {
            ++col;
            if (col == 1) {
               while (iter < &c) {
                  char f = *(iter++);
                  if ((f >= '0') && (f <= '9')) {
                     order_id = 10 * order_id + f - '0';
                  }
               }
               // from_chars(last, &c, order_id);
               // cout << "order_id:" << order_id << endl;
            }
            if (col == 4) {
               last = (&c) + 1;
               iter = (&c);
            } else if (col == 5) {
               unsigned v;
               while (iter < &c) {
                  char f = *(iter++);
                  if ((f >= '0') && (f <= '9')) { v = 10 * v + f - '0'; }
               }
               // from_chars(last, &c, v);
               map.insert({order_id, v});
               // cout << "v:" << v << endl;
               break;
            }
         }
      }
   }
}

/*
void JoinQuery::getCustomerIds(string file, string segmentParam,
                               unordered_set<int> &ids)
{
   int handle = open(file, O_RDONLY);
   auto size = lseek(handle, 0, SEEK_END) + 7;
   auto data = mmap(nullptr, size, PROT_READ, MAP_SHARED, handle, 0);

   ifstream in(file);
   string line;
   bool init;
   int cust_id;
   while (getline(in, line)) {
      const char *last = nullptr;
      unsigned col = 0;
      init = 0;
      for (char &c : line) {
         if (!init) {
            last = (&c);
            init = 1;
         }
         if (c == '|') {
            ++col;
            if (col == 1) {
               from_chars(last, &c, cust_id);
               // cout << "cust_id:" << cust_id << endl;
            }
            if (col == 6) {
               last = (&c) + 1;
            } else if (col == 7) {
               int size = (&c) - last;
               string mkt(last, size);
               if (mkt == segmentParam) ids.insert(cust_id);
               // cout << "mkt:" << mkt << endl;
               // cout << "seg:" << segmentParam << endl;
               break;
            }
         }
      }
   }
   munmap(data, size);
   close(handle);
}
*/
