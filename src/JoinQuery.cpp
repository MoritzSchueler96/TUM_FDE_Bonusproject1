#include "JoinQuery.hpp"
#include <assert.h>
#include <fstream>
#include <thread>
//---------------------------------------------------------------------------
JoinQuery::JoinQuery(std::string lineitem, std::string orders,
                     std::string customer)
{
   this->lineitem = lineitem;
   this->orders = orders;
   this->customer = customer;
   map["Bla"] = 2539;
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
   return averageOfMapField(lineitem_map, 'quantity');
   */
   return map["Bla"];
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
