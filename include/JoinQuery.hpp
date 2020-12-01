#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

using namespace std;
//---------------------------------------------------------------------------
class JoinQuery
/// Class which provides answers to queries on lineitem orders and customer
{
   public:
   unordered_multimap<unsigned, unsigned> lineitem_map;
   unordered_map<unsigned, unsigned> orders_map;
   vector<string> customer_ids;

   /**************************************************************************
    *  The constructor receives paths to data from the TPC-H benchmark.
    *  You can explore an example dataset from this benchmark on
    *  http://www.hyper-db.de/interface.html
    *  Furthermore, another example dataset is provided in
    *  test/data/tpch/sf0_001
    *
    *  Parameters to constructor:
    *     lineitem: path to a lineitem.tbl
    *     orders: path to a orders.tbl
    *     customer: path to a customer.tbl
    ************************************************************************/
   JoinQuery(std::string lineitem, std::string orders, std::string customer);

   /**************************************************************************
    *  Computes avg(l_quantity)*100 for the following query.
    *
    *  select avg(l_quantity)*100
    *  from lineitem, orders, customer
    *  where
    *   l_orderkey = o_orderkey and
    *   o_custkey = c_custkey and
    *   c_mktsegment = <segmentParam>
    *
    *  where the tables lineitem, orders and customer are those identified by
    *  the paths given in the constructor.
    *
    *  Parameters:
    *     segmentParam: string to use instead of <segmentParam> in query
    *
    *  Returns: avg(l_quantity) * 100
    *     In the data files, l_quantity is of type integer.
    *     Therefore you should return avg(l_quantity) * 100
    *     as integer (more specific C++ type: size_t) as well.
    *
    *
    * average of quantity of lineitem but only a subset
    *
    *
    ************************************************************************/
   size_t avg(std::string segmentParam);
   /// Returns line count of given file
   size_t lineCount(std::string rel);

   void getCustomerIds(const char *file, vector<string> &ids);

   void getOrderMap(const char *file, unordered_map<unsigned, unsigned> &map);
   void getLineMap(const char *file,
                   unordered_multimap<unsigned, unsigned> &map);
};
//---------------------------------------------------------------------------
