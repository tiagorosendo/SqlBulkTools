using System.Collections.Generic;
using SqlBulkTools.BulkCopy;

// ReSharper disable once CheckNamespace
namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class BulkForCollection<T>
    {
        private readonly BulkOperations _bulk;
        private readonly IEnumerable<T> _list;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="bulk"></param>
        /// <param name="list"></param>
        public BulkForCollection(BulkOperations bulk, IEnumerable<T> list)
        {
            _bulk = bulk;
            _list = list;
        }

        /// <summary>
        /// Set the name of table for operation to take place. Registering a table is Required.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <returns></returns>
        public BulkTable<T> WithTable(string tableName)
        {
            var table = BulkOperationsHelper.GetTableAndSchema(tableName);
            return new BulkTable<T>(_bulk, _list, table.Name, table.Schema);
        }
    }
}
