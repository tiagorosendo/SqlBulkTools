using System.Collections.Generic;
using System.Data.SqlClient;
// ReSharper disable UnusedMember.Global

// ReSharper disable once CheckNamespace
namespace SqlBulkTools.QueryOperations
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class QueryForObject<T>
    {
        private readonly T _entity;
        private readonly List<SqlParameter> _sqlParams;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="entity"></param>
        /// <param name="sqlParams"></param>
        public QueryForObject(T entity, List<SqlParameter> sqlParams)
        {
            _entity = entity;
            _sqlParams = sqlParams;
        }

        /// <summary>
        /// Set the name of table for operation to take place. Registering a table is Required.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <returns></returns>
        public QueryTable<T> WithTable(string tableName)
        {
            var table = BulkOperationsHelper.GetTableAndSchema(tableName);
            return new QueryTable<T>(_entity, table.Name, table.Schema, _sqlParams);
        }
    }
}
