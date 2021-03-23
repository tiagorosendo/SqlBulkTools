using System;
using System.Collections.Generic;
using System.Data.SqlClient;

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
        private Dictionary<string, Type> _propTypes;

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

        public QueryForObject<T> WithPropertyTypes(Dictionary<string, Type> propTypes)
        {
            this._propTypes = propTypes;
            return this;
        }

        /// <summary>
        /// Set the name of table for operation to take place. Registering a table is Required.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <returns></returns>
        public QueryTable<T> WithTable(string tableName)
        {
            var table = BulkOperationsHelper.GetTableAndSchema(tableName);
            return new QueryTable<T>(_entity, _propTypes, table.Name, table.Schema, _sqlParams);
        }
    }
}
