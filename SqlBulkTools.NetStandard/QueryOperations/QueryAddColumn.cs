using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq.Expressions;
using System.Reflection;
// ReSharper disable UnusedMember.Global

// ReSharper disable once CheckNamespace
namespace SqlBulkTools.QueryOperations
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class QueryAddColumn<T>
    {
        private readonly T _singleEntity;
        private readonly string _tableName;
        private Dictionary<string, string> CustomColumnMappings { get; }
        private readonly HashSet<string> _columns;
        private readonly string _schema;
        private readonly List<SqlParameter> _sqlParams;
        private readonly List<PropertyInfo> _propertyInfoList;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="singleEntity"></param>
        /// <param name="tableName"></param>
        /// <param name="columns"></param>
        /// <param name="schema"></param>
        /// <param name="sqlParams"></param>
        /// <param name="propertyInfoList"></param>
        public QueryAddColumn(T singleEntity, string tableName, HashSet<string> columns, string schema, List<SqlParameter> sqlParams, List<PropertyInfo> propertyInfoList)
        {
            _singleEntity = singleEntity;
            _tableName = tableName;
            _columns = columns;
            _schema = schema;
            CustomColumnMappings = new Dictionary<string, string>();
            _sqlParams = sqlParams;
            _propertyInfoList = propertyInfoList;
        }

        /// <summary>
        /// Add each column that you want to include in the query. Only include the columns that are relevant to the 
        /// procedure for best performance. 
        /// </summary>
        /// <param name="columnName">Column name as represented in database</param>
        /// <returns></returns>
        public QueryAddColumn<T> AddColumn(Expression<Func<T, object>> columnName)
        {
            var propertyName = BulkOperationsHelper.GetPropertyName(columnName);
            _columns.Add(propertyName);
            return this;
        }

        /// <summary>
        /// Inserts a single entity. This method uses a parameterized query. 
        /// </summary>
        /// <returns></returns>
        public QueryInsertReady<T> Insert()
        {
            return new QueryInsertReady<T>(_singleEntity, _tableName, _schema, _columns, CustomColumnMappings, _sqlParams, _propertyInfoList);
        }

        /// <summary>
        /// Attempts to update the target entity (using the mandatory MatchTargetOn property). 
        /// If the target entity doesn't exist, insert a new record. This method uses a parameterized query.
        /// </summary>
        /// <returns></returns>
        public QueryUpsertReady<T> Upsert()
        {
            return new QueryUpsertReady<T>(_singleEntity, _tableName, _schema, _columns, CustomColumnMappings, _sqlParams, _propertyInfoList);
        }



        /// <summary>
        /// All rows matching the condition(s) selected will be updated. If you need to update a collection of objects that can't be
        /// matched by a generic condition, use the BulkUpdate method instead. This method uses a parameterized query. 
        /// </summary>
        /// <returns></returns>
        public QueryUpdateCondition<T> Update()
        {
            return new QueryUpdateCondition<T>(_singleEntity, _tableName, _schema, _columns, CustomColumnMappings, _sqlParams, _propertyInfoList);
        }
    }
}
