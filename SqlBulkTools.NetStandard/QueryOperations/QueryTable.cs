using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq.Expressions;

namespace SqlBulkTools.QueryOperations
{
    /// <summary>
    /// Configurable options for table. 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class QueryTable<T>
    {
        private readonly T _singleEntity;
        private HashSet<string> Columns { get; set; }
        private string _schema;
        private readonly string _tableName;
        private Dictionary<string, string> CustomColumnMappings { get; set; }
        private readonly List<SqlParameter> _sqlParams;
        private readonly List<PropInfo> _propertyInfoList;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="singleEntity"></param>
        /// <param name="tableName"></param>
        /// <param name="schema"></param>
        /// <param name="sqlParams"></param>
        public QueryTable(T singleEntity, Dictionary<string, Type> propTypes, string tableName, string schema, List<SqlParameter> sqlParams)
        {
            _singleEntity = singleEntity;
            _schema = schema;
            Columns = new HashSet<string>();
            CustomColumnMappings = new Dictionary<string, string>();
            _tableName = tableName;
            Columns = new HashSet<string>();
            CustomColumnMappings = new Dictionary<string, string>();
            _sqlParams = sqlParams;
            _propertyInfoList = PropInfoList.From<T>(propTypes);
        }

        /// <summary>
        /// Add each column that you want to include in the query.
        /// </summary>
        /// <param name="columnName">Column name as represented in database</param>
        /// <returns></returns>
        public QueryAddColumn<T> AddColumn(Expression<Func<T, object>> columnName)
        {
            var propertyName = BulkOperationsHelper.GetPropertyName(columnName);
            Columns.Add(propertyName);
            return new QueryAddColumn<T>(_singleEntity, _tableName, Columns, _schema, _sqlParams, _propertyInfoList);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public QueryAddColumnList<T> AddAllColumns()
        {
            Columns = BulkOperationsHelper.GetAllValueTypeAndStringColumns(_propertyInfoList, typeof(T));

            return new QueryAddColumnList<T>(_singleEntity, _tableName, Columns, _schema, _sqlParams, _propertyInfoList);
        }

        /// <summary>
        /// Explicitly set a schema. If a schema is not added, the system default schema name 'dbo' will used.
        /// </summary>
        /// <param name="schema"></param>
        /// <returns></returns>
        public QueryTable<T> WithSchema(string schema)
        {
            if (_schema != Constants.DefaultSchemaName)
                throw new SqlBulkToolsException("Schema has already been defined in WithTable method.");

            _schema = schema;
            return this;
        }
    }
}
