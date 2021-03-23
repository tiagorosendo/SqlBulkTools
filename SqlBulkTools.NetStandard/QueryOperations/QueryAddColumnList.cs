using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq.Expressions;

namespace SqlBulkTools.QueryOperations
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class QueryAddColumnList<T>
    {
        private readonly T _singleEntity;
        private readonly string _tableName;
        private Dictionary<string, string> CustomColumnMappings { get; }
        private HashSet<string> _columns;
        private readonly string _schema;
        private List<SqlParameter> _sqlParams;
        private readonly List<PropInfo> _propertyInfoList;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="singleEntity"></param>
        /// <param name="tableName"></param>
        /// <param name="columns"></param>
        /// <param name="schema"></param>
        /// <param name="sqlParams"></param>
        /// <param name="propertyInfoList"></param>
        public QueryAddColumnList(T singleEntity, string tableName, HashSet<string> columns, string schema,
            List<SqlParameter> sqlParams, List<PropInfo> propertyInfoList)
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
        /// 
        /// </summary>
        /// <returns></returns>
        public QueryInsertReady<T> Insert()
        {
            return new QueryInsertReady<T>(_singleEntity, _tableName, _schema, _columns, CustomColumnMappings,
                _sqlParams, _propertyInfoList);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public QueryUpsertReady<T> Upsert()
        {
            return new QueryUpsertReady<T>(_singleEntity, _tableName, _schema, _columns, CustomColumnMappings,
                 _sqlParams, _propertyInfoList);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public QueryUpdateCondition<T> Update()
        {
            return new QueryUpdateCondition<T>(_singleEntity, _tableName, _schema, _columns, CustomColumnMappings, 
                 _sqlParams, _propertyInfoList);
        }  

        /// <summary>
        /// Removes a column that you want to be excluded. 
        /// </summary>
        /// <param name="columnName"></param>
        /// <returns></returns>
        /// <exception cref="SqlBulkToolsException"></exception>
        public QueryAddColumnList<T> RemoveColumn(Expression<Func<T, object>> columnName)
        {
            var propertyName = BulkOperationsHelper.GetPropertyName(columnName);
            if (_columns.Contains(propertyName))
                _columns.Remove(propertyName);

            else
                throw new SqlBulkToolsException("Could not remove the column with name "
                    + columnName +
                    ". This could be because it's not a value or string type and therefore not included.");

            return this;
        }

        /// <summary>
        /// By default SqlBulkTools will attempt to match the model property names to SQL column names (case insensitive). 
        /// If any of your model property names do not match 
        /// the SQL table column(s) as defined in given table, then use this method to set up a custom mapping.  
        /// </summary>
        /// <param name="source">
        /// The object member that has a different name in SQL table. 
        /// </param>
        /// <param name="destination">
        /// The actual name of column as represented in SQL table. 
        /// </param>
        /// <returns></returns>
        public QueryAddColumnList<T> CustomColumnMapping(Expression<Func<T, object>> source, string destination)
        {
            var propertyName = BulkOperationsHelper.GetPropertyName(source);
            CustomColumnMappings.Add(propertyName, destination);
            return this;
        }
    }
}
