using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq.Expressions;
using SqlBulkTools.Enumeration;

// ReSharper disable UnusedMember.Global

// ReSharper disable once CheckNamespace
namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class DeleteQueryCondition<T>
    {
        private readonly string _tableName;
        private readonly string _schema;
        private readonly List<PredicateCondition> _whereConditions;
        private readonly List<SqlParameter> _parameters;
        private int _conditionSortOrder;
        private readonly Dictionary<string, string> _collationColumnDic;
        private readonly Dictionary<string, string> _customColumnMappings;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="schema"></param>
        ///// <param name="sqlTimeout"></param>
        public DeleteQueryCondition(string tableName, string schema) //, int sqlTimeout = 0)
        {
            _tableName = tableName;
            _schema = schema;
            _whereConditions = new List<PredicateCondition>();
            _parameters = new List<SqlParameter>();
            _collationColumnDic = new Dictionary<string, string>();
            _customColumnMappings = new Dictionary<string, string>();
            _conditionSortOrder = 1;
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
        public DeleteQueryCondition<T> CustomColumnMapping(Expression<Func<T, object>> source, string destination)
        {
            var propertyName = BulkOperationsHelper.GetPropertyName(source);
            _customColumnMappings.Add(propertyName, destination);
            return this;
        }

        /// <summary>
        /// Specify a condition.
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        public DeleteQueryReady<T> Where(Expression<Func<T, bool>> expression)
        {
            // _whereConditions list will only ever contain one element.
            BulkOperationsHelper.AddPredicate(expression, PredicateType.Where, _whereConditions, _parameters, 
                _conditionSortOrder, Constants.UniqueParamIdentifier);

            _conditionSortOrder++;

            return new DeleteQueryReady<T>(_tableName, _schema, _conditionSortOrder, 
                _whereConditions, _parameters, _collationColumnDic, _customColumnMappings);
        }

        /// <summary>
        /// Specify a condition.
        /// </summary>
        /// <param name="expression"></param>
        /// <param name="collation">Only explicitly set the collation if there is a collation conflict.</param>
        /// <returns></returns>
        /// <exception cref="SqlBulkToolsException"></exception>
        public DeleteQueryReady<T> Where(Expression<Func<T, bool>> expression, string collation)
        {
            // _whereConditions list will only ever contain one element.
            BulkOperationsHelper.AddPredicate(expression, PredicateType.Where, _whereConditions, _parameters,
                _conditionSortOrder, Constants.UniqueParamIdentifier);

            _conditionSortOrder++;

            var leftName = BulkOperationsHelper.GetExpressionLeftName(expression, PredicateType.Or, "Collation");
            _collationColumnDic.Add(BulkOperationsHelper.GetActualColumn(_customColumnMappings, leftName), collation);

            return new DeleteQueryReady<T>(_tableName, _schema, _conditionSortOrder,
                _whereConditions, _parameters, _collationColumnDic, _customColumnMappings);
        }

        /// <summary>
        /// Please understand the consequences before using this method. This will delete all records in the table. 
        /// </summary>
        /// <returns></returns>
        public DeleteAllRecordsQueryReady<T> AllRecords()
        {
            return new DeleteAllRecordsQueryReady<T>(_tableName, _schema);
        }

    }
}
