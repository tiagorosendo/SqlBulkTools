using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq.Expressions;
using SqlBulkTools.Enumeration;

// ReSharper disable once CheckNamespace
namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class QueryUpdateCondition<T>
    {
        private readonly T _singleEntity;
        private readonly string _tableName;
        private readonly string _schema;
        private readonly HashSet<string> _columns;
        private readonly Dictionary<string, string> _customColumnMappings;
        private readonly List<PredicateCondition> _whereConditions;
        private int _conditionSortOrder;
        private readonly List<SqlParameter> _sqlParams;
        private readonly Dictionary<string, string> _collationColumnDic;
        private readonly List<PropInfo> _propertyInfoList;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="singleEntity"></param>
        /// <param name="tableName"></param>
        /// <param name="schema"></param>
        /// <param name="columns"></param>
        /// <param name="customColumnMappings"></param>
        /// <param name="sqlParams"></param>
        /// <param name="propertyInfoList"></param>
        public QueryUpdateCondition(T singleEntity, string tableName, string schema, HashSet<string> columns, 
            Dictionary<string, string> customColumnMappings, List<SqlParameter> sqlParams, List<PropInfo> propertyInfoList)
        {
            _singleEntity = singleEntity;
            _tableName = tableName;
            _schema = schema;
            _columns = columns;
            _customColumnMappings = customColumnMappings;
            _whereConditions = new List<PredicateCondition>();
            _collationColumnDic = new Dictionary<string, string>();
            _sqlParams = sqlParams;
            _conditionSortOrder = 1;
            _propertyInfoList = propertyInfoList;
        }

        /// <summary>
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        public QueryUpdateReady<T> Where(Expression<Func<T, bool>> expression)
        {
            // _whereConditions list will only ever contain one element.
            BulkOperationsHelper.AddPredicate(expression, PredicateType.Where, _whereConditions, _sqlParams, 
                _conditionSortOrder, appendParam: Constants.UniqueParamIdentifier);

            _conditionSortOrder++;

            return new QueryUpdateReady<T>(_singleEntity, _tableName, _schema, _columns, _customColumnMappings, 
                _conditionSortOrder, _whereConditions, _sqlParams, _collationColumnDic, _propertyInfoList);
        }       

        /// <summary>
        /// Specify a condition.
        /// </summary>
        /// <param name="expression"></param>
        /// <param name="collation">Only explicitly set the collation if there is a collation conflict.</param>
        /// <returns></returns>
        /// <exception cref="SqlBulkToolsException"></exception>
        public QueryUpdateReady<T> Where(Expression<Func<T, bool>> expression, string collation)
        {
            // _whereConditions list will only ever contain one element.
            BulkOperationsHelper.AddPredicate(expression, PredicateType.Where, _whereConditions, _sqlParams,
                _conditionSortOrder, Constants.UniqueParamIdentifier);

            _conditionSortOrder++;

            string leftName = BulkOperationsHelper.GetExpressionLeftName(expression, PredicateType.Or, "Collation");
            _collationColumnDic.Add(BulkOperationsHelper.GetActualColumn(_customColumnMappings, leftName), collation);

            return new QueryUpdateReady<T>(_singleEntity, _tableName, _schema, _columns, _customColumnMappings,
                _conditionSortOrder, _whereConditions, _sqlParams, _collationColumnDic, _propertyInfoList);
        }
    }
}
