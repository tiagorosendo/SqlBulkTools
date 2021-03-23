using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq.Expressions;
using SqlBulkTools.Enumeration;

namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    public abstract class AbstractOperation<T>
    {
#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member
        protected readonly BulkOperations bulk;
        // ReSharper disable InconsistentNaming
        protected ColumnDirectionType _outputIdentity;
        protected string _identityColumn;
        protected readonly Dictionary<int, T> _outputIdentityDic;
        protected bool _disableAllIndexes;
        protected int _sqlTimeout;
        protected readonly HashSet<string> _columns;
        protected readonly string _schema;
        protected readonly string _tableName;
        protected readonly Dictionary<string, string> _customColumnMappings;
        protected readonly IEnumerable<T> _list;
        protected readonly List<string> _matchTargetOn;
        protected List<PredicateCondition> _updatePredicates;
        protected List<PredicateCondition> _deletePredicates;
        protected List<SqlParameter> _parameters;
        protected readonly Dictionary<string, string> _collationColumnDic;
        protected int _conditionSortOrder;
        protected readonly BulkCopySettings _bulkCopySettings;
        protected string _tableHint;
        protected readonly Dictionary<string, int> _ordinalDic;
        protected readonly List<PropInfo> _propertyInfoList;
#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member

        /// <summary>
        /// 
        /// </summary>
        /// <param name="list"></param>
        /// <param name="tableName"></param>
        /// <param name="schema"></param>
        /// <param name="columns"></param>
        /// <param name="customColumnMappings"></param>
        /// <param name="bulkCopySettings"></param>
        /// <param name="propertyInfoList"></param>
        protected AbstractOperation(BulkOperations bulk, IEnumerable<T> list, string tableName, string schema, HashSet<string> columns,
            Dictionary<string, string> customColumnMappings, BulkCopySettings bulkCopySettings, List<PropInfo> propertyInfoList)
        {
            this.bulk = bulk;
            _list = list;
            _tableName = tableName;
            _schema = schema;
            _columns = columns;
            _disableAllIndexes = false;
            _customColumnMappings = customColumnMappings;
            _identityColumn = null;
            _collationColumnDic = new Dictionary<string, string>();
            _outputIdentityDic = new Dictionary<int, T>();
            _outputIdentity = ColumnDirectionType.Input;
            _matchTargetOn = new List<string>();
            _bulkCopySettings = bulkCopySettings;
            _tableHint = "HOLDLOCK";
            _ordinalDic = new Dictionary<string, int>();
            _propertyInfoList = propertyInfoList;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="columnName"></param>
        /// <exception cref="SqlBulkToolsException"></exception>

        protected void SetIdentity(string columnName)
        {
            if (columnName == null)
                throw new SqlBulkToolsException("SetIdentityColumn column name can't be null");

            if (_identityColumn == null)
                _identityColumn = BulkOperationsHelper.GetActualColumn(_customColumnMappings, columnName);
            else
                throw new SqlBulkToolsException("Can't have more than one identity column");
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="columnName"></param>
        /// <exception cref="SqlBulkToolsException"></exception>

        protected void SetIdentity(Expression<Func<T, object>> columnName)
        {
            SetIdentity(BulkOperationsHelper.GetPropertyName(columnName));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="parameterToCheck"></param>
        /// <typeparam name="TParameter"></typeparam>
        /// <returns></returns>
        public static TParameter GetParameterValue<TParameter>(Expression<Func<TParameter>> parameterToCheck)
        {
            TParameter parameterValue = (TParameter)parameterToCheck.Compile().Invoke();

            return parameterValue;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="columnName"></param>
        /// <param name="outputIdentity"></param>
        protected void SetIdentity(string columnName, ColumnDirectionType outputIdentity)
        {
            _outputIdentity = outputIdentity;
            SetIdentity(columnName);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="columnName"></param>
        /// <param name="outputIdentity"></param>
        protected void SetIdentity(Expression<Func<T, object>> columnName, ColumnDirectionType outputIdentity)
        {
            _outputIdentity = outputIdentity;
            SetIdentity(columnName);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="propertyName"></param>
        /// <param name="collation"></param>
        /// <returns></returns>
        protected void SetCollation(string propertyName, string collation)
        {
            if (propertyName == null)
                throw new SqlBulkToolsException("Collation can't be null");

            _collationColumnDic.Add(BulkOperationsHelper.GetActualColumn(_customColumnMappings, propertyName), collation);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <exception cref="SqlBulkToolsException"></exception>
        protected void MatchTargetCheck()
        {
            if (_matchTargetOn.Count == 0)
            {
                throw new SqlBulkToolsException("MatchTargetOn list is empty when it's required for this operation. " +
                                                    "This is usually the primary key of your table but can also be more than one " +
                                                    "column depending on your business rules.");
            }
        }
    }
}
