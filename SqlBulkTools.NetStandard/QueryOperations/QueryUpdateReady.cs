using SqlBulkTools.Enumeration;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

// ReSharper disable once CheckNamespace
namespace SqlBulkTools
{
    /// <summary>
    ///
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class QueryUpdateReady<T> : ITransaction
    {
        private readonly T _singleEntity;
        private readonly string _tableName;
        private readonly string _schema;
        private readonly HashSet<string> _columns;
        private readonly Dictionary<string, string> _customColumnMappings;
        private readonly List<PredicateCondition> _whereConditions;
        private readonly List<PredicateCondition> _andConditions;
        private readonly List<PredicateCondition> _orConditions;
        private readonly List<SqlParameter> _sqlParams;
        private int _conditionSortOrder;
        private string _identityColumn;
        private readonly Dictionary<string, string> _collationColumnDic;
        private int? _batchQuantity;
        private List<PropInfo> _propertyInfoList;

        /// <summary>
        ///
        /// </summary>
        /// <param name="singleEntity"></param>
        /// <param name="tableName"></param>
        /// <param name="schema"></param>
        /// <param name="columns"></param>
        /// <param name="customColumnMappings"></param>
        /// <param name="conditionSortOrder"></param>
        /// <param name="whereConditions"></param>
        /// <param name="sqlParams"></param>
        /// <param name="collationColumnDic"></param>
        /// <param name="propertyInfoList"></param>
        public QueryUpdateReady(T singleEntity, string tableName, string schema, HashSet<string> columns, Dictionary<string, string> customColumnMappings,
            int conditionSortOrder, List<PredicateCondition> whereConditions, List<SqlParameter> sqlParams, Dictionary<string, string> collationColumnDic, List<PropInfo> propertyInfoList)
        {
            _singleEntity = singleEntity;
            _tableName = tableName;
            _schema = schema;
            _columns = columns;
            _customColumnMappings = customColumnMappings;
            _conditionSortOrder = conditionSortOrder;
            _whereConditions = whereConditions;
            _andConditions = new List<PredicateCondition>();
            _orConditions = new List<PredicateCondition>();
            _sqlParams = sqlParams;
            _identityColumn = null;
            _collationColumnDic = collationColumnDic;
            _batchQuantity = null;
            _propertyInfoList = propertyInfoList;
        }

        /// <summary>
        /// Sets the identity column for the table.
        /// </summary>
        /// <param name="columnName"></param>
        /// <returns></returns>
        public QueryUpdateReady<T> SetIdentityColumn(Expression<Func<T, object>> columnName)
        {
            var propertyName = BulkOperationsHelper.GetPropertyName(columnName);

            if (propertyName == null)
                throw new SqlBulkToolsException("SetIdentityColumn column name can't be null");

            if (_identityColumn == null)
                _identityColumn = BulkOperationsHelper.GetActualColumn(_customColumnMappings, propertyName);
            else
                throw new SqlBulkToolsException("Can't have more than one identity column");

            return this;
        }

        /// <summary>
        /// Specify an additional condition to match on.
        /// </summary>
        /// <param name="expression">Only explicitly set the collation if there is a collation conflict.</param>
        /// <returns></returns>
        /// <exception cref="SqlBulkToolsException"></exception>
        public QueryUpdateReady<T> And(Expression<Func<T, bool>> expression)
        {
            BulkOperationsHelper.AddPredicate(expression, PredicateType.And, _andConditions, _sqlParams, _conditionSortOrder, appendParam: Constants.UniqueParamIdentifier);
            _conditionSortOrder++;
            return this;
        }

        /// <summary>
        /// Specify an additional condition to match on.
        /// </summary>
        /// <param name="expression">Only explicitly set the collation if there is a collation conflict.</param>
        /// <param name="collation"></param>
        /// <returns></returns>
        /// <exception cref="SqlBulkToolsException">Only explicitly set the collation if there is a collation conflict.</exception>
        public QueryUpdateReady<T> And(Expression<Func<T, bool>> expression, string collation)
        {
            BulkOperationsHelper.AddPredicate(expression, PredicateType.And, _andConditions, _sqlParams, _conditionSortOrder, appendParam: Constants.UniqueParamIdentifier);
            _conditionSortOrder++;

            string leftName = BulkOperationsHelper.GetExpressionLeftName(expression, PredicateType.And, "Collation");
            _collationColumnDic.Add(BulkOperationsHelper.GetActualColumn(_customColumnMappings, leftName), collation);

            return this;
        }

        /// <summary>
        /// Specify an additional condition to match on.
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        /// <exception cref="SqlBulkToolsException"></exception>
        public QueryUpdateReady<T> Or(Expression<Func<T, bool>> expression)
        {
            BulkOperationsHelper.AddPredicate(expression, PredicateType.Or, _orConditions, _sqlParams, _conditionSortOrder, appendParam: Constants.UniqueParamIdentifier);
            _conditionSortOrder++;

            return this;
        }

        /// <summary>
        /// Specify an additional condition to match on.
        /// </summary>
        /// <param name="expression"></param>
        /// <param name="collation">Only explicitly set the collation if there is a collation conflict.</param>
        /// <returns></returns>
        /// <exception cref="SqlBulkToolsException"></exception>
        public QueryUpdateReady<T> Or(Expression<Func<T, bool>> expression, string collation)
        {
            BulkOperationsHelper.AddPredicate(expression, PredicateType.Or, _orConditions, _sqlParams, _conditionSortOrder, appendParam: Constants.UniqueParamIdentifier);
            _conditionSortOrder++;

            string leftName = BulkOperationsHelper.GetExpressionLeftName(expression, PredicateType.Or, "Collation");
            _collationColumnDic.Add(BulkOperationsHelper.GetActualColumn(_customColumnMappings, leftName), collation);

            return this;
        }

        /// <summary>
        /// The maximum number of records to update per transaction.
        /// </summary>
        /// <param name="batchQuantity"></param>
        /// <returns></returns>
        public QueryUpdateReady<T> SetBatchQuantity(int batchQuantity)
        {
            _batchQuantity = batchQuantity;
            return this;
        }

        public int Commit(IDbConnection connection, IDbTransaction transaction = null)
        {
            if (connection is SqlConnection == false)
                throw new ArgumentException("Parameter must be a SqlConnection instance");

            return Commit((SqlConnection)connection, (SqlTransaction)transaction);
        }

        public Task<int> CommitAsync(IDbConnection connection, IDbTransaction transaction = null, CancellationToken cancellationToken = default)
        {
            if (connection is SqlConnection == false)
                throw new ArgumentException("Parameter must be a SqlConnection instance");

            return CommitAsync((SqlConnection)connection, (SqlTransaction)transaction, cancellationToken);
        }

        /// <summary>
        /// Commits a transaction to database. A valid setup must exist for the operation to be
        /// successful.
        /// </summary>
        /// <param name="connection"></param>
        /// <returns></returns>
        public int Commit(SqlConnection connection, SqlTransaction transaction)
        {
            int affectedRows = 0;
            if (_singleEntity == null)
            {
                return affectedRows;
            }

            if (connection.State == ConnectionState.Closed)
                connection.Open();

            SqlCommand command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;

            command.CommandText = GetQuery(connection);

            if (_sqlParams.Count > 0)
            {
                command.Parameters.AddRange(_sqlParams.ToArray());
            }

            affectedRows = command.ExecuteNonQuery();

            return affectedRows;
        }

        /// <summary>
        /// Commits a transaction to database asynchronously. A valid setup must exist for the operation to be
        /// successful.
        /// </summary>
        /// <param name="connection"></param>
        /// <returns></returns>
        public async Task<int> CommitAsync(SqlConnection connection, SqlTransaction transaction, CancellationToken cancellationToken)
        {
            int affectedRows = 0;
            if (_singleEntity == null)
            {
                return affectedRows;
            }

            if (connection.State == ConnectionState.Closed)
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

            SqlCommand command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;

            command.CommandText = GetQuery(connection);

            if (_sqlParams.Count > 0)
            {
                command.Parameters.AddRange(_sqlParams.ToArray());
            }

            affectedRows = await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

            return affectedRows;
        }

        private string GetQuery(SqlConnection connection)
        {
            string fullQualifiedTableName = BulkOperationsHelper.GetFullQualifyingTableName(connection.Database, _schema,
                _tableName);

            BulkOperationsHelper.AddSqlParamsForQuery(_propertyInfoList, _sqlParams, _columns, _singleEntity, customColumns: _customColumnMappings);
            var concatenatedQuery = _whereConditions.Concat(_andConditions).Concat(_orConditions).OrderBy(x => x.SortOrder);
            BulkOperationsHelper.DoColumnMappings(_customColumnMappings, _columns);

            string batchQtyStart = _batchQuantity != null ? "UpdateMore:\n" : string.Empty;
            string batchQty = _batchQuantity != null ? $"TOP ({_batchQuantity}) " : string.Empty;
            string batchQtyRepeat = _batchQuantity != null ? $"\nIF @@ROWCOUNT != 0\ngoto UpdateMore" : string.Empty;

            string comm = $"{batchQtyStart}UPDATE {batchQty}{fullQualifiedTableName} " +
            $"{BulkOperationsHelper.BuildUpdateSet(_columns, null, _identityColumn)}" +
            $"{BulkOperationsHelper.BuildPredicateQuery(concatenatedQuery, _collationColumnDic, _customColumnMappings)}{batchQtyRepeat}";

            return comm;
        }
    }
}
