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
    public class BulkDelete<T> : AbstractOperation<T>, ITransaction
    {
        private Dictionary<string, bool> _nullableColumnDic;

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
        public BulkDelete(BulkOperations bulk, IEnumerable<T> list, string tableName, string schema, HashSet<string> columns,
            Dictionary<string, string> customColumnMappings, BulkCopySettings bulkCopySettings, List<PropInfo> propertyInfoList)
            :
            base(bulk, list, tableName, schema, columns, customColumnMappings, bulkCopySettings, propertyInfoList)
        {
            _deletePredicates = new List<PredicateCondition>();
            _parameters = new List<SqlParameter>();
            _conditionSortOrder = 1;
            _nullableColumnDic = new Dictionary<string, bool>();
        }

        /// <summary>
        /// At least one MatchTargetOn is required for correct configuration. MatchTargetOn is the matching clause for evaluating
        /// each row in table. This is usally set to the unique identifier in the table (e.g. Id). Multiple MatchTargetOn members are allowed
        /// for matching composite relationships.
        /// </summary>
        /// <param name="columnName"></param>
        /// <returns></returns>
        public BulkDelete<T> MatchTargetOn(string columnName)
        {
            if (string.IsNullOrWhiteSpace(columnName))
                throw new ArgumentNullException(nameof(columnName));

            _matchTargetOn.Add(columnName);

            return this;
        }

        /// <summary>
        /// At least one MatchTargetOn is required for correct configuration. MatchTargetOn is the matching clause for evaluating
        /// each row in table. This is usally set to the unique identifier in the table (e.g. Id). Multiple MatchTargetOn members are allowed
        /// for matching composite relationships.
        /// </summary>
        /// <param name="columnName"></param>
        /// <returns></returns>
        public BulkDelete<T> MatchTargetOn(Expression<Func<T, object>> columnName)
        {
            return MatchTargetOn(BulkOperationsHelper.GetPropertyName(columnName));
        }

        /// <summary>
        /// At least one MatchTargetOn is required for correct configuration. MatchTargetOn is the matching clause for evaluating
        /// each row in table. This is usally set to the unique identifier in the table (e.g. Id). Multiple MatchTargetOn members are allowed
        /// for matching composite relationships.
        /// </summary>
        /// <param name="columnName"></param>
        /// <param name="collation">Only explicitly set the collation if there is a collation conflict.</param>
        /// <returns></returns>
        public BulkDelete<T> MatchTargetOn(string columnName, string collation)
        {
            SetCollation(columnName, collation);
            return MatchTargetOn(columnName);
        }

        /// <summary>
        /// At least one MatchTargetOn is required for correct configuration. MatchTargetOn is the matching clause for evaluating
        /// each row in table. This is usally set to the unique identifier in the table (e.g. Id). Multiple MatchTargetOn members are allowed
        /// for matching composite relationships.
        /// </summary>
        /// <param name="columnName"></param>
        /// <param name="collation">Only explicitly set the collation if there is a collation conflict.</param>
        /// <returns></returns>
        public BulkDelete<T> MatchTargetOn(Expression<Func<T, object>> columnName, string collation)
        {
            return MatchTargetOn(BulkOperationsHelper.GetPropertyName(columnName), collation);
        }

        /// <summary>
        /// Sets the table hint to be used in the merge query. HOLDLOCk is the default that will be used if one is not set.
        /// </summary>
        /// <param name="tableHint"></param>
        /// <returns></returns>
        public BulkDelete<T> SetTableHint(string tableHint)
        {
            _tableHint = tableHint;
            return this;
        }

        /// <summary>
        /// Only delete records when the target satisfies a speicific requirement. This is used in conjunction with MatchTargetOn.
        /// See help docs for examples.
        /// </summary>
        /// <param name="predicate"></param>
        /// <returns></returns>
        public BulkDelete<T> DeleteWhen(Expression<Func<T, bool>> predicate)
        {
            BulkOperationsHelper.AddPredicate(predicate, PredicateType.Delete, _deletePredicates, _parameters, _conditionSortOrder, Constants.UniqueParamIdentifier);
            _conditionSortOrder++;

            return this;
        }

        /// <summary>
        /// Sets the identity column for the table. Required if an Identity column exists in table and one of the two
        /// following conditions is met: (1) MatchTargetOn list contains an identity column (2) AddAllColumns is used in setup.
        /// </summary>
        /// <param name="columnName"></param>
        /// <returns></returns>
        public BulkDelete<T> SetIdentityColumn(string columnName)
        {
            base.SetIdentity(columnName);
            return this;
        }

        /// <summary>
        /// Sets the identity column for the table. Required if an Identity column exists in table and one of the two
        /// following conditions is met: (1) MatchTargetOn list contains an identity column (2) AddAllColumns is used in setup.
        /// </summary>
        /// <param name="columnName"></param>
        /// <returns></returns>
        public BulkDelete<T> SetIdentityColumn(Expression<Func<T, object>> columnName)
        {
            base.SetIdentity(columnName);
            return this;
        }

        /// <summary>
        /// Sets the identity column for the table. Required if an Identity column exists in table and one of the two
        /// following conditions is met: (1) MatchTargetOn list contains an identity column (2) AddAllColumns is used in setup.
        /// </summary>
        /// <param name="columnName"></param>
        /// <param name="outputIdentity"></param>
        /// <returns></returns>
        public BulkDelete<T> SetIdentityColumn(string columnName, ColumnDirectionType outputIdentity)
        {
            base.SetIdentity(columnName, outputIdentity);
            return this;
        }

        /// <summary>
        /// Sets the identity column for the table. Required if an Identity column exists in table and one of the two
        /// following conditions is met: (1) MatchTargetOn list contains an identity column (2) AddAllColumns is used in setup.
        /// </summary>
        /// <param name="columnName"></param>
        /// <param name="outputIdentity"></param>
        /// <returns></returns>
        public BulkDelete<T> SetIdentityColumn(Expression<Func<T, object>> columnName, ColumnDirectionType outputIdentity)
        {
            base.SetIdentity(columnName, outputIdentity);
            return this;
        }

        public BulkDelete<T> WithTimeout(int timeout)
        {
            this._sqlTimeout = timeout;
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
            int affectedRecords = 0;
            if (!_list.Any())
            {
                return affectedRecords;
            }

            base.MatchTargetCheck();

            DataTable dt = BulkOperationsHelper.CreateDataTable<T>(_propertyInfoList, _columns, _customColumnMappings, _ordinalDic, _matchTargetOn, _outputIdentity);
            dt = BulkOperationsHelper.ConvertListToDataTable(_propertyInfoList, dt, _list, _columns, _ordinalDic, _outputIdentityDic);

            // Must be after ToDataTable is called.
            BulkOperationsHelper.DoColumnMappings(_customColumnMappings, _columns);
            BulkOperationsHelper.DoColumnMappings(_customColumnMappings, _deletePredicates);

            if (connection.State == ConnectionState.Closed)
                connection.Open();

            var dtCols = BulkOperationsHelper.GetDatabaseSchema(bulk, connection, _schema, _tableName);

            SqlCommand command = connection.CreateCommand();
            command.Connection = connection;
            command.CommandTimeout = _sqlTimeout;
            command.Transaction = transaction;

            _nullableColumnDic = BulkOperationsHelper.GetNullableColumnDic(dtCols);

            //Creating temp table on database
            command.CommandText = BulkOperationsHelper.BuildCreateTempTable(_columns, dtCols, _outputIdentity);
            command.ExecuteNonQuery();

            BulkOperationsHelper.InsertToTmpTable(connection, dt, _bulkCopySettings, transaction);

            string comm = BulkOperationsHelper.GetOutputCreateTableCmd(_outputIdentity, Constants.TempOutputTableName,
            OperationType.InsertOrUpdate, _identityColumn);

            if (!string.IsNullOrWhiteSpace(comm))
            {
                command.CommandText = comm;
                command.ExecuteNonQuery();
            }

            comm = GetCommand(connection);

            command.CommandText = comm;

            if (_parameters.Count > 0)
            {
                command.Parameters.AddRange(_parameters.ToArray());
            }

            affectedRecords = command.ExecuteNonQuery();

            if (_outputIdentity == ColumnDirectionType.InputOutput)
            {
                BulkOperationsHelper.LoadFromTmpOutputTable(command, _propertyInfoList, _identityColumn, _outputIdentityDic, OperationType.Delete, _list);
            }

            return affectedRecords;
        }

        /// <summary>
        /// Commits a transaction to database asynchronously. A valid setup must exist for the operation to be
        /// successful.
        /// </summary>
        /// <param name="connection"></param>
        /// <returns></returns>
        public async Task<int> CommitAsync(SqlConnection connection, SqlTransaction transaction, CancellationToken cancellationToken)
        {
            int affectedRecords = 0;
            if (!_list.Any())
            {
                return affectedRecords;
            }

            base.MatchTargetCheck();

            DataTable dt = BulkOperationsHelper.CreateDataTable<T>(_propertyInfoList, _columns, _customColumnMappings, _ordinalDic, _matchTargetOn, _outputIdentity);
            dt = BulkOperationsHelper.ConvertListToDataTable(_propertyInfoList, dt, _list, _columns, _ordinalDic, _outputIdentityDic);

            // Must be after ToDataTable is called.
            BulkOperationsHelper.DoColumnMappings(_customColumnMappings, _columns);
            BulkOperationsHelper.DoColumnMappings(_customColumnMappings, _deletePredicates);

            if (connection.State != ConnectionState.Open)
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

            var dtCols = BulkOperationsHelper.GetDatabaseSchema(bulk, connection, _schema, _tableName);

            SqlCommand command = connection.CreateCommand();
            command.Connection = connection;
            command.CommandTimeout = _sqlTimeout;
            command.Transaction = transaction;

            _nullableColumnDic = BulkOperationsHelper.GetNullableColumnDic(dtCols);

            //Creating temp table on database
            command.CommandText = BulkOperationsHelper.BuildCreateTempTable(_columns, dtCols, _outputIdentity);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

            await BulkOperationsHelper.InsertToTmpTableAsync(connection, dt, _bulkCopySettings, transaction, cancellationToken).ConfigureAwait(false);

            string comm = BulkOperationsHelper.GetOutputCreateTableCmd(_outputIdentity, Constants.TempOutputTableName,
            OperationType.InsertOrUpdate, _identityColumn);

            if (!string.IsNullOrWhiteSpace(comm))
            {
                command.CommandText = comm;
                await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            comm = GetCommand(connection);

            command.CommandText = comm;

            if (_parameters.Count > 0)
            {
                command.Parameters.AddRange(_parameters.ToArray());
            }

            affectedRecords = command.ExecuteNonQuery();

            if (_outputIdentity == ColumnDirectionType.InputOutput)
            {
                await BulkOperationsHelper.LoadFromTmpOutputTableAsync(command, _propertyInfoList, _identityColumn, _outputIdentityDic, OperationType.Delete, _list, cancellationToken).ConfigureAwait(false);
            }

            return affectedRecords;
        }

        private string GetCommand(SqlConnection connection)
        {
            string comm = "MERGE INTO " + BulkOperationsHelper.GetFullQualifyingTableName(connection.Database, _schema, _tableName) + $" WITH ({_tableHint}) AS Target " +
                          "USING " + Constants.TempTableName + " AS Source " +
                          BulkOperationsHelper.BuildJoinConditionsForInsertOrUpdate(_matchTargetOn.ToArray(),
                          Constants.SourceAlias, Constants.TargetAlias, base._collationColumnDic, _nullableColumnDic) +
                          "WHEN MATCHED " + BulkOperationsHelper.BuildPredicateQuery(_matchTargetOn.ToArray(), _deletePredicates, Constants.TargetAlias, base._collationColumnDic) +
                          "THEN DELETE " +
                          BulkOperationsHelper.GetOutputIdentityCmd(_identityColumn, _outputIdentity, Constants.TempOutputTableName,
                          OperationType.Delete) + "; " +
                          "DROP TABLE " + Constants.TempTableName + ";";

            return comm;
        }
    }
}