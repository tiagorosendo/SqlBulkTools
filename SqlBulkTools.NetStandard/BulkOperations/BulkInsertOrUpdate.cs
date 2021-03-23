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
    public class BulkInsertOrUpdate<T> : AbstractOperation<T>, ITransaction
    {
        private bool _deleteWhenNotMatchedFlag;
        private readonly HashSet<string> _excludeFromUpdate;
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
        public BulkInsertOrUpdate(BulkOperations bulk, IEnumerable<T> list, string tableName, string schema, HashSet<string> columns,
            Dictionary<string, string> customColumnMappings, BulkCopySettings bulkCopySettings, List<PropInfo> propertyInfoList) :

            base(bulk, list, tableName, schema, columns, customColumnMappings, bulkCopySettings, propertyInfoList)
        {
            _deleteWhenNotMatchedFlag = false;
            _updatePredicates = new List<PredicateCondition>();
            _deletePredicates = new List<PredicateCondition>();
            _parameters = new List<SqlParameter>();
            _conditionSortOrder = 1;
            _excludeFromUpdate = new HashSet<string>();
            _nullableColumnDic = new Dictionary<string, bool>();
        }

        /// <summary>
        /// At least one MatchTargetOn is required for correct configuration. MatchTargetOn is the matching clause for evaluating
        /// each row in table. This is usally set to the unique identifier in the table (e.g. Id). Multiple MatchTargetOn members are allowed
        /// for matching composite relationships.
        /// </summary>
        /// <param name="columnName"></param>
        /// <returns></returns>
        public BulkInsertOrUpdate<T> MatchTargetOn(string columnName)
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
        public BulkInsertOrUpdate<T> MatchTargetOn(Expression<Func<T, object>> columnName)
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
        public BulkInsertOrUpdate<T> MatchTargetOn(string columnName, string collation)
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
        public BulkInsertOrUpdate<T> MatchTargetOn(Expression<Func<T, object>> columnName, string collation)
        {
            return MatchTargetOn(BulkOperationsHelper.GetPropertyName(columnName), collation);
        }

        /// <summary>
        /// Sets the identity column for the table. Required if an Identity column exists in table and one of the two
        /// following conditions is met: (1) MatchTargetOn list contains an identity column (2) AddAllColumns is used in setup.
        /// </summary>
        /// <param name="columnName"></param>
        /// <returns></returns>
        public BulkInsertOrUpdate<T> SetIdentityColumn(string columnName)
        {
            SetIdentity(columnName);
            return this;
        }

        /// <summary>
        /// Sets the identity column for the table. Required if an Identity column exists in table and one of the two
        /// following conditions is met: (1) MatchTargetOn list contains an identity column (2) AddAllColumns is used in setup.
        /// </summary>
        /// <param name="columnName"></param>
        /// <returns></returns>
        public BulkInsertOrUpdate<T> SetIdentityColumn(Expression<Func<T, object>> columnName)
        {
            SetIdentity(columnName);
            return this;
        }

        /// <summary>
        /// Sets the identity column for the table. Required if an Identity column exists in table and one of the two
        /// following conditions is met: (1) MatchTargetOn list contains an identity column (2) AddAllColumns is used in setup.
        /// </summary>
        /// <param name="columnName"></param>
        /// <param name="outputIdentity"></param>
        /// <returns></returns>
        public BulkInsertOrUpdate<T> SetIdentityColumn(string columnName, ColumnDirectionType outputIdentity)
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
        public BulkInsertOrUpdate<T> SetIdentityColumn(Expression<Func<T, object>> columnName, ColumnDirectionType outputIdentity)
        {
            base.SetIdentity(columnName, outputIdentity);
            return this;
        }

        /// <summary>
        /// Exclude a property from the update statement. Useful for when you want to include CreatedDate or Guid for inserts only.
        /// </summary>
        /// <param name="columnName"></param>
        /// <returns></returns>
        public BulkInsertOrUpdate<T> ExcludeColumnFromUpdate(string columnName)
        {
            if (columnName == null)
                throw new SqlBulkToolsException("ExcludeColumnFromUpdate column name can't be null");

            if (!_columns.Contains(columnName))
            {
                throw new SqlBulkToolsException("ExcludeColumnFromUpdate could not exclude column from update because column could not " +
                                                "be recognised. Call AddAllColumns() or AddColumn() for this column first.");
            }
            _excludeFromUpdate.Add(columnName);

            return this;
        }

        /// <summary>
        /// Exclude a property from the update statement. Useful for when you want to include CreatedDate or Guid for inserts only.
        /// </summary>
        /// <param name="columnName"></param>
        /// <returns></returns>
        public BulkInsertOrUpdate<T> ExcludeColumnFromUpdate(Expression<Func<T, object>> columnName)
        {
            var propertyName = BulkOperationsHelper.GetPropertyName(columnName);

            return ExcludeColumnFromUpdate(propertyName);
        }

        /// <summary>
        /// Only delete records when the target satisfies a speicific requirement. This is used in conjunction with MatchTargetOn.
        /// See help docs for examples
        /// </summary>
        /// <param name="predicate"></param>
        /// <returns></returns>
        public BulkInsertOrUpdate<T> DeleteWhen(Expression<Func<T, bool>> predicate)
        {
            _deleteWhenNotMatchedFlag = true;
            BulkOperationsHelper.AddPredicate(predicate, PredicateType.Delete, _deletePredicates, _parameters, _conditionSortOrder, Constants.UniqueParamIdentifier);
            _conditionSortOrder++;

            return this;
        }

        /// <summary>
        /// Sets the table hint to be used in the merge query. HOLDLOCk is the default that will be used if one is not set.
        /// </summary>
        /// <param name="tableHint"></param>
        /// <returns></returns>
        public BulkInsertOrUpdate<T> SetTableHint(string tableHint)
        {
            _tableHint = tableHint;
            return this;
        }

        /// <summary>
        /// Only update records when the target satisfies a speicific requirement. This is used in conjunction with MatchTargetOn.
        /// See help docs for examples.
        /// </summary>
        /// <param name="predicate"></param>
        /// <returns></returns>
        /// <exception cref="SqlBulkToolsException"></exception>
        public BulkInsertOrUpdate<T> UpdateWhen(Expression<Func<T, bool>> predicate)
        {
            BulkOperationsHelper.AddPredicate(predicate, PredicateType.Update, _updatePredicates, _parameters, _conditionSortOrder, Constants.UniqueParamIdentifier);
            _conditionSortOrder++;

            return this;
        }

        /// <summary>
        /// If a target record can't be matched to a source record, it's deleted. Notes: (1) This is false by default. (2) Use at your own risk.
        /// </summary>
        /// <param name="flag"></param>
        /// <returns></returns>
        public BulkInsertOrUpdate<T> DeleteWhenNotMatched(bool flag)
        {
            _deleteWhenNotMatchedFlag = flag;
            return this;
        }


        public BulkInsertOrUpdate<T> WithTimeout(int timeout)
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
        /// <exception cref="SqlBulkToolsException"></exception>
        /// <exception cref="IdentityException"></exception>
        public int Commit(SqlConnection connection, SqlTransaction transaction)
        {
            int affectedRows = 0;
            if (!_list.Any())
            {
                return affectedRows;
            }

            if (!_deleteWhenNotMatchedFlag && _deletePredicates.Count > 0)
                throw new SqlBulkToolsException($"{BulkOperationsHelper.GetPredicateMethodName(PredicateType.Delete)} only usable on BulkInsertOrUpdate " +
                                                $"method when 'DeleteWhenNotMatched' is set to true.");

            base.MatchTargetCheck();

            DataTable dt = BulkOperationsHelper.CreateDataTable<T>(_propertyInfoList, _columns, _customColumnMappings, _ordinalDic, _matchTargetOn, _outputIdentity);
            dt = BulkOperationsHelper.ConvertListToDataTable(_propertyInfoList, dt, _list, _columns, _ordinalDic, _outputIdentityDic);

            // Must be after ToDataTable is called.
            BulkOperationsHelper.DoColumnMappings(_customColumnMappings, _columns, _matchTargetOn);
            BulkOperationsHelper.DoColumnMappings(_customColumnMappings, _deletePredicates);
            BulkOperationsHelper.DoColumnMappings(_customColumnMappings, _updatePredicates);

            if (connection.State != ConnectionState.Open)
                connection.Open();

            var dtCols = BulkOperationsHelper.GetDatabaseSchema(bulk, connection, _schema, _tableName);

            try
            {
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

                affectedRows = command.ExecuteNonQuery();

                if (_outputIdentity == ColumnDirectionType.InputOutput)
                {
                    BulkOperationsHelper.LoadFromTmpOutputTable(command, _propertyInfoList, _identityColumn, _outputIdentityDic, OperationType.InsertOrUpdate, _list);
                }

                return affectedRows;
            }
            catch (SqlException e)
            {
                for (int i = 0; i < e.Errors.Count; i++)
                {
                    // Error 8102 is identity error.
                    if (e.Errors[i].Number == 8102)
                    {
                        // Expensive but neccessary to inform user of an important configuration setup.
                        throw new IdentityException(e.Errors[i].Message);
                    }
                }

                throw;
            }
        }

        /// <summary>
        /// Commits a transaction to database asynchronously. A valid setup must exist for the operation to be
        /// successful.
        /// </summary>
        /// <param name="connection"></param>
        /// <returns></returns>
        /// <exception cref="SqlBulkToolsException"></exception>
        /// <exception cref="IdentityException"></exception>
        public async Task<int> CommitAsync(SqlConnection connection, SqlTransaction transaction, CancellationToken cancellationToken)
        {
            int affectedRows = 0;
            if (!_list.Any())
            {
                return affectedRows;
            }

            if (!_deleteWhenNotMatchedFlag && _deletePredicates.Count > 0)
                throw new SqlBulkToolsException($"{BulkOperationsHelper.GetPredicateMethodName(PredicateType.Delete)} only usable on BulkInsertOrUpdate " +
                                                $"method when 'DeleteWhenNotMatched' is set to true.");

            base.MatchTargetCheck();

            DataTable dt = BulkOperationsHelper.CreateDataTable<T>(_propertyInfoList, _columns, _customColumnMappings, _ordinalDic, _matchTargetOn, _outputIdentity);
            dt = BulkOperationsHelper.ConvertListToDataTable(_propertyInfoList, dt, _list, _columns, _ordinalDic, _outputIdentityDic);

            // Must be after ToDataTable is called.
            BulkOperationsHelper.DoColumnMappings(_customColumnMappings, _columns, _matchTargetOn);
            BulkOperationsHelper.DoColumnMappings(_customColumnMappings, _deletePredicates);
            BulkOperationsHelper.DoColumnMappings(_customColumnMappings, _updatePredicates);

            if (connection.State != ConnectionState.Open)
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

            var dtCols = BulkOperationsHelper.GetDatabaseSchema(bulk, connection, _schema, _tableName);

            try
            {
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

                affectedRows = await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

                if (_outputIdentity == ColumnDirectionType.InputOutput)
                {
                    await BulkOperationsHelper.LoadFromTmpOutputTableAsync(command, _propertyInfoList, _identityColumn, _outputIdentityDic, OperationType.InsertOrUpdate, _list, cancellationToken).ConfigureAwait(false);
                }

                return affectedRows;
            }
            catch (SqlException e)
            {
                for (int i = 0; i < e.Errors.Count; i++)
                {
                    // Error 8102 is identity error.
                    if (e.Errors[i].Number == 8102)
                    {
                        // Expensive but neccessary to inform user of an important configuration setup.
                        throw new IdentityException(e.Errors[i].Message);
                    }
                }

                throw;
            }
        }

        private string GetCommand(SqlConnection connection)
        {
            string comm =
                    "MERGE INTO " + BulkOperationsHelper.GetFullQualifyingTableName(connection.Database, _schema, _tableName) +
                    $" WITH ({_tableHint}) AS Target " +
                    "USING " + Constants.TempTableName + " AS Source " +
                    BulkOperationsHelper.BuildJoinConditionsForInsertOrUpdate(_matchTargetOn.ToArray(),
                        Constants.SourceAlias, Constants.TargetAlias, base._collationColumnDic, _nullableColumnDic) +
                    "WHEN MATCHED " + BulkOperationsHelper.BuildPredicateQuery(_matchTargetOn.ToArray(), _updatePredicates, Constants.TargetAlias, base._collationColumnDic) +
                    "THEN UPDATE " +
                    BulkOperationsHelper.BuildUpdateSet(_columns, Constants.SourceAlias, Constants.TargetAlias, _identityColumn, _excludeFromUpdate) +
                    "WHEN NOT MATCHED BY TARGET THEN " +
                    BulkOperationsHelper.BuildInsertSet(_columns, Constants.SourceAlias, _identityColumn) +
                    (_deleteWhenNotMatchedFlag ? " WHEN NOT MATCHED BY SOURCE " + BulkOperationsHelper.BuildPredicateQuery(_matchTargetOn.ToArray(),
                    _deletePredicates, Constants.TargetAlias, base._collationColumnDic) +
                    "THEN DELETE " : " ") +
                    BulkOperationsHelper.GetOutputIdentityCmd(_identityColumn, _outputIdentity, Constants.TempOutputTableName,
                        OperationType.InsertOrUpdate) + "; " +
                    "DROP TABLE " + Constants.TempTableName + ";";

            return comm;
        }
    }
}