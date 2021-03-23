using System;
using System.Data;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

// ReSharper disable once CheckNamespace
namespace SqlBulkTools
{
    /// <summary>
    ///
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class DeleteAllRecordsQueryReady<T> : ITransaction
    {
        private readonly string _tableName;
        private readonly string _schema;
        private int? _batchQuantity;

        /// <summary>
        ///
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="schema"></param>
        public DeleteAllRecordsQueryReady(string tableName, string schema)
        {
            _tableName = tableName;
            _schema = schema;
            _batchQuantity = null;
        }

        /// <summary>
        /// The maximum number of records to delete per transaction.
        /// </summary>
        /// <param name="batchQuantity"></param>
        /// <returns></returns>
        public DeleteAllRecordsQueryReady<T> SetBatchQuantity(int batchQuantity)
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
            if (connection.State == ConnectionState.Closed)
                connection.Open();

            SqlCommand command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;

            command.CommandText = GetQuery(connection);

            int affectedRows = command.ExecuteNonQuery();

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
            if (connection.State == ConnectionState.Closed)
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

            SqlCommand command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;

            command.CommandText = command.CommandText = GetQuery(connection);

            int affectedRows = await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

            return affectedRows;
        }

        private string GetQuery(SqlConnection connection)
        {
            string fullQualifiedTableName = BulkOperationsHelper.GetFullQualifyingTableName(connection.Database, _schema,
                 _tableName);

            string batchQtyStart = _batchQuantity != null ? "DeleteMore:\n" : string.Empty;
            string batchQty = _batchQuantity != null ? $"TOP ({_batchQuantity}) " : string.Empty;
            string batchQtyRepeat = _batchQuantity != null ? $"\nIF @@ROWCOUNT != 0\ngoto DeleteMore" : string.Empty;

            string comm = $"{batchQtyStart}DELETE {batchQty}FROM {fullQualifiedTableName} {batchQtyRepeat}";

            return comm;
        }
    }
}