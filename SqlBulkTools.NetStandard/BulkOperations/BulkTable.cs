using System;
using System.Collections.Generic;
using System.Linq.Expressions;

// ReSharper disable once CheckNamespace
namespace SqlBulkTools.BulkCopy
{
    /// <summary>
    /// Configurable options for table. 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class BulkTable<T>
    {
        private readonly BulkOperations bulk;
        private readonly IEnumerable<T> _list;
        private HashSet<string> Columns { get; set; }
        private string _schema;
        private readonly string _tableName;
        private Dictionary<string, string> CustomColumnMappings { get; set; }
        private BulkCopySettings _bulkCopySettings;
        private readonly List<PropInfo> _propertyInfoList;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="list"></param>
        /// <param name="tableName"></param>
        /// <param name="schema"></param>
        public BulkTable(BulkOperations bulk, IEnumerable<T> list, Dictionary<string, Type> propTypes, string tableName, string schema)
        {
            this.bulk = bulk;
            _list = list;
            _schema = schema;
            Columns = new HashSet<string>();
            CustomColumnMappings = new Dictionary<string, string>();
            _tableName = tableName;
            Columns = new HashSet<string>();
            CustomColumnMappings = new Dictionary<string, string>();
            _bulkCopySettings = new BulkCopySettings();
            _propertyInfoList = PropInfoList.From<T>(propTypes);
        }

        /// <summary>
        /// Add each column that you want to include in the query. Only include the columns that are relevant to the procedure for best performance. 
        /// </summary>
        /// <param name="columnName">Column name as represented in database</param>
        /// <returns></returns>
        public BulkAddColumn<T> AddColumn(string columnName)
        {
            Columns.Add(columnName);
            return new BulkAddColumn<T>(bulk, _list, _tableName, Columns, CustomColumnMappings, _schema, _bulkCopySettings, _propertyInfoList);
        }

        /// <summary>
        /// Add each column that you want to include in the query. Only include the columns that are relevant to the procedure for best performance. 
        /// </summary>
        /// <param name="columnName">Column name as represented in database</param>
        /// <returns></returns>
        public BulkAddColumn<T> AddColumn(Expression<Func<T, object>> columnName)
        {
            return AddColumn(BulkOperationsHelper.GetPropertyName(columnName));
        }

        public BulkAddColumn<T> AddColumns(params string[] columnNames)
        {
            foreach (var column in columnNames)
            {
                Columns.Add(column);
            }
            return new BulkAddColumn<T>(bulk, _list, _tableName, Columns, CustomColumnMappings, _schema, _bulkCopySettings, _propertyInfoList);
        }

        public BulkAddColumn<T> AddColumns(params Expression<Func<T, object>>[] columnNames)
        {
            foreach (var column in columnNames)
            {
                var propertyName = BulkOperationsHelper.GetPropertyName(column);
                Columns.Add(propertyName);
            }
            return new BulkAddColumn<T>(bulk, _list, _tableName, Columns, CustomColumnMappings, _schema, _bulkCopySettings, _propertyInfoList);
        }

        /// <summary>
        /// Add each column that you want to include in the query. Only include the columns that are relevant to the 
        /// procedure for best performance. 
        /// </summary>
        /// <param name="columnName">Column name as represented in database</param>
        /// <param name="destination">The actual name of column as represented in SQL table. By default SqlBulkTools will attempt to match the model property names to SQL column names (case insensitive). 
        /// If any of your model property names do not match 
        /// the SQL table column(s) as defined in given table, then use this overload to set up a custom mapping. </param>
        /// <returns></returns>
        public BulkAddColumn<T> AddColumn(string columnName, string destination)
        {
            if (destination == null)
                throw new ArgumentNullException(nameof(destination));

            Columns.Add(columnName);

            CustomColumnMappings.Add(columnName, destination);

            return new BulkAddColumn<T>(bulk, _list, _tableName, Columns, CustomColumnMappings, _schema, _bulkCopySettings, _propertyInfoList);
        }

        /// <summary>
        /// Add each column that you want to include in the query. Only include the columns that are relevant to the 
        /// procedure for best performance. 
        /// </summary>
        /// <param name="columnName">Column name as represented in database</param>
        /// <param name="destination">The actual name of column as represented in SQL table. By default SqlBulkTools will attempt to match the model property names to SQL column names (case insensitive). 
        /// If any of your model property names do not match 
        /// the SQL table column(s) as defined in given table, then use this overload to set up a custom mapping. </param>
        /// <returns></returns>
        public BulkAddColumn<T> AddColumn(Expression<Func<T, object>> columnName, string destination)
        {
            return AddColumn(BulkOperationsHelper.GetPropertyName(columnName), destination);
        }

        /// <summary>
        /// Adds all properties in model that are either value, string, char[] or byte[] type. 
        /// </summary>
        /// <returns></returns>
        public BulkAddColumnList<T> AddAllColumns()
        {
            Columns = BulkOperationsHelper.GetAllValueTypeAndStringColumns(_propertyInfoList, typeof(T));
            return new BulkAddColumnList<T>(bulk, _list, _tableName, Columns, CustomColumnMappings, _schema, _bulkCopySettings, _propertyInfoList);
        }

        /// <summary>
        /// Explicitly set a schema. If a schema is not added, the system default schema name 'dbo' will used.
        /// </summary>
        /// <param name="schema"></param>
        /// <returns></returns>
        public BulkTable<T> WithSchema(string schema)
        {
            if (_schema != Constants.DefaultSchemaName)
                throw new SqlBulkToolsException("Schema has already been defined in WithTable method.");

            _schema = schema;
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="settings"></param>
        /// <returns></returns>
        public BulkTable<T> WithBulkCopySettings(BulkCopySettings settings)
        {
            _bulkCopySettings = settings;
            return this;
        }
    }
}
