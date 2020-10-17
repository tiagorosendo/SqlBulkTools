using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
// ReSharper disable UnusedMember.Global

// ReSharper disable once CheckNamespace
namespace SqlBulkTools.BulkCopy
{
    /// <summary>
    /// Configurable options for table. 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class BulkTable<T>
    {
        private readonly BulkOperations _bulk;
        private readonly IEnumerable<T> _list;
        private HashSet<string> Columns { get; set; }
        private string _schema;
        private readonly string _tableName;
        private Dictionary<string, string> CustomColumnMappings { get; }
        private BulkCopySettings _bulkCopySettings;
        private readonly List<PropertyInfo> _propertyInfoList;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="bulk"></param>
        /// <param name="list"></param>
        /// <param name="tableName"></param>
        /// <param name="schema"></param>
        public BulkTable(BulkOperations bulk, IEnumerable<T> list, string tableName, string schema)
        {
            _bulk = bulk;
            _list = list;
            _schema = schema;
            Columns = new HashSet<string>();
            CustomColumnMappings = new Dictionary<string, string>();
            _tableName = tableName;
            Columns = new HashSet<string>();
            CustomColumnMappings = new Dictionary<string, string>();
            _bulkCopySettings = new BulkCopySettings();
            _propertyInfoList = typeof(T).GetProperties().OrderBy(x => x.Name).ToList();
        }

        /// <summary>
        /// Add each column that you want to include in the query. Only include the columns that are relevant to the procedure for best performance. 
        /// </summary>
        /// <param name="columnName">Column name as represented in database</param>
        /// <returns></returns>
        public BulkAddColumn<T> AddColumn(Expression<Func<T, object>> columnName)
        {
            var propertyName = BulkOperationsHelper.GetPropertyName(columnName);
            Columns.Add(propertyName);
            return new BulkAddColumn<T>(_bulk, _list, _tableName, Columns, CustomColumnMappings, _schema, _bulkCopySettings, _propertyInfoList);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="columnNames"></param>
        /// <returns></returns>
        public BulkAddColumn<T> AddColumns(params Expression<Func<T, object>>[] columnNames)
        {
            foreach (var column in columnNames)
            {
                var propertyName = BulkOperationsHelper.GetPropertyName(column);
                Columns.Add(propertyName);
            }
            return new BulkAddColumn<T>(_bulk, _list, _tableName, Columns, CustomColumnMappings, _schema, _bulkCopySettings, _propertyInfoList);
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
            if (destination == null)
                throw new ArgumentNullException(nameof(destination));

            var propertyName = BulkOperationsHelper.GetPropertyName(columnName);
            Columns.Add(propertyName);

            CustomColumnMappings.Add(propertyName, destination);

            return new BulkAddColumn<T>(_bulk, _list, _tableName, Columns, CustomColumnMappings, _schema, _bulkCopySettings, _propertyInfoList);
        }

        /// <summary>
        /// Adds all properties in model that are either value, string, char[] or byte[] type. 
        /// </summary>
        /// <returns></returns>
        public BulkAddColumnList<T> AddAllColumns()
        {
            Columns = BulkOperationsHelper.GetAllValueTypeAndStringColumns(_propertyInfoList);
            return new BulkAddColumnList<T>(_bulk, _list, _tableName, Columns, CustomColumnMappings, _schema, _bulkCopySettings, _propertyInfoList);
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