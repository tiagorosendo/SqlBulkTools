using System;

// ReSharper disable once CheckNamespace
namespace SqlBulkTools
{
    internal class SqlBulkToolsException : Exception
    {
        public SqlBulkToolsException(string message) : base(message)
        {

        }
    }
}
