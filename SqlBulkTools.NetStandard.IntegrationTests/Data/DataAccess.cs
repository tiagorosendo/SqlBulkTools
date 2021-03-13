using Crane.Interface;
using Crane.SqlServer;
using Microsoft.Extensions.Configuration;
using SqlBulkTools.TestCommon.Model;
using System.Collections.Generic;
using System.Linq;

namespace SqlBulkTools.IntegrationTests.Data
{
    public class DataAccess
    {
        private string _connectionString;
        public string ConnectionString
        {
            get
            {
                if (string.IsNullOrEmpty(_connectionString))
                {
                    var config = new ConfigurationBuilder()
                        .AddJsonFile("appconfig.json")
                        .Build();
                    _connectionString = config["connectionString"];
                }
                return _connectionString;
            }
        }

        public List<Book> GetBookList(string isbn = null)
        {
            ICraneAccess dataAccess = new SqlServerAccess(ConnectionString);

            return dataAccess
                .Query()
                .AddSqlParameter("@Isbn", isbn)
                .ExecuteReader<Book>("dbo.GetBooks")
                .ToList();
        }

        public int GetBookCount()
        {
            ICraneAccess dataAccess = new SqlServerAccess(ConnectionString);

            return dataAccess
                .Query()
                .ExecuteScalar<int>("dbo.GetBookCount");
        }

        public List<SchemaTest1> GetSchemaTest1List()
        {
            ICraneAccess dataAccess = new SqlServerAccess(ConnectionString);

            return dataAccess
                .Query()
                .AddSqlParameter("@Schema", "dbo")
                .ExecuteReader<SchemaTest1>("dbo.GetSchemaTest")
                .ToList();
        }

        public List<SchemaTest2> GetSchemaTest2List()
        {
            ICraneAccess dataAccess = new SqlServerAccess(ConnectionString);

            return dataAccess
                .Query()
                .AddSqlParameter("@Schema", "AnotherSchema")
                .ExecuteReader<SchemaTest2>("dbo.GetSchemaTest")
                .ToList();
        }

        public List<CustomColumnMappingTest> GetCustomColumnMappingTests()
        {
            ICraneAccess dataAccess = new SqlServerAccess(ConnectionString);

            return dataAccess
                .Query()
                .CustomColumnMapping<CustomColumnMappingTest>(x => x.NaturalIdTest, "NaturalId")
                .CustomColumnMapping<CustomColumnMappingTest>(x => x.ColumnXIsDifferent, "ColumnX")
                .CustomColumnMapping<CustomColumnMappingTest>(x => x.ColumnYIsDifferentInDatabase, "ColumnY")
                .ExecuteReader<CustomColumnMappingTest>("dbo.GetCustomColumnMappingTests")
                .ToList();
        }

        public List<ReservedColumnNameTest> GetReservedColumnNameTests()
        {
            ICraneAccess dataAccess = new SqlServerAccess(ConnectionString);

            return dataAccess
                .Query()
                .ExecuteReader<ReservedColumnNameTest>("dbo.GetReservedColumnNameTests")
                .ToList();
        }

        public int GetComplexTypeModelCount()
        {
            ICraneAccess dataAccess = new SqlServerAccess(ConnectionString);

            return dataAccess
                .Query()
                .ExecuteScalar<int>("dbo.GetComplexModelCount");
        }

        public void ReseedBookIdentity(int idStart)
        {
            ICraneAccess dataAccess = new SqlServerAccess(ConnectionString);

            dataAccess
                .Command()
                .AddSqlParameter("@IdStart", idStart)
                .ExecuteNonQuery("dbo.ReseedBookIdentity");
        }

        public List<CustomIdentityColumnNameTest> GetCustomIdentityColumnNameTestList()
        {
            ICraneAccess dataAccess = new SqlServerAccess(ConnectionString);

            return dataAccess
                .Query()
                .CustomColumnMapping<CustomIdentityColumnNameTest>(x => x.Id, "ID_COMPANY")
                .ExecuteReader<CustomIdentityColumnNameTest>("dbo.GetCustomIdentityColumnNameTestList")
                .ToList();
        }
    }
}
