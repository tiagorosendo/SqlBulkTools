using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Linq;
using SprocMapperLibrary.SqlServer;
using SqlBulkTools.TestCommon.Model;

namespace SqlBulkTools.IntegrationTests.Data
{
    public class DataAccess
    {
        public List<Book> GetBookList(string isbn = null)
        {
            using (SqlConnection conn = new SqlConnection(ConfigurationManager
                .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
            {
                var books = conn.Sproc()
                    .AddSqlParameter("@Isbn", isbn)
                    .ExecuteReader<Book>("dbo.GetBooks", true)
                    .ToList();

                return books;
            }
        }

        public int GetBookCount()
        {
            using (SqlConnection conn = new SqlConnection(ConfigurationManager
                .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
            {
                var bookCount = conn.Sproc()
                    .ExecuteScalar<int>("dbo.GetBookCount");
                return bookCount;
            }
        }

        public List<SchemaTest1> GetSchemaTest1List()
        {
            using (SqlConnection conn = new SqlConnection(ConfigurationManager
                .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
            {
                var schemaTestList = conn.Sproc()
                    .AddSqlParameter("@Schema", "dbo")
                    .ExecuteReader<SchemaTest1>("dbo.GetSchemaTest", true)
                    .ToList();

                return schemaTestList;
            }
        }

        public List<SchemaTest2> GetSchemaTest2List()
        {
            using (SqlConnection conn = new SqlConnection(ConfigurationManager
                .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
            {
                var schemaTestList = conn.Sproc()
                    .AddSqlParameter("@Schema", "AnotherSchema")
                    .ExecuteReader<SchemaTest2>("dbo.GetSchemaTest", true)
                    .ToList();

                return schemaTestList;
            }
        }

        public List<CustomColumnMappingTest> GetCustomColumnMappingTests()
        {
            using (SqlConnection conn = new SqlConnection(ConfigurationManager
                .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
            {
                var customColumnMappingTests = conn                  
                    .Sproc()
                    .CustomColumnMapping<CustomColumnMappingTest>(x => x.NaturalIdTest, "NaturalId")
                    .CustomColumnMapping<CustomColumnMappingTest>(x => x.ColumnXIsDifferent, "ColumnX")
                    .CustomColumnMapping<CustomColumnMappingTest>(x => x.ColumnYIsDifferentInDatabase, "ColumnY")
                    .ExecuteReader<CustomColumnMappingTest>("dbo.GetCustomColumnMappingTests")                                        
                    .ToList();

                return customColumnMappingTests;
            }
        }

        public List<ReservedColumnNameTest> GetReservedColumnNameTests()
        {
            using (SqlConnection conn = new SqlConnection(ConfigurationManager
                .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
            {
                var reservedColumnNameTests = conn
                    .Sproc()
                    .ExecuteReader<ReservedColumnNameTest>("dbo.GetReservedColumnNameTests")
                    .ToList();

                return reservedColumnNameTests;
            }
        }

        public int GetComplexTypeModelCount()
        {
            using (
                SqlConnection conn =
                    new SqlConnection(ConfigurationManager.ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
            {
                return conn.Sproc()
                    .ExecuteScalar<int>("dbo.GetComplexModelCount");
            }
        }

        public void ReseedBookIdentity(int idStart)
        {
            using (SqlConnection conn = new SqlConnection(ConfigurationManager
                .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
            {
                conn.Sproc()
                    .AddSqlParameter("@IdStart", idStart)
                    .ExecuteNonQuery("dbo.ReseedBookIdentity");
            }
        }

        public List<CustomIdentityColumnNameTest> GetCustomIdentityColumnNameTestList()
        {
            using (SqlConnection conn = new SqlConnection(ConfigurationManager
                .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
            {
                return conn.Sproc()
                    .CustomColumnMapping<CustomIdentityColumnNameTest>(x => x.Id, "ID_COMPANY")
                    .ExecuteReader<CustomIdentityColumnNameTest>("dbo.GetCustomIdentityColumnNameTestList")
                    .ToList();
            }
        }
    }
}
