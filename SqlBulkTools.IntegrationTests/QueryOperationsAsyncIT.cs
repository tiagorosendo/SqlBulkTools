using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using System.Transactions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlBulkTools.Enumeration;
using SqlBulkTools.TestCommon.Model;
using SqlBulkTools.TestCommon;
using SqlBulkTools.IntegrationTests.Data;

namespace SqlBulkTools.IntegrationTests
{
    [TestClass]
    public class QueryOperationsAsyncIt
    {
        private BookRandomizer _randomizer;
        private DataAccess _dataAccess;

        [TestInitialize]
        public void Setup()
        {
            _dataAccess = new DataAccess();
            _randomizer = new BookRandomizer();
        }

        [TestMethod]
        public async Task SqlBulkTools_UpdateQuery_SetPriceOnSingleEntity()
        {
            await DeleteAllBooks();
            BulkOperations bulk = new BulkOperations();

            List<Book> books = _randomizer.GetRandomCollection(30);

            var bookToTest = books[5];
            bookToTest.Price = 50;
            var isbn = bookToTest.ISBN;
            int updatedRecords = 0;

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(ConfigurationManager
                    .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
                {
                    await bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsert()
                        .CommitAsync(conn);

                    // Update price to 100
                    updatedRecords = await bulk.Setup<Book>()
                        .ForObject(new Book() { Price = 100 })
                        .WithTable("Books")
                        .AddColumn(x => x.Price)
                        .Update()
                        .Where(x => x.ISBN == isbn)
                        .CommitAsync(conn);

                }

                trans.Complete();
            }

            Assert.IsTrue(updatedRecords == 1);
            Assert.AreEqual(100, _dataAccess.GetBookList(isbn).Single().Price);
        }

        [TestMethod]
        public async Task SqlBulkTools_UpdateQuery_SetPriceAndDescriptionOnSingleEntity()
        {
            await DeleteAllBooks();
            BulkOperations bulk = new BulkOperations();

            List<Book> books = _randomizer.GetRandomCollection(30);

            var bookToTest = books[5];
            bookToTest.Price = 50;
            var isbn = bookToTest.ISBN;

            int updatedRecords = 0;

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(ConfigurationManager
                    .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
                {
                    await bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsert()
                        .CommitAsync(conn);

                    // Update price to 100

                    updatedRecords = await bulk.Setup<Book>()
                        .ForObject(new Book()
                        {
                            Price = 100,
                            Description = "Somebody will want me now! Yay"
                        })
                        .WithTable("Books")
                        .AddColumn(x => x.Price)
                        .AddColumn(x => x.Description)
                        .Update()
                        .Where(x => x.ISBN == isbn)
                        .CommitAsync(conn);

                }

                trans.Complete();
            }

            var firstBook = _dataAccess.GetBookList(isbn).Single();

            Assert.IsTrue(updatedRecords == 1);
            Assert.AreEqual(100, firstBook.Price);
            Assert.AreEqual("Somebody will want me now! Yay", firstBook.Description);
        }

        [TestMethod]
        public async Task SqlBulkTools_UpdateQuery_MultipleConditionsTrue()
        {
            await DeleteAllBooks();
            BulkOperations bulk = new BulkOperations();

            List<Book> books = _randomizer.GetRandomCollection(30);

            for (int i = 0; i < books.Count; i++)
            {
                if (i < 20)
                {
                    books[i].Price = 15;
                }
                else
                    books[i].Price = 25;
            }

            var bookToTest = books[5];
            var isbn = bookToTest.ISBN;
            int updatedRecords = 0;

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(ConfigurationManager
                    .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
                {
                    await bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsert()
                        .CommitAsync(conn);

                    updatedRecords = await bulk.Setup<Book>()
                        .ForObject(new Book() { Price = 100, WarehouseId = 5 })
                        .WithTable("Books")
                        .AddColumn(x => x.Price)
                        .AddColumn(x => x.WarehouseId)
                        .Update()
                        .Where(x => x.ISBN == isbn)
                        .And(x => x.Price == 15)
                        .CommitAsync(conn);
                }

                trans.Complete();
            }

            Assert.AreEqual(1, updatedRecords);
            Assert.AreEqual(100, _dataAccess.GetBookList(isbn).Single().Price);
            Assert.AreEqual(5, _dataAccess.GetBookList(isbn).Single().WarehouseId);
        }

        [TestMethod]
        public async Task SqlBulkTools_UpdateQuery_MultipleConditionsFalse()
        {
            await DeleteAllBooks();
            BulkOperations bulk = new BulkOperations();

            List<Book> books = _randomizer.GetRandomCollection(30);

            for (int i = 0; i < books.Count; i++)
            {
                if (i < 20)
                {
                    books[i].Price = 15;
                }
                else
                    books[i].Price = 25;
            }

            var bookToTest = books[5];
            var isbn = bookToTest.ISBN;
            int updatedRecords = 0;

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(ConfigurationManager
                    .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
                {
                    await bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsert()
                        .CommitAsync(conn);

                    // Update price to 100

                    updatedRecords = await bulk.Setup<Book>()
                        .ForObject(new Book() { Price = 100, WarehouseId = 5 })
                        .WithTable("Books")
                        .AddColumn(x => x.Price)
                        .AddColumn(x => x.WarehouseId)
                        .Update()
                        .Where(x => x.ISBN == isbn)
                        .And(x => x.Price == 16)
                        .CommitAsync(conn);
                }

                trans.Complete();
            }

            Assert.IsTrue(updatedRecords == 0);
        }

        [TestMethod]
        public async Task SqlBulkTools_DeleteQuery_DeleteSingleEntity()
        {
            await DeleteAllBooks();
            BulkOperations bulk = new BulkOperations();

            List<Book> books = _randomizer.GetRandomCollection(30);

            var bookIsbn = books[5].ISBN;
            int deletedRecords = 0;

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(ConfigurationManager
                    .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
                {
                    await bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsert()
                        .CommitAsync(conn);

                    deletedRecords = await bulk.Setup<Book>()
                        .ForDeleteQuery()
                        .WithTable("Books")
                        .Delete()
                        .Where(x => x.ISBN == bookIsbn)
                        .CommitAsync(conn);

                }

                trans.Complete();
            }


            Assert.IsTrue(deletedRecords == 1);
            Assert.AreEqual(29, _dataAccess.GetBookCount());
        }

        [TestMethod]
        public async Task SqlBulkTools_DeleteQuery_DeleteWhenNotNullWithSchema()
        {
            BulkOperations bulk = new BulkOperations();
            List<SchemaTest2> col = new List<SchemaTest2>();

            for (int i = 0; i < 30; i++)
            {
                col.Add(new SchemaTest2() { ColumnA = "ColumnA " + i });
            }

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(ConfigurationManager
                    .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
                {
                    bulk.Setup<SchemaTest2>()
                        .ForDeleteQuery()
                        .WithTable("SchemaTest")
                        .WithSchema("AnotherSchema")
                        .Delete()
                        .AllRecords()
                        .Commit(conn);

                    await bulk.Setup<SchemaTest2>()
                        .ForCollection(col)
                        .WithTable("SchemaTest")
                        .WithSchema("AnotherSchema")
                        .AddAllColumns()
                        .BulkInsert()
                        .CommitAsync(conn);


                    await bulk.Setup<SchemaTest2>()
                        .ForDeleteQuery()
                        .WithTable("SchemaTest")
                        .WithSchema("AnotherSchema")
                        .Delete()
                        .Where(x => x.ColumnA != null)
                        .CommitAsync(conn);

                }

                trans.Complete();
            }


            Assert.AreEqual(0, _dataAccess.GetSchemaTest2List().Count);
        }



        [TestMethod]
        public async Task SqlBulkTools_DeleteQuery_DeleteWhenNullWithWithSchema()
        {
            await DeleteAllBooks();
            BulkOperations bulk = new BulkOperations();
            List<SchemaTest2> col = new List<SchemaTest2>();

            for (int i = 0; i < 30; i++)
            {
                col.Add(new SchemaTest2() { ColumnA = null });
            }

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(ConfigurationManager
                    .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
                {
                    await bulk.Setup<SchemaTest2>()
                        .ForCollection(col)
                        .WithTable("SchemaTest")
                        .WithSchema("AnotherSchema")
                        .AddAllColumns()
                        .BulkInsert()
                        .CommitAsync(conn);

                    await bulk.Setup<SchemaTest2>()
                        .ForDeleteQuery()
                        .WithTable("SchemaTest")
                        .WithSchema("AnotherSchema")
                        .Delete()
                        .Where(x => x.ColumnA == null)
                        .CommitAsync(conn);
                }

                trans.Complete();
            }

            Assert.AreEqual(0, _dataAccess.GetSchemaTest2List().Count);
        }

        [TestMethod]
        public async Task SqlBulkTools_DeleteQuery_DeleteWithMultipleConditions()
        {
            await DeleteAllBooks();
            BulkOperations bulk = new BulkOperations();

            List<Book> books = _randomizer.GetRandomCollection(30);

            for (int i = 0; i < books.Count; i++)
            {
                if (i < 6)
                {
                    books[i].Price = 1 + (i * 100);
                    books[i].WarehouseId = 1;
                    books[i].Description = null;
                }
            }

            int deletedRecords = 0;

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(ConfigurationManager
                    .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
                {

                    await bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsert()
                        .CommitAsync(conn);

                    deletedRecords = await bulk.Setup<Book>()
                        .ForDeleteQuery()
                        .WithTable("Books")
                        .Delete()
                        .Where(x => x.WarehouseId == 1)
                        .And(x => x.Price >= 100)
                        .And(x => x.Description == null)
                        .CommitAsync(conn);

                }

                trans.Complete();
            }

            Assert.AreEqual(5, deletedRecords);
            Assert.AreEqual(25, _dataAccess.GetBookList().Count);
        }

        [TestMethod]
        public async Task SqlBulkTools_Insert_ManualAddColumn()
        {
            await DeleteAllBooks();
            BulkOperations bulk = new BulkOperations();
            int insertedRecords = 0;
            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(ConfigurationManager
                    .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
                {
                    insertedRecords = await bulk.Setup<Book>()
                        .ForObject(new Book() { BestSeller = true, Description = "Greatest dad in the world", Title = "Hello World", ISBN = "1234567", Price = 23.99M })
                        .WithTable("Books")
                        .AddColumn(x => x.Title)
                        .AddColumn(x => x.ISBN)
                        .AddColumn(x => x.BestSeller)
                        .AddColumn(x => x.Description)
                        .AddColumn(x => x.Price)
                        .Insert()
                        .SetIdentityColumn(x => x.Id, ColumnDirectionType.InputOutput)
                        .CommitAsync(conn);
                }

                trans.Complete();
            }

            Assert.AreEqual(1, insertedRecords);
            Assert.IsNotNull(_dataAccess.GetBookList("1234567").SingleOrDefault());
        }

        [TestMethod]
        public async Task SqlBulkTools_Insert_AddAllColumns()
        {
            await DeleteAllBooks();
            BulkOperations bulk = new BulkOperations();
            int insertedRecords = 0;
            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(ConfigurationManager
                    .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
                {
                    insertedRecords = await bulk.Setup<Book>()
                        .ForObject(new Book()
                        {
                            BestSeller = true,
                            Description = "Greatest dad in the world",
                            Title = "Hello World",
                            ISBN = "1234567",
                            Price = 23.99M
                        })
                        .WithTable("Books")
                        .AddAllColumns()
                        .Insert()
                        .SetIdentityColumn(x => x.Id)
                        .CommitAsync(conn);

                }

                trans.Complete();
            }

            Assert.AreEqual(1, insertedRecords);
            Assert.IsNotNull(_dataAccess.GetBookList("1234567").SingleOrDefault());
        }

        [TestMethod]
        public async Task SqlBulkTools_Upsert_AddAllColumns()
        {
            await DeleteAllBooks();
            using (TransactionScope tx = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection con = new SqlConnection(ConfigurationManager
                    .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
                {
                    var bulk = new BulkOperations();
                    await bulk.Setup<Book>()
                    .ForObject(new Book()
                    {
                        BestSeller = true,
                        Description = "Greatest dad in the world",
                        Title = "Hello World",
                        ISBN = "1234567",
                        Price = 23.99M
                    })
                    .WithTable("Books")
                    .AddAllColumns()
                    .Upsert()
                    .SetIdentityColumn(x => x.Id, ColumnDirectionType.InputOutput)
                    .MatchTargetOn(x => x.Id)
                    .CommitAsync(con);
                }

                tx.Complete();
            }

            Assert.AreEqual(1, _dataAccess.GetBookCount());
            Assert.IsNotNull(_dataAccess.GetBookList("1234567").SingleOrDefault());
        }

        [TestMethod]
        public async Task SqlBulkTools_Upsert_AddAllColumnsWithExistingRecord()
        {
            await DeleteAllBooks();
            BulkOperations bulk = new BulkOperations();

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection con = new SqlConnection(ConfigurationManager
                    .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
                {

                    Book book = new Book()
                    {
                        BestSeller = true,
                        Description = "Greatest dad in the world",
                        Title = "Hello World",
                        ISBN = "1234567",
                        Price = 23.99M
                    };

                    await bulk.Setup<Book>()
                        .ForObject(book)
                        .WithTable("Books")
                        .AddAllColumns()
                        .Insert()
                        .SetIdentityColumn(x => x.Id, ColumnDirectionType.InputOutput)
                        .CommitAsync(con);

                    await bulk.Setup<Book>()
                    .ForObject(new Book()
                    {
                        Id = book.Id,
                        BestSeller = true,
                        Description = "Greatest dad in the world",
                        Title = "Hello Greggo",
                        ISBN = "1234567",
                        Price = 23.99M
                    })
                    .WithTable("Books")
                    .AddAllColumns()
                    .Upsert()
                    .SetIdentityColumn(x => x.Id, ColumnDirectionType.Input)
                    .MatchTargetOn(x => x.Id)
                    .CommitAsync(con);
                }

                trans.Complete();
            }

            Assert.AreEqual(1, _dataAccess.GetBookCount());
            Assert.IsNotNull(_dataAccess.GetBookList().SingleOrDefault(x => x.Title == "Hello Greggo"));
        }

        [TestMethod]
        public async Task SqlBulkTools_Insert_CustomColumnMapping()
        {
            BulkOperations bulk = new BulkOperations();

            List<CustomColumnMappingTest> col = new List<CustomColumnMappingTest>();

            for (int i = 0; i < 30; i++)
            {
                col.Add(new CustomColumnMappingTest() { NaturalIdTest = i, ColumnXIsDifferent = "ColumnX " + i, ColumnYIsDifferentInDatabase = i });
            }

            var customColumn = new CustomColumnMappingTest()
            {
                NaturalIdTest = 1,
                ColumnXIsDifferent = $"ColumnX 1",
                ColumnYIsDifferentInDatabase = 1
            };

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(ConfigurationManager
                    .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
                {

                    bulk.Setup<CustomColumnMappingTest>()
                        .ForDeleteQuery()
                        .WithTable("CustomColumnMappingTests")
                        .Delete()
                        .AllRecords()
                        .Commit(conn);

                    await bulk.Setup<CustomColumnMappingTest>()
                        .ForObject(customColumn)
                        .WithTable("CustomColumnMappingTests")
                        .AddAllColumns()
                        .CustomColumnMapping(x => x.ColumnXIsDifferent, "ColumnX")
                        .CustomColumnMapping(x => x.ColumnYIsDifferentInDatabase, "ColumnY")
                        .CustomColumnMapping(x => x.NaturalIdTest, "NaturalId")
                        .Insert()
                        .CommitAsync(conn);
                }

                trans.Complete();
            }

            // Assert
            Assert.IsTrue(_dataAccess.GetCustomColumnMappingTests().First().ColumnXIsDifferent == "ColumnX 1");
        }

        [TestMethod]
        public async Task SqlBulkTools_Upsert_CustomColumnMapping()
        {
            BulkOperations bulk = new BulkOperations();

            var customColumn = new CustomColumnMappingTest()
            {
                NaturalIdTest = 1,
                ColumnXIsDifferent = "ColumnX " + 1,
                ColumnYIsDifferentInDatabase = 3
            };

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(ConfigurationManager
                    .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
                {

                    bulk.Setup<CustomColumnMappingTest>()
                        .ForDeleteQuery()
                        .WithTable("CustomColumnMappingTests")
                        .Delete()
                        .AllRecords()
                        .Commit(conn);

                    await bulk.Setup<CustomColumnMappingTest>()
                        .ForObject(customColumn)
                        .WithTable("CustomColumnMappingTests")
                        .AddAllColumns()
                        .CustomColumnMapping(x => x.ColumnXIsDifferent, "ColumnX")
                        .CustomColumnMapping(x => x.ColumnYIsDifferentInDatabase, "ColumnY")
                        .CustomColumnMapping(x => x.NaturalIdTest, "NaturalId")
                        .Upsert()
                        .MatchTargetOn(x => x.NaturalIdTest)
                        .CommitAsync(conn);
                }

                trans.Complete();
            }

            // Assert
            Assert.IsTrue(_dataAccess.GetCustomColumnMappingTests().First().ColumnYIsDifferentInDatabase == 3);
        }

        [TestMethod]
        public async Task SqlBulkTools_Update_CustomColumnMapping()
        {
            BulkOperations bulk = new BulkOperations();

            List<CustomColumnMappingTest> col = new List<CustomColumnMappingTest>();

            for (int i = 0; i < 30; i++)
            {
                col.Add(new CustomColumnMappingTest() { NaturalIdTest = i, ColumnXIsDifferent = "ColumnX " + i, ColumnYIsDifferentInDatabase = i });
            }

            var customColumn = new CustomColumnMappingTest()
            {
                NaturalIdTest = 1,
                ColumnXIsDifferent = "ColumnX " + 1,
                ColumnYIsDifferentInDatabase = 1
            };

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(ConfigurationManager
                    .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
                {
                    bulk.Setup<CustomColumnMappingTest>()
                        .ForDeleteQuery()
                        .WithTable("CustomColumnMappingTests")
                        .Delete()
                        .AllRecords()
                        .Commit(conn);

                    await bulk.Setup<CustomColumnMappingTest>()
                        .ForObject(customColumn)
                        .WithTable("CustomColumnMappingTests")
                        .AddAllColumns()
                        .CustomColumnMapping(x => x.ColumnXIsDifferent, "ColumnX")
                        .CustomColumnMapping(x => x.ColumnYIsDifferentInDatabase, "ColumnY")
                        .CustomColumnMapping(x => x.NaturalIdTest, "NaturalId")
                        .Insert()
                        .CommitAsync(conn);

                    customColumn.ColumnXIsDifferent = "updated";


                    await bulk.Setup<CustomColumnMappingTest>()
                        .ForObject(customColumn)
                        .WithTable("CustomColumnMappingTests")
                        .AddAllColumns()
                        .CustomColumnMapping(x => x.ColumnXIsDifferent, "ColumnX")
                        .CustomColumnMapping(x => x.ColumnYIsDifferentInDatabase, "ColumnY")
                        .CustomColumnMapping(x => x.NaturalIdTest, "NaturalId")
                        .Update()
                        .Where(x => x.NaturalIdTest == 1)
                        .CommitAsync(conn);
                }

                trans.Complete();
            }

            // Assert
            Assert.IsTrue(_dataAccess.GetCustomColumnMappingTests().First().ColumnXIsDifferent == "updated");
        }

        private async Task DeleteAllBooks()
        {
            BulkOperations bulk = new BulkOperations();

            using (TransactionScope tx = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(ConfigurationManager
                    .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
                {
                    await bulk.Setup<Book>()
                        .ForDeleteQuery()
                        .WithTable("Books")
                        .Delete()
                        .AllRecords()
                        .SetBatchQuantity(500)
                        .CommitAsync(conn);
                }

                tx.Complete();
            }
        }
    }
}