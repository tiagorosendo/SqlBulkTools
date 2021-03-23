using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using System.Transactions;
using SqlBulkTools.Enumeration;
using SqlBulkTools.TestCommon.Model;
using SqlBulkTools.TestCommon;
using SqlBulkTools.IntegrationTests.Data;
using Xunit;

namespace SqlBulkTools.IntegrationTests
{
    [Collection("IntegrationTests")]
    public class QueryOperationsAsyncIt
    {
        private readonly BookRandomizer _randomizer = new BookRandomizer();
        private readonly DataAccess _dataAccess = new DataAccess();

        [Fact]
        public async Task SqlBulkTools_UpdateQuery_SetPriceOnSingleEntity()
        {
            await DeleteAllBooks();
            var bulk = new BulkOperations();

            var books = _randomizer.GetRandomCollection(30);

            var bookToTest = books[5];
            bookToTest.Price = 50;
            var isbn = bookToTest.ISBN;
            var updatedRecords = 0;

            using (var trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
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

            Assert.True(updatedRecords == 1);
            Assert.Equal(100, _dataAccess.GetBookList(isbn).Single().Price);
        }

        [Fact]
        public async Task SqlBulkTools_UpdateQuery_SetPriceAndDescriptionOnSingleEntity()
        {
            await DeleteAllBooks();
            var bulk = new BulkOperations();

            var books = _randomizer.GetRandomCollection(30);

            var bookToTest = books[5];
            bookToTest.Price = 50;
            var isbn = bookToTest.ISBN;

            var updatedRecords = 0;

            using (var trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
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

            Assert.True(updatedRecords == 1);
            Assert.Equal(100, firstBook.Price);
            Assert.Equal("Somebody will want me now! Yay", firstBook.Description);
        }

        [Fact]
        public async Task SqlBulkTools_UpdateQuery_MultipleConditionsTrue()
        {
            await DeleteAllBooks();
            var bulk = new BulkOperations();

            var books = _randomizer.GetRandomCollection(30);

            for (var i = 0; i < books.Count; i++)
            {
                books[i].Price = i < 20 ? 15 : 25;
            }

            var bookToTest = books[5];
            var isbn = bookToTest.ISBN;
            var updatedRecords = 0;

            using (var trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
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

            Assert.Equal(1, updatedRecords);
            Assert.Equal(100, _dataAccess.GetBookList(isbn).Single().Price);
            Assert.Equal(5, _dataAccess.GetBookList(isbn).Single().WarehouseId);
        }

        [Fact]
        public async Task SqlBulkTools_UpdateQuery_MultipleConditionsFalse()
        {
            await DeleteAllBooks();
            var bulk = new BulkOperations();

            var books = _randomizer.GetRandomCollection(30);

            for (var i = 0; i < books.Count; i++)
            {
                books[i].Price = i < 20 ? 15 : 25;
            }

            var bookToTest = books[5];
            var isbn = bookToTest.ISBN;
            var updatedRecords = 0;

            using (var trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
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

            Assert.True(updatedRecords == 0);
        }

        [Fact]
        public async Task SqlBulkTools_DeleteQuery_DeleteSingleEntity()
        {
            await DeleteAllBooks();
            var bulk = new BulkOperations();

            var books = _randomizer.GetRandomCollection(30);

            var bookIsbn = books[5].ISBN;
            var deletedRecords = 0;

            using (var trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
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

            Assert.True(deletedRecords == 1);
            Assert.Equal(29, _dataAccess.GetBookCount());
        }

        [Fact]
        public async Task SqlBulkTools_DeleteQuery_DeleteWhenNotNullWithSchema()
        {
            var bulk = new BulkOperations();
            var col = new List<SchemaTest2>();

            for (var i = 0; i < 30; i++)
            {
                col.Add(new SchemaTest2() { ColumnA = "ColumnA " + i });
            }

            using (var trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
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

            Assert.Empty(_dataAccess.GetSchemaTest2List());
        }

        [Fact]
        public async Task SqlBulkTools_DeleteQuery_DeleteWhenNullWithWithSchema()
        {
            await DeleteAllBooks();
            var bulk = new BulkOperations();
            var col = new List<SchemaTest2>();

            for (var i = 0; i < 30; i++)
            {
                col.Add(new SchemaTest2() { ColumnA = null });
            }

            using (var trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
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

            Assert.Empty(_dataAccess.GetSchemaTest2List());
        }

        [Fact]
        public async Task SqlBulkTools_DeleteQuery_DeleteWithMultipleConditions()
        {
            await DeleteAllBooks();
            var bulk = new BulkOperations();

            var books = _randomizer.GetRandomCollection(30);

            for (var i = 0; i < books.Count; i++)
            {
                if (i < 6)
                {
                    books[i].Price = 1 + (i * 100);
                    books[i].WarehouseId = 1;
                    books[i].Description = null;
                }
            }

            var deletedRecords = 0;

            using (var trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
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

            Assert.Equal(5, deletedRecords);
            Assert.Equal(25, _dataAccess.GetBookList().Count);
        }

        [Fact]
        public async Task SqlBulkTools_Insert_ManualAddColumn()
        {
            await DeleteAllBooks();
            var bulk = new BulkOperations();
            var insertedRecords = 0;
            using (var trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
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

            Assert.Equal(1, insertedRecords);
            Assert.NotNull(_dataAccess.GetBookList("1234567").SingleOrDefault());
        }

        [Fact]
        public async Task SqlBulkTools_Insert_AddAllColumns()
        {
            await DeleteAllBooks();
            var bulk = new BulkOperations();
            var insertedRecords = 0;
            using (var trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
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

            Assert.Equal(1, insertedRecords);
            Assert.NotNull(_dataAccess.GetBookList("1234567").SingleOrDefault());
        }

        [Fact]
        public async Task SqlBulkTools_Upsert_AddAllColumns()
        {
            await DeleteAllBooks();
            using (var tx = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (var con = new SqlConnection(_dataAccess.ConnectionString))
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

            Assert.Equal(1, _dataAccess.GetBookCount());
            Assert.NotNull(_dataAccess.GetBookList("1234567").SingleOrDefault());
        }

        [Fact]
        public async Task SqlBulkTools_Upsert_AddAllColumnsWithExistingRecord()
        {
            await DeleteAllBooks();
            var bulk = new BulkOperations();

            using (var trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (var con = new SqlConnection(_dataAccess.ConnectionString))
                {

                    var book = new Book()
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

            Assert.Equal(1, _dataAccess.GetBookCount());
            Assert.NotNull(_dataAccess.GetBookList().SingleOrDefault(x => x.Title == "Hello Greggo"));
        }

        [Fact]
        public async Task SqlBulkTools_Insert_CustomColumnMapping()
        {
            var bulk = new BulkOperations();

            var col = new List<CustomColumnMappingTest>();

            for (var i = 0; i < 30; i++)
            {
                col.Add(new CustomColumnMappingTest() { NaturalIdTest = i, ColumnXIsDifferent = "ColumnX " + i, ColumnYIsDifferentInDatabase = i });
            }

            var customColumn = new CustomColumnMappingTest()
            {
                NaturalIdTest = 1,
                ColumnXIsDifferent = $"ColumnX 1",
                ColumnYIsDifferentInDatabase = 1
            };

            using (var trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
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
            Assert.True(_dataAccess.GetCustomColumnMappingTests().First().ColumnXIsDifferent == "ColumnX 1");
        }

        [Fact]
        public async Task SqlBulkTools_Upsert_CustomColumnMapping()
        {
            var bulk = new BulkOperations();

            var customColumn = new CustomColumnMappingTest()
            {
                NaturalIdTest = 1,
                ColumnXIsDifferent = "ColumnX " + 1,
                ColumnYIsDifferentInDatabase = 3
            };

            using (var trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
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
            Assert.True(_dataAccess.GetCustomColumnMappingTests().First().ColumnYIsDifferentInDatabase == 3);
        }

        [Fact]
        public async Task SqlBulkTools_Update_CustomColumnMapping()
        {
            var bulk = new BulkOperations();

            var col = new List<CustomColumnMappingTest>();

            for (var i = 0; i < 30; i++)
            {
                col.Add(new CustomColumnMappingTest() { NaturalIdTest = i, ColumnXIsDifferent = "ColumnX " + i, ColumnYIsDifferentInDatabase = i });
            }

            var customColumn = new CustomColumnMappingTest()
            {
                NaturalIdTest = 1,
                ColumnXIsDifferent = "ColumnX " + 1,
                ColumnYIsDifferentInDatabase = 1
            };

            using (var trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
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
            Assert.True(_dataAccess.GetCustomColumnMappingTests().First().ColumnXIsDifferent == "updated");
        }

        private async Task DeleteAllBooks()
        {
            var bulk = new BulkOperations();

            using var tx = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled);

            using (var conn = new SqlConnection(_dataAccess.ConnectionString))
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
