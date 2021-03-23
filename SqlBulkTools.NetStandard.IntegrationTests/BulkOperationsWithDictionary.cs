using AutoFixture;
using Microsoft.SqlServer.Types;
using SqlBulkTools.Enumeration;
using SqlBulkTools.IntegrationTests.Data;
using SqlBulkTools.TestCommon;
using SqlBulkTools.TestCommon.Model;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Transactions;
using Xunit;

namespace SqlBulkTools.IntegrationTests
{
    internal static class HelperExtensions
    {
        public static List<Dictionary<string, object>> ToDictionaryList<T>(this IEnumerable<T> list)
        {
            var properties = typeof(T).GetProperties();
            return list.Select(entity => properties.ToDictionary(entity)).ToList();
        }

        public static Dictionary<string, object> ToDictionary(this PropertyInfo[] propertyInfos, object entity) => 
            propertyInfos.ToDictionary(p => p.Name, p => p.GetValue(entity));

        public static Dictionary<string, object> ToDictionary<T>(this T entity) => 
            typeof(T).GetProperties().ToDictionary(entity);

        public static Dictionary<string, Type> ToPropertyTypes(this Type type) =>
            type.GetProperties().ToDictionary(p => p.Name, p => p.PropertyType);
    }

    [Collection("IntegrationTests")]
    public class BulkOperationsWithDictionary
    {
        private const int _repeatTimes = 1;
        private readonly DataAccess _dataAccess = new DataAccess();
        private readonly BookRandomizer _randomizer = new BookRandomizer();
        private List<Book> _bookCollection;

        [Fact]
        public void SqlBulkTools_BulkInsertOrUpdate_PassesWithCustomIdentityColumn()
        {
            var bulk = new BulkOperations();
            var customIdentityColumnList = new List<CustomIdentityColumnNameTest>();

            for (var i = 0; i < 30; i++)
            {
                customIdentityColumnList.Add(new CustomIdentityColumnNameTest
                {
                    ColumnA = i.ToString()               
                });
            }

            using (var trans = new TransactionScope())
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    bulk.Setup<CustomIdentityColumnNameTest>()
                        .ForDeleteQuery()
                        .WithTable("CustomIdentityColumnNameTest")
                        .Delete()
                        .AllRecords()
                        .Commit(conn);

                    bulk.Setup()
                        .ForCollection(customIdentityColumnList.ToDictionaryList())
                        .WithPropertyTypes(typeof(CustomIdentityColumnNameTest).ToPropertyTypes())
                        .WithTable("CustomIdentityColumnNameTest")
                        .AddColumn("Id", "ID_COMPANY")
                        .AddColumn("ColumnA")
                        .BulkInsertOrUpdate()
                        .SetIdentityColumn("Id")
                        .MatchTargetOn("ColumnA")
                        .Commit(conn);
                }

                trans.Complete();
            }

            Assert.True(_dataAccess.GetCustomIdentityColumnNameTestList().Count == 30);
        }

        [Fact]
        public void SqlBulkTools_BulkInsert()
        {
            const int rows = 1000;

            BulkDelete(_dataAccess.GetBookList());
            _bookCollection = new List<Book>();
            _bookCollection.AddRange(_randomizer.GetRandomCollection(rows));
            var results = new List<long>();

            Trace.WriteLine("Testing BulkInsert with " + rows + " rows");

            for (var i = 0; i < _repeatTimes; i++)
            {
                var time = BulkInsert(_bookCollection);

                results.Add(time);
            }
            var avg = results.Average(l => l);

            Trace.WriteLine("Average result (" + _repeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");

            Assert.Equal(rows * _repeatTimes, _dataAccess.GetBookCount());
        }

        [Fact]
        public void SqlBulkTools_BulkInsert_WithAllColumns()
        {
            const int rows = 1000;

            BulkDelete(_dataAccess.GetBookList());

            var randomCollection = _randomizer.GetRandomCollection(rows);

            BulkInsertAllColumns(randomCollection);

            var expected = randomCollection.First();
            var actual = _dataAccess.GetBookList(isbn: expected.ISBN).First();

            Assert.Equal(expected.Title, actual.Title);
            Assert.Equal(expected.Description, actual.Description);
            Assert.Equal(expected.Price, actual.Price);
            Assert.Equal(expected.WarehouseId, actual.WarehouseId);
            Assert.Equal(expected.BestSeller, actual.BestSeller);

            Assert.Equal(rows * _repeatTimes, _dataAccess.GetBookCount());
        }

        [Fact]
        public void SqlBulkTools_BulkInsertOrUpdate()
        {
            const int rows = 500, newRows = 500;

            BulkDelete(_dataAccess.GetBookList());
            var fixture = new Fixture();
            _bookCollection = _randomizer.GetRandomCollection(rows);

            var results = new List<long>();

            Trace.WriteLine("Testing BulkInsertOrUpdate with " + (rows + newRows) + " rows");

            for (var i = 0; i < _repeatTimes; i++)
            {
                BulkInsert(_bookCollection);

                // Update some rows
                for (var j = 0; j < 200; j++)
                {
                    var newBook = fixture.Build<Book>().Without(s => s.ISBN).Create();
                    var prevIsbn = _bookCollection[j].ISBN;
                    _bookCollection[j] = newBook;
                    _bookCollection[j].ISBN = prevIsbn;
                }

                // Add new rows
                _bookCollection.AddRange(_randomizer.GetRandomCollection(newRows));


                var time = BulkInsertOrUpdate(_bookCollection);
                results.Add(time);

                Assert.Equal(rows + newRows, _dataAccess.GetBookCount());

            }

            var avg = results.Average(l => l);
            Trace.WriteLine("Average result (" + _repeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");
        }

        [Fact]
        public void SqlBulkTools_BulkInsertOrUpdateAllColumns()
        {
            const int rows = 1000, newRows = 500;

            BulkDelete(_dataAccess.GetBookList());
            var fixture = new Fixture();
            _bookCollection = _randomizer.GetRandomCollection(rows);

            var results = new List<long>();

            Trace.WriteLine("Testing BulkInsertOrUpdateAllColumns with " + (rows + newRows) + " rows");

            for (var i = 0; i < _repeatTimes; i++)
            {
                BulkInsert(_bookCollection);

                // Update some rows
                for (var j = 0; j < 200; j++)
                {
                    var newBook = fixture.Build<Book>().Without(s => s.ISBN).Create();
                    var prevIsbn = _bookCollection[j].ISBN;
                    _bookCollection[j] = newBook;
                    _bookCollection[j].ISBN = prevIsbn;
                }

                // Add new rows
                _bookCollection.AddRange(_randomizer.GetRandomCollection(newRows));


                var time = BulkInsertOrUpdateAllColumns(_bookCollection);
                results.Add(time);

                Assert.Equal(rows + newRows, _dataAccess.GetBookCount());

            }

            var avg = results.Average(l => l);
            Trace.WriteLine("Average result (" + _repeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");
        }

        [Fact]
        public void SqlBulkTools_BulkUpdate()
        {
            const int rows = 500;

            var fixture = new Fixture();
            fixture.Customizations.Add(new PriceBuilder());
            fixture.Customizations.Add(new IsbnBuilder());
            fixture.Customizations.Add(new TitleBuilder());

            BulkDelete(_dataAccess.GetBookList());

            var results = new List<long>();

            Trace.WriteLine("Testing BulkUpdate with " + rows + " rows");

            for (var i = 0; i < _repeatTimes; i++)
            {

                _bookCollection = _randomizer.GetRandomCollection(rows);
                BulkInsert(_bookCollection);

                // Update half the rows
                for (var j = 0; j < rows / 2; j++)
                {
                    var newBook = fixture.Build<Book>().Without(s => s.Id).Without(s => s.ISBN).Create();
                    var prevIsbn = _bookCollection[j].ISBN;
                    _bookCollection[j] = newBook;
                    _bookCollection[j].ISBN = prevIsbn;

                }

                var time = BulkUpdate(_bookCollection);
                results.Add(time);

                var testUpdate = _dataAccess.GetBookList().FirstOrDefault();
                Assert.Equal(_bookCollection[0].Price, testUpdate?.Price);
                Assert.Equal(_bookCollection[0].Title, testUpdate?.Title);
                Assert.Equal(_dataAccess.GetBookCount(), _bookCollection.Count);

                BulkDelete(_bookCollection);
            }
            var avg = results.Average(l => l);
            Trace.WriteLine("Average result (" + _repeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");
        }

        [Fact]
        public void SqlBulkTools_BulkUpdateOnIdentityColumn()
        {
            const int rows = 500;

            var fixture = new Fixture();
            fixture.Customizations.Add(new PriceBuilder());
            fixture.Customizations.Add(new IsbnBuilder());
            fixture.Customizations.Add(new TitleBuilder());
            var bulk = new BulkOperations();

            BulkDelete(_dataAccess.GetBookList());

            var books = _randomizer.GetRandomCollection(rows).ToDictionaryList();

            using (var trans = new TransactionScope())
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    bulk.Setup()
                    .ForCollection(books)
                    .WithPropertyTypes(typeof(Book).ToPropertyTypes())
                    .WithTable("Books")
                    .AddAllColumns()
                    .BulkInsert()
                    .SetIdentityColumn("Id", ColumnDirectionType.InputOutput)
                    .Commit(conn);

                    // Update half the rows
                    for (var j = 0; j < rows / 2; j++)
                    {
                        var newBook = fixture.Build<Book>().Without(s => s.Id).Without(s => s.ISBN).Create();
                        var prevId = books[j]["Id"];
                        books[j] = newBook.ToDictionary();
                        books[j]["Id"] = prevId;
                    }

                    bulk.Setup()
                        .ForCollection(books)
                        .WithPropertyTypes(typeof(Book).ToPropertyTypes())
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkUpdate()
                        .MatchTargetOn("Id")
                        .SetIdentityColumn("Id")
                        .Commit(conn);
                }

                trans.Complete();
            }

            var testUpdate = _dataAccess.GetBookList().FirstOrDefault();
            Assert.Equal(books[0]["Price"], testUpdate?.Price);
            Assert.Equal(books[0]["Title"], testUpdate?.Title);
            Assert.Equal(books.Count, _dataAccess.GetBookCount());
        }

        [Fact]
        public void SqlBulkTools_BulkDelete()
        {
            const int rows = 500;

            _bookCollection = _randomizer.GetRandomCollection(rows);
            BulkDelete(_dataAccess.GetBookList());

            var results = new List<long>();

            Trace.WriteLine("Testing BulkDelete with " + rows + " rows");

            for (var i = 0; i < _repeatTimes; i++)
            {
                BulkInsert(_bookCollection);
                var time = BulkDelete(_bookCollection);
                results.Add(time);
                Assert.Equal(0, _dataAccess.GetBookCount());
            }
            var avg = results.Average(l => l);
            Trace.WriteLine("Average result (" + _repeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");

        }

        [Fact]
        public void SqlBulkTools_IdentityColumnWhenNotSet_ThrowsIdentityException()
        {
            // Arrange
            BulkDelete(_dataAccess.GetBookList());
            _bookCollection = _randomizer.GetRandomCollection(20);

            var bulk = new BulkOperations();

            using var conn = new SqlConnection(_dataAccess.ConnectionString);

            Assert.Throws<IdentityException>(() => 
                bulk.Setup()
                    .ForCollection(_bookCollection.ToDictionaryList())
                    .WithPropertyTypes(typeof(Book).ToPropertyTypes())
                    .WithTable("Books")
                    .AddAllColumns()
                    .BulkUpdate()
                    .MatchTargetOn("Id")
                    .Commit(conn));
        }

        [Fact]
        public void SqlBulkTools_IdentityColumnSet_UpdatesTargetWhenSetIdentityColumn()
        {
            // Arrange
            BulkDelete(_dataAccess.GetBookList());
            var bulk = new BulkOperations();
            _bookCollection = _randomizer.GetRandomCollection(20);
            var testDesc = "New Description";

            BulkInsert(_bookCollection);

            _bookCollection = _dataAccess.GetBookList();
            _bookCollection.First().Description = testDesc;

            using (var trans = new TransactionScope())
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    bulk.Setup()
                        .ForCollection(_bookCollection.ToDictionaryList())
                        .WithPropertyTypes(typeof(Book).ToPropertyTypes())
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkUpdate()
                        .SetIdentityColumn("Id")
                        .MatchTargetOn("Id")
                        .Commit(conn);
                }

                trans.Complete();
            }
            // Assert
            Assert.Equal(testDesc, _dataAccess.GetBookList().First().Description);
        }

        [Fact]
        public void SqlBulkTools_WithConflictingTableName_DeletesAndInsertsToCorrectTable()
        {
            // Arrange           
            var bulk = new BulkOperations();

            var conflictingSchemaCol = new List<SchemaTest2>();

            for (var i = 0; i < 30; i++)
            {
                conflictingSchemaCol.Add(new SchemaTest2() { ColumnA = "ColumnA " + i });
            }

            using (var trans = new TransactionScope())
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    bulk.Setup()
                        .ForCollection(conflictingSchemaCol.ToDictionaryList())
                        .WithPropertyTypes(typeof(SchemaTest2).ToPropertyTypes())
                        .WithTable("SchemaTest")
                        .WithSchema("AnotherSchema")
                        .AddColumn("ColumnA")
                        .BulkDelete()
                        .MatchTargetOn("ColumnA")
                        .Commit(conn); // Remove existing rows

                    bulk.Setup()
                        .ForCollection(conflictingSchemaCol.ToDictionaryList())
                        .WithPropertyTypes(typeof(SchemaTest2).ToPropertyTypes())
                        .WithTable("SchemaTest")
                        .WithSchema("AnotherSchema")
                        .AddAllColumns()
                        .BulkInsert()
                        .Commit(conn); // Add new rows
                }

                trans.Complete();
            }

            // Assert
            Assert.True(_dataAccess.GetSchemaTest2List().Any());
        }    

        [Fact]
        public void SqlBulkTools_WithCustomSchema_WhenWithTableIncludesSchemaName()
        {
            // Arrange           
            var bulk = new BulkOperations();

            var conflictingSchemaCol = new List<SchemaTest2>();

            for (var i = 0; i < 30; i++)
            {
                conflictingSchemaCol.Add(new SchemaTest2() { ColumnA = "ColumnA " + i });
            }

            using (var trans = new TransactionScope())
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    bulk.Setup()
                        .ForCollection(conflictingSchemaCol.ToDictionaryList())
                        .WithPropertyTypes(typeof(SchemaTest2).ToPropertyTypes())
                        .WithTable("AnotherSchema.SchemaTest")
                        .AddColumn("ColumnA")
                        .BulkDelete()
                        .MatchTargetOn("ColumnA")
                        .Commit(conn); // Remove existing rows

                    bulk.Setup()
                        .ForCollection(conflictingSchemaCol.ToDictionaryList())
                        .WithPropertyTypes(typeof(SchemaTest2).ToPropertyTypes())
                        .WithTable("[AnotherSchema].[SchemaTest]")
                        .AddAllColumns()
                        .BulkInsert()
                        .Commit(conn); // Add new rows
                }

                trans.Complete();
            }

            // Assert
            Assert.True(_dataAccess.GetSchemaTest2List().Any());
        }

        [Fact]
        public void SqlBulkTools_ThrowsException_WhenTableNameIsIncorrect()
        {
            // Arrange           
            var bulk = new BulkOperations();

            using var trans = new TransactionScope();

            using (var conn = new SqlConnection(_dataAccess.ConnectionString))
            {
                Assert.Throws<SqlBulkToolsException>(() =>
                    bulk.Setup()
                        .ForCollection(new List<SchemaTest2>().ToDictionaryList())
                        .WithPropertyTypes(typeof(SchemaTest2).ToPropertyTypes())
                        .WithTable("SchemaTest.AnotherSchema.TooManyPeriods")
                        .AddColumn("ColumnA")
                        .BulkDelete()
                        .MatchTargetOn("ColumnA")
                        .Commit(conn));
            }

            trans.Complete();
        }

        [Fact]
        public void SqlBulkTools_ThrowsException_WhenSchemaDefinedTwice()
        {
            // Arrange           
            var bulk = new BulkOperations();

            using var trans = new TransactionScope();

            using (var conn = new SqlConnection(_dataAccess.ConnectionString))
            {
                Assert.Throws<SqlBulkToolsException>(() =>
                    bulk.Setup()
                        .ForCollection(new List<SchemaTest2>().ToDictionaryList())
                        .WithPropertyTypes(typeof(SchemaTest2).ToPropertyTypes())
                        .WithTable("SchemaTest.AnotherSchema")
                        .WithSchema("YetAnotherSchema")
                        .AddColumn("ColumnA")
                        .BulkDelete()
                        .MatchTargetOn("ColumnA")
                        .Commit(conn));
            }

            trans.Complete();
        }

        [Fact]
        public void SqlBulkTools_BulkDeleteOnId_AddItemsThenRemovesAllItems()
        {
            // Arrange           
            var bulk = new BulkOperations();

            var col = new List<SchemaTest1>();

            for (var i = 0; i < 30; i++)
            {
                col.Add(new SchemaTest1() { ColumnB = "ColumnA " + i });
            }

            // Act
            using (var trans = new TransactionScope())
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {

                    bulk.Setup()
                        .ForCollection(col.ToDictionaryList())
                        .WithPropertyTypes(typeof(SchemaTest1).ToPropertyTypes())
                        .WithTable("SchemaTest") // Don't specify schema. Default schema dbo is used. 
                        .AddAllColumns()
                        .BulkInsert()
                        .Commit(conn);
                }
                trans.Complete();
            }           

            using (var secondConn = new SqlConnection(_dataAccess.ConnectionString))
            {
                var allItems = _dataAccess.GetSchemaTest1List();
                bulk.Setup()
                    .ForCollection(allItems.ToDictionaryList())
                    .WithPropertyTypes(typeof(SchemaTest1).ToPropertyTypes())
                    .WithTable("SchemaTest")
                    .AddColumn("Id")
                    .BulkDelete()
                    .MatchTargetOn("Id")
                    .Commit(secondConn);
            }

            // Assert
            Assert.False(_dataAccess.GetSchemaTest1List().Any());
        }

        [Fact]
        public void SqlBulkTools_BulkUpdate_PartialUpdateOnlyUpdatesSelectedColumns()
        {
            // Arrange
            var bulk = new BulkOperations();
            _bookCollection = _randomizer.GetRandomCollection(30);

            BulkDelete(_dataAccess.GetBookList());
            BulkInsert(_bookCollection);

            // Update just the price on element 5
            var elemToUpdate = 5;
            decimal updatedPrice = 9999999;
            var originalElement = _bookCollection.ElementAt(elemToUpdate);
            _bookCollection.ElementAt(elemToUpdate).Price = updatedPrice;

            using (var trans = new TransactionScope())
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    // Act           
                    bulk.Setup()
                        .ForCollection(_bookCollection.ToDictionaryList())
                        .WithPropertyTypes(typeof(Book).ToPropertyTypes())
                        .WithTable("Books")
                        .AddColumn("Price")
                        .BulkUpdate()
                        .MatchTargetOn("ISBN")
                        .Commit(conn);
                }

                trans.Complete();
            }

            // Assert
            Assert.Equal(updatedPrice, _dataAccess.GetBookList(originalElement.ISBN).First().Price);

            /* Profiler shows: MERGE INTO [SqlBulkTools].[dbo].[Books] WITH (HOLDLOCK) AS Target USING #TmpTable 
             * AS Source ON Target.ISBN = Source.ISBN WHEN MATCHED THEN UPDATE SET Target.Price = Source.Price, 
             * Target.ISBN = Source.ISBN ; DROP TABLE #TmpTable; */
        }

        [Fact]
        public void SqlBulkTools_BulkInsertWithColumnMappings_CorrectlyMapsColumns()
        {
            var bulk = new BulkOperations();

            var col = new List<CustomColumnMappingTest>();

            for (var i = 0; i < 30; i++)
            {
                col.Add(new CustomColumnMappingTest() { NaturalIdTest = i, ColumnXIsDifferent = "ColumnX " + i, ColumnYIsDifferentInDatabase = i });
            }

            using (var trans = new TransactionScope())
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    bulk.Setup<CustomColumnMappingTest>()
                        .ForDeleteQuery()
                        .WithTable("CustomColumnMappingTests")
                        .Delete()
                        .AllRecords()
                        .Commit(conn);

                    bulk.Setup()
                        .ForCollection(col.ToDictionaryList())
                        .WithPropertyTypes(typeof(CustomColumnMappingTest).ToPropertyTypes())
                        .WithTable("CustomColumnMappingTests")
                        .AddAllColumns()
                        .CustomColumnMapping("ColumnXIsDifferent", "ColumnX")
                        .CustomColumnMapping("ColumnYIsDifferentInDatabase", "ColumnY")
                        .CustomColumnMapping("NaturalIdTest", "NaturalId")
                        .BulkInsert()
                        .Commit(conn);
                }

                trans.Complete();
            }

            // Assert
            Assert.True(_dataAccess.GetCustomColumnMappingTests().Any());
        }

        [Fact]
        public void SqlBulkTools_BulkInsertOrUpdateWithColumnMappings_CorrectlyMapsColumns()
        {
            var bulk = new BulkOperations();

            var col = new List<CustomColumnMappingTest>();

            for (var i = 0; i < 30; i++)
            {
                col.Add(new CustomColumnMappingTest() { NaturalIdTest = i, ColumnXIsDifferent = "ColumnX " + i, ColumnYIsDifferentInDatabase = i });
            }

            using (var trans = new TransactionScope())
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    bulk.Setup<CustomColumnMappingTest>()
                        .ForDeleteQuery()
                        .WithTable("CustomColumnMappingTests")
                        .Delete()
                        .AllRecords()
                        .SetBatchQuantity(5)
                        .Commit(conn);

                    bulk.Setup()
                        .ForCollection(col.ToDictionaryList())
                        .WithPropertyTypes(typeof(CustomColumnMappingTest).ToPropertyTypes())
                        .WithTable("CustomColumnMappingTests")
                        .AddAllColumns()
                        .CustomColumnMapping("ColumnXIsDifferent", "ColumnX")
                        .CustomColumnMapping("ColumnYIsDifferentInDatabase", "ColumnY")
                        .CustomColumnMapping("NaturalIdTest", "NaturalId")
                        .BulkInsertOrUpdate()
                        .MatchTargetOn("NaturalIdTest")
                        //.UpdateWhen(x => x.ColumnXIsDifferent != "me")
                        .Commit(conn);
                }

                trans.Complete();
            }

            // Assert
            Assert.True(_dataAccess.GetCustomColumnMappingTests().Any());
        }

        [Fact]
        public void SqlBulkTools_BulkInsertOrUpdateWithManualColumnMappings_CorrectlyMapsColumns()
        {
            var bulk = new BulkOperations();

            var col = new List<CustomColumnMappingTest>();

            for (var i = 0; i < 30; i++)
            {
                col.Add(new CustomColumnMappingTest() { NaturalIdTest = i, ColumnXIsDifferent = "ColumnX " + i, ColumnYIsDifferentInDatabase = i });
            }

            using (var trans = new TransactionScope())
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    bulk.Setup<CustomColumnMappingTest>()
                        .ForDeleteQuery()
                        .WithTable("CustomColumnMappingTests")
                        .Delete()
                        .AllRecords()
                        .Commit(conn);

                    bulk.Setup()
                        .ForCollection(col.ToDictionaryList())
                        .WithPropertyTypes(typeof(CustomColumnMappingTest).ToPropertyTypes())
                        .WithTable("CustomColumnMappingTests")
                        .AddColumn("ColumnXIsDifferent", "ColumnX")
                        .AddColumn("ColumnYIsDifferentInDatabase", "ColumnY")
                        .AddColumn("NaturalIdTest", "NaturalId")
                        .BulkInsertOrUpdate()
                        .MatchTargetOn("NaturalIdTest")
                        //.UpdateWhen(x => x.ColumnXIsDifferent != "me")
                        .Commit(conn);
                }

                trans.Complete();
            }

            // Assert
            Assert.True(_dataAccess.GetCustomColumnMappingTests().Any());
        }

        [Fact]
        public void SqlBulkTools_BulkUpdateWithManualColumnMappings_CorrectlyMapsColumns()
        {
            var bulk = new BulkOperations();

            var colObjects = new List<CustomColumnMappingTest>();

            for (var i = 0; i < 30; i++)
            {
                colObjects.Add(new CustomColumnMappingTest() { NaturalIdTest = i, ColumnXIsDifferent = "ColumnX " + i, ColumnYIsDifferentInDatabase = i });
            }

            var col = colObjects.ToDictionaryList();

            using (var trans = new TransactionScope())
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    bulk.Setup<CustomColumnMappingTest>()
                        .ForDeleteQuery()
                        .WithTable("CustomColumnMappingTests")
                        .Delete()
                        .AllRecords()
                        .Commit(conn);

                    bulk.Setup()
                        .ForCollection(col)
                        .WithPropertyTypes(typeof(CustomColumnMappingTest).ToPropertyTypes())
                        .WithTable("CustomColumnMappingTests")
                        .AddColumn("ColumnXIsDifferent", "ColumnX")
                        .AddColumn("ColumnYIsDifferentInDatabase", "ColumnY")
                        .AddColumn("NaturalIdTest", "NaturalId")
                        .BulkInsert()
                        .Commit(conn);

                    foreach (var item in col)
                    {
                        item["ColumnXIsDifferent"] = "Updated";
                    }

                    bulk.Setup()
                        .ForCollection(col)
                        .WithPropertyTypes(typeof(CustomColumnMappingTest).ToPropertyTypes())
                        .WithTable("CustomColumnMappingTests")
                        .AddColumn("ColumnXIsDifferent", "ColumnX")
                        .AddColumn("NaturalIdTest", "NaturalId")
                        .BulkUpdate()
                        .MatchTargetOn("NaturalIdTest")
                        .Commit(conn);
                }

                trans.Complete();
            }

            // Assert
            Assert.True(_dataAccess.GetCustomColumnMappingTests().First().ColumnXIsDifferent == "Updated");
        }

        [Fact]
        public void SqlBulkTools_BulkUpdateWithColumnMappings_CorrectlyMapsColumns()
        {
            var bulk = new BulkOperations();

            var col = new List<CustomColumnMappingTest>();

            for (var i = 0; i < 30; i++)
            {
                col.Add(new CustomColumnMappingTest() { NaturalIdTest = i, ColumnXIsDifferent = "ColumnX " + i, ColumnYIsDifferentInDatabase = i });
            }

            using (var trans = new TransactionScope())
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    bulk.Setup<CustomColumnMappingTest>()
                        .ForDeleteQuery()
                        .WithTable("CustomColumnMappingTests")
                        .Delete()
                        .AllRecords()
                        .Commit(conn);

                    bulk.Setup()
                        .ForCollection(col.ToDictionaryList())
                        .WithPropertyTypes(typeof(CustomColumnMappingTest).ToPropertyTypes())
                        .WithTable("CustomColumnMappingTests")
                        .AddAllColumns()
                        .CustomColumnMapping("ColumnXIsDifferent", "ColumnX")
                        .CustomColumnMapping("ColumnYIsDifferentInDatabase", "ColumnY")
                        .CustomColumnMapping("NaturalIdTest", "NaturalId")
                        .BulkInsert()
                        .Commit(conn);

                    foreach (var item in col)
                    {
                        item.ColumnXIsDifferent = "Updated";
                    }

                    bulk.Setup()
                        .ForCollection(col.ToDictionaryList())
                        .WithPropertyTypes(typeof(CustomColumnMappingTest).ToPropertyTypes())
                        .WithTable("CustomColumnMappingTests")
                        .AddAllColumns()
                        .CustomColumnMapping("ColumnXIsDifferent", "ColumnX")
                        .CustomColumnMapping("ColumnYIsDifferentInDatabase", "ColumnY")
                        .CustomColumnMapping("NaturalIdTest", "NaturalId")
                        .BulkUpdate()
                        .MatchTargetOn("NaturalIdTest")
                        .Commit(conn);
                }

                trans.Complete();
            }

            // Assert
            Assert.True(_dataAccess.GetCustomColumnMappingTests().First().ColumnXIsDifferent == "Updated");
        }

        [Fact]
        public void SqlBulkTools_WhenUsingReservedSqlKeywords()
        {
            var bulk = new BulkOperations();

            var list = new List<ReservedColumnNameTest>();

            for (var i = 0; i < 30; i++)
            {
                list.Add(new ReservedColumnNameTest() { Key = i });
            }

            using (var trans = new TransactionScope())
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    bulk.Setup<ReservedColumnNameTest>()
                        .ForDeleteQuery()
                        .WithTable("ReservedColumnNameTests")
                        .Delete()
                        .AllRecords()
                        .Commit(conn);

                    bulk.Setup()
                        .ForCollection(list.ToDictionaryList())
                        .WithPropertyTypes(typeof(ReservedColumnNameTest).ToPropertyTypes())
                        .WithTable("ReservedColumnNameTests")
                        .AddAllColumns()
                        .BulkInsertOrUpdate()
                        .MatchTargetOn("Id")
                        .SetIdentityColumn("Id")
                        .Commit(conn);
                }

                trans.Complete();
            }

            Assert.True(_dataAccess.GetReservedColumnNameTests().Count == 30);
        }

        [Fact]
        public void SqlBulkTools_BulkInsertOrUpdate_TestIdentityOutput()
        {
            BulkDelete(_dataAccess.GetBookList());
            var bulk = new BulkOperations();

            var books = _randomizer.GetRandomCollection(30).ToDictionaryList();

            using (var trans = new TransactionScope())
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    bulk.Setup()
                        .ForCollection(books)
                        .WithPropertyTypes(typeof(Book).ToPropertyTypes())
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsertOrUpdate()
                        .MatchTargetOn("ISBN")
                        .SetIdentityColumn("Id", ColumnDirectionType.InputOutput)
                        .Commit(conn);
                }

                trans.Complete();
            }

            var test = _dataAccess.GetBookList().ElementAt(10); // Random book within the 30 elements
            var expected = books.Single(x => (string)x["ISBN"] == test.ISBN);

            Assert.Equal(expected["Id"], test.Id);
        }

        [Fact]
        public void SqlBulkTools_BulkInsertOrUpdate_TestNullComparisonWithMatchTargetOn()
        {
            BulkDelete(_dataAccess.GetBookList());
            var bulk = new BulkOperations();

            var books = _randomizer.GetRandomCollection(30);

            books.ElementAt(0).Title = "Test_Null_Comparison";
            books.ElementAt(0).ISBN = null;
            BulkInsert(books);

            using (var trans = new TransactionScope())
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    bulk.Setup()
                        .ForCollection(books.ToDictionaryList())
                        .WithPropertyTypes(typeof(Book).ToPropertyTypes())
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsertOrUpdate()
                        .MatchTargetOn("ISBN")
                        .SetIdentityColumn("Id")
                        .Commit(conn);
                }

                trans.Complete();
            }

            var test = _dataAccess.GetBookList().Single(x => x.Title == "Test_Null_Comparison");

            Assert.Equal(30, _dataAccess.GetBookList().Count);
            Assert.Null(test.ISBN);
        }

        [Fact]
        public void SqlBulkTools_BulkInsertOrUpdate_ExcludeColumnTest()
        {
            // Remove existing records for a fresh test
            BulkDelete(_dataAccess.GetBookList());
            var bulk = new BulkOperations();
            // Get a list with random data
            var books = _randomizer.GetRandomCollection(30);

            // Set the original date as the date Donald Trump somehow won the US election. 
            var originalDate = new DateTime(2016, 11, 9);
            // Set the new date as the date Trump's presidency will end
            var updatedDate = new DateTime(2020, 11, 9);

            // Add dates to initial list
            books.ForEach(x =>
            {
                x.CreatedAt = originalDate;
                x.ModifiedAt = originalDate;
            });

            using (var trans = new TransactionScope())
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    // Insert initial list
                    bulk.Setup()
                        .ForCollection(books.ToDictionaryList())
                        .WithPropertyTypes(typeof(Book).ToPropertyTypes())
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsert()
                        .SetIdentityColumn("Id")
                        .Commit(conn);

                    // Update list with new dates
                    books.ForEach(x =>
                    {
                        x.CreatedAt = updatedDate;
                        x.ModifiedAt = updatedDate;
                    });

                    // Insert a random record
                    books.Add(new Book() { CreatedAt = updatedDate, ModifiedAt = updatedDate, Price = 29.99M, Title = "Trump likes woman", ISBN = "1234567891011" });

                    bulk.Setup()
                        .ForCollection(books.ToDictionaryList())
                        .WithPropertyTypes(typeof(Book).ToPropertyTypes())
                        .WithTable("Books")
                        .AddAllColumns() // Both ModifiedAt and CreatedAt are added implicitly here
                        .BulkInsertOrUpdate()
                        .MatchTargetOn("ISBN")
                        .SetIdentityColumn("Id")
                        .ExcludeColumnFromUpdate("CreatedAt") // Insert or update with new dates but ignore created date. 
                        .Commit(conn);
                }

                trans.Complete();
            }
            var updatedIsbn = books[10].ISBN;
            var addedIsbn = books.Last().ISBN;
            var updatedBookUnderTest = _dataAccess.GetBookList(updatedIsbn).First();
            var createdBookUnderTest = _dataAccess.GetBookList(addedIsbn).First();

            Assert.Equal(updatedDate, updatedBookUnderTest.ModifiedAt); // The ModifiedAt should be updated
            Assert.Equal(originalDate, updatedBookUnderTest.CreatedAt); // The CreatedAt should be unchanged       
            Assert.Equal(updatedDate, createdBookUnderTest.CreatedAt); // CreatedAt should be new date because it was an insert
        }

        [Fact]
        public void SqlBulkTools_BulkInsertOrUpdateWithSelectedColumns_TestIdentityOutput()
        {
            BulkDelete(_dataAccess.GetBookList());
            var bulk = new BulkOperations();

            var books = _randomizer.GetRandomCollection(30).ToDictionaryList();

            using (var trans = new TransactionScope())
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    bulk.Setup()
                        .ForCollection(books)
                        .WithPropertyTypes(typeof(Book).ToPropertyTypes())
                        .WithTable("Books")
                        .AddColumn("ISBN")
                        .AddColumn("Description")
                        .AddColumn("Title")
                        .AddColumn("Price")
                        .BulkInsertOrUpdate()
                        .MatchTargetOn("ISBN")
                        .SetIdentityColumn("Id", ColumnDirectionType.InputOutput)
                        .Commit(conn);
                }

                trans.Complete();
            }

            var test = _dataAccess.GetBookList().ElementAt(10); // Random book within the 30 elements
            var expected = books.Single(x => (string)x["ISBN"] == test.ISBN);

            Assert.Equal(expected["Id"], test.Id);
        }

        [Fact]
        public void SqlBulkTools_BulkInsert_TestIdentityOutput()
        {
            BulkDelete(_dataAccess.GetBookList());

            var bulk = new BulkOperations();
            var books = _randomizer.GetRandomCollection(30).ToDictionaryList();

            using (var trans = new TransactionScope())
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    bulk.Setup()
                        .ForCollection(_randomizer.GetRandomCollection(60).ToDictionaryList())
                        .WithPropertyTypes(typeof(Book).ToPropertyTypes())
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsert()
                        .Commit(conn);

                    bulk.Setup()
                        .ForCollection(books)
                        .WithPropertyTypes(typeof(Book).ToPropertyTypes())
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsert()
                        .SetIdentityColumn("Id", ColumnDirectionType.InputOutput)
                        .Commit(conn);
                }

                trans.Complete();
            }

            var test = _dataAccess.GetBookList().ElementAt(80); // Random between random items before test and total items after test. 
            var expected = books.Single(x => (string)x["ISBN"] == test.ISBN);

            Assert.Equal(expected["Id"], test.Id);
        }

        [Fact]
        public void SqlBulkTools_BulkInsertWithSelectedColumns_TestIdentityOutput()
        {
            BulkDelete(_dataAccess.GetBookList());

            var books = _randomizer.GetRandomCollection(30).ToDictionaryList();

            using (var trans = new TransactionScope())
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    var bulk = new BulkOperations();
                    bulk.Setup()
                        .ForCollection(books)
                        .WithPropertyTypes(typeof(Book).ToPropertyTypes())
                        .WithTable("Books")
                        .WithBulkCopySettings(new BulkCopySettings()
                        {
                            BatchSize = 5000
                        })
                        .AddColumn("Title")
                        .AddColumn("Price")
                        .AddColumn("Description")
                        .AddColumn("ISBN")
                        .AddColumn("PublishDate")
                        .BulkInsert()
                        .TmpDisableAllNonClusteredIndexes()
                        .SetIdentityColumn("Id", ColumnDirectionType.InputOutput)
                        .Commit(conn);
                }

                trans.Complete();
            }

            var actual = _dataAccess.GetBookList().ElementAt(15); // Random book within the 30 elements
            var expected = books.Single(x => (string)x["ISBN"] == actual.ISBN);

            Assert.Equal(expected["Id"], actual.Id);
            Assert.Equal(expected["Title"], actual.Title);
            Assert.Equal(expected["Price"], actual.Price);
            Assert.Equal(expected["Description"], actual.Description);
            Assert.Equal(expected["ISBN"], actual.ISBN);
        }

        [Fact]
        public void SqlBulkTools_BulkDeleteWithSelectedColumns_TestIdentityOutput()
        {
            BulkDelete(_dataAccess.GetBookList());

            _dataAccess.ReseedBookIdentity(10);

            var books = _randomizer.GetRandomCollection(30);
            BulkInsert(books);

            var bookdicts = books.ToDictionaryList();

            using (var trans = new TransactionScope())
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    var bulk = new BulkOperations();
                    bulk.Setup()
                        .ForCollection(bookdicts)
                        .WithPropertyTypes(typeof(Book).ToPropertyTypes())
                        .WithTable("Books")
                        .WithBulkCopySettings(new BulkCopySettings()
                        {
                            BatchSize = 5000
                        })
                        .AddColumn("ISBN")
                        .BulkDelete()
                        .MatchTargetOn("ISBN")
                        .SetIdentityColumn("Id", ColumnDirectionType.InputOutput)
                        .Commit(conn);
                }

                trans.Complete();
            }

            var test = bookdicts.First();

            Assert.True((int)test["Id"] == 10 || (int)test["Id"] == 11);

            // Reset identity seed back to default
            _dataAccess.ReseedBookIdentity(0);
        }

        [Fact]
        public void SqlBulkTools_BulkUpdateWithSelectedColumns_TestIdentityOutput()
        {
            BulkDelete(_dataAccess.GetBookList());

            var bulk = new BulkOperations();

            var books = _randomizer.GetRandomCollection(30);
            BulkInsert(books);

            var bookdicts = books.ToDictionaryList();

            using (var trans = new TransactionScope())
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    bulk.Setup()
                        .ForCollection(bookdicts)
                        .WithPropertyTypes(typeof(Book).ToPropertyTypes())
                        .WithTable("Books")
                        .AddColumn("ISBN")
                        .AddColumn("Description")
                        .AddColumn("Title")
                        .AddColumn("Price")
                        .BulkUpdate()
                        .MatchTargetOn("ISBN")
                        .SetIdentityColumn("Id", ColumnDirectionType.InputOutput)
                        .Commit(conn);
                }

                trans.Complete();
            }

            var test = _dataAccess.GetBookList().ElementAt(10); // Random book within the 30 elements
            var expected = bookdicts.Single(x => (string)x["ISBN"] == test.ISBN);

            Assert.Equal(expected["Id"], test.Id);
        }

        [Fact]
        public void SqlBulkTools_BulkInsertAddInvalidDataType_ThrowsSqlBulkToolsExceptionException()
        {
            var bulk = new BulkOperations();

            var books = _randomizer.GetRandomCollection(30);
            BulkInsert(books);

            using var trans = new TransactionScope();

            using (var conn = new SqlConnection(_dataAccess.ConnectionString))
            {
                Assert.Throws<SqlBulkToolsException>(() =>
                    bulk.Setup()
                        .ForCollection(books.ToDictionaryList())
                        .WithPropertyTypes(typeof(Book).ToPropertyTypes())
                        .WithTable("Books")
                        .AddColumn("ISBN")
                        .AddColumn("InvalidType")
                        .BulkInsert()
                        .Commit(conn));
            }

            trans.Complete();
        }

        [Fact]
        public void SqlBulkTools_BulkInsertWithGenericType()
        {
            BulkDelete(_dataAccess.GetBookList());
            var bulk = new BulkOperations();

            _bookCollection = _randomizer.GetRandomCollection(30);

            using (var trans = new TransactionScope())
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    bulk.Setup()
                    .ForCollection(_bookCollection.Select(x => new { x.Description, x.ISBN, x.Id, x.Price }).ToDictionaryList())
                    .WithPropertyTypes(typeof(Book).ToPropertyTypes())
                    .WithTable("Books")
                    .AddColumn("Id")
                    .AddColumn("Description")
                    .AddColumn("ISBN")
                    .AddColumn("Price")
                    .BulkInsert()
                    .SetIdentityColumn("Id")
                    .Commit(conn);
                }

                trans.Complete();
            }

            Assert.True(_dataAccess.GetBookList().Any());
        }

        [Fact]
        public void SqlBulkTools_BulkInsertOrUpdate_TestDataTypes()
        { 
            BulkDelete(_dataAccess.GetBookList());

            var todaysDate = DateTime.Today;
            var guid = Guid.NewGuid();

            var bulk = new BulkOperations();
            var dataTypeTest = new List<TestDataType>()
            {
                new TestDataType()
                {
                    BigIntTest = 342324324324324324,
                    TinyIntTest = 126,
                    DateTimeTest = todaysDate,
                    DateTime2Test = new DateTime(2008, 12, 12, 10, 20, 30),
                    DateTest = new DateTime(2007, 7, 5, 20, 30, 10),
                    TimeTest = new TimeSpan(23, 32, 23),
                    SmallDateTimeTest = new DateTime(2005, 7, 14),
                    BinaryTest = new byte[] {0, 3, 3, 2, 4, 3},
                    VarBinaryTest = new byte[] {3, 23, 33, 243},
                    DecimalTest = 178.43M,
                    MoneyTest = 24333.99M,
                    SmallMoneyTest = 103.32M,
                    RealTest = 32.53F,
                    NumericTest = 154343.3434342M,
                    FloatTest = 232.43F,
                    FloatTest2 = 43243.34,
                    TextTest = "This is some text.",
                    GuidTest = guid,
                    CharTest = "Some",
                    XmlTest = "<title>The best SQL Bulk tool</title>",
                    NCharTest = "SomeText",
                    ImageTest = new byte[] {3,3,32,4},
                    TestSqlGeometry = SqlGeometry.Point(-2.74612, 53.881238, 4326),
                    TestSqlGeography = SqlGeography.Point(-5, 43.432, 4326)
                }
            };

            using (var trans = new TransactionScope())
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    bulk.Setup<TestDataType>()
                        .ForDeleteQuery()
                        .WithTable("TestDataTypes")
                        .Delete()
                        .AllRecords()
                        .Commit(conn);

                    bulk.Setup()
                        .ForCollection(dataTypeTest.ToDictionaryList())
                        .WithPropertyTypes(typeof(TestDataType).ToPropertyTypes())
                        .WithTable("TestDataTypes")
                        .AddAllColumns()
                        .BulkInsertOrUpdate()
                        .MatchTargetOn("GuidTest")
                        .Commit(conn);
                }

                trans.Complete();
            }

            using (var conn = new SqlConnection(_dataAccess.ConnectionString))
            using (var command = new SqlCommand("SELECT TOP 1 * FROM [dbo].[TestDataTypes]", conn)
            {
                CommandType = CommandType.Text
            })
            {
                conn.Open();

                using var reader = command.ExecuteReader();
                while (reader.Read())
                {
                    Assert.Equal(232.43F, reader["FloatTest"]);
                    Assert.Equal(43243.34, reader["FloatTest2"]);
                    Assert.Equal(178.43M, reader["DecimalTest"]);
                    Assert.Equal(24333.99M, reader["MoneyTest"]);
                    Assert.Equal(103.32M, reader["SmallMoneyTest"]);
                    Assert.Equal(32.53F, reader["RealTest"]);
                    Assert.Equal(154343.3434342M, reader["NumericTest"]);
                    Assert.Equal(todaysDate, reader["DateTimeTest"]);
                    Assert.Equal(new DateTime(2008, 12, 12, 10, 20, 30), reader["DateTime2Test"]);
                    Assert.Equal(new DateTime(2005, 7, 14), reader["SmallDateTimeTest"]);
                    Assert.Equal(new DateTime(2007, 7, 5), reader["DateTest"]);
                    Assert.Equal(new TimeSpan(23, 32, 23), reader["TimeTest"]);
                    Assert.Equal(guid, reader["GuidTest"]);
                    Assert.Equal("This is some text.", reader["TextTest"]);
                    Assert.Equal("Some", reader["CharTest"].ToString().Trim());
                    Assert.Equal(126, (byte)reader["TinyIntTest"]);
                    Assert.Equal(342324324324324324, reader["BigIntTest"]);
                    Assert.Equal("<title>The best SQL Bulk tool</title>", reader["XmlTest"]);
                    Assert.Equal("SomeText", reader["NCharTest"].ToString().Trim());
                    Assert.Equal(new byte[] { 3, 3, 32, 4 }, (byte[])reader["ImageTest"]);
                    Assert.Equal(new byte[] { 0, 3, 3, 2, 4, 3 }, (byte[])reader["BinaryTest"]);
                    Assert.Equal(new byte[] { 3, 23, 33, 243 }, (byte[])reader["VarBinaryTest"]);
                    Assert.NotNull(reader["TestSqlGeometry"]);
                    Assert.NotNull(reader["TestSqlGeography"]);
                }
            }
        }

        private long BulkInsert(IEnumerable<Book> col)
        {
            var bulk = new BulkOperations();
            var watch = Stopwatch.StartNew();
            using (var trans = new TransactionScope())
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    bulk.Setup()
                        .ForCollection(col.ToDictionaryList())
                        .WithPropertyTypes(typeof(Book).ToPropertyTypes())
                        .WithTable("Books")
                        .WithBulkCopySettings(new BulkCopySettings()
                        {
                            BatchSize = 5000
                        })
                        .AddColumn("Title")
                        .AddColumn("Price")
                        .AddColumn("Description")
                        .AddColumn("ISBN")
                        .AddColumn("PublishDate")
                        .BulkInsert()
                        .TmpDisableAllNonClusteredIndexes()
                        .Commit(conn);
                }

                trans.Complete();
            }
            watch.Stop();
            var elapsedMs = watch.ElapsedMilliseconds;

            return elapsedMs;
        }

        private long BulkInsertAllColumns(IEnumerable<Book> col)
        {
            var bulk = new BulkOperations();
            var watch = Stopwatch.StartNew();
            using (var trans = new TransactionScope(
                                TransactionScopeOption.RequiresNew,
                                new TimeSpan(0, 5, 0)))
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    bulk.Setup()
                        .ForCollection(col.ToDictionaryList())
                        .WithPropertyTypes(typeof(Book).ToPropertyTypes())
                        .WithTable("Books")
                        .WithBulkCopySettings(new BulkCopySettings()
                        {
                            BatchSize = 8000,
                            BulkCopyTimeout = 500
                        })
                        .AddAllColumns()
                        .BulkInsert()
                        .TmpDisableAllNonClusteredIndexes()
                        .Commit(conn);
                }

                trans.Complete();
            }
            watch.Stop();
            var elapsedMs = watch.ElapsedMilliseconds;

            return elapsedMs;
        }

        private long BulkInsertOrUpdate(IEnumerable<Book> col)
        {
            var bulk = new BulkOperations();
            var watch = Stopwatch.StartNew();
            using (var trans = new TransactionScope())
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    bulk.Setup()
                        .ForCollection(col.ToDictionaryList())
                        .WithPropertyTypes(typeof(Book).ToPropertyTypes())
                        .WithTable("Books")
                        .AddColumn("Title")
                        .AddColumn("Price")
                        .AddColumn("Description")
                        .AddColumn("ISBN")
                        .AddColumn("PublishDate")
                        .BulkInsertOrUpdate()
                        .MatchTargetOn("ISBN")
                        .Commit(conn);
                }

                trans.Complete();
            }
            watch.Stop();
            var elapsedMs = watch.ElapsedMilliseconds;

            return elapsedMs;
        }

        private long BulkInsertOrUpdateAllColumns(IEnumerable<Book> col)
        {
            var bulk = new BulkOperations();

            var watch = Stopwatch.StartNew();
            using (var trans = new TransactionScope())
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    bulk.Setup()
                        .ForCollection(col.ToDictionaryList())
                        .WithPropertyTypes(typeof(Book).ToPropertyTypes())
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsertOrUpdate()
                        .SetIdentityColumn("Id")
                        .MatchTargetOn("ISBN")
                        .Commit(conn);
                }

                trans.Complete();
            }
            watch.Stop();
            var elapsedMs = watch.ElapsedMilliseconds;

            return elapsedMs;
        }

        private long BulkUpdate(IEnumerable<Book> col)
        {
            var bulk = new BulkOperations();
            var watch = Stopwatch.StartNew();
            using (var trans = new TransactionScope())
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    bulk.Setup()
                        .ForCollection(col.ToDictionaryList())
                        .WithPropertyTypes(typeof(Book).ToPropertyTypes())
                        .WithTable("Books")
                        .AddColumn("Title")
                        .AddColumn("Price")
                        .AddColumn("Description")
                        .AddColumn("PublishDate")
                        .BulkUpdate()
                        .MatchTargetOn("ISBN")
                        .Commit(conn);
                }

                trans.Complete();
            }
            watch.Stop();
            var elapsedMs = watch.ElapsedMilliseconds;

            return elapsedMs;
        }

        private long BulkDelete(IEnumerable<Book> col)
        {
            var bulk = new BulkOperations();

            var watch = Stopwatch.StartNew();
            using (var trans = new TransactionScope())
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    bulk.Setup()
                        .ForCollection(col.ToDictionaryList())
                        .WithPropertyTypes(typeof(Book).ToPropertyTypes())
                        .WithTable("Books")
                        .AddColumn("ISBN")
                        .BulkDelete()
                        .MatchTargetOn("ISBN")
                        .Commit(conn);
                }

                trans.Complete();
            }
            watch.Stop();
            var elapsedMs = watch.ElapsedMilliseconds;

            return elapsedMs;
        }
    }
}
