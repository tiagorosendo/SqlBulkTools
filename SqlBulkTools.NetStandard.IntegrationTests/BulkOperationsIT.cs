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
using System.Transactions;
using Xunit;

namespace SqlBulkTools.IntegrationTests
{
    [Collection("IntegrationTests")]
    public class BulkOperationsIt
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

                    bulk.Setup<CustomIdentityColumnNameTest>()
                        .ForCollection(customIdentityColumnList)
                        .WithTable("CustomIdentityColumnNameTest")
                        .AddColumn(x => x.Id, "ID_COMPANY")
                        .AddColumn(x => x.ColumnA)
                        .BulkInsertOrUpdate()
                        .SetIdentityColumn(x => x.Id)
                        .MatchTargetOn(x => x.ColumnA)
                        .Commit(conn);
                }

                trans.Complete();
            }

            Assert.True(_dataAccess.GetCustomIdentityColumnNameTestList().Count == 30);
        }

        [Fact]
        public void SqlBulkTools_BulkInsertForComplexType_AddAllColumns()
        {
            var bulk = new BulkOperations();
            var complexTypeModelList = new List<ComplexTypeModel>();

            for (var i = 0; i < 30; i++)
            {
                complexTypeModelList.Add(new ComplexTypeModel
                {
                    AverageEstimate = new EstimatedStats
                    {
                        TotalCost = 23.20
                    },
                    MinEstimate = new EstimatedStats
                    {
                        TotalCost = 10.20
                    },
                    Competition = 30,
                    SearchVolume = 234.34
                });
            }

            using (var trans = new TransactionScope())
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    bulk.Setup<ComplexTypeModel>()
                        .ForDeleteQuery()
                        .WithTable("ComplexTypeTest")
                        .Delete()
                        .AllRecords()
                        .Commit(conn);

                    bulk.Setup<ComplexTypeModel>()
                        .ForCollection(complexTypeModelList)
                        .WithTable("ComplexTypeTest")
                        .AddAllColumns()
                        .BulkInsert()
                        .SetIdentityColumn(x => x.Id)
                        .Commit(conn);
                }

                trans.Complete();
            }

            Assert.True(_dataAccess.GetComplexTypeModelCount() > 0);
        }

        [Fact]
        public void SqlBulkTools_BulkInsertOrUpdateForComplexType_AddAllColumns()
        {
            var bulk = new BulkOperations();
            var complexTypeModelList = new List<ComplexTypeModel>();

            for (var i = 0; i < 30; i++)
            {
                complexTypeModelList.Add(new ComplexTypeModel
                {
                    AverageEstimate = new EstimatedStats
                    {
                        TotalCost = 23.20
                    },
                    MinEstimate = new EstimatedStats
                    {
                        TotalCost = 10.20
                    },
                    Competition = 30,
                    SearchVolume = 234.34

                });
            }

            using (var trans = new TransactionScope())
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    bulk.Setup<ComplexTypeModel>()
                        .ForDeleteQuery()
                        .WithTable("ComplexTypeTest")
                        .Delete()
                        .AllRecords();

                    bulk.Setup<ComplexTypeModel>()
                        .ForCollection(complexTypeModelList)
                        .WithTable("ComplexTypeTest")
                        .AddAllColumns()
                        .BulkInsertOrUpdate()
                        .MatchTargetOn(x => x.Id)
                        .SetIdentityColumn(x => x.Id)
                        .Commit(conn);
                }

                trans.Complete();
            }

            Assert.True(_dataAccess.GetComplexTypeModelCount() > 0);
        }

        [Fact]
        public void SqlBulkTools_BulkInsertForComplexType_AddColumnsManually()
        {
            var bulk = new BulkOperations();
            var complexTypeModelList = new List<ComplexTypeModel>();

            for (var i = 0; i < 30; i++)
            {
                complexTypeModelList.Add(new ComplexTypeModel
                {
                    AverageEstimate = new EstimatedStats
                    {
                        TotalCost = 23.20
                    },
                    MinEstimate = new EstimatedStats
                    {
                        TotalCost = 10.20
                    },
                    Competition = 30,
                    SearchVolume = 234.34

                });
            }

            using (var trans = new TransactionScope())
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    bulk.Setup<ComplexTypeModel>()
                        .ForDeleteQuery()
                        .WithTable("ComplexTypeTest")
                        .Delete()
                        .AllRecords();

                    bulk.Setup<ComplexTypeModel>()
                        .ForCollection(complexTypeModelList)
                        .WithTable("ComplexTypeTest")
                        .AddColumn(x => x.AverageEstimate.CreationDate)
                        .AddColumn(x => x.AverageEstimate.TotalCost)
                        .AddColumn(x => x.Competition)
                        .AddColumn(x => x.MinEstimate.CreationDate, "MinEstimate_CreationDate") // Testing custom column mapping
                        .AddColumn(x => x.MinEstimate.TotalCost)
                        .AddColumn(x => x.SearchVolume)                                                
                        .BulkInsert()
                        .SetIdentityColumn(x => x.Id)
                        .Commit(conn);
                }

                trans.Complete();
            }

            Assert.True(_dataAccess.GetComplexTypeModelCount() > 0);
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
            _bookCollection = _randomizer.GetRandomCollection(rows);

            using (var trans = new TransactionScope())
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    bulk.Setup<Book>()
                    .ForCollection(_bookCollection)
                    .WithTable("Books")
                    .AddAllColumns()
                    .BulkInsert()
                    .SetIdentityColumn(x => x.Id, ColumnDirectionType.InputOutput)
                    .Commit(conn);


                    // Update half the rows
                    for (var j = 0; j < rows / 2; j++)
                    {
                        var newBook = fixture.Build<Book>().Without(s => s.Id).Without(s => s.ISBN).Create();
                        var prevId = _bookCollection[j].Id;
                        _bookCollection[j] = newBook;
                        _bookCollection[j].Id = prevId;

                    }

                    bulk.Setup<Book>()
                        .ForCollection(_bookCollection)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkUpdate()
                        .MatchTargetOn(x => x.Id)
                        .SetIdentityColumn(x => x.Id)
                        .Commit(conn);
                }

                trans.Complete();
            }

            var testUpdate = _dataAccess.GetBookList().FirstOrDefault();
            Assert.Equal(_bookCollection[0].Price, testUpdate?.Price);
            Assert.Equal(_bookCollection[0].Title, testUpdate?.Title);
            Assert.Equal(_bookCollection.Count, _dataAccess.GetBookCount());
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
                bulk.Setup<Book>()
                    .ForCollection(_bookCollection)
                    .WithTable("Books")
                    .AddAllColumns()
                    .BulkUpdate()
                    .MatchTargetOn(x => x.Id)
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
                    bulk.Setup<Book>()
                        .ForCollection(_bookCollection)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkUpdate()
                        .SetIdentityColumn(x => x.Id)
                        .MatchTargetOn(x => x.Id)
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
                    bulk.Setup<SchemaTest2>()
                        .ForCollection(conflictingSchemaCol)
                        .WithTable("SchemaTest")
                        .WithSchema("AnotherSchema")
                        .AddColumn(x => x.ColumnA)
                        .BulkDelete()
                        .MatchTargetOn(x => x.ColumnA)
                        .Commit(conn); // Remove existing rows

                    bulk.Setup<SchemaTest2>()
                        .ForCollection(conflictingSchemaCol)
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
                    bulk.Setup<SchemaTest2>()
                        .ForCollection(conflictingSchemaCol)
                        .WithTable("AnotherSchema.SchemaTest")
                        .AddColumn(x => x.ColumnA)
                        .BulkDelete()
                        .MatchTargetOn(x => x.ColumnA)
                        .Commit(conn); // Remove existing rows

                    bulk.Setup<SchemaTest2>()
                        .ForCollection(conflictingSchemaCol)
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
                    bulk.Setup<SchemaTest2>()
                        .ForCollection(new List<SchemaTest2>())
                        .WithTable("SchemaTest.AnotherSchema.TooManyPeriods")
                        .AddColumn(x => x.ColumnA)
                        .BulkDelete()
                        .MatchTargetOn(x => x.ColumnA)
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
                    bulk.Setup<SchemaTest2>()
                        .ForCollection(new List<SchemaTest2>())
                        .WithTable("SchemaTest.AnotherSchema")
                        .WithSchema("YetAnotherSchema")
                        .AddColumn(x => x.ColumnA)
                        .BulkDelete()
                        .MatchTargetOn(x => x.ColumnA)
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

                    bulk.Setup<SchemaTest1>()
                        .ForCollection(col)
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
                bulk.Setup<SchemaTest1>()
                    .ForCollection(allItems)
                    .WithTable("SchemaTest")
                    .AddColumn(x => x.Id)
                    .BulkDelete()
                    .MatchTargetOn(x => x.Id)
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
                    bulk.Setup<Book>()
                        .ForCollection(_bookCollection)
                        .WithTable("Books")
                        .AddColumn(x => x.Price)
                        .BulkUpdate()
                        .MatchTargetOn(x => x.ISBN)
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

                    bulk.Setup<CustomColumnMappingTest>()
                        .ForCollection(col)
                        .WithTable("CustomColumnMappingTests")
                        .AddAllColumns()
                        .CustomColumnMapping(x => x.ColumnXIsDifferent, "ColumnX")
                        .CustomColumnMapping(x => x.ColumnYIsDifferentInDatabase, "ColumnY")
                        .CustomColumnMapping(x => x.NaturalIdTest, "NaturalId")
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

                    bulk.Setup<CustomColumnMappingTest>()
                        .ForCollection(col)
                        .WithTable("CustomColumnMappingTests")
                        .AddAllColumns()
                        .CustomColumnMapping(x => x.ColumnXIsDifferent, "ColumnX")
                        .CustomColumnMapping(x => x.ColumnYIsDifferentInDatabase, "ColumnY")
                        .CustomColumnMapping(x => x.NaturalIdTest, "NaturalId")
                        .BulkInsertOrUpdate()
                        .MatchTargetOn(x => x.NaturalIdTest)
                        .UpdateWhen(x => x.ColumnXIsDifferent != "me")
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

                    bulk.Setup<CustomColumnMappingTest>()
                        .ForCollection(col)
                        .WithTable("CustomColumnMappingTests")
                        .AddColumn(x => x.ColumnXIsDifferent, "ColumnX")
                        .AddColumn(x => x.ColumnYIsDifferentInDatabase, "ColumnY")
                        .AddColumn(x => x.NaturalIdTest, "NaturalId")
                        .BulkInsertOrUpdate()
                        .MatchTargetOn(x => x.NaturalIdTest)
                        .UpdateWhen(x => x.ColumnXIsDifferent != "me")
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

                    bulk.Setup<CustomColumnMappingTest>()
                        .ForCollection(col)
                        .WithTable("CustomColumnMappingTests")
                        .AddColumn(x => x.ColumnXIsDifferent, "ColumnX")
                        .AddColumn(x => x.ColumnYIsDifferentInDatabase, "ColumnY")
                        .AddColumn(x => x.NaturalIdTest, "NaturalId")
                        .BulkInsert()
                        .Commit(conn);

                    foreach (var item in col)
                    {
                        item.ColumnXIsDifferent = "Updated";
                    }

                    bulk.Setup<CustomColumnMappingTest>()
                        .ForCollection(col)
                        .WithTable("CustomColumnMappingTests")
                        .AddColumn(x => x.ColumnXIsDifferent, "ColumnX")
                        .AddColumn(x => x.NaturalIdTest, "NaturalId")
                        .BulkUpdate()
                        .MatchTargetOn(x => x.NaturalIdTest)
                        .UpdateWhen(x => x.ColumnXIsDifferent != "me")
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

                    bulk.Setup<CustomColumnMappingTest>()
                        .ForCollection(col)
                        .WithTable("CustomColumnMappingTests")
                        .AddAllColumns()
                        .CustomColumnMapping(x => x.ColumnXIsDifferent, "ColumnX")
                        .CustomColumnMapping(x => x.ColumnYIsDifferentInDatabase, "ColumnY")
                        .CustomColumnMapping(x => x.NaturalIdTest, "NaturalId")
                        .BulkInsert()
                        .Commit(conn);

                    foreach (var item in col)
                    {
                        item.ColumnXIsDifferent = "Updated";
                    }

                    bulk.Setup<CustomColumnMappingTest>()
                        .ForCollection(col)
                        .WithTable("CustomColumnMappingTests")
                        .AddAllColumns()
                        .CustomColumnMapping(x => x.ColumnXIsDifferent, "ColumnX")
                        .CustomColumnMapping(x => x.ColumnYIsDifferentInDatabase, "ColumnY")
                        .CustomColumnMapping(x => x.NaturalIdTest, "NaturalId")
                        .BulkUpdate()
                        .MatchTargetOn(x => x.NaturalIdTest)
                        .UpdateWhen(x => x.ColumnXIsDifferent != "me")
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
            //_db.ReservedColumnNameTest.RemoveRange(_db.ReservedColumnNameTest.ToList());
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

                    bulk.Setup<ReservedColumnNameTest>()
                        .ForCollection(list)
                        .WithTable("ReservedColumnNameTests")
                        .AddAllColumns()
                        .BulkInsertOrUpdate()
                        .MatchTargetOn(x => x.Id)
                        .SetIdentityColumn(x => x.Id)
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

            var books = _randomizer.GetRandomCollection(30);

            using (var trans = new TransactionScope())
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsertOrUpdate()
                        .MatchTargetOn(x => x.ISBN)
                        .SetIdentityColumn(x => x.Id, ColumnDirectionType
                        .InputOutput).Commit(conn);
                }

                trans.Complete();
            }

            var test = _dataAccess.GetBookList().ElementAt(10); // Random book within the 30 elements
            var expected = books.Single(x => x.ISBN == test.ISBN);

            Assert.Equal(expected.Id, test.Id);
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
                    bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsertOrUpdate()
                        .MatchTargetOn(x => x.ISBN)
                        .SetIdentityColumn(x => x.Id)
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
                    bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsert()
                        .SetIdentityColumn(x => x.Id)
                        .Commit(conn);

                    // Update list with new dates
                    books.ForEach(x =>
                    {
                        x.CreatedAt = updatedDate;
                        x.ModifiedAt = updatedDate;
                    });

                    // Insert a random record
                    books.Add(new Book() { CreatedAt = updatedDate, ModifiedAt = updatedDate, Price = 29.99M, Title = "Trump likes woman", ISBN = "1234567891011" });

                    bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns() // Both ModifiedAt and CreatedAt are added implicitly here
                        .BulkInsertOrUpdate()
                        .MatchTargetOn(x => x.ISBN)
                        .SetIdentityColumn(x => x.Id)
                        .ExcludeColumnFromUpdate(x => x.CreatedAt) // Insert or update with new dates but ignore created date. 
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

            var books = _randomizer.GetRandomCollection(30);

            using (var trans = new TransactionScope())
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddColumn(x => x.ISBN)
                        .AddColumn(x => x.Description)
                        .AddColumn(x => x.Title)
                        .AddColumn(x => x.Price)
                        .BulkInsertOrUpdate()
                        .MatchTargetOn(x => x.ISBN)
                        .SetIdentityColumn(x => x.Id, ColumnDirectionType.InputOutput)
                        .Commit(conn);
                }

                trans.Complete();
            }

            var test = _dataAccess.GetBookList().ElementAt(10); // Random book within the 30 elements
            var expected = books.Single(x => x.ISBN == test.ISBN);

            Assert.Equal(expected.Id, test.Id);
        }

        [Fact]
        public void SqlBulkTools_BulkInsert_TestIdentityOutput()
        {
            BulkDelete(_dataAccess.GetBookList());

            var bulk = new BulkOperations();
            var books = _randomizer.GetRandomCollection(30);

            using (var trans = new TransactionScope())
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    bulk.Setup<Book>()
                        .ForCollection(_randomizer.GetRandomCollection(60))
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsert()
                        .Commit(conn);

                    bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsert()
                        .SetIdentityColumn(x => x.Id, ColumnDirectionType.InputOutput)
                        .Commit(conn);
                }

                trans.Complete();
            }

            var test = _dataAccess.GetBookList().ElementAt(80); // Random between random items before test and total items after test. 
            var expected = books.Single(x => x.ISBN == test.ISBN);

            Assert.Equal(expected.Id, test.Id);
        }



        [Fact]
        public void SqlBulkTools_BulkInsertWithSelectedColumns_TestIdentityOutput()
        {
            BulkDelete(_dataAccess.GetBookList());

            var books = _randomizer.GetRandomCollection(30);

            using (var trans = new TransactionScope())
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    var bulk = new BulkOperations();
                    bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .WithBulkCopySettings(new BulkCopySettings()
                        {
                            BatchSize = 5000
                        })
                        .AddColumn(x => x.Title)
                        .AddColumn(x => x.Price)
                        .AddColumn(x => x.Description)
                        .AddColumn(x => x.ISBN)
                        .AddColumn(x => x.PublishDate)
                        .BulkInsert()
                        .TmpDisableAllNonClusteredIndexes()
                        .SetIdentityColumn(x => x.Id, ColumnDirectionType.InputOutput)
                        .Commit(conn);
                }

                trans.Complete();
            }

            var actual = _dataAccess.GetBookList().ElementAt(15); // Random book within the 30 elements
            var expected = books.Single(x => x.ISBN == actual.ISBN);

            Assert.Equal(expected.Id, actual.Id);
            Assert.Equal(expected.Title, actual.Title);
            Assert.Equal(expected.Price, actual.Price);
            Assert.Equal(expected.Description, actual.Description);
            Assert.Equal(expected.ISBN, actual.ISBN);
        }

        [Fact]
        public void SqlBulkTools_BulkDeleteWithSelectedColumns_TestIdentityOutput()
        {

            BulkDelete(_dataAccess.GetBookList());

            //using (
            //    var conn = new SqlConnection(ConfigurationManager.ConnectionStrings["SqlBulkToolsTest"].ConnectionString)
            //    )
            //using (var command = new SqlCommand(
            //    "DBCC CHECKIDENT ('[dbo].[Books]', RESEED, 10);", conn)
            //{
            //    CommandType = CommandType.Text
            //})
            //{
            //    conn.Open();
            //    command.ExecuteNonQuery();
            //}

            _dataAccess.ReseedBookIdentity(10);

            var books = _randomizer.GetRandomCollection(30);
            BulkInsert(books);

            using (var trans = new TransactionScope())
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    var bulk = new BulkOperations();
                    bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .WithBulkCopySettings(new BulkCopySettings()
                        {
                            BatchSize = 5000
                        })
                        .AddColumn(x => x.ISBN)
                        .BulkDelete()
                        .MatchTargetOn(x => x.ISBN)
                        .SetIdentityColumn(x => x.Id, ColumnDirectionType.InputOutput)
                        .Commit(conn);
                }

                trans.Complete();
            }

            var test = books.First();

            Assert.True(test.Id == 10 || test.Id == 11);

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

            using (var trans = new TransactionScope())
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddColumn(x => x.ISBN)
                        .AddColumn(x => x.Description)
                        .AddColumn(x => x.Title)
                        .AddColumn(x => x.Price)
                        .BulkUpdate()
                        .MatchTargetOn(x => x.ISBN)
                        .SetIdentityColumn(x => x.Id, ColumnDirectionType.InputOutput)
                        .Commit(conn);
                }

                trans.Complete();
            }

            var test = _dataAccess.GetBookList().ElementAt(10); // Random book within the 30 elements
            var expected = books.Single(x => x.ISBN == test.ISBN);

            Assert.Equal(expected.Id, test.Id);
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
                    bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddColumn(x => x.ISBN)
                        .AddColumn(x => x.InvalidType)
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
                    .ForCollection(_bookCollection.Select(x => new { x.Description, x.ISBN, x.Id, x.Price }))
                    .WithTable("Books")
                    .AddColumn(x => x.Id)
                    .AddColumn(x => x.Description)
                    .AddColumn(x => x.ISBN)
                    .AddColumn(x => x.Price)
                    .BulkInsert()
                    .SetIdentityColumn(x => x.Id)
                    .Commit(conn);
                }

                trans.Complete();
            }

            Assert.True(_dataAccess.GetBookList().Any());
        }

        [Fact]
        public void SqlBulkTools_BulkInsertWithoutSetter_ThrowsMeaningfulException()
        {
            BulkDelete(_dataAccess.GetBookList());
            var bulk = new BulkOperations();

            _bookCollection = _randomizer.GetRandomCollection(30);

            using var trans = new TransactionScope();

            using (var conn = new SqlConnection(_dataAccess.ConnectionString))
            {
                Assert.Throws<SqlBulkToolsException>(() =>
                    bulk.Setup()
                        .ForCollection(
                            _bookCollection.Select(
                                x => new { x.Description, x.ISBN, x.Id, x.Price }))
                        .WithTable("Books")
                        .AddColumn(x => x.Id)
                        .AddColumn(x => x.Description)
                        .AddColumn(x => x.ISBN)
                        .AddColumn(x => x.Price)
                        .BulkInsert()
                        .SetIdentityColumn(x => x.Id, ColumnDirectionType.InputOutput)
                        .Commit(conn));
            }

            trans.Complete();

        }

        [Fact]
        public void SqlBulkTools_BulkInsertOrUpdateWithPrivateIdentityField_ThrowsMeaningfulException()
        {
            BulkDelete(_dataAccess.GetBookList());
            var bulk = new BulkOperations();

            var books = _randomizer.GetRandomCollection(30);
            var booksWithPrivateIdentity = new List<BookWithPrivateIdentity>();

            books.ForEach(x => booksWithPrivateIdentity.Add(new BookWithPrivateIdentity()
            {
                ISBN = x.ISBN,
                Description = x.Description,
                Price = x.Price

            }));

            using var trans = new TransactionScope();

            using (var conn = new SqlConnection(_dataAccess.ConnectionString))
            {
                Assert.Throws<SqlBulkToolsException>(() =>
                    bulk.Setup<BookWithPrivateIdentity>()
                        .ForCollection(booksWithPrivateIdentity)
                        .WithTable("Books")
                        .AddColumn(x => x.Id)
                        .AddColumn(x => x.Description)
                        .AddColumn(x => x.ISBN)
                        .AddColumn(x => x.Price)
                        .BulkInsertOrUpdate()
                        .MatchTargetOn(x => x.ISBN)
                        .SetIdentityColumn(x => x.Id, ColumnDirectionType.InputOutput)
                        .Commit(conn));
            }

            trans.Complete();
        }

        [Fact]
        public void SqlBulkTools_BulkInsertOrUpdateWithDeletePredicate_OnlyDeletesRecordsFromSpecifiedWarehouse()
        {
            BulkDelete(_dataAccess.GetBookList());
            var bulk = new BulkOperations();

            var books = _randomizer.GetRandomCollection(30);

            for (var i = 0; i < books.Count; i++)
            {
                books[i].WarehouseId = i > books.Count / 2 - 1 ? 1 : 2;
            }

            using (var trans = new TransactionScope())
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {

                    bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsert()
                        .Commit(conn);

                    books = _randomizer.GetRandomCollection(30);

                    for (var i = 0; i < books.Count; i++)
                    {
                        books[i].WarehouseId = i > books.Count / 2 - 1 ? 1 : 2;
                    }

                    // Only delete if WarehouseId is 1
                    bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsertOrUpdate()
                        .MatchTargetOn(x => x.ISBN)
                        .DeleteWhen(x => x.WarehouseId == 1)
                        .SetIdentityColumn(x => x.Id)
                        .Commit(conn);

                }

                trans.Complete();
            }

            // 15 were initially added with warehouse id 2, 15 more were added in second insert. 
            Assert.Equal(30, _dataAccess.GetBookList().Count(x => x.WarehouseId == 2));

            // 15 were initially added with warehouse id 1. 15 were deleted in second call and 15 were inserted.  
            Assert.Equal(15, _dataAccess.GetBookList().Count(x => x.WarehouseId == 1));
        }

        [Fact]
        public void SqlBulkTools_BulkUpdateWithPredicate_OnlyUpdateWhenWarehouseIs1()
        {
            BulkDelete(_dataAccess.GetBookList());
            var bulk = new BulkOperations();

            var books = _randomizer.GetRandomCollection(30);

            for (var i = 0; i < books.Count; i++)
            {
                books[i].WarehouseId = i > books.Count / 2 - 1 ? 1 : 2;
            }

            using (var trans = new TransactionScope())
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsert()
                        .Commit(conn);

                    for (var i = 0; i < books.Count; i++)
                    {
                        books[i].Price = 99999999;
                    }

                    // Only update if warehouse is 1
                    bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkUpdate()
                        .MatchTargetOn(x => x.ISBN)
                        .SetIdentityColumn(x => x.Id)
                        .UpdateWhen(x => x.WarehouseId == 1)
                        .Commit(conn);
                }

                trans.Complete();
            }

            Assert.Equal(99999999, _dataAccess.GetBookList().First(x => x.WarehouseId == 1).Price);
            Assert.NotEqual(99999999, _dataAccess.GetBookList().First(x => x.WarehouseId == 2).Price);
        }

        [Fact]
        public void SqlBulkTools_BulkUpdateWithPredicate_WhenBestSellerTrue()
        {
            BulkDelete(_dataAccess.GetBookList());
            var bulk = new BulkOperations();

            var books = _randomizer.GetRandomCollection(30);

            books[17].BestSeller = true;

            using (var trans = new TransactionScope())
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {

                    bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsert()
                        .Commit(conn);

                    books[17].Price = 1234567;

                    bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkUpdate()
                        .MatchTargetOn(x => x.ISBN)
                        .SetIdentityColumn(x => x.Id)
                        .UpdateWhen(x => x.BestSeller == true)
                        .Commit(conn);
                }

                trans.Complete();
            }

            var isbn = books[17].ISBN;

            Assert.Equal(1234567, _dataAccess.GetBookList(isbn).First().Price);
        }

        [Fact]
        public void SqlBulkTools_BulkUpdateWithPredicate_WhenBestSellerFalse()
        {
            BulkDelete(_dataAccess.GetBookList());
            var bulk = new BulkOperations();

            var books = _randomizer.GetRandomCollection(30);

            foreach (var book in books)
            {
                book.BestSeller = true;
            }

            books[17].BestSeller = false;

            using (var trans = new TransactionScope())
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsert()
                        .Commit(conn);

                    books[17].Price = 1234567;

                    bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkUpdate()
                        .MatchTargetOn(x => x.ISBN)
                        .SetIdentityColumn(x => x.Id)
                        .UpdateWhen(x => x.BestSeller == false)
                        .Commit(conn);
                }

                trans.Complete();
            }

            var isbn = books[17].ISBN;

            Assert.Equal(1234567, _dataAccess.GetBookList(isbn).First().Price);
        }

        [Fact]
        public void SqlBulkTools_BulkUpdateWithPredicate_OnlyUpdateWhenPriceLessThanOrEqualTo20()
        {
            BulkDelete(_dataAccess.GetBookList());
            var bulk = new BulkOperations();

            var books = _randomizer.GetRandomCollection(30);

            for (var i = 0; i < books.Count; i++)
            {
                books[i].Price = 21;
            }

            books[0].Price = 15;

            using (var trans = new TransactionScope())
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsert()
                        .Commit(conn);

                    books[0].Price = 17;

                    // Only update if price less than or equal to 20
                    bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddColumn(x => x.Price)
                        .BulkUpdate()
                        .MatchTargetOn(x => x.ISBN)
                        .UpdateWhen(x => x.Price <= 20).Commit(conn);
                }

                trans.Complete();

            }
            var isbn = books[0].ISBN;

            Assert.Equal(1, _dataAccess.GetBookList().Count(x => x.Price <= 20));
            Assert.Equal(17, _dataAccess.GetBookList(isbn).First().Price);
        }

        [Fact]
        public void SqlBulkTools_BulkUpdateWithPredicate_OnlyDeleteWhenWarehouseIs1()
        {
            BulkDelete(_dataAccess.GetBookList());
            var bulk = new BulkOperations();

            var books = _randomizer.GetRandomCollection(30);

            for (var i = 0; i < books.Count; i++)
            {
                books[i].WarehouseId = i > books.Count / 2 - 1 ? 1 : 2;
            }

            using (var trans = new TransactionScope())
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsert()
                        .Commit(conn);

                    // Only delete if warehouse is 1
                    bulk.Setup()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddColumn(x => x.ISBN)
                        .BulkDelete()
                        .MatchTargetOn(x => x.ISBN)
                        .DeleteWhen(x => x.WarehouseId == 1)
                        .Commit(conn);
                }

                trans.Complete();
            }

            Assert.DoesNotContain(_dataAccess.GetBookList(), x => x.WarehouseId == 1);
        }

        [Fact]
        public void SqlBulkTools_BulkDeleteWithMultiplePredicate_WhenDescriptionIsNullAndPriceMoreThan10()
        {
            BulkDelete(_dataAccess.GetBookList());

            var bulk = new BulkOperations();

            var books = _randomizer.GetRandomCollection(30);

            for (var i = 0; i < books.Count; i++)
            {
                if (i < 5)
                {
                    books[i].Description = null;
                    books[i].Price = 12;
                }
                else
                {
                    books[i].Price = 30;
                }
            }

            using (var trans = new TransactionScope())
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsert()
                        .Commit(conn);

                    // Only delete when price more than 10 and description is null
                    bulk.Setup()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddColumn(x => x.ISBN)
                        .BulkDelete()
                        .MatchTargetOn(x => x.ISBN)
                        .DeleteWhen(x => x.Price > 10)
                        .DeleteWhen(x => x.Description == null)
                        .Commit(conn);
                }

                trans.Complete();
            }

            Assert.Equal(25, _dataAccess.GetBookList().Count);
        }

        [Fact]
        public void SqlBulkTools_BulkDeleteWithMultiplePredicate_WhenDescriptionIsNotNullAndPriceLessThan10()
        {
            BulkDelete(_dataAccess.GetBookList());
            var bulk = new BulkOperations();

            var books = _randomizer.GetRandomCollection(30);

            for (var i = 0; i < books.Count; i++)
            {
                if (i >= 10 && i < 15)
                {
                    books[i].Description = null;
                    books[i].Price = 5;
                }
                else
                {
                    books[i].Price = 9;
                }
            }

            using (var trans = new TransactionScope())
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsert()
                        .Commit(conn);

                    // Only delete when price more than 10 and description is null
                    bulk.Setup()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddColumn(x => x.ISBN)
                        .BulkDelete()
                        .MatchTargetOn(x => x.ISBN)
                        .DeleteWhen(x => x.Price < 10)
                        .DeleteWhen(x => x.Description != null)
                        .Commit(conn);
                }

                trans.Complete();
            }

            Assert.Equal(5, _dataAccess.GetBookList().Count);
        }

        [Fact]
        public void SqlBulkTools_BulkUpdateWithPredicate_OnlyDeleteWhenPriceMoreThan50()
        {
            BulkDelete(_dataAccess.GetBookList());
            var bulk = new BulkOperations();

            var books = _randomizer.GetRandomCollection(30);

            for (var i = 0; i < books.Count; i++)
            {
                books[i].Price = i != 23 && i != 5 ? 51 : 32;
            }

            using (var trans = new TransactionScope())
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsert()
                        .Commit(conn);

                    // Only delete if price more than 50
                    bulk.Setup()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddColumn(x => x.ISBN)
                        .BulkDelete()
                        .MatchTargetOn(x => x.ISBN)
                        .DeleteWhen(x => x.Price > 50)
                        .Commit(conn); ;
                }

                trans.Complete();
            }

            Assert.Equal(2, _dataAccess.GetBookList().Count);
        }

        [Fact]
        public void SqlBulkTools_BulkUpdateWithNullablePredicate()
        {
            BulkDelete(_dataAccess.GetBookList());
            var bulk = new BulkOperations();

            var books = _randomizer.GetRandomCollection(30);

            using (var trans = new TransactionScope())
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsert()
                        .SetIdentityColumn(x => x.Id, ColumnDirectionType.InputOutput)
                        .Commit(conn);
                }

                trans.Complete();
            }

            using (var trans = new TransactionScope())
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    var book = new Book()
                    {
                        TestNullableInt = 40
                    };

                    bulk.Setup<Book>()
                        .ForObject(new Book() { TestNullableInt = book.TestNullableInt })
                        .WithTable("Books")
                        .AddColumn(x => x.TestNullableInt)
                        .Update()
                        .Where(x => x.Id == 10)
                        .And(x => x.TestNullableInt == book.TestNullableInt)
                        .Commit(conn);
                }

                trans.Complete();
            }
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

                    bulk.Setup<TestDataType>()
                        .ForCollection(dataTypeTest)
                        .WithTable("TestDataTypes")
                        .AddAllColumns()
                        .BulkInsertOrUpdate()
                        .MatchTargetOn(x => x.GuidTest)
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
            var watch = System.Diagnostics.Stopwatch.StartNew();
            using (var trans = new TransactionScope())
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    bulk.Setup<Book>()
                        .ForCollection(col)
                        .WithTable("Books")
                        .WithBulkCopySettings(new BulkCopySettings()
                        {
                            BatchSize = 5000
                        })
                        .AddColumn(x => x.Title)
                        .AddColumn(x => x.Price)
                        .AddColumn(x => x.Description)
                        .AddColumn(x => x.ISBN)
                        .AddColumn(x => x.PublishDate)
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
            var watch = System.Diagnostics.Stopwatch.StartNew();
            using (var trans = new TransactionScope(
                                TransactionScopeOption.RequiresNew,
                                new TimeSpan(0, 5, 0)))
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    bulk.Setup<Book>()
                        .ForCollection(col)
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
            var watch = System.Diagnostics.Stopwatch.StartNew();
            using (var trans = new TransactionScope())
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    bulk.Setup<Book>()
                        .ForCollection(col)
                        .WithTable("Books")
                        .AddColumn(x => x.Title)
                        .AddColumn(x => x.Price)
                        .AddColumn(x => x.Description)
                        .AddColumn(x => x.ISBN)
                        .AddColumn(x => x.PublishDate)
                        .BulkInsertOrUpdate()
                        .MatchTargetOn(x => x.ISBN)
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

            var watch = System.Diagnostics.Stopwatch.StartNew();
            using (var trans = new TransactionScope())
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    bulk.Setup<Book>()
                        .ForCollection(col)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsertOrUpdate()
                        .SetIdentityColumn(x => x.Id)
                        .MatchTargetOn(x => x.ISBN)
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
                    bulk.Setup<Book>()
                        .ForCollection(col)
                        .WithTable("Books")
                        .AddColumn(x => x.Title)
                        .AddColumn(x => x.Price)
                        .AddColumn(x => x.Description)
                        .AddColumn(x => x.PublishDate)
                        .BulkUpdate()
                        .MatchTargetOn(x => x.ISBN)
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

            var watch = System.Diagnostics.Stopwatch.StartNew();
            using (var trans = new TransactionScope())
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    bulk.Setup<Book>()
                        .ForCollection(col)
                        .WithTable("Books")
                        .AddColumn(x => x.ISBN)
                        .BulkDelete()
                        .MatchTargetOn(x => x.ISBN)
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
