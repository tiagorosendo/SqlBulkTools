using System;
using System.Collections.Generic;
using System.Data;
using SqlBulkTools.TestCommon;
using SqlBulkTools.TestCommon.Model;
using Xunit;
// ReSharper disable CheckNamespace

namespace SqlBulkTools.UnitTests
{
    public class DataTableOperationsTests
    {
        [Fact]
        public void DataTableTools_GetColumn_RetrievesColumn()
        {
            // Arrange
            var dtOps = new DataTableOperations();

            dtOps.SetupDataTable<Book>()
                .ForCollection(null)
                .AddColumn(x => x.ISBN)
                .AddColumn(x => x.Price)
                .PrepareDataTable();

            const string expected1 = "ISBN";
            var expected2 = "Price";

            // Act
            var result1 = dtOps.GetColumn<Book>(x => x.ISBN);
            var result2 = dtOps.GetColumn<Book>(x => x.Price);

            // Assert
            Assert.Equal(expected1, result1);
            Assert.Equal(expected2, result2);
        }

        [Fact]
        public void DataTableTools_GetColumn_ThrowSqlBulkToolsExceptionWhenNoSetup()
        {
            // Arrange
            var dtOps = new DataTableOperations();

            // Act and Assert
            Assert.Throws<SqlBulkToolsException>(() => dtOps.GetColumn<Book>(x => x.Description));
            
        }

        [Fact]
        public void DataTableTools_GetColumn_ThrowSqlBulkToolsExceptionWhenTypeMismatch()
        {
            // Arrange
            var dtOps = new DataTableOperations();
            dtOps.SetupDataTable<Book>()
                .ForCollection(new List<Book> { new Book { Description = "A book" } })
                .AddAllColumns()
                .PrepareDataTable();

            // Act and Assert
            Assert.Throws<SqlBulkToolsException>(() => dtOps.GetColumn<BookDto>(x => x.Id));

        }

        [Fact]
//        [ExpectedException(typeof(SqlBulkToolsException))]
        public void DataTableTools_GetColumn_ThrowSqlBulkToolsExceptionWhenColumnMappingNotFound()
        {
            // Arrange
            var dtOps = new DataTableOperations();

            dtOps.SetupDataTable<Book>()
                .ForCollection(null)
                .AddColumn(x => x.ISBN)
                .AddColumn(x => x.Price)
                .PrepareDataTable();

            // Act and Assert
            Assert.Throws<SqlBulkToolsException>(() => dtOps.GetColumn<Book>(x => x.Description));
        }

        [Fact]
        public void DataTableTools_GetColumn_CustomColumnMapsCorrectly()
        {
            // Arrange
            var expected = "PublishingDate";
            var dtOps = new DataTableOperations();

            dtOps.SetupDataTable<Book>()
                .ForCollection(null)
                .AddAllColumns()
                .CustomColumnMapping(x => x.PublishDate, expected)
                .PrepareDataTable();

            // Act
            var result = dtOps.GetColumn<Book>(x => x.PublishDate);

            // Assert
            Assert.Equal(expected, result);
        }

        [Fact]
//        [ExpectedException(typeof(SqlBulkToolsException))]
        public void DataTableTools_GetColumn_WhenColumnRemovedFromSetup()
        {
            // Arrange
            var dtOps = new DataTableOperations();

            dtOps.SetupDataTable<Book>()
                .ForCollection(null)
                .AddAllColumns()
                .RemoveColumn(x => x.Description)
                .PrepareDataTable();

            // Act and Assert
            Assert.Throws<SqlBulkToolsException>(() => dtOps.GetColumn<Book>(x => x.Description));

        }

        [Fact]
        public void DataTableTools_PrepareDataTable_WithThreeColumnsAdded()
        {
            var randomizer = new BookRandomizer();
            var books = randomizer.GetRandomCollection(30);
            
            
            var dtOps = new DataTableOperations();
            var dt = dtOps.SetupDataTable<Book>()
                .ForCollection(books)
                .AddColumn(x => x.ISBN)
                .AddColumn(x => x.Price)
                .AddColumn(x => x.PublishDate)
                .CustomColumnMapping(x => x.PublishDate, "SomeOtherMapping")
                .PrepareDataTable();

            Assert.Equal("ISBN", dt.Columns[dtOps.GetColumn<Book>(x => x.ISBN)].ColumnName);
            Assert.Equal("Price", dt.Columns[dtOps.GetColumn<Book>(x => x.Price)].ColumnName);
            Assert.Equal("SomeOtherMapping", dt.Columns[dtOps.GetColumn<Book>(x => x.PublishDate)].ColumnName);
            Assert.Equal(typeof(DateTime), dt.Columns[dtOps.GetColumn<Book>(x => x.PublishDate)].DataType);
        }

        [Fact]
        public void DataTableTools_BuildPreparedDataTable_AddsRows()
        {
            var rowCount = 30;
            var randomizer = new BookRandomizer();

            var dtOps = new DataTableOperations();
            var books = randomizer.GetRandomCollection(rowCount);

            // ReSharper disable once RedundantAssignment
            var dt = dtOps.SetupDataTable<Book>()
                .ForCollection(books)
                .AddAllColumns()
                .PrepareDataTable();
            dt = dtOps.BuildPreparedDataTable();

            Assert.Equal(rowCount, dt.Rows.Count);
            Assert.Equal(books[10].ISBN, dt.Rows[10].Field<string>(dtOps.GetColumn<Book>(x => x.ISBN)));
            Assert.Equal(books[10].Description, dt.Rows[10].Field<string>(dtOps.GetColumn<Book>(x => x.Description)));
        }

        [Fact]
        public void DataTableTools_BuildPreparedDataTable_WithCustomDataTableSettings()
        {
            const long autoIncrementSeedTest = 21312;
            var randomizer = new BookRandomizer();

            var dtOps = new DataTableOperations();
            var books = randomizer.GetRandomCollection(30);

            var dt = dtOps.SetupDataTable<Book>()
                .ForCollection(books)
                .AddAllColumns()
                .PrepareDataTable();

            dt.Columns[dtOps.GetColumn<Book>(x => x.Id)].AutoIncrementSeed = autoIncrementSeedTest;

            dt = dtOps.BuildPreparedDataTable();

            Assert.Equal(dt.Columns[dtOps.GetColumn<Book>(x => x.Id)].AutoIncrementSeed, autoIncrementSeedTest);

        }
    }
}
